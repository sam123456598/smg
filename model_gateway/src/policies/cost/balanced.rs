//! `cache-aware-balanced`: expected wait less a capped, relative prefix credit.
//!
//! The affinity-group default ranks on overlap first and lets the expected-wait selector break
//! ties among equal holders, so a hot prefix's holder keeps winning until a gate trips;
//! `least_load` ranks on expected wait alone and never sees the cache. This policy prices both
//! in seconds and takes the lowest:
//!
//! ```text
//! cost_i   = (waiting_i + in_flight_i × p̄ − credit_i × block_size) / R + λ · k_i / (1 − k_i)
//! credit_i = min(lead, cap) − min(lead − overlap_i, cap)        lead = max_j overlap_j
//! ```
//!
//! - `waiting_i` is the prefill work the engine reports queued (its waiting uncached tokens; zero
//!   when the report carries no token count), plus anything the router booked optimistically
//!   since. The waiting *count* is not priced on top of it: every waiting request is in flight
//!   from this router, so the next term already carries it.
//! - `in_flight_i × p̄` is one mean prefill for every request this router still has in flight on
//!   the worker. The count is live: it falls as requests complete, so it needs no load report to
//!   track a burst and never grows stale between polls the way a since-poll dispatch tally does.
//!   `p̄` starts at `mean_prefill_tokens` and follows the prompts actually routed (an exponential
//!   mean), so one in-flight request is worth one prompt of this traffic, not a constant.
//! - `credit_i` is the prefix work the worker saves, in blocks, capped at `affinity_cap_tokens`
//!   and taken relative to the fleet's best holder: the leader earns the cap (its whole overlap
//!   when that is smaller), a holder `d` blocks behind the leader earns `cap − d`, a cold worker
//!   nothing. Relative, so a session's holders compete on load among themselves rather than on
//!   the prompt's length; capped, so a long prompt cannot buy a place in a deep queue.
//! - `R` is one drain rate for the whole fleet (the mean of the candidates' reported generation
//!   rates, else the host's default), so the token-work ranks as token-work: an engine's
//!   aggregate generation rate rises with the sequences it is running, and pricing each worker
//!   at its own rate would read the busiest worker as the fastest drain and feed it more.
//! - `λ · k / (1 − k)` is `least_load`'s KV-pressure barrier on the reported KV share.
//!
//! Saturated workers are not the policy's business: the gateway's worker-overload protection
//! (on by default, waiting requests 8 and KV share 0.8, steering) leaves a worker over the
//! thresholds out of every policy's candidates while another worker is under them and routes to
//! the least-loaded one when all are over. The first cut of this policy carried its own copy of
//! that veto; in a simulation with a tight cache it turned every queue of eight into a session
//! migration and the fleet thrashed, so one mechanism, the gateway's, is the rule. Lowest cost
//! wins, equal costs draw uniformly (or by URL with `tie_break: deterministic`), and a
//! `temperature` above zero softens the argmin into a cost-softmax draw.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    inputs::{CandidateInputs, RequestInputs},
    policy::{Needs, WorkerScorer, WorkerSelectionPolicy},
    softmax::{LowestCostPicker, TieBreak},
};
use crate::worker::expected_wait::{
    DEFAULT_KV_PRESSURE_WEIGHT, DEFAULT_MEAN_PREFILL_TOKENS, DEFAULT_THROUGHPUT,
};

pub const POLICY_NAME: &str = "cache-aware-balanced";

/// Weight of the newest prompt in the running mean prefill length.
const MEAN_PREFILL_ALPHA: f64 = 0.05;

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BalancedParams {
    /// Most prefix tokens credited to any worker.
    pub affinity_cap_tokens: usize,
    /// Starting value of the mean prefill length (tokens) that prices each in-flight request and
    /// a reported waiting count without a token count; the routed prompts take it from there.
    pub mean_prefill_tokens: u32,
    /// Weight of the KV-pressure barrier, in seconds.
    pub kv_pressure_weight: f64,
    /// Cost-softmax temperature; `0` is the exact argmin.
    pub temperature: f64,
    /// How equal costs resolve at temperature zero.
    pub tie_break: TieBreak,
}

impl Default for BalancedParams {
    fn default() -> Self {
        Self {
            affinity_cap_tokens: 16_384,
            mean_prefill_tokens: DEFAULT_MEAN_PREFILL_TOKENS,
            kv_pressure_weight: DEFAULT_KV_PRESSURE_WEIGHT,
            temperature: 0.0,
            tie_break: TieBreak::Uniform,
        }
    }
}

impl BalancedParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.mean_prefill_tokens == 0 {
            return Err("mean_prefill_tokens must be at least 1".into());
        }
        if !self.kv_pressure_weight.is_finite() || self.kv_pressure_weight < 0.0 {
            return Err("kv_pressure_weight must be a finite non-negative number".into());
        }
        if !self.temperature.is_finite() || self.temperature < 0.0 {
            return Err("temperature must be a finite non-negative number".into());
        }
        Ok(())
    }
}

/// Queued and in-flight prefill work less the capped, relative affinity credit, in seconds at
/// the fleet's drain rate, plus the KV-pressure barrier.
#[derive(Debug)]
struct WorkLessAffinity {
    affinity_cap_tokens: usize,
    kv_pressure_weight: f64,
    /// Running mean prompt length in tokens, as `f64` bits.
    mean_prefill_tokens: AtomicU64,
}

impl WorkLessAffinity {
    fn new(params: &BalancedParams) -> Self {
        Self {
            affinity_cap_tokens: params.affinity_cap_tokens,
            kv_pressure_weight: params.kv_pressure_weight,
            mean_prefill_tokens: AtomicU64::new(f64::from(params.mean_prefill_tokens).to_bits()),
        }
    }

    /// Fold this request's prompt into the running mean and return the mean.
    fn observe_prompt(&self, prompt_tokens: usize) -> f64 {
        let current = f64::from_bits(self.mean_prefill_tokens.load(Ordering::Relaxed));
        let next = current + MEAN_PREFILL_ALPHA * (prompt_tokens as f64 - current);
        self.mean_prefill_tokens
            .store(next.to_bits(), Ordering::Relaxed);
        next
    }

    #[cfg(test)]
    fn mean_prefill(&self) -> f64 {
        f64::from_bits(self.mean_prefill_tokens.load(Ordering::Relaxed))
    }
}

/// One drain rate for the fleet: the mean of the candidates' positive reported rates, else the
/// host's default.
fn fleet_drain_rate(candidates: &[CandidateInputs<'_>]) -> f64 {
    let (sum, count) = candidates
        .iter()
        .filter_map(|candidate| candidate.drain_tokens_per_sec)
        .filter(|rate| *rate > 0.0)
        .fold((0.0, 0usize), |(sum, count), rate| (sum + rate, count + 1));
    if count > 0 {
        sum / count as f64
    } else {
        DEFAULT_THROUGHPUT
    }
}

impl WorkerScorer for WorkLessAffinity {
    fn score(
        &self,
        request: &RequestInputs<'_>,
        candidates: &[CandidateInputs<'_>],
        costs: &mut [f64],
    ) {
        let block_size = request.block_size.max(1) as f64;
        let mean_prefill = self.observe_prompt(request.prompt_tokens);
        let drain = fleet_drain_rate(candidates);
        let cap = self.affinity_cap_tokens as f64 / block_size;
        let lead = candidates
            .iter()
            .map(|candidate| candidate.device_blocks.max(0.0))
            .fold(0.0, f64::max);
        let leader_credit = lead.min(cap);
        for (candidate, cost) in candidates.iter().zip(costs) {
            let overlap = candidate.device_blocks.max(0.0);
            let credit_tokens = (leader_credit - (lead - overlap).min(cap)).max(0.0) * block_size;
            // The engine's queued prefill tokens, as reported (zero when the
            // report carries no token count, as the vLLM servicer's does).
            // The waiting count is not priced on top: every waiting request
            // is also in flight from this router, and the in-flight term
            // below already carries one mean prefill for each.
            let waiting = candidate.active_prefill_or_zero() as f64;
            let in_flight = candidate.active_requests as f64 * mean_prefill;
            let k = candidate.kv_usage.unwrap_or(0.0).clamp(0.0, 0.999);
            let kv_wait = self.kv_pressure_weight * k / (1.0 - k);
            *cost += (waiting + in_flight - credit_tokens) / drain + kv_wait;
        }
    }
}

pub(super) fn policy(params: BalancedParams) -> WorkerSelectionPolicy {
    WorkerSelectionPolicy::new(
        POLICY_NAME,
        Needs::FLEET_WITH_EXPECTED_WAIT,
        Vec::new(),
        vec![Box::new(WorkLessAffinity::new(&params))],
        Box::new(LowestCostPicker {
            temperature: params.temperature,
            tie_break: params.tie_break,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policies::cost::Pick;

    const BLOCK: usize = 16;
    const DRAIN: f64 = 2_000.0;

    fn request() -> RequestInputs<'static> {
        RequestInputs {
            prompt_tokens: 4_096,
            block_size: BLOCK,
            request_blocks: 256,
            avg_load: 0.0,
            prefix_hashes: None,
        }
    }

    /// A worker holding `overlap` blocks with `waiting` tokens queued at the engine.
    fn candidate(
        idx: usize,
        url: &'static str,
        overlap: f64,
        waiting: u64,
    ) -> CandidateInputs<'static> {
        CandidateInputs {
            idx,
            url,
            device_blocks: overlap,
            host_blocks: 0.0,
            disk_blocks: 0.0,
            effective_score: overlap,
            active_requests: 0,
            active_prefill_tokens: Some(waiting),
            decode_blocks: None,
            kv_usage: Some(0.0),
            queue_depth: Some(0),
            running_requests: Some(0),
            taint: 1.0,
            expected_wait_secs: Some(waiting as f64 / DRAIN),
            drain_tokens_per_sec: Some(DRAIN),
            dispatched_since_report: 0,
        }
    }

    fn params(cap_blocks: usize) -> BalancedParams {
        BalancedParams {
            affinity_cap_tokens: cap_blocks * BLOCK,
            tie_break: TieBreak::Deterministic,
            ..BalancedParams::default()
        }
    }

    fn scored(policy_params: BalancedParams, inputs: &[CandidateInputs<'_>]) -> Vec<f64> {
        let scorer = WorkLessAffinity::new(&policy_params);
        let mut costs = vec![0.0; inputs.len()];
        scorer.score(&request(), inputs, &mut costs);
        costs
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn credit_is_capped_and_relative_to_the_best_holder() {
        // Cap 16 blocks: the leader (100 blocks) earns the cap, a holder ten
        // blocks behind earns six, a cold worker nothing; equal queues, so the
        // costs are the queue less the credit, in seconds.
        let inputs = [
            candidate(0, "a", 100.0, 2_000),
            candidate(1, "b", 90.0, 2_000),
            candidate(2, "c", 0.0, 2_000),
        ];
        let costs = scored(params(16), &inputs);
        let second = |blocks: f64| blocks * BLOCK as f64 / DRAIN;
        assert!(close(costs[0], 1.0 - second(16.0)), "{costs:?}");
        assert!(close(costs[1], 1.0 - second(6.0)), "{costs:?}");
        assert!(close(costs[2], 1.0), "{costs:?}");

        // A leader below the cap earns its whole overlap, and a follower
        // exactly what it holds.
        let inputs = [
            candidate(0, "a", 10.0, 2_000),
            candidate(1, "b", 4.0, 2_000),
        ];
        let costs = scored(params(16), &inputs);
        assert!(close(costs[0], 1.0 - second(10.0)), "{costs:?}");
        assert!(close(costs[1], 1.0 - second(4.0)), "{costs:?}");
    }

    #[test]
    fn a_shorter_queue_beats_affinity_once_the_credit_is_spent() {
        let policy = policy(params(16));
        // The credit is worth 16 × 16 = 256 tokens. The holder wins while its
        // queue is within that of the cold worker's, and loses beyond it.
        let close = [
            candidate(0, "cold", 0.0, 1_000),
            candidate(1, "holder", 100.0, 1_200),
        ];
        assert_eq!(policy.select(&request(), &close), Pick::Final(1));
        let far = [
            candidate(0, "cold", 0.0, 1_000),
            candidate(1, "holder", 100.0, 1_400),
        ];
        assert_eq!(policy.select(&request(), &far), Pick::Final(0));
    }

    #[test]
    fn each_request_in_flight_costs_one_mean_prefill() {
        let policy = policy(BalancedParams {
            mean_prefill_tokens: 1_000,
            ..params(16)
        });
        // With the first routed prompt (4,096 tokens) the mean moves from
        // 1,000 to 1,154.8; two requests in flight on the cold worker outweigh
        // the holder's 256-token credit many times over.
        let mut cold = candidate(0, "cold", 0.0, 0);
        cold.active_requests = 2;
        let holder = candidate(1, "holder", 100.0, 0);
        assert_eq!(
            policy.select(&request(), &[cold.clone(), holder.clone()]),
            Pick::Final(1)
        );
        // One more in flight on the holder than on the cold worker, and the
        // credit is no longer enough.
        let mut busy_holder = holder.clone();
        busy_holder.active_requests = 3;
        assert_eq!(
            policy.select(&request(), &[cold, busy_holder]),
            Pick::Final(0)
        );
    }

    #[test]
    fn the_mean_prefill_follows_the_routed_prompts() {
        let scorer = WorkLessAffinity::new(&BalancedParams::default());
        assert!(close(scorer.mean_prefill(), 1_024.0));
        let inputs = [candidate(0, "a", 0.0, 0)];
        let mut costs = [0.0];
        let long = RequestInputs {
            prompt_tokens: 8_192,
            ..request()
        };
        for _ in 0..50 {
            scorer.score(&long, &inputs, &mut costs);
        }
        let mean = scorer.mean_prefill();
        assert!(
            (7_000.0..8_192.0).contains(&mean),
            "after fifty 8,192-token prompts the mean is {mean}"
        );
    }

    #[test]
    fn a_waiting_count_is_carried_by_the_in_flight_term_not_priced_twice() {
        // A report with waiting requests but no token count (the vLLM
        // servicer's): the queue costs nothing by itself, the router's
        // in-flight count prices the same requests once.
        let mut counted = candidate(0, "counted", 0.0, 0);
        counted.queue_depth = Some(3);
        counted.active_requests = 3;
        let idle = candidate(1, "idle", 0.0, 0);
        let costs = scored(
            BalancedParams {
                mean_prefill_tokens: 1_000,
                ..params(16)
            },
            &[counted, idle],
        );
        // The first prompt moves the mean to 1,154.8 tokens; three requests
        // in flight are three of those.
        let mean = 1_000.0 + MEAN_PREFILL_ALPHA * (4_096.0 - 1_000.0);
        assert!(close(costs[0], 3.0 * mean / DRAIN), "{costs:?}");
        assert!(close(costs[1], 0.0), "{costs:?}");
    }

    #[test]
    fn the_fleet_drains_at_one_rate() {
        // One worker reports twice the rate of the other; the work is still
        // priced at the fleet mean, so equal queues cost the same.
        let mut fast = candidate(0, "fast", 0.0, 2_000);
        fast.drain_tokens_per_sec = Some(2.0 * DRAIN);
        let slow = candidate(1, "slow", 0.0, 2_000);
        let costs = scored(params(16), &[fast, slow]);
        assert!(close(costs[0], costs[1]), "{costs:?}");
        assert!(close(costs[0], 2_000.0 / (1.5 * DRAIN)), "{costs:?}");

        // Without any reported rate the host's default prices the work.
        let mut dark = candidate(0, "dark", 0.0, 2_000);
        dark.drain_tokens_per_sec = None;
        let costs = scored(params(16), &[dark]);
        assert!(close(costs[0], 2_000.0 / DEFAULT_THROUGHPUT), "{costs:?}");
    }

    #[test]
    fn kv_pressure_adds_the_barrier() {
        let mut full = candidate(0, "full", 0.0, 0);
        full.kv_usage = Some(0.5);
        let empty = candidate(1, "empty", 0.0, 0);
        let costs = scored(params(16), &[full, empty]);
        assert!(close(costs[0], DEFAULT_KV_PRESSURE_WEIGHT), "{costs:?}");
        assert!(close(costs[1], 0.0), "{costs:?}");
    }

    #[test]
    fn parameters_are_validated() {
        assert!(BalancedParams::default().validate().is_ok());
        for bad in [
            BalancedParams {
                temperature: -1.0,
                ..BalancedParams::default()
            },
            BalancedParams {
                mean_prefill_tokens: 0,
                ..BalancedParams::default()
            },
            BalancedParams {
                kv_pressure_weight: f64::NAN,
                ..BalancedParams::default()
            },
        ] {
            assert!(bad.validate().is_err());
        }
    }
}
