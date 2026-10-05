//! `cache-aware-balanced`: the host's expected wait less a capped, relative prefix credit.
//!
//! The affinity-group default ranks on overlap first and lets the expected-wait selector break
//! ties among equal holders, so a hot prefix's holder keeps winning until a gate trips;
//! `least_load` ranks on expected wait alone and never sees the cache. This policy prices both
//! in seconds and takes the lowest:
//!
//! ```text
//! cost_i   = wait_i − credit_i × block_size / drain_i
//! credit_i = min(lead, cap) − min(lead − overlap_i, cap)        lead = max_j overlap_j
//! ```
//!
//! - `wait_i` is the host's expected wait on worker `i`, the `least_load` score: queued token-work
//!   plus what this router dispatched since the worker's last report, over its drain rate, plus
//!   the KV-pressure barrier (`Needs::expected_wait`).
//! - `credit_i` is the prefix work the worker saves, in blocks, capped at `affinity_cap_tokens`
//!   and taken relative to the fleet's best holder: the leader earns the cap (its whole overlap
//!   when that is smaller), a holder `d` blocks behind the leader earns `cap − d`, a cold worker
//!   nothing. Relative, so a session's holders compete on load among themselves rather than on
//!   the prompt's length; capped, so a long prompt cannot buy a place in a deep queue.
//! - the credit is priced at the drain rate the wait was priced at, so a token saved is worth
//!   exactly a token not waited for.
//!
//! Before scoring, a saturation veto drops workers whose waiting queue (the report plus the
//! router's dispatches since it) or KV share is at or above a threshold: the two signals and the
//! inclusive comparison of the gateway's worker-overload protection ([`OverloadThresholds`]),
//! under this policy's own thresholds. The veto fails open: when every worker trips it the pick
//! is `None` and the host routes by expected wait alone; nothing is shed. Lowest cost wins, equal
//! costs draw uniformly (or by URL with `tie_break: deterministic`), and a `temperature` above
//! zero softens the argmin into a cost-softmax draw.

use super::{
    inputs::{CandidateInputs, RequestInputs},
    policy::{Needs, WorkerFilter, WorkerScorer, WorkerSelectionPolicy},
    softmax::{LowestCostPicker, TieBreak},
};
use crate::worker::{expected_wait::DEFAULT_THROUGHPUT, overload::OverloadThresholds};

pub const POLICY_NAME: &str = "cache-aware-balanced";

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BalancedParams {
    /// Most prefix tokens credited to any worker.
    pub affinity_cap_tokens: usize,
    /// Waiting requests (reported, plus dispatched since the report) at or above which a worker
    /// is vetoed; `0` disables the signal.
    pub saturation_waiting_requests: usize,
    /// KV share at or above which a worker is vetoed; `1.0` or more disables the signal.
    pub saturation_kv_usage: f64,
    /// Cost-softmax temperature; `0` is the exact argmin.
    pub temperature: f64,
    /// How equal costs resolve at temperature zero.
    pub tie_break: TieBreak,
}

impl Default for BalancedParams {
    fn default() -> Self {
        Self {
            affinity_cap_tokens: 16_384,
            saturation_waiting_requests: 8,
            saturation_kv_usage: 0.8,
            temperature: 0.0,
            tie_break: TieBreak::Uniform,
        }
    }
}

impl BalancedParams {
    pub fn validate(&self) -> Result<(), String> {
        if !self.saturation_kv_usage.is_finite() || self.saturation_kv_usage <= 0.0 {
            return Err(
                "saturation_kv_usage must be a positive number (1.0 or more disables it)".into(),
            );
        }
        if !self.temperature.is_finite() || self.temperature < 0.0 {
            return Err("temperature must be a finite non-negative number".into());
        }
        Ok(())
    }

    /// The saturation veto in the worker-protection form; a disabled signal is `None`.
    fn thresholds(&self) -> OverloadThresholds {
        OverloadThresholds {
            waiting_requests: (self.saturation_waiting_requests > 0)
                .then_some(self.saturation_waiting_requests),
            token_usage: (self.saturation_kv_usage < 1.0).then_some(self.saturation_kv_usage),
        }
    }
}

/// The saturation veto: the worker-protection predicate on the gathered inputs. A worker that
/// reports neither signal is never vetoed; there is no evidence to veto on.
#[derive(Debug)]
struct Saturation {
    thresholds: OverloadThresholds,
}

impl WorkerFilter for Saturation {
    fn keep(&self, _request: &RequestInputs<'_>, candidate: &CandidateInputs<'_>) -> bool {
        let waiting = candidate.queue_depth.map(|queued| {
            i64::try_from(queued.saturating_add(candidate.dispatched_since_report))
                .unwrap_or(i64::MAX)
        });
        !self.thresholds.exceeded_by(waiting, candidate.kv_usage)
    }
}

/// Expected wait less the capped, relative affinity credit, in seconds.
#[derive(Debug)]
struct WaitLessAffinity {
    affinity_cap_tokens: usize,
}

impl WorkerScorer for WaitLessAffinity {
    fn score(
        &self,
        request: &RequestInputs<'_>,
        candidates: &[CandidateInputs<'_>],
        costs: &mut [f64],
    ) {
        let block_size = request.block_size.max(1) as f64;
        let cap = self.affinity_cap_tokens as f64 / block_size;
        let lead = candidates
            .iter()
            .map(|candidate| candidate.device_blocks.max(0.0))
            .fold(0.0, f64::max);
        let leader_credit = lead.min(cap);
        for (candidate, cost) in candidates.iter().zip(costs) {
            let overlap = candidate.device_blocks.max(0.0);
            let credit_blocks = (leader_credit - (lead - overlap).min(cap)).max(0.0);
            let drain = candidate
                .drain_tokens_per_sec
                .filter(|rate| *rate > 0.0)
                .unwrap_or(DEFAULT_THROUGHPUT);
            // Without the host's reading (hand-built inputs), the reported
            // waiting token-work over the drain rate stands in for the wait.
            let wait = candidate
                .expected_wait_secs
                .unwrap_or_else(|| candidate.active_prefill_or_zero() as f64 / drain);
            *cost += wait - credit_blocks * block_size / drain;
        }
    }
}

pub(super) fn policy(params: BalancedParams) -> WorkerSelectionPolicy {
    WorkerSelectionPolicy::new(
        POLICY_NAME,
        Needs::FLEET_WITH_EXPECTED_WAIT,
        vec![Box::new(Saturation {
            thresholds: params.thresholds(),
        })],
        vec![Box::new(WaitLessAffinity {
            affinity_cap_tokens: params.affinity_cap_tokens,
        })],
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

    /// A worker holding `overlap` blocks with the given expected wait.
    fn candidate(
        idx: usize,
        url: &'static str,
        overlap: f64,
        wait: f64,
    ) -> CandidateInputs<'static> {
        CandidateInputs {
            idx,
            url,
            device_blocks: overlap,
            host_blocks: 0.0,
            disk_blocks: 0.0,
            effective_score: overlap,
            active_requests: 0,
            active_prefill_tokens: Some((wait * DRAIN) as u64),
            decode_blocks: None,
            kv_usage: Some(0.1),
            queue_depth: Some(0),
            running_requests: Some(0),
            taint: 1.0,
            expected_wait_secs: Some(wait),
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
        let scorer = WaitLessAffinity {
            affinity_cap_tokens: policy_params.affinity_cap_tokens,
        };
        let mut costs = vec![0.0; inputs.len()];
        scorer.score(&request(), inputs, &mut costs);
        costs
    }

    #[test]
    fn credit_is_capped_and_relative_to_the_best_holder() {
        // Cap 16 blocks: the leader (100 blocks) earns the cap, a holder ten
        // blocks behind earns six, a cold worker nothing; equal waits, so the
        // costs are the negated credits in seconds.
        let inputs = [
            candidate(0, "a", 100.0, 1.0),
            candidate(1, "b", 90.0, 1.0),
            candidate(2, "c", 0.0, 1.0),
        ];
        let costs = scored(params(16), &inputs);
        let second = |blocks: f64| blocks * BLOCK as f64 / DRAIN;
        assert!((costs[0] - (1.0 - second(16.0))).abs() < 1e-9, "{costs:?}");
        assert!((costs[1] - (1.0 - second(6.0))).abs() < 1e-9, "{costs:?}");
        assert!((costs[2] - 1.0).abs() < 1e-9, "{costs:?}");

        // A leader below the cap earns its whole overlap, and a follower
        // exactly what it holds.
        let inputs = [candidate(0, "a", 10.0, 1.0), candidate(1, "b", 4.0, 1.0)];
        let costs = scored(params(16), &inputs);
        assert!((costs[0] - (1.0 - second(10.0))).abs() < 1e-9, "{costs:?}");
        assert!((costs[1] - (1.0 - second(4.0))).abs() < 1e-9, "{costs:?}");
    }

    #[test]
    fn a_shorter_wait_beats_affinity_once_the_credit_is_spent() {
        let policy = policy(params(16));
        // The credit is worth 16 × 16 / 2000 = 0.128 s. The holder wins while
        // its wait is within that of the cold worker's, and loses beyond it.
        let close = [
            candidate(0, "cold", 0.0, 0.5),
            candidate(1, "holder", 100.0, 0.6),
        ];
        assert_eq!(policy.select(&request(), &close), Pick::Final(1));
        let far = [
            candidate(0, "cold", 0.0, 0.5),
            candidate(1, "holder", 100.0, 0.7),
        ];
        assert_eq!(policy.select(&request(), &far), Pick::Final(0));
    }

    #[test]
    fn saturated_workers_are_vetoed_and_the_veto_fails_open() {
        let policy = policy(params(16));
        let mut holder = candidate(1, "holder", 100.0, 0.0);
        holder.queue_depth = Some(8);
        let inputs = [candidate(0, "cold", 0.0, 0.5), holder.clone()];
        assert_eq!(
            policy.select(&request(), &inputs),
            Pick::Final(0),
            "eight waiting requests veto the holder"
        );

        // The router's own dispatches since the report count toward the queue.
        holder.queue_depth = Some(5);
        holder.dispatched_since_report = 3;
        let inputs = [candidate(0, "cold", 0.0, 0.5), holder.clone()];
        assert_eq!(policy.select(&request(), &inputs), Pick::Final(0));
        holder.dispatched_since_report = 2;
        let inputs = [candidate(0, "cold", 0.0, 0.5), holder.clone()];
        assert_eq!(policy.select(&request(), &inputs), Pick::Final(1));

        // The KV share vetoes on its own.
        let mut full = candidate(1, "holder", 100.0, 0.0);
        full.kv_usage = Some(0.8);
        let inputs = [candidate(0, "cold", 0.0, 0.5), full];
        assert_eq!(policy.select(&request(), &inputs), Pick::Final(0));

        // Every worker saturated: no pick, the host falls back to expected
        // wait over the fleet instead of shedding.
        let mut cold = candidate(0, "cold", 0.0, 0.5);
        cold.queue_depth = Some(9);
        holder.queue_depth = Some(8);
        let inputs = [cold, holder];
        assert_eq!(policy.select(&request(), &inputs), Pick::None);

        // A worker that reports nothing is never vetoed.
        let mut dark = candidate(0, "dark", 0.0, 0.5);
        dark.queue_depth = None;
        dark.kv_usage = None;
        let mut other = candidate(1, "other", 0.0, 0.5);
        other.queue_depth = Some(9);
        let inputs = [dark, other];
        assert_eq!(policy.select(&request(), &inputs), Pick::Final(0));
    }

    #[test]
    fn disabled_signals_never_veto() {
        let policy = policy(BalancedParams {
            saturation_waiting_requests: 0,
            saturation_kv_usage: 1.0,
            ..params(16)
        });
        let mut holder = candidate(1, "holder", 100.0, 0.0);
        holder.queue_depth = Some(500);
        holder.kv_usage = Some(1.0);
        let inputs = [candidate(0, "cold", 0.0, 0.5), holder];
        assert_eq!(policy.select(&request(), &inputs), Pick::Final(1));
    }

    #[test]
    fn without_the_hosts_reading_the_reported_queue_stands_in() {
        let mut deep = candidate(0, "deep", 0.0, 0.0);
        deep.expected_wait_secs = None;
        deep.drain_tokens_per_sec = None;
        deep.active_prefill_tokens = Some(4_000);
        let mut idle = candidate(1, "idle", 0.0, 0.0);
        idle.expected_wait_secs = None;
        idle.drain_tokens_per_sec = None;
        idle.active_prefill_tokens = Some(0);
        let costs = scored(params(16), &[deep, idle]);
        assert!(
            (costs[0] - 4_000.0 / DEFAULT_THROUGHPUT).abs() < 1e-9,
            "{costs:?}"
        );
        assert_eq!(costs[1], 0.0);
    }

    #[test]
    fn parameters_are_validated() {
        assert!(BalancedParams::default().validate().is_ok());
        assert!(BalancedParams {
            saturation_kv_usage: 0.0,
            ..BalancedParams::default()
        }
        .validate()
        .is_err());
        assert!(BalancedParams {
            temperature: -1.0,
            ..BalancedParams::default()
        }
        .validate()
        .is_err());
        let thresholds = BalancedParams {
            saturation_waiting_requests: 0,
            saturation_kv_usage: 2.0,
            ..BalancedParams::default()
        }
        .thresholds();
        assert!(!thresholds.is_enabled());
    }
}
