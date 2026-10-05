//! The reference prefill-load cost: the published cost formula used by the replay harness as its
//! comparison baseline, and nothing else. Compiled only with the `bench-policies` feature; the
//! catalog does not know the name without it.
//!
//! ```text
//! credit  = c · decay · device + 0.75 · host + 0.25 · disk
//! prefill = max(0, (active_prefill_tokens + prompt_tokens) / block_size − credit)
//! cost    = prefill_load_scale · prefill + decode_blocks + w_req · active_requests, × taint
//! decay   = 1 / (1 + k · ((active_prefill − min_active_prefill) / block_size) / request_blocks)
//! ```
//!
//! Inputs the host supplies differ from the formula's origin in three ways, all documented on
//! the inputs: `decode_blocks` is the host's in-flight estimate (requests in flight × this
//! request's blocks, plus credited output blocks) rather than a per-sequence ledger; there is
//! no shared-cache credit term; and ties at temperature zero go to the smallest worker URL unless
//! `tie_break: uniform`.

use super::{
    inputs::{CandidateInputs, RequestInputs},
    policy::{Needs, WorkerScorer, WorkerSelectionPolicy},
    softmax::{LowestCostPicker, TieBreak},
};

pub const POLICY_NAME: &str = "reference-cost";

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReferenceCostParams {
    pub overlap_score_credit: f64,
    pub overlap_score_credit_decay: f64,
    pub prefill_load_scale: f64,
    pub decode_active_request_weight: f64,
    pub host_cache_hit_weight: f64,
    pub disk_cache_hit_weight: f64,
    pub router_temperature: f64,
    pub tie_break: TieBreak,
}

impl Default for ReferenceCostParams {
    fn default() -> Self {
        Self {
            overlap_score_credit: 1.0,
            overlap_score_credit_decay: 0.0,
            prefill_load_scale: 1.0,
            decode_active_request_weight: 0.0,
            host_cache_hit_weight: 0.75,
            disk_cache_hit_weight: 0.25,
            router_temperature: 0.0,
            tie_break: TieBreak::Deterministic,
        }
    }
}

impl ReferenceCostParams {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("overlap_score_credit", self.overlap_score_credit),
            (
                "overlap_score_credit_decay",
                self.overlap_score_credit_decay,
            ),
            ("prefill_load_scale", self.prefill_load_scale),
            (
                "decode_active_request_weight",
                self.decode_active_request_weight,
            ),
            ("host_cache_hit_weight", self.host_cache_hit_weight),
            ("disk_cache_hit_weight", self.disk_cache_hit_weight),
            ("router_temperature", self.router_temperature),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("{name} must be a finite non-negative number"));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ReferenceCostScorer {
    params: ReferenceCostParams,
}

impl WorkerScorer for ReferenceCostScorer {
    fn score(
        &self,
        request: &RequestInputs<'_>,
        candidates: &[CandidateInputs<'_>],
        costs: &mut [f64],
    ) {
        let p = &self.params;
        let block = request.block_size.max(1) as f64;
        let request_blocks = request.request_blocks.max(1) as f64;
        let needs_decay = p.overlap_score_credit_decay > 0.0;
        let min_prefill = if needs_decay {
            candidates
                .iter()
                .map(CandidateInputs::active_prefill_or_zero)
                .min()
                .unwrap_or(0)
        } else {
            0
        };
        for (candidate, cost) in candidates.iter().zip(costs) {
            let decay = if needs_decay {
                let excess_blocks = candidate
                    .active_prefill_or_zero()
                    .saturating_sub(min_prefill) as f64
                    / block;
                1.0 / (1.0 + p.overlap_score_credit_decay * excess_blocks / request_blocks)
            } else {
                1.0
            };
            let credit = p.overlap_score_credit * decay * candidate.device_blocks
                + p.host_cache_hit_weight * candidate.host_blocks
                + p.disk_cache_hit_weight * candidate.disk_blocks;
            let raw_tokens = match candidate.active_prefill_tokens {
                Some(active) => active as f64 + request.prompt_tokens as f64,
                None => request.prompt_tokens as f64,
            };
            let prefill = (raw_tokens / block - credit).max(0.0);
            let decode = candidate
                .decode_blocks
                .unwrap_or(candidate.active_requests as f64 * request_blocks);
            let logit = p.prefill_load_scale * prefill
                + decode
                + p.decode_active_request_weight * candidate.active_requests as f64;
            *cost += logit * candidate.taint;
        }
    }
}

pub(super) fn policy(params: ReferenceCostParams) -> WorkerSelectionPolicy {
    WorkerSelectionPolicy::new(
        POLICY_NAME,
        Needs::FLEET_WITH_LOADS,
        Vec::new(),
        vec![Box::new(ReferenceCostScorer { params })],
        Box::new(LowestCostPicker {
            temperature: params.router_temperature,
            tie_break: params.tie_break,
        }),
    )
}
