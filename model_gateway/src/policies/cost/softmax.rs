//! Shared selection arithmetic: the two softmax draws, deterministic tie-breaking and the
//! lowest-cost picker built on them.

use std::cmp::Ordering;

use rand::RngExt;

use super::{
    inputs::{CandidateInputs, RequestInputs},
    policy::{Pick, WorkerPicker},
};

/// Softmax selection over min-max normalised *scores* (higher is better): the draw cache-aware
/// routing has always used. The normalisation makes temperature scale-free, the best candidate's
/// exponent is exactly 0 (overflow-safe), a degenerate spread is a uniform draw, and the
/// inverse-CDF walk falls back to the last row against floating-point drift. Returns a position.
pub fn sample_by_score_temperature(scores: &[f64], temperature: f32) -> Option<usize> {
    let first = *scores.first()?;
    let (min, max) = scores
        .iter()
        .fold((first, first), |(min, max), &s| (min.min(s), max.max(s)));
    let range = max - min;
    if range <= 0.0 {
        return Some(rand::rng().random_range(0..scores.len()));
    }
    let weights: Vec<f64> = scores
        .iter()
        .map(|&s| (((s - min) / range - 1.0) / f64::from(temperature)).exp())
        .collect();
    let total: f64 = weights.iter().sum();
    let draw = rand::rng().random::<f64>() * total;
    let mut cumulative = 0.0;
    for (position, weight) in weights.iter().enumerate() {
        cumulative += weight;
        if cumulative >= draw {
            return Some(position);
        }
    }
    Some(scores.len() - 1)
}

/// Softmax selection over *costs* (lower is better) normalised to their range, as the reference
/// cost's picker does: `p_i ∝ exp(-(c_i - c_min) / (range · T))`. Equal costs draw uniformly.
pub fn sample_by_cost_temperature(costs: &[f64], temperature: f64) -> Option<usize> {
    let first = *costs.first()?;
    let (min, max) = costs
        .iter()
        .fold((first, first), |(min, max), &c| (min.min(c), max.max(c)));
    let range = max - min;
    let spread = range.partial_cmp(&0.0) == Some(Ordering::Greater);
    let warm = temperature.partial_cmp(&0.0) == Some(Ordering::Greater);
    if !spread || !warm {
        return Some(rand::rng().random_range(0..costs.len()));
    }
    let weights: Vec<f64> = costs
        .iter()
        .map(|&c| (-(c - min) / (range * temperature)).exp())
        .collect();
    let total: f64 = weights.iter().sum();
    let draw = rand::rng().random::<f64>() * total;
    let mut cumulative = 0.0;
    for (position, weight) in weights.iter().enumerate() {
        cumulative += weight;
        if cumulative >= draw {
            return Some(position);
        }
    }
    Some(costs.len() - 1)
}

/// The lowest cost among `rows`; ties go to the smallest worker URL, so two routers (or two
/// replays) given the same inputs make the same choice. NaN costs never win.
pub fn lowest_cost_deterministic(
    candidates: &[CandidateInputs<'_>],
    costs: &[f64],
    rows: impl IntoIterator<Item = usize>,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    for row in rows {
        let cost = costs[row];
        if cost.is_nan() {
            continue;
        }
        best = match best {
            None => Some(row),
            Some(current) => {
                let current_cost = costs[current];
                if cost < current_cost
                    || (cost == current_cost && candidates[row].url < candidates[current].url)
                {
                    Some(row)
                } else {
                    Some(current)
                }
            }
        };
    }
    best
}

/// How a picker resolves equal costs at temperature zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TieBreak {
    /// Smallest worker URL wins (reproducible replays).
    Deterministic,
    /// Uniform draw among the tied rows.
    Uniform,
}

/// Lowest cost, with the configured tie-break at temperature zero and a cost-softmax draw above.
#[derive(Debug)]
pub(super) struct LowestCostPicker {
    pub temperature: f64,
    pub tie_break: TieBreak,
}

impl WorkerPicker for LowestCostPicker {
    fn pick(
        &self,
        _request: &RequestInputs<'_>,
        candidates: &[CandidateInputs<'_>],
        costs: &[f64],
    ) -> Pick {
        pick_lowest(candidates, costs, self.temperature, self.tie_break)
            .map_or(Pick::None, Pick::Final)
    }
}

/// Lowest cost with the configured tie-break, or a cost-softmax draw when `temperature > 0`.
pub fn pick_lowest(
    candidates: &[CandidateInputs<'_>],
    costs: &[f64],
    temperature: f64,
    tie_break: TieBreak,
) -> Option<usize> {
    if candidates.is_empty() {
        return None;
    }
    if temperature > 0.0 {
        return sample_by_cost_temperature(costs, temperature);
    }
    match tie_break {
        TieBreak::Deterministic => lowest_cost_deterministic(candidates, costs, 0..costs.len()),
        TieBreak::Uniform => {
            let min = costs
                .iter()
                .copied()
                .filter(|c| !c.is_nan())
                .fold(f64::INFINITY, f64::min);
            let tied: Vec<usize> = (0..costs.len()).filter(|&i| costs[i] == min).collect();
            if tied.is_empty() {
                None
            } else {
                Some(tied[rand::rng().random_range(0..tied.len())])
            }
        }
    }
}
