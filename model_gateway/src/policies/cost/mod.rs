//! Cost-function worker selection.
//!
//! Cache-aware routing gathers, once per request, what it knows about every eligible worker (prefix
//! overlap by tier, in-flight requests, the backend's waiting-prefill and KV-usage reports, anything
//! the router booked optimistically since) and hands that to a *selection policy*: a pipeline of
//! filters, additive cost scorers and one picker, registered by name with its own parameters.
//!
//! - [`catalog::DEFAULT_POLICY`] reproduces the pre-policy cache-aware decision exactly (an affinity
//!   group that the host resolves with its pressure gate and expected-wait selector).
//! - [`accounting::OptimisticAccounting`] closes the window between a dispatch and the engine's
//!   first event, when enabled.
//! - With the `bench-policies` feature the catalog also builds the replay harness's comparison
//!   baseline (`reference-cost`, the published prefill-load cost formula); it is not part of the
//!   product and cannot be selected without the feature.
//!
//! Design notes from the routers surveyed while building this layer, kept here because they shape
//! the next cache-aware candidate:
//! - credit a worker's prefix overlap *relative to the fleet's best holder* and *capped* (a few
//!   thousand tokens), so a full holder is not out-bid by load alone and a marginal holder earns
//!   little, which keeps sessions sticky at scale without pinning a hot prefix to one worker;
//! - account in-flight prefill tokens at dispatch (sized by the uncached part of the prompt) and
//!   release them at completion, so a burst of siblings cannot herd onto the worker whose load
//!   report has not caught up;
//! - veto a worker on two signals *before* concentration builds, the waiting count and the active
//!   KV share, each with its own threshold; on real engines the waiting count trips first, on a
//!   fast-draining mock the KV share does.
//!
//! The cost of the stage itself is measured by `benches/policy_selection.rs`.

pub mod accounting;
pub mod catalog;
pub mod inputs;
pub mod policy;
pub mod softmax;

mod default;
#[cfg(feature = "bench-policies")]
mod reference_cost;

#[cfg(test)]
mod sim_tests;

pub use accounting::OptimisticAccounting;
pub use catalog::{build, default_policy, CatalogError, DEFAULT_POLICY, POLICY_NAMES};
pub use inputs::{CandidateInputs, RequestInputs};
pub use policy::{Needs, Pick, WorkerFilter, WorkerPicker, WorkerScorer, WorkerSelectionPolicy};
pub use softmax::TieBreak;
