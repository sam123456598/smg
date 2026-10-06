//! Cost-function worker selection.
//!
//! Cache-aware routing gathers, once per request, what it knows about every eligible worker (prefix
//! overlap by tier, in-flight requests, the backend's waiting-prefill and KV-usage reports, anything
//! the router booked optimistically since) and hands that to a *selection policy*: a pipeline of
//! filters, additive cost scorers and one picker, registered by name with its own parameters.
//!
//! - [`catalog::DEFAULT_POLICY`] reproduces the pre-policy cache-aware decision exactly (an affinity
//!   group that the host resolves with its pressure gate and expected-wait selector).
//! - `cache-aware-balanced` (measured, bench-only: compiled with the `bench-policies` feature,
//!   not selectable without it) prices the engine's queued prefill, one mean prefill per request
//!   the router has in flight, and a capped prefix credit taken relative to the fleet's best
//!   holder, in seconds at one fleet drain rate, and takes the lowest; saturated workers are the
//!   gateway's worker-overload protection's business, not the policy's. On the mock fleet it
//!   beats the published prefill-load cost by low single digits and, with fresh load reports,
//!   trails the default on goodput while flattening the fleet; the scoreboard carries its rows.
//! - [`accounting::OptimisticAccounting`] closes the window between a dispatch and the engine's
//!   first event, when enabled.
//!
//! Design notes from the routers surveyed while building this layer, kept here because they are
//! what `cache-aware-balanced` is built from:
//! - credit a worker's prefix overlap *relative to the fleet's best holder* and *capped* (a few
//!   thousand tokens), so a full holder is not out-bid by load alone and a marginal holder earns
//!   little, which keeps sessions sticky at scale without pinning a hot prefix to one worker;
//! - account in-flight prefill tokens at dispatch (sized by the uncached part of the prompt) and
//!   release them at completion, so a burst of siblings cannot herd onto the worker whose load
//!   report has not caught up;
//! - leave a worker over the waiting-count or KV-share threshold to the gateway's worker-overload
//!   protection, which steers around it before concentration builds; a second copy of that veto
//!   inside the policy thrashed a tight cache.
//!
//! The cost of the stage itself is measured by `benches/policy_selection.rs`.

pub mod accounting;
pub mod catalog;
pub mod inputs;
pub mod policy;
pub mod softmax;

#[cfg(feature = "bench-policies")]
mod balanced;
mod default;

#[cfg(test)]
mod sim_tests;

pub use accounting::OptimisticAccounting;
pub use catalog::{build, default_policy, CatalogError, DEFAULT_POLICY, POLICY_NAMES};
pub use inputs::{CandidateInputs, RequestInputs};
pub use policy::{Needs, Pick, WorkerFilter, WorkerPicker, WorkerScorer, WorkerSelectionPolicy};
pub use softmax::TieBreak;
