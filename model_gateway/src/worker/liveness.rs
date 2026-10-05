//! Progress-based liveness beside the health check.
//!
//! The health checker needs `failure_threshold * check_interval` to exclude a
//! worker, tens of seconds with the defaults. The transport knows sooner: with
//! HTTP/2 keepalive pings every second, a worker that dies, freezes or falls
//! behind a partition fails its KV event stream, its load poll and its
//! in-flight streams within about two seconds. This module turns those
//! failures into a routing veto and clears it on the first successful contact.
//!
//! Two vetoes, both read by routing through [`Worker::stall_reason`]:
//!
//! - **unreachable**: a connection failure (load poll, KV event stream) while
//!   nothing has been heard from the worker for the stall threshold. Any
//!   successful contact clears it. Health probes count as contact when they
//!   pass; a failed probe is left to the health state machine, because probe
//!   timeouts on a slow but streaming worker are not unreachability.
//!
//! Neither veto touches the worker's health status: the health checker keeps
//! its own state machine, and the veto is simply gone once the worker talks.

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use tracing::{info, warn};

use super::{worker::StallReason, Worker};
use crate::observability::metrics::Metrics;

const DEFAULT_STALL: Duration = Duration::from_secs(2);
const DEFAULT_WEDGE: Duration = Duration::from_secs(3);

/// How often the sweep runs.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_millis(250);

static THRESHOLDS: OnceLock<(Duration, Duration)> = OnceLock::new();
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Set the stall and wedge thresholds from the gateway configuration. The
/// first call wins; the defaults are two and three seconds.
pub(crate) fn configure(stall: Duration, wedge: Duration) {
    let _ = THRESHOLDS.set((stall, wedge));
}

fn thresholds() -> (Duration, Duration) {
    THRESHOLDS
        .get()
        .copied()
        .unwrap_or((DEFAULT_STALL, DEFAULT_WEDGE))
}

/// Milliseconds since the gateway started: the clock behind the workers'
/// contact and progress stamps.
pub(crate) fn now_ms() -> u64 {
    u64::try_from(EPOCH.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Whether a gRPC status describes the connection rather than the request:
/// what a dead peer, a reset or a failed keepalive produce. A deadline or an
/// engine-side error says the worker is slow or wrong, not gone, and a slow
/// worker that still streams tokens must not flap in and out of routing.
pub(crate) fn is_transport_failure(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Cancelled
            | tonic::Code::Aborted
    )
}

/// A successful interaction (a poll answered, a probe passed, an event batch,
/// a response): the worker is reachable, so an unreachable veto ends here.
pub(crate) fn on_contact(worker: &Arc<dyn Worker>) {
    worker.note_contact();
    if worker.stall_reason() == Some(StallReason::Unreachable) {
        set(worker, None, "contact");
    }
}

/// A transport failure from `what`. Vetoes the worker when nothing has been
/// heard from it for the stall threshold; a failure right after a contact is
/// remembered, and [`sweep`] vetoes the worker once the threshold passes
/// without a contact.
pub(crate) fn on_contact_failed(worker: &Arc<dyn Worker>, what: &'static str) {
    worker.note_transport_failure();
    if worker.stall_reason().is_some() {
        return;
    }
    if stalled(worker.contact_age(), thresholds().0) {
        set(worker, Some(StallReason::Unreachable), what);
    }
}

/// Periodic check: a worker whose transport failed and that has then stayed
/// silent for the stall threshold is vetoed now, not at its next failed poll.
/// Keepalive pings fail a dead connection within about two seconds, so this
/// puts the exclusion at the threshold itself.
pub(crate) fn sweep(worker: &Arc<dyn Worker>) {
    let stall = thresholds().0;
    if worker.stall_reason().is_some() {
        return;
    }
    if worker.transport_failure_pending() && stalled(worker.contact_age(), stall) {
        set(
            worker,
            Some(StallReason::Unreachable),
            "silent since a transport failure",
        );
        return;
    }
}

/// The unreachable rule on its inputs.
fn stalled(contact_age: Duration, stall: Duration) -> bool {
    contact_age >= stall
}

fn set(worker: &Arc<dyn Worker>, reason: Option<StallReason>, cause: &'static str) {
    let previous = worker.stall_reason();
    if !worker.set_stall(reason) {
        return;
    }
    match reason {
        Some(reason) => {
            warn!(
                worker_url = %worker.url(),
                reason = reason.as_str(),
                cause,
                contact_age_ms = u64::try_from(worker.contact_age().as_millis()).unwrap_or(u64::MAX),
                "Worker vetoed by liveness"
            );
            Metrics::set_worker_stalled(worker.url(), reason.as_str(), true);
        }
        None => {
            info!(worker_url = %worker.url(), cause, "Worker re-admitted by liveness");
            if let Some(previous) = previous {
                Metrics::set_worker_stalled(worker.url(), previous.as_str(), false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::BasicWorkerBuilder;

    fn worker() -> Arc<dyn Worker> {
        Arc::new(BasicWorkerBuilder::new("http://w1:8000").build())
    }

    #[test]
    fn unreachable_needs_a_stall_not_just_a_failure() {
        assert!(!stalled(Duration::from_millis(500), DEFAULT_STALL));
        assert!(stalled(Duration::from_secs(2), DEFAULT_STALL));
    }

    #[test]
    fn a_veto_removes_the_worker_from_routing_and_contact_restores_it() {
        let w = worker();
        assert!(w.stall_reason().is_none());
        set(&w, Some(StallReason::Unreachable), "test");
        assert_eq!(w.stall_reason(), Some(StallReason::Unreachable));
        assert!(w.routing_state().stalled);
        assert!(!w.routing_state().eligible());
        assert!(!w.is_healthy_and_eligible());
        on_contact(&w);
        assert!(w.stall_reason().is_none());
        assert!(!w.routing_state().stalled);
    }

    #[test]
    fn a_failure_right_after_contact_is_a_blip() {
        let w = worker();
        w.note_contact();
        on_contact_failed(&w, "test");
        assert!(w.stall_reason().is_none());
        assert!(
            w.transport_failure_pending(),
            "but it is remembered for the sweep"
        );
        sweep(&w);
        assert!(
            w.stall_reason().is_none(),
            "the sweep waits for the threshold"
        );
        w.note_contact();
        assert!(!w.transport_failure_pending(), "a contact forgets it");
    }
}
