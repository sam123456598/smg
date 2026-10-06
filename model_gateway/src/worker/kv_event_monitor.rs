//! Per-worker KV cache event subscription manager.
//!
//! `KvEventMonitor` spawns a background tokio task per gRPC worker that subscribes
//! to KV cache events and feeds them into a shared [`KvIndex`] (one per model;
//! the positional indexer or the run index, per `--kv-index`).
//! This enables event-driven cache-aware routing as an alternative to the approximate
//! radix tree approach.
//!
//! Lifecycle:
//! - `on_worker_added` — spawns streaming task, creates indexer if needed
//! - `on_worker_removed` — signals graceful shutdown, task cleans up indexer
//! - `stop` — signals shutdown to all tasks, clears state

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use futures::FutureExt as _;
use kv_index::{
    salt::{content_hash_with_seed, namespace_seed},
    ApplyError, SequenceHash, StoredBlock,
};
use smg_grpc_client::common_proto::{
    kv_cache_event, KvBlock, KvBlocksRemoved, KvBlocksStored, KvCacheEvent, KvCacheLocality,
    KvCacheTier, KvEventBatch, KvSnapshotChunk,
};
use tokio::{
    sync::{oneshot, Mutex, Semaphore},
    task::JoinHandle,
};
use tracing::{debug, error, info, warn};

use super::{
    kv_event_recovery::{Admission, RankState, ResyncReason},
    kv_index_backend::{KvIndex, KvIndexKind, WorkerBlocks},
    liveness,
};
use crate::{
    observability::metrics::Metrics,
    policies::utils::PeriodicTask,
    worker::{ConnectionMode, Worker, UNKNOWN_MODEL_ID},
};

/// Default jump size for new positional indexers.
const DEFAULT_JUMP_SIZE: usize = 64;

/// Interval between positional-indexer prune cycles (matches the routing
/// policies' default eviction cadence).
const PRUNE_INTERVAL_SECS: u64 = 30;

/// Initial reconnection delay after stream failure.
const INITIAL_RECONNECT_DELAY_MS: u64 = 100;

/// Resolves when the worker is heard from (see `Worker::contact_wake`), or
/// never for a worker without a notifier.
async fn woken(wake: Option<&Arc<tokio::sync::Notify>>) {
    match wake {
        Some(notify) => notify.notified().await,
        None => std::future::pending().await,
    }
}

/// How long a subscription call may take to answer with headers. A port that
/// accepts but does not serve yet (an engine still starting) hangs the call.
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(2);

/// A contact with the worker while a subscription call is pending (a poll
/// answered, a probe passed) says the worker serves now: a call still without
/// an answer this long after the contact is abandoned and retried. A live
/// server answers in milliseconds, so the contacts of a healthy worker (every
/// token it streams is one) never cut a call short.
const SUBSCRIBE_RETRY_GRACE: Duration = Duration::from_millis(500);

/// The least a reconnect waits, contact or not: a server that keeps closing
/// the stream of a worker that is otherwise talking must not be hammered.
const RECONNECT_FLOOR: Duration = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

/// Maximum backoff between subscription attempts. Kept short: a worker that
/// restarts is healthy again within a few seconds, and until the stream is
/// back the blocks it stores are invisible to routing (the servicers resume
/// after the cursor and never resend them). A connect attempt is cheap.
const MAX_RECONNECT_DELAY_MS: u64 = 5_000;

/// Positional-index cleanup is CPU-bound and can touch many blocks. Keep it
/// off Tokio workers and bound concurrent purges during fleet-wide drains.
const MAX_CONCURRENT_INDEX_REMOVALS: usize = 4;
static INDEX_REMOVAL_PERMITS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_INDEX_REMOVALS);

/// Manages per-worker KV cache event subscriptions.
///
/// Each gRPC worker gets a dedicated tokio task that subscribes to the backend's
/// KV cache event stream and feeds events into a shared [`KvIndex`]
/// (one per `model_id`). Workers serving the same model share the same indexer.
pub struct KvEventMonitor {
    /// Per-model KV indexes: model_id → shared indexer.
    /// Arc-wrapped so the prune task can share the map WITHOUT holding (even
    /// weakly) the monitor itself: `PeriodicTask` joins its thread on drop, so
    /// a task that could ever own the last monitor reference would run the
    /// monitor's drop — and thus its own join — on its own thread.
    pub(crate) indexers: Arc<DashMap<String, Arc<KvIndex>>>,
    /// Per-model block sizes learned from KV events or set via WorkerSpec.
    /// Used by CacheAwarePolicy to chunk request tokens at query time.
    /// Arc-wrapped so subscription tasks can update it from events.
    block_sizes: Arc<DashMap<String, usize>>,
    /// Per-worker subscription handles: worker_url → subscription info.
    /// Mutex matches LoadMonitor pattern for atomic abort + remove.
    worker_handles: Mutex<HashMap<String, WorkerSubscription>>,
    /// Which index new models get.
    kind: KvIndexKind,
    /// Jump size for new positional indexers.
    jump_size: usize,
    /// Periodic indexer prune, held so it aborts when the monitor drops.
    /// Set once by [`start_prune_task`](Self::start_prune_task); sync mutex
    /// because it is touched only at startup, never on event paths.
    prune_task: parking_lot::Mutex<Option<PeriodicTask>>,
}

/// Tracks a single worker's subscription state.
struct WorkerSubscription {
    handle: JoinHandle<()>,
    model_id: String,
    /// Signals the subscription task to shut down gracefully.
    /// The task owns its `WorkerBlocks` and cleans up the indexer on exit.
    shutdown_tx: oneshot::Sender<()>,
}

/// A worker's subscription state: one admission cursor per data-parallel
/// rank (every publisher numbers its own batches) over the worker's one
/// index state, whose copies are pooled across ranks (see
/// [`WorkerIndexState`]). Cursors and copy counts never mix.
#[derive(Default)]
struct WorkerStreamState {
    ranks: HashMap<i32, RankState>,
    index: WorkerIndexState,
    /// A relay snapshot whose chunks are still arriving on the stream.
    snapshot: Option<SnapshotProgress>,
}

/// Where an in-band relay snapshot stands (`KvSnapshotChunk`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotProgress {
    count: u32,
    applied: u32,
    blocks: u64,
}

impl WorkerStreamState {
    /// The cursor to send when resubscribing: rank 0's last applied sequence
    /// (the servicers replay rank 0's publisher). Other ranks keep their own
    /// cursors and dedup what arrives.
    fn resume_sequence(&self) -> u64 {
        self.ranks.get(&0).map_or(0, RankState::resume_from)
    }

    /// A new stream connection was made: every rank's next batch is its
    /// first on it.
    fn reconnected(&mut self) {
        for rank in self.ranks.values_mut() {
            rank.reconnected();
        }
    }

    fn degraded_ranks(&self) -> usize {
        self.ranks
            .values()
            .filter(|rank| rank.is_degraded())
            .count()
    }

    /// The stream ended with a snapshot still arriving: what was applied is
    /// a partial live set, so the next subscription asks from zero (and gets
    /// a whole snapshot) instead of resuming after the last chunk's stamp.
    /// Returns whether a snapshot was abandoned.
    fn abandon_snapshot(&mut self) -> bool {
        let Some(progress) = self.snapshot.take() else {
            return false;
        };
        for cursor in self.ranks.values_mut() {
            cursor.reset();
        }
        debug_assert!(progress.applied < progress.count);
        true
    }
}

/// What `admit_batch` did with a batch.
#[derive(Debug, PartialEq, Eq)]
enum BatchOutcome {
    /// Applied to the index.
    Applied,
    /// Dropped (duplicate) or held (snapshot tail).
    Skipped,
    /// A gap: the caller reconnects asking for a replay from `expected`.
    Gap { expected: u64, received: u64 },
}

/// Result of processing a stream connection to completion.
enum StreamResult {
    /// Stream closed normally (server-side).
    Ended,
    /// Stream produced an error.
    Error(tonic::Status),
    /// Detected a gap in sequence numbers.
    GapDetected { expected: u64, received: u64 },
}

impl KvEventMonitor {
    /// A monitor whose models get positional indexers.
    ///
    /// `jump_size` is the positional indexer's historical tuning knob.
    /// Pass `None` for the default (64).
    pub fn new(jump_size: Option<usize>) -> Self {
        Self::with_kind(KvIndexKind::Positional, jump_size)
    }

    /// A monitor whose models get indexes of `kind` (see `--kv-index`).
    pub fn with_kind(kind: KvIndexKind, jump_size: Option<usize>) -> Self {
        let jump_size = jump_size.unwrap_or(DEFAULT_JUMP_SIZE).max(1);
        Self {
            indexers: Arc::new(DashMap::new()),
            block_sizes: Arc::new(DashMap::new()),
            worker_handles: Mutex::new(HashMap::new()),
            kind,
            jump_size,
            prune_task: parking_lot::Mutex::new(None),
        }
    }

    /// The kind of index this monitor builds per model.
    pub fn kind(&self) -> KvIndexKind {
        self.kind
    }

    /// Prune every model's positional indexer with the given bounds.
    /// `ttl_secs`/`max_entries` of 0 disable the respective pass — see
    /// [`KvIndex::prune`]. The run index has no prune and is left alone.
    pub fn prune_all(&self, ttl_secs: u64, max_entries: usize) {
        Self::prune_indexers(&self.indexers, ttl_secs, max_entries);
    }

    fn prune_indexers(indexers: &DashMap<String, Arc<KvIndex>>, ttl_secs: u64, max_entries: usize) {
        let ttl = u32::try_from(ttl_secs).unwrap_or(u32::MAX - 1);
        let ttl = (ttl > 0).then_some(ttl);
        let max = (max_entries > 0).then_some(max_entries);
        if ttl.is_none() && max.is_none() {
            return;
        }
        for entry in indexers {
            let Some(stats) = entry.value().prune(ttl, max) else {
                continue;
            };
            if stats.evicted_ttl + stats.evicted_capacity > 0 {
                info!(
                    model_id = %entry.key(),
                    evicted_ttl = stats.evicted_ttl,
                    evicted_capacity = stats.evicted_capacity,
                    remaining = stats.remaining,
                    "Pruned positional indexer"
                );
            }
        }
    }

    /// Start the periodic indexer prune. No-op when both bounds are 0/unset.
    /// The task shares only the indexer map — never a reference to the monitor
    /// itself — so it can never be the one to run the monitor's drop (and with
    /// it its own thread's join; see the `indexers` field docs). The handle is
    /// held by the monitor, so the task stops when the monitor drops.
    pub fn start_prune_task(&self, ttl_secs: u64, max_entries: usize) {
        if ttl_secs == 0 && max_entries == 0 {
            return;
        }
        if self.kind == KvIndexKind::Run {
            warn!(
                ttl_secs,
                max_entries,
                "The run index has no prune: it holds what the engines report and \
                 shrinks with their removals; --kv-indexer-ttl-secs and \
                 --kv-indexer-max-entries apply to --kv-index positional only"
            );
            return;
        }
        let indexers = Arc::clone(&self.indexers);
        let task = PeriodicTask::spawn(PRUNE_INTERVAL_SECS, "KvIndexerPrune", move || {
            Self::prune_indexers(&indexers, ttl_secs, max_entries);
        });
        *self.prune_task.lock() = Some(task);
        info!(
            ttl_secs,
            max_entries,
            interval_secs = PRUNE_INTERVAL_SECS,
            "Started positional-indexer prune task"
        );
    }

    /// Start a KV event subscription for a worker.
    ///
    /// Spawns a background tokio task that subscribes to KV cache events via
    /// server-streaming gRPC and applies them to the model's `KvIndex`.
    /// Duplicate calls for the same worker URL are no-ops.
    pub async fn on_worker_added(&self, worker: &Arc<dyn Worker>) {
        let url = worker.url().to_string();
        // Normalize model_id to match routing's normalize_model_key — empty → "unknown".
        let model_id = Self::normalize_model_id(worker.model_id());

        // Only gRPC workers stream KV events. HTTP proxies don't, and a ZMQ
        // EngineCore returns `unimplemented` for SubscribeKvEvents — subscribing
        // there would just be a wasted handshake + round-trip per registration.
        if *worker.connection_mode() != ConnectionMode::Grpc {
            debug!(worker_url = %url, mode = %worker.connection_mode(), "non-gRPC worker, skipping KV event subscription");
            return;
        }

        let mut handles = self.worker_handles.lock().await;
        if handles.contains_key(&url) {
            debug!(worker_url = %url, "KV event subscription already active, skipping");
            return;
        }

        let indexer = self
            .indexers
            .entry(model_id.clone())
            .or_insert_with(|| Arc::new(KvIndex::new(self.kind, self.jump_size)))
            .clone();
        // Seed block_size provisionally from WorkerSpec. The event stream will
        // overwrite this with the backend's actual page size once received.
        if let Some(bs) = worker.metadata().spec.kv_block_size {
            if bs > 0 {
                self.block_sizes.entry(model_id.clone()).or_insert(bs);
            } else {
                warn!(worker_url = %url, "Worker reports kv_block_size=0, ignoring");
            }
        }

        let worker = Arc::clone(worker);
        let worker_url = url.clone();
        let block_sizes = Arc::clone(&self.block_sizes);

        info!(
            worker_url = %url,
            model_id = %model_id,
            "Starting KV event subscription"
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let loop_model_id = model_id.clone();
        let task_url = url.clone();

        #[expect(
            clippy::disallowed_methods,
            reason = "KV event monitor: runs for the lifetime of the worker, \
                      handle is stored and graceful shutdown is sent on removal"
        )]
        let handle = tokio::spawn(async move {
            // Catch panics here so they surface when they happen — a bare
            // JoinError would only be observed at worker removal, leaving the
            // index silently frozen for this worker until then.
            let result = std::panic::AssertUnwindSafe(Self::subscription_loop(
                worker,
                worker_url,
                indexer,
                block_sizes,
                loop_model_id,
                shutdown_rx,
            ))
            .catch_unwind()
            .await;
            if let Err(payload) = result {
                let msg = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .map(String::from)
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "(non-string panic)".into());
                error!(
                    worker_url = %task_url,
                    panic.message = %msg,
                    "KV event subscription task panicked; KV events from this \
                     worker no longer feed cache-aware routing"
                );
                Metrics::record_kv_event_subscription_failure(&task_url, "panic");
            }
        });

        handles.insert(
            url,
            WorkerSubscription {
                handle,
                model_id,
                shutdown_tx,
            },
        );
    }

    /// Stop the KV event subscription for a worker.
    ///
    /// Sends a graceful shutdown signal. The subscription task cleans up its
    /// own `WorkerBlocks` using the indexer's per-worker reverse map; that
    /// CPU-bound cleanup runs on the bounded blocking pool rather than a Tokio
    /// runtime worker.
    pub async fn on_worker_removed(&self, worker_url: &str) {
        let subscription = {
            let mut handles = self.worker_handles.lock().await;
            handles.remove(worker_url)
        };

        let Some(sub) = subscription else {
            return;
        };
        info!(worker_url = %worker_url, "Stopping KV event subscription");
        // Signal graceful shutdown — task cleans up its worker_blocks in the indexer.
        let _ = sub.shutdown_tx.send(());
        // Panics are caught inside the task; a JoinError here (abort or a
        // panic that escaped the guard) must still be surfaced, not discarded.
        if let Err(e) = sub.handle.await {
            error!(
                worker_url = %worker_url,
                error = %e,
                "KV event subscription task failed"
            );
            Metrics::record_kv_event_subscription_failure(worker_url, "join_error");
        }

        // Re-check under lock whether this was the last worker for the model.
        // Must re-acquire lock after shutdown to avoid TOCTOU with concurrent
        // on_worker_added that may have added a new worker for the same model
        // between our first lock release and this point.
        let should_remove_indexer = {
            let handles = self.worker_handles.lock().await;
            !handles.values().any(|other| other.model_id == sub.model_id)
        };

        if should_remove_indexer {
            self.indexers.remove(&sub.model_id);
            self.block_sizes.remove(&sub.model_id);
        }
    }

    /// Stop all subscriptions and clean up.
    pub async fn stop(&self) {
        let subscriptions: HashMap<String, WorkerSubscription> = {
            let mut handles = self.worker_handles.lock().await;
            std::mem::take(&mut *handles)
        };

        if !subscriptions.is_empty() {
            info!(
                count = subscriptions.len(),
                "Stopping all KV event subscriptions"
            );
            for (url, sub) in subscriptions {
                debug!(worker_url = %url, "Stopping KV event subscription");
                let _ = sub.shutdown_tx.send(());
                if let Err(e) = sub.handle.await {
                    error!(
                        worker_url = %url,
                        error = %e,
                        "KV event subscription task failed"
                    );
                    Metrics::record_kv_event_subscription_failure(&url, "join_error");
                }
            }
        }

        self.indexers.clear();
        self.block_sizes.clear();
    }

    /// Get the indexer for a model (used by `CacheAwarePolicy` for queries).
    pub fn get_indexer(&self, model_id: &str) -> Option<Arc<KvIndex>> {
        self.indexers.get(model_id).map(|r| Arc::clone(&r))
    }

    /// Get the block size for a model (learned from events or set via `set_block_size`).
    pub fn block_size(&self, model_id: &str) -> Option<usize> {
        self.block_sizes.get(model_id).map(|v| *v)
    }

    /// Set the block size for a model (e.g. from WorkerSpec during registration).
    /// Does not overwrite a value already learned from events.
    pub fn set_block_size(&self, model_id: &str, block_size: usize) {
        self.block_sizes
            .entry(model_id.to_string())
            .or_insert(block_size);
    }

    /// Check if any subscription is running.
    pub async fn is_running(&self) -> bool {
        !self.worker_handles.lock().await.is_empty()
    }

    /// Normalize model_id to match routing's `normalize_model_key`.
    /// Empty model IDs map to UNKNOWN_MODEL_ID for consistent keying.
    fn normalize_model_id(model_id: &str) -> String {
        if model_id.is_empty() {
            UNKNOWN_MODEL_ID.to_string()
        } else {
            model_id.to_string()
        }
    }

    // -----------------------------------------------------------------------
    // Subscription loop
    // -----------------------------------------------------------------------

    /// Learn `block_size` from the first `KvBlock` in a stored event.
    ///
    /// Called once per model when the first stored event arrives, providing
    /// ground truth from the backend. `CacheAwarePolicy` uses this to chunk
    /// request tokens into blocks for overlap scoring.
    ///
    /// Overwrites any provisional value seeded from `WorkerSpec` since the
    /// event stream reflects the backend's actual page size.
    fn learn_block_size(
        block_sizes: &DashMap<String, usize>,
        model_id: &str,
        learned: &mut bool,
        batch: &KvEventBatch,
    ) {
        if *learned {
            return;
        }
        for event in &batch.events {
            if let Some(kv_cache_event::Data::Stored(stored)) = &event.data {
                if let Some(block) = stored.blocks.first() {
                    if block.block_size > 0 {
                        let bs = block.block_size as usize;
                        block_sizes.insert(model_id.to_string(), bs);
                        info!(
                            model_id = %model_id,
                            block_size = bs,
                            "Learned block_size from KV event"
                        );
                        *learned = true;
                        return;
                    }
                }
            }
        }
    }

    /// Main subscription loop for a single worker.
    ///
    /// Owns the `WorkerBlocks` for this worker and cleans it up on exit.
    /// Exits when `shutdown_rx` fires or the backend returns `Unimplemented`.
    async fn remove_indexer_worker(
        indexer: Arc<KvIndex>,
        worker_id: u32,
        worker_url: &str,
        worker_blocks: WorkerIndexState,
    ) {
        let Ok(permit) = INDEX_REMOVAL_PERMITS.acquire().await else {
            error!(worker_id, "Positional-index cleanup semaphore closed");
            return;
        };
        let WorkerIndexState {
            blocks, counters, ..
        } = worker_blocks;
        if counters != WorkerIndexCounters::default() {
            debug!(
                worker_id,
                ?counters,
                "KV events the positional index did not take as is"
            );
        }
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            indexer.remove_worker(worker_id, blocks);
        })
        .await;

        if let Err(error) = result {
            error!(worker_id, %error, "Positional-index worker cleanup task failed");
        }
        Metrics::set_kv_index_blocks(worker_url, 0);
    }

    async fn subscription_loop(
        worker: Arc<dyn Worker>,
        worker_url: String,
        indexer: Arc<KvIndex>,
        block_sizes: Arc<DashMap<String, usize>>,
        model_id: String,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let worker_id = match indexer.intern_worker(&worker_url) {
            Ok(id) => id,
            Err(e) => {
                error!(
                    worker_url = %worker_url,
                    error = %e,
                    "Failed to intern worker; KV events from this worker will \
                     not feed cache-aware routing"
                );
                Metrics::record_kv_event_subscription_failure(&worker_url, "intern_failed");
                return;
            }
        };
        let mut state = WorkerStreamState::default();
        // A contact with the worker (a poll answered, a probe passed) ends the
        // reconnect backoff early: a worker that is back gets its stream back
        // at once instead of after the remaining delay.
        let wake = worker.contact_wake();
        let mut reconnect_delay_ms = INITIAL_RECONNECT_DELAY_MS;
        let mut block_size_learned = false;

        /// Sleep with shutdown check. Returns `true` if shutdown was signaled.
        /// A contact with the worker ends the sleep early, but never before
        /// `RECONNECT_FLOOR`.
        macro_rules! sleep_or_shutdown {
            ($delay:expr, $rx:expr) => {{
                let delay: Duration = $delay;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => false,
                    () = async {
                        tokio::time::sleep(delay.min(RECONNECT_FLOOR)).await;
                        woken(wake.as_ref()).await;
                    } => false,
                    _ = &mut *$rx => true,
                }
            }};
        }

        loop {
            let backend_client = match worker.get_backend_client().await {
                Ok(Some(client)) => client,
                Ok(None) => {
                    // HTTP workers are filtered in on_worker_added, so this should
                    // be unreachable. Retry defensively rather than exiting and
                    // leaving a stale entry in worker_handles.
                    warn!(
                        worker_url = %worker_url,
                        delay_ms = reconnect_delay_ms,
                        "Worker has no backend client yet, retrying"
                    );
                    if sleep_or_shutdown!(
                        Duration::from_millis(reconnect_delay_ms),
                        &mut shutdown_rx
                    ) {
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                    continue;
                }
                Err(e) => {
                    warn!(
                        worker_url = %worker_url,
                        error = %e,
                        delay_ms = reconnect_delay_ms,
                        "Failed to get backend client, retrying"
                    );
                    if sleep_or_shutdown!(
                        Duration::from_millis(reconnect_delay_ms),
                        &mut shutdown_rx
                    ) {
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                    continue;
                }
            };

            let start_seq = state.resume_sequence();
            // A live server answers a subscription with headers at once; one
            // that does not within the deadline is unreachable (a half-open
            // connection behind a partition), and must not hang the loop.
            let subscribed = tokio::select! {
                attempt = tokio::time::timeout(
                    SUBSCRIBE_DEADLINE,
                    backend_client.subscribe_kv_events(start_seq),
                ) => attempt.unwrap_or_else(|_| {
                    Err(tonic::Status::unavailable(format!(
                        "SubscribeKvEvents did not answer within {SUBSCRIBE_DEADLINE:?}"
                    )))
                }),
                // The worker was heard from on another path (a poll, a probe)
                // and this call still hangs: a fresh one will land.
                () = async {
                    woken(wake.as_ref()).await;
                    tokio::time::sleep(SUBSCRIBE_RETRY_GRACE).await;
                } => continue,
            };
            let stream = match subscribed {
                Ok(stream) => {
                    info!(
                        worker_url = %worker_url,
                        start_seq,
                        "KV event stream connected"
                    );
                    Metrics::record_kv_event_subscription(&worker_url);
                    reconnect_delay_ms = INITIAL_RECONNECT_DELAY_MS;
                    state.reconnected();
                    liveness::on_contact(&worker);
                    stream
                }
                Err(e) => {
                    // If the backend doesn't implement SubscribeKvEvents (e.g. vLLM),
                    // stop retrying — this RPC will never succeed.
                    if e.code() == tonic::Code::Unimplemented {
                        warn!(
                            worker_url = %worker_url,
                            "Backend does not implement SubscribeKvEvents, \
                             disabling KV event subscription for this worker"
                        );
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    if e.code() == tonic::Code::OutOfRange {
                        warn!(
                            worker_url = %worker_url,
                            start_seq,
                            "KV event replay cursor expired; clearing worker state and requesting a current snapshot"
                        );
                        Self::reset_worker(
                            &indexer,
                            worker_id,
                            &mut state,
                            &worker_url,
                            ResyncReason::OutOfRange,
                        );
                        reconnect_delay_ms = INITIAL_RECONNECT_DELAY_MS;
                        continue;
                    }
                    if liveness::is_transport_failure(e.code()) {
                        liveness::on_contact_failed(&worker, "kv subscribe");
                    }
                    warn!(
                        worker_url = %worker_url,
                        error = %e,
                        delay_ms = reconnect_delay_ms,
                        "Failed to subscribe to KV events, retrying"
                    );
                    if sleep_or_shutdown!(
                        Duration::from_millis(reconnect_delay_ms),
                        &mut shutdown_rx
                    ) {
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                    continue;
                }
            };

            let on_batch = |batch: &KvEventBatch| {
                liveness::on_contact(&worker);
                Self::learn_block_size(&block_sizes, &model_id, &mut block_size_learned, batch);
            };
            let stream_result = tokio::select! {
                result = Self::process_stream(
                    stream, &worker_url, worker_id, &indexer, &mut state, on_batch,
                ) => result,
                _ = &mut shutdown_rx => {
                    Self::remove_indexer_worker(
                        Arc::clone(&indexer),
                        worker_id,
                        &worker_url,
                        state.index,
                    )
                    .await;
                    return;
                }
            };

            if state.abandon_snapshot() {
                warn!(
                    worker_url = %worker_url,
                    "KV event stream ended during a relay snapshot; the next subscription \
                     starts over from zero"
                );
            }
            match stream_result {
                StreamResult::Ended => {
                    info!(
                        worker_url = %worker_url,
                        resume_from = state.resume_sequence(),
                        delay_ms = reconnect_delay_ms,
                        "KV event stream ended, reconnecting"
                    );
                    // Backoff to avoid tight reconnect loop if server keeps
                    // closing the stream cleanly (e.g., rolling connections).
                    if sleep_or_shutdown!(
                        Duration::from_millis(reconnect_delay_ms),
                        &mut shutdown_rx
                    ) {
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                }
                StreamResult::Error(e) => {
                    if e.code() == tonic::Code::DataLoss {
                        warn!(
                            worker_url = %worker_url,
                            error = %e,
                            resume_from = state.resume_sequence(),
                            "KV event subscriber fell behind; clearing worker state and requesting a current snapshot"
                        );
                        Self::reset_worker(
                            &indexer,
                            worker_id,
                            &mut state,
                            &worker_url,
                            ResyncReason::DataLoss,
                        );
                        reconnect_delay_ms = INITIAL_RECONNECT_DELAY_MS;
                        continue;
                    }
                    if liveness::is_transport_failure(e.code()) {
                        liveness::on_contact_failed(&worker, "kv stream");
                    }
                    warn!(
                        worker_url = %worker_url,
                        error = %e,
                        resume_from = state.resume_sequence(),
                        delay_ms = reconnect_delay_ms,
                        "KV event stream error, reconnecting"
                    );
                    if sleep_or_shutdown!(
                        Duration::from_millis(reconnect_delay_ms),
                        &mut shutdown_rx
                    ) {
                        Self::remove_indexer_worker(
                            Arc::clone(&indexer),
                            worker_id,
                            &worker_url,
                            state.index,
                        )
                        .await;
                        return;
                    }
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                }
                StreamResult::GapDetected { expected, received } => {
                    warn!(
                        worker_url = %worker_url,
                        expected,
                        received,
                        "Sequence gap detected, reconnecting for replay from seq {expected}"
                    );
                    // No backoff: gap replay is a normal recovery path, and the
                    // rank state asks for it once; if the server skips ahead
                    // again the gap is settled instead of retried.
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stream processing + proto conversion
    // -----------------------------------------------------------------------

    /// Process batches from a single stream connection.
    async fn process_stream(
        mut stream: tonic::Streaming<KvEventBatch>,
        worker_url: &str,
        worker_id: u32,
        indexer: &KvIndex,
        state: &mut WorkerStreamState,
        mut on_batch: impl FnMut(&KvEventBatch),
    ) -> StreamResult {
        use tokio_stream::StreamExt;

        while let Some(result) = stream.next().await {
            let batch = match result {
                Ok(batch) => batch,
                Err(e) => return StreamResult::Error(e),
            };
            if let BatchOutcome::Gap { expected, received } =
                Self::admit_batch(&batch, worker_url, worker_id, indexer, state, &mut on_batch)
            {
                return StreamResult::GapDetected { expected, received };
            }
        }

        StreamResult::Ended
    }

    /// Run one batch through its rank's admission cursor and apply it to the
    /// worker's index state.
    fn admit_batch(
        batch: &KvEventBatch,
        worker_url: &str,
        worker_id: u32,
        indexer: &KvIndex,
        state: &mut WorkerStreamState,
        on_batch: &mut impl FnMut(&KvEventBatch),
    ) -> BatchOutcome {
        if let Some(chunk) = &batch.snapshot {
            return Self::admit_snapshot_chunk(
                batch, chunk, worker_url, worker_id, indexer, state, on_batch,
            );
        }
        let rank = batch.dp_rank.unwrap_or(0);
        let seq = batch.sequence_number;
        let clears = batch.events.iter().any(|event| {
            matches!(&event.data, Some(kv_cache_event::Data::Cleared(cleared)) if cleared.ownership.is_none())
        });
        let admission = state.ranks.entry(rank).or_default().admit(seq, clears);
        let mut degraded_changed = false;
        match admission {
            Admission::Apply => {}
            Admission::Stale => {
                debug!(worker_url = %worker_url, rank, received = seq, "Skipping stale KV event batch");
                Metrics::record_kv_event_batch(worker_url, "stale");
                return BatchOutcome::Skipped;
            }
            Admission::Restart => {
                warn!(
                    worker_url = %worker_url,
                    rank,
                    received = seq,
                    "KV event publisher restarted; clearing the worker's index state"
                );
                Self::clear_worker(worker_id, indexer, state, rank);
                Metrics::record_kv_event_resync(
                    worker_url,
                    ResyncReason::PublisherRestart.as_str(),
                );
                degraded_changed = true;
            }
            Admission::Replay { expected } => {
                Metrics::record_kv_event_gap(worker_url, "replay_requested", seq - expected);
                return BatchOutcome::Gap {
                    expected,
                    received: seq,
                };
            }
            Admission::Unrecovered { missed, cleared } => {
                warn!(
                    worker_url = %worker_url,
                    rank,
                    missed,
                    cleared,
                    "KV event gap could not be replayed; continuing from the live stream"
                );
                if cleared {
                    Self::clear_worker(worker_id, indexer, state, rank);
                    Metrics::record_kv_event_resync(worker_url, ResyncReason::GapCleared.as_str());
                }
                Metrics::record_kv_event_gap(
                    worker_url,
                    if cleared {
                        "unrecovered_cleared"
                    } else {
                        "unrecovered_kept"
                    },
                    missed,
                );
                degraded_changed = true;
            }
            Admission::Buffered => {
                if let Some(cursor) = state.ranks.get_mut(&rank) {
                    if !cursor.buffer_live(batch.clone()) {
                        Metrics::record_kv_event_batch(worker_url, "tail_overflow");
                    }
                    Metrics::set_kv_event_tail_depth(worker_url, cursor.tail_len());
                }
                return BatchOutcome::Skipped;
            }
        }

        on_batch(batch);
        for event in &batch.events {
            Self::apply_event(event, worker_id, indexer, &mut state.index);
        }
        Self::record_lag(worker_url, batch.timestamp);
        Metrics::record_kv_event_batch(worker_url, "applied");
        Metrics::set_kv_index_blocks(worker_url, indexer.worker_block_count(worker_id));
        if degraded_changed {
            Metrics::set_kv_event_degraded_ranks(worker_url, state.degraded_ranks());
        }
        BatchOutcome::Applied
    }

    /// A chunk of a relay state snapshot (`KvSnapshotChunk`): the live set the
    /// relay recorded, replacing the worker's state. Chunk 0 clears the worker
    /// and every cursor and counts a `snapshot` resync; every chunk is applied
    /// outside the admission rules and moves its rank's cursor to its stamp,
    /// which the relay chose so that live events continue after the last one.
    fn admit_snapshot_chunk(
        batch: &KvEventBatch,
        chunk: &KvSnapshotChunk,
        worker_url: &str,
        worker_id: u32,
        indexer: &KvIndex,
        state: &mut WorkerStreamState,
        on_batch: &mut impl FnMut(&KvEventBatch),
    ) -> BatchOutcome {
        let rank = batch.dp_rank.unwrap_or(0);
        if chunk.index == 0 || state.snapshot.is_none() {
            if chunk.index != 0 {
                warn!(
                    worker_url = %worker_url,
                    rank,
                    index = chunk.index,
                    "KV event relay snapshot arrived without its first chunk; taking it as a resync"
                );
            }
            info!(
                worker_url = %worker_url,
                rank,
                chunks = chunk.count,
                blocks = chunk.blocks,
                unknown_before = chunk.unknown_before,
                through = batch.sequence_number + u64::from(chunk.count.saturating_sub(chunk.index + 1)),
                "KV event relay served a state snapshot; replacing the worker's index state"
            );
            if chunk.unknown_before > 0 {
                warn!(
                    worker_url = %worker_url,
                    rank,
                    unknown_before = chunk.unknown_before,
                    "KV event relay snapshot starts late: the engine's blocks from before the \
                     relay's record are unknown; the worker's index is partial until they leave \
                     the engine (rank degraded)"
                );
            }
            Self::apply_cleared(worker_id, indexer, &mut state.index);
            for cursor in state.ranks.values_mut() {
                cursor.reset();
            }
            Metrics::record_kv_event_resync(worker_url, ResyncReason::Snapshot.as_str());
            state.snapshot = Some(SnapshotProgress {
                count: chunk.count.max(1),
                applied: 0,
                blocks: chunk.blocks,
            });
        }
        let cursor = state.ranks.entry(rank).or_default();
        cursor.resync_to(batch.sequence_number);
        if chunk.unknown_before > 0 {
            cursor.mark_degraded();
        }
        on_batch(batch);
        for event in &batch.events {
            Self::apply_event(event, worker_id, indexer, &mut state.index);
        }
        Self::record_lag(worker_url, batch.timestamp);
        Metrics::record_kv_event_batch(worker_url, "snapshot");
        if let Some(progress) = &mut state.snapshot {
            progress.applied += 1;
            if progress.applied >= progress.count {
                info!(
                    worker_url = %worker_url,
                    chunks = progress.count,
                    blocks = progress.blocks,
                    through = batch.sequence_number,
                    "KV event relay snapshot applied; continuing with live events"
                );
                state.snapshot = None;
                Metrics::set_kv_event_degraded_ranks(worker_url, state.degraded_ranks());
            }
        }
        BatchOutcome::Applied
    }

    /// One rank's publisher lost its history (a restart, or an unreplayable
    /// gap too large to keep): the worker's pooled index state goes with it,
    /// and the other ranks' cursors start over so their next batch is taken
    /// as a first one instead of clearing the index a second time.
    fn clear_worker(worker_id: u32, indexer: &KvIndex, state: &mut WorkerStreamState, rank: i32) {
        Self::apply_cleared(worker_id, indexer, &mut state.index);
        for (other, cursor) in &mut state.ranks {
            if *other != rank {
                cursor.reset();
            }
        }
    }

    /// The server declared its history gone: drop the worker's index state
    /// and every cursor, so the next stream is taken from wherever it starts.
    fn reset_worker(
        indexer: &KvIndex,
        worker_id: u32,
        state: &mut WorkerStreamState,
        worker_url: &str,
        reason: ResyncReason,
    ) {
        Self::apply_cleared(worker_id, indexer, &mut state.index);
        for cursor in state.ranks.values_mut() {
            cursor.reset();
        }
        state.snapshot = None;
        Metrics::record_kv_event_resync(worker_url, reason.as_str());
        Metrics::set_kv_event_degraded_ranks(worker_url, 0);
        Metrics::set_kv_index_blocks(worker_url, indexer.worker_block_count(worker_id));
    }

    /// Age of a batch when applied, from the publisher's wall-clock stamp.
    fn record_lag(worker_url: &str, published_at: f64) {
        if published_at <= 0.0 {
            return;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let lag = now - published_at;
        if lag.is_finite() && lag >= 0.0 {
            Metrics::record_kv_event_lag(worker_url, lag);
        }
    }

    /// Apply a single KV cache event to the indexer.
    pub(crate) fn apply_event(
        event: &KvCacheEvent,
        worker_id: u32,
        indexer: &KvIndex,
        worker_blocks: &mut WorkerIndexState,
    ) {
        let Some(ref data) = event.data else {
            return;
        };

        match data {
            kv_cache_event::Data::Stored(stored) => {
                Self::apply_stored(stored, worker_id, indexer, worker_blocks);
            }
            kv_cache_event::Data::Removed(removed) => {
                Self::apply_removed(removed, worker_id, indexer, worker_blocks);
            }
            kv_cache_event::Data::Cleared(cleared) => {
                if worker_blocks.admits(None, None, None, cleared.ownership.as_deref()) {
                    Self::apply_cleared(worker_id, indexer, worker_blocks);
                }
            }
        }
    }

    /// Convert proto `KvBlocksStored` and apply to the indexer.
    ///
    /// Blocks on the disk and external tiers, in cache groups other than main
    /// attention, not local to the worker, or owned by a residency agent are
    /// counted and skipped. Content hashes are computed under the event's
    /// LoRA name and cache salt, so a salted block matches only a request
    /// hashed under the same namespace.
    fn apply_stored(
        stored: &KvBlocksStored,
        worker_id: u32,
        indexer: &KvIndex,
        worker_blocks: &mut WorkerIndexState,
    ) {
        if !worker_blocks.admits(
            stored.kv_cache_spec_kind.as_deref(),
            stored.group_idx,
            stored.locality,
            stored.ownership.as_deref(),
        ) {
            return;
        }
        let first_level = stored.blocks.first().and_then(|block| block.cache_level);
        let Some(tier) = indexed_tier(stored.tier, first_level) else {
            worker_blocks.counters.untracked_tier += 1;
            return;
        };

        let seed = namespace_seed(stored.lora_name.as_deref(), stored.cache_salt.as_deref());
        let blocks: Vec<StoredBlock> = stored
            .blocks
            .iter()
            .map(|block| convert_kv_block(block, seed))
            .collect();
        worker_blocks.note_stored(&blocks, tier);

        let parent_seq_hash = stored.parent_block_hash.map(SequenceHash::from);

        match indexer.apply_stored(
            worker_id,
            &blocks,
            parent_seq_hash,
            &mut worker_blocks.blocks,
        ) {
            Ok(()) => {}
            Err(ApplyError::WorkerNotTracked | ApplyError::ParentBlockNotFound) => {
                // Cold start or parent evicted — retry without parent to start a new chain.
                if let Err(e) =
                    indexer.apply_stored(worker_id, &blocks, None, &mut worker_blocks.blocks)
                {
                    warn!(
                        worker_id = worker_id,
                        error = %e,
                        "Failed to apply stored event after fallback"
                    );
                }
            }
        }
    }

    /// Convert proto `KvBlocksRemoved` and apply to the indexer.
    ///
    /// A removal names one tier; a block leaves the index only when no
    /// indexed copy remains on another tier.
    fn apply_removed(
        removed: &KvBlocksRemoved,
        worker_id: u32,
        indexer: &KvIndex,
        worker_blocks: &mut WorkerIndexState,
    ) {
        if !worker_blocks.admits(
            None,
            removed.group_idx,
            removed.locality,
            removed.ownership.as_deref(),
        ) {
            return;
        }
        let Some(tier) = indexed_tier(removed.tier, removed.cache_level) else {
            worker_blocks.counters.untracked_tier += 1;
            return;
        };

        let hashes = removed.block_hashes.iter().map(|&h| SequenceHash::from(h));
        let seq_hashes: Vec<SequenceHash> =
            if tier == IndexedTier::Device && worker_blocks.copies.is_empty() {
                hashes.collect()
            } else {
                hashes
                    .filter(|&seq_hash| worker_blocks.release(seq_hash, tier))
                    .collect()
            };

        indexer.apply_removed(worker_id, &seq_hashes, &mut worker_blocks.blocks);
    }

    /// Drop every block of a worker from the indexer and forget its copies.
    fn apply_cleared(worker_id: u32, indexer: &KvIndex, worker_blocks: &mut WorkerIndexState) {
        indexer.apply_cleared(worker_id, &mut worker_blocks.blocks);
        worker_blocks.copies.clear();
    }
}

/// Convert a proto `KvBlock` to a kv-index `StoredBlock`, hashing its tokens
/// under `seed` (see [`namespace_seed`]).
fn convert_kv_block(block: &KvBlock, seed: u64) -> StoredBlock {
    StoredBlock {
        seq_hash: SequenceHash::from(block.block_hash),
        content_hash: content_hash_with_seed(&block.token_ids, seed),
    }
}

/// The residency tiers the index tracks: the device, and the host cache the
/// engine restores from without recompute. Disk and external copies are not
/// indexed; a hit there costs an engine-side fetch the router cannot price.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexedTier {
    Device,
    Host,
}

/// The tier an event names: its `tier` when set, else a block's
/// `cache_level` (absent means the device). `None` when the index does not
/// track that tier.
fn indexed_tier(tier: Option<i32>, cache_level: Option<i32>) -> Option<IndexedTier> {
    let tier = match tier.and_then(|tier| KvCacheTier::try_from(tier).ok()) {
        Some(KvCacheTier::Unspecified) | None => match cache_level.unwrap_or(0) {
            0 => KvCacheTier::Device,
            1 => KvCacheTier::Host,
            2 => KvCacheTier::Disk,
            _ => KvCacheTier::External,
        },
        Some(tier) => tier,
    };
    match tier {
        KvCacheTier::Unspecified | KvCacheTier::Device => Some(IndexedTier::Device),
        KvCacheTier::Host => Some(IndexedTier::Host),
        KvCacheTier::Disk | KvCacheTier::External => None,
    }
}

/// Cache-group kinds whose blocks hold the main attention KV, the ones
/// prefix matching is about. Sliding-window and Mamba groups are skipped.
const MAIN_ATTENTION_KINDS: [&str; 3] = ["full_attention", "mla_attention", "sink_full_attention"];

/// The most physical copies of one block counted per tier. vLLM's opt-in
/// `kv_cache_report_mode: full` re-announces whole hit chains without
/// removals, which would grow a count without bound; the cap turns that into
/// at most this many extra removals before the block leaves the index.
const COPIES_CAP: u8 = 8;

/// What the positional index did not take at face value, by reason; logged
/// when the worker's subscription ends.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerIndexCounters {
    /// Stores and removals on tiers the index does not track.
    pub(crate) untracked_tier: u64,
    /// Events for cache groups other than main attention.
    pub(crate) non_main_group: u64,
    /// Events for blocks not local to the worker.
    pub(crate) remote: u64,
    /// Events owned by a residency agent rather than the engine.
    pub(crate) foreign_owner: u64,
    /// Removals on a tier that held no counted copy of the block.
    pub(crate) unknown_copy: u64,
    /// Stores of a block already indexed on that tier (a second physical copy).
    pub(crate) duplicate_copies: u64,
    /// Copy counts that hit [`COPIES_CAP`].
    pub(crate) capped_copies: u64,
}

/// Physical copies of one block per tier, all of the worker's ranks pooled:
/// the worker URL is the routing target and its copies are interchangeable
/// for a prefix hit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Copies {
    device: u8,
    host: u8,
}

impl Copies {
    fn on(&mut self, tier: IndexedTier) -> &mut u8 {
        match tier {
            IndexedTier::Device => &mut self.device,
            IndexedTier::Host => &mut self.host,
        }
    }

    fn none(self) -> bool {
        self.device == 0 && self.host == 0
    }
}

/// A worker's share of the positional index: the indexer's reverse map plus
/// the physical copies of each block per tier, once the worker reports more
/// than one copy or a tier other than the device.
///
/// The engines do not deduplicate physical blocks: vLLM recomputes the last
/// block of an exact resend into a second copy with the same hash and emits
/// `BlockRemoved` per copy, and SGLang's HiCache keeps a host copy next to
/// the device one. The relay forwards every store and removal, so this state
/// counts copies and lets a removal evict the block only when none remains.
///
/// Counting is sparse. A block gets an entry in `copies` only on a host
/// store or on a second store of an indexed hash; an indexed block without
/// an entry is a single device copy. A worker that never duplicates and
/// never offloads pays one lookup per stored block and no memory.
#[derive(Default)]
pub(crate) struct WorkerIndexState {
    /// The indexer's caller-owned reverse map for this worker.
    pub(crate) blocks: WorkerBlocks,
    /// Copies per tier of blocks with a host copy or more than one copy.
    copies: HashMap<SequenceHash, Copies>,
    /// Cache groups whose kind is not main attention.
    non_main_groups: Vec<u32>,
    pub(crate) counters: WorkerIndexCounters,
}

impl WorkerIndexState {
    /// Whether an event with these attributes belongs in the index. A store
    /// names its group's kind; later events for that group may omit it, so
    /// non-main groups are remembered.
    fn admits(
        &mut self,
        kind: Option<&str>,
        group_idx: Option<u32>,
        locality: Option<i32>,
        ownership: Option<&str>,
    ) -> bool {
        if ownership.is_some_and(|owner| owner.eq_ignore_ascii_case("kvcr")) {
            self.counters.foreign_owner += 1;
            return false;
        }
        if locality.is_some_and(|locality| locality == KvCacheLocality::Remote as i32) {
            self.counters.remote += 1;
            return false;
        }
        let main = match kind {
            Some(kind) => {
                let main = MAIN_ATTENTION_KINDS.contains(&kind);
                if let Some(group) = group_idx {
                    if main {
                        self.non_main_groups.retain(|&known| known != group);
                    } else if !self.non_main_groups.contains(&group) {
                        self.non_main_groups.push(group);
                    }
                }
                main
            }
            None => !group_idx.is_some_and(|group| self.non_main_groups.contains(&group)),
        };
        if !main {
            self.counters.non_main_group += 1;
        }
        main
    }

    /// Count a store on `tier`, before the indexer applies it. A second copy
    /// of an indexed block, or any host copy, opens the block's entry; the
    /// implicit single device copy is credited when it does.
    fn note_stored(&mut self, blocks: &[StoredBlock], tier: IndexedTier) {
        for block in blocks {
            let indexed = self.blocks.contains_key(block.seq_hash);
            let entry = match self.copies.entry(block.seq_hash) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(vacant) => match tier {
                    IndexedTier::Device if !indexed => continue,
                    _ => vacant.insert(Copies {
                        device: u8::from(indexed),
                        host: 0,
                    }),
                },
            };
            let count = entry.on(tier);
            if *count > 0 {
                self.counters.duplicate_copies += 1;
            }
            if *count < COPIES_CAP {
                *count += 1;
            } else {
                self.counters.capped_copies += 1;
            }
        }
    }

    /// Drop one copy of a block on `tier`; `true` when no counted copy
    /// remains and the block should leave the index.
    fn release(&mut self, seq_hash: SequenceHash, tier: IndexedTier) -> bool {
        match self.copies.entry(seq_hash) {
            Entry::Occupied(mut entry) => {
                let count = entry.get_mut().on(tier);
                if *count == 0 {
                    self.counters.unknown_copy += 1;
                    return false;
                }
                *count -= 1;
                if entry.get().none() {
                    entry.remove();
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => match tier {
                IndexedTier::Device => true,
                IndexedTier::Host => {
                    self.counters.unknown_copy += 1;
                    false
                }
            },
        }
    }
}

impl Drop for KvEventMonitor {
    fn drop(&mut self) {
        if let Ok(mut handles) = self.worker_handles.try_lock() {
            for (_, sub) in handles.drain() {
                let _ = sub.shutdown_tx.send(());
                sub.handle.abort(); // Can't await in Drop, abort as fallback
            }
        }
    }
}

impl fmt::Debug for KvEventMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvEventMonitor")
            .field("models", &self.indexers.len())
            .field("block_sizes", &self.block_sizes.len())
            .field("kind", &self.kind)
            .field("jump_size", &self.jump_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use kv_index::{
        compute_content_hash, compute_request_content_hashes,
        salt::namespaced_request_content_hashes, ContentHash, XXH3_SEED,
    };
    use smg_grpc_client::common_proto::KvCacheCleared;

    use super::*;

    // -----------------------------------------------------------------------
    // Proto → kv-index conversion
    // -----------------------------------------------------------------------

    #[test]
    fn test_convert_kv_block() {
        let block = KvBlock {
            block_hash: 42,
            token_ids: vec![1, 2, 3, 4],
            block_size: 4,
            lora_id: None,
            cache_level: None,
            ..Default::default()
        };
        let stored = convert_kv_block(&block, XXH3_SEED);
        assert_eq!(stored.seq_hash, SequenceHash::from(42i64));
        assert_eq!(stored.content_hash, compute_content_hash(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_convert_kv_block_negative_hash() {
        let block = KvBlock {
            block_hash: -1,
            token_ids: vec![10, 20],
            block_size: 2,
            lora_id: None,
            cache_level: None,
            ..Default::default()
        };
        let stored = convert_kv_block(&block, XXH3_SEED);
        assert_eq!(stored.seq_hash, SequenceHash(u64::MAX));
    }

    #[test]
    fn test_convert_kv_block_empty_tokens() {
        let block = KvBlock {
            block_hash: 100,
            token_ids: vec![],
            block_size: 0,
            lora_id: None,
            cache_level: None,
            ..Default::default()
        };
        let stored = convert_kv_block(&block, XXH3_SEED);
        assert_eq!(stored.seq_hash, SequenceHash::from(100i64));
        assert_eq!(stored.content_hash, compute_content_hash(&[]));
    }

    // -----------------------------------------------------------------------
    // apply_event integration with the KV index
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_stored_no_parent() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let stored = KvBlocksStored {
            blocks: vec![
                KvBlock {
                    block_hash: 1,
                    token_ids: vec![10, 20, 30, 40],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                },
                KvBlock {
                    block_hash: 2,
                    token_ids: vec![50, 60, 70, 80],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                },
            ],
            parent_block_hash: None,
            ..Default::default()
        };

        KvEventMonitor::apply_stored(&stored, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 2);
    }

    #[test]
    fn test_apply_stored_with_parent() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let stored1 = KvBlocksStored {
            blocks: vec![KvBlock {
                block_hash: 1,
                token_ids: vec![10, 20, 30, 40],
                block_size: 4,
                lora_id: None,
                cache_level: None,
                ..Default::default()
            }],
            parent_block_hash: None,
            ..Default::default()
        };
        KvEventMonitor::apply_stored(&stored1, w1, &indexer, &mut wb);

        let stored2 = KvBlocksStored {
            blocks: vec![KvBlock {
                block_hash: 2,
                token_ids: vec![50, 60, 70, 80],
                block_size: 4,
                lora_id: None,
                cache_level: None,
                ..Default::default()
            }],
            parent_block_hash: Some(1),
            ..Default::default()
        };
        KvEventMonitor::apply_stored(&stored2, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 2);
    }

    #[test]
    fn test_apply_stored_fallback_on_worker_not_tracked() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://new-worker:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        // Pass parent_block_hash for an untracked worker — should fallback to no parent.
        let stored = KvBlocksStored {
            blocks: vec![KvBlock {
                block_hash: 1,
                token_ids: vec![10, 20, 30, 40],
                block_size: 4,
                lora_id: None,
                cache_level: None,
                ..Default::default()
            }],
            parent_block_hash: Some(999),
            ..Default::default()
        };
        KvEventMonitor::apply_stored(&stored, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);
    }

    #[test]
    fn test_apply_removed() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let stored = KvBlocksStored {
            blocks: vec![
                KvBlock {
                    block_hash: 1,
                    token_ids: vec![10, 20, 30, 40],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                },
                KvBlock {
                    block_hash: 2,
                    token_ids: vec![50, 60, 70, 80],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                },
            ],
            parent_block_hash: None,
            ..Default::default()
        };
        KvEventMonitor::apply_stored(&stored, w1, &indexer, &mut wb);

        let removed = KvBlocksRemoved {
            block_hashes: vec![2],
            cache_level: None,
            ..Default::default()
        };
        KvEventMonitor::apply_removed(&removed, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);
    }

    #[test]
    fn test_apply_cleared_event() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let stored = KvBlocksStored {
            blocks: vec![KvBlock {
                block_hash: 1,
                token_ids: vec![10, 20, 30, 40],
                block_size: 4,
                lora_id: None,
                cache_level: None,
                ..Default::default()
            }],
            parent_block_hash: None,
            ..Default::default()
        };
        KvEventMonitor::apply_stored(&stored, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);

        KvEventMonitor::apply_cleared(w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);
    }

    #[test]
    fn test_apply_event_dispatch_stored() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let event = KvCacheEvent {
            event_id: 1,
            data: Some(kv_cache_event::Data::Stored(KvBlocksStored {
                blocks: vec![KvBlock {
                    block_hash: 42,
                    token_ids: vec![1, 2, 3, 4],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                }],
                parent_block_hash: None,
                ..Default::default()
            })),
        };

        KvEventMonitor::apply_event(&event, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);
    }

    #[test]
    fn test_apply_event_dispatch_removed() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let stored_event = KvCacheEvent {
            event_id: 1,
            data: Some(kv_cache_event::Data::Stored(KvBlocksStored {
                blocks: vec![KvBlock {
                    block_hash: 1,
                    token_ids: vec![1, 2, 3, 4],
                    block_size: 4,
                    lora_id: None,
                    cache_level: None,
                    ..Default::default()
                }],
                parent_block_hash: None,
                ..Default::default()
            })),
        };
        KvEventMonitor::apply_event(&stored_event, w1, &indexer, &mut wb);

        let removed_event = KvCacheEvent {
            event_id: 2,
            data: Some(kv_cache_event::Data::Removed(KvBlocksRemoved {
                block_hashes: vec![1],
                cache_level: None,
                ..Default::default()
            })),
        };
        KvEventMonitor::apply_event(&removed_event, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);
    }

    #[test]
    fn test_apply_event_dispatch_cleared() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        KvEventMonitor::apply_event(
            &KvCacheEvent {
                event_id: 1,
                data: Some(kv_cache_event::Data::Stored(KvBlocksStored {
                    blocks: vec![KvBlock {
                        block_hash: 1,
                        token_ids: vec![1, 2, 3, 4],
                        block_size: 4,
                        lora_id: None,
                        cache_level: None,
                        ..Default::default()
                    }],
                    parent_block_hash: None,
                    ..Default::default()
                })),
            },
            w1,
            &indexer,
            &mut wb,
        );

        // Clear
        KvEventMonitor::apply_event(
            &KvCacheEvent {
                event_id: 2,
                data: Some(kv_cache_event::Data::Cleared(KvCacheCleared::default())),
            },
            w1,
            &indexer,
            &mut wb,
        );
        assert_eq!(indexer.current_size(), 0);
    }

    #[test]
    fn test_apply_event_no_data() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let event = KvCacheEvent {
            event_id: 1,
            data: None,
        };
        KvEventMonitor::apply_event(&event, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_monitor_new() {
        let monitor = KvEventMonitor::new(None);
        assert!(!monitor.is_running().await);
    }

    #[tokio::test]
    async fn test_monitor_new_clamps_zero_jump_size() {
        let monitor = KvEventMonitor::new(Some(0));
        assert_eq!(monitor.jump_size, 1);
    }

    #[tokio::test]
    async fn test_get_indexer_nonexistent() {
        let monitor = KvEventMonitor::new(None);
        assert!(monitor.get_indexer("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_stop_empty_monitor() {
        let monitor = KvEventMonitor::new(None);
        monitor.stop().await;
    }

    #[tokio::test]
    async fn test_on_worker_removed_nonexistent() {
        let monitor = KvEventMonitor::new(None);
        monitor.on_worker_removed("http://nonexistent:8000").await;
    }

    #[tokio::test]
    async fn test_remove_indexer_worker_runs_cleanup_off_runtime() {
        let indexer = Arc::new(KvIndex::positional(64));
        let worker_id = indexer.intern_worker("http://w1:8000").unwrap();
        let mut worker_blocks = WorkerIndexState::default();
        indexer
            .apply_stored(
                worker_id,
                &[StoredBlock {
                    seq_hash: SequenceHash(1),
                    content_hash: compute_content_hash(&[1, 2, 3]),
                }],
                None,
                &mut worker_blocks.blocks,
            )
            .unwrap();

        KvEventMonitor::remove_indexer_worker(
            Arc::clone(&indexer),
            worker_id,
            "grpc://w1:9000",
            worker_blocks,
        )
        .await;

        assert_eq!(indexer.current_size(), 0);
    }

    // -----------------------------------------------------------------------
    // block_size learning
    // -----------------------------------------------------------------------

    #[test]
    fn test_set_block_size() {
        let monitor = KvEventMonitor::new(None);

        // Initially no block_size
        assert!(monitor.block_size("llama").is_none());

        // Set it
        monitor.set_block_size("llama", 32);
        assert_eq!(monitor.block_size("llama"), Some(32));

        // set_block_size doesn't overwrite existing value
        monitor.set_block_size("llama", 64);
        assert_eq!(monitor.block_size("llama"), Some(32));
    }

    #[tokio::test]
    async fn test_stop_clears_block_sizes() {
        let monitor = KvEventMonitor::new(None);
        monitor.set_block_size("llama", 16);
        assert_eq!(monitor.block_size("llama"), Some(16));

        monitor.stop().await;
        assert!(monitor.block_size("llama").is_none());
    }

    #[test]
    fn test_prune_all_enforces_capacity_ceiling() {
        let monitor = KvEventMonitor::new(None);
        let indexer = monitor
            .indexers
            .entry("llama".to_string())
            .or_insert_with(|| Arc::new(KvIndex::positional(64)))
            .clone();

        let worker = indexer.intern_worker("http://w1:8000").unwrap();
        let mut worker_blocks = WorkerBlocks::default();
        // Ten independent single-block chains → ten index entries.
        for i in 0u64..10 {
            let block = StoredBlock {
                seq_hash: SequenceHash(1000 + i),
                content_hash: ContentHash(2000 + i),
            };
            indexer
                .apply_stored(worker, &[block], None, &mut worker_blocks)
                .unwrap();
        }
        assert_eq!(indexer.entry_count(), 10);

        // Disabled bounds → no-op.
        monitor.prune_all(0, 0);
        assert_eq!(indexer.entry_count(), 10);

        // Freshly stored entries sit inside the capacity-eviction grace and
        // are spared — a prune must not race a store batch's accounting.
        monitor.prune_all(0, 5);
        assert_eq!(indexer.entry_count(), 10);

        // Age them past the grace (the indexer's test clock is crate-private
        // to kv_index, so this test uses the real clock) and the ceiling is
        // enforced down to the low-water mark (5 - 5/10 = 5).
        std::thread::sleep(Duration::from_secs(3));
        monitor.prune_all(0, 5);
        assert_eq!(indexer.entry_count(), 5);
    }

    #[tokio::test]
    async fn test_start_prune_task_noop_when_disabled() {
        let monitor = Arc::new(KvEventMonitor::new(None));
        monitor.start_prune_task(0, 0);
        assert!(monitor.prune_task.lock().is_none());

        monitor.start_prune_task(60, 0);
        assert!(monitor.prune_task.lock().is_some());
    }

    // -----------------------------------------------------------------------
    // Tiers, namespaces and cache groups
    // -----------------------------------------------------------------------

    const TOKENS: [u32; 4] = [10, 20, 30, 40];

    fn stored_event(hash: i64, tokens: &[u32]) -> KvBlocksStored {
        KvBlocksStored {
            blocks: vec![KvBlock {
                block_hash: hash,
                token_ids: tokens.to_vec(),
                block_size: tokens.len() as i32,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn removed_event(hash: i64) -> KvBlocksRemoved {
        KvBlocksRemoved {
            block_hashes: vec![hash],
            ..Default::default()
        }
    }

    fn routable(indexer: &KvIndex, worker: u32, hashes: &[ContentHash]) -> bool {
        indexer
            .find_matches(hashes, false)
            .scores
            .get(&worker)
            .is_some_and(|&depth| depth > 0)
    }

    #[test]
    fn salted_stores_match_only_their_namespace() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let mut stored = stored_event(1, &TOKENS);
        stored.lora_name = Some("adapter".to_string());
        stored.cache_salt = Some("tenant-a".to_string());
        KvEventMonitor::apply_stored(&stored, w1, &indexer, &mut wb);

        let same = namespaced_request_content_hashes(&TOKENS, 4, Some("adapter"), Some("tenant-a"));
        assert!(routable(&indexer, w1, &same));
        let plain = compute_request_content_hashes(&TOKENS, 4);
        assert!(!routable(&indexer, w1, &plain));
        let lora_only = namespaced_request_content_hashes(&TOKENS, 4, Some("adapter"), None);
        assert!(!routable(&indexer, w1, &lora_only));
    }

    #[test]
    fn device_removal_keeps_a_block_still_on_the_host() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        let mut on_host = stored_event(1, &TOKENS);
        on_host.tier = Some(KvCacheTier::Host as i32);
        KvEventMonitor::apply_stored(&on_host, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);

        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(
            routable(&indexer, w1, &hashes),
            "the host copy keeps the block routable"
        );

        let mut from_host = removed_event(1);
        from_host.tier = Some(KvCacheTier::Host as i32);
        KvEventMonitor::apply_removed(&from_host, w1, &indexer, &mut wb);
        assert!(!routable(&indexer, w1, &hashes));
        assert_eq!(indexer.current_size(), 0);
        assert!(wb.copies.is_empty());
    }

    #[test]
    fn cache_level_stands_in_for_the_tier() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        let mut on_host = stored_event(1, &TOKENS);
        on_host.blocks[0].cache_level = Some(1);
        KvEventMonitor::apply_stored(&on_host, w1, &indexer, &mut wb);
        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);

        let mut from_host = removed_event(1);
        from_host.cache_level = Some(1);
        KvEventMonitor::apply_removed(&from_host, w1, &indexer, &mut wb);
        assert!(
            routable(&indexer, w1, &hashes),
            "the device copy keeps the block routable"
        );

        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(!routable(&indexer, w1, &hashes));
    }

    #[test]
    fn host_removal_without_a_host_copy_evicts_nothing() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        let mut from_host = removed_event(1);
        from_host.tier = Some(KvCacheTier::Host as i32);
        KvEventMonitor::apply_removed(&from_host, w1, &indexer, &mut wb);
        assert!(routable(&indexer, w1, &hashes));
        assert_eq!(wb.counters.unknown_copy, 1);
    }

    #[test]
    fn disk_and_external_tiers_are_counted_not_indexed() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let mut on_disk = stored_event(1, &TOKENS);
        on_disk.tier = Some(KvCacheTier::Disk as i32);
        KvEventMonitor::apply_stored(&on_disk, w1, &indexer, &mut wb);
        let mut external = stored_event(2, &TOKENS);
        external.blocks[0].cache_level = Some(3);
        KvEventMonitor::apply_stored(&external, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        let mut from_disk = removed_event(1);
        from_disk.tier = Some(KvCacheTier::Disk as i32);
        KvEventMonitor::apply_removed(&from_disk, w1, &indexer, &mut wb);
        assert_eq!(
            indexer.current_size(),
            1,
            "a disk removal does not touch the device copy"
        );
        assert_eq!(wb.counters.untracked_tier, 3);
    }

    #[test]
    fn non_main_attention_groups_are_skipped_once_their_kind_is_known() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let mut sliding = stored_event(1, &TOKENS);
        sliding.group_idx = Some(1);
        sliding.kv_cache_spec_kind = Some("sliding_window".to_string());
        KvEventMonitor::apply_stored(&sliding, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);

        let mut full = stored_event(2, &TOKENS);
        full.group_idx = Some(0);
        full.kv_cache_spec_kind = Some("full_attention".to_string());
        KvEventMonitor::apply_stored(&full, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);

        // Later events for group 1 omit the kind; the group is remembered.
        let mut later = stored_event(3, &[50, 60, 70, 80]);
        later.group_idx = Some(1);
        KvEventMonitor::apply_stored(&later, w1, &indexer, &mut wb);
        let mut removal = removed_event(2);
        removal.group_idx = Some(1);
        KvEventMonitor::apply_removed(&removal, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 1);
        assert_eq!(wb.counters.non_main_group, 3);

        removal.group_idx = Some(0);
        KvEventMonitor::apply_removed(&removal, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);
    }

    #[test]
    fn remote_and_residency_agent_events_are_skipped() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();

        let mut remote = stored_event(1, &TOKENS);
        remote.locality = Some(KvCacheLocality::Remote as i32);
        KvEventMonitor::apply_stored(&remote, w1, &indexer, &mut wb);
        let mut agent = stored_event(1, &TOKENS);
        agent.ownership = Some("kvcr".to_string());
        KvEventMonitor::apply_stored(&agent, w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        let cleared = KvCacheEvent {
            event_id: 1,
            data: Some(kv_cache_event::Data::Cleared(KvCacheCleared {
                ownership: Some("kvcr".to_string()),
            })),
        };
        KvEventMonitor::apply_event(&cleared, w1, &indexer, &mut wb);
        assert_eq!(
            indexer.current_size(),
            1,
            "an agent's clear leaves the engine's blocks"
        );
        assert_eq!(wb.counters.remote, 1);
        assert_eq!(wb.counters.foreign_owner, 2);
    }

    #[test]
    fn clearing_forgets_copies_and_residency() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        let mut on_host = stored_event(1, &TOKENS);
        on_host.tier = Some(KvCacheTier::Host as i32);
        KvEventMonitor::apply_stored(&on_host, w1, &indexer, &mut wb);
        assert!(!wb.copies.is_empty());

        KvEventMonitor::apply_cleared(w1, &indexer, &mut wb);
        assert_eq!(indexer.current_size(), 0);
        assert!(wb.copies.is_empty());

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(
            !routable(&indexer, w1, &hashes),
            "no stale host bit survives a clear"
        );
    }

    #[test]
    fn two_copies_need_two_removals() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        // vLLM recomputes the last block of an exact resend into a second
        // physical copy with the same hash and removes the copies one by one.
        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        assert!(wb.copies.is_empty(), "a single device copy costs no entry");
        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        assert_eq!(wb.counters.duplicate_copies, 1);

        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(
            routable(&indexer, w1, &hashes),
            "the other copy is still cached"
        );
        assert_eq!(indexer.current_size(), 1);

        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(!routable(&indexer, w1, &hashes));
        assert_eq!(indexer.current_size(), 0);
        assert!(wb.copies.is_empty());
    }

    #[test]
    fn device_and_host_copies_are_counted_per_tier() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);
        let mut on_host = stored_event(1, &TOKENS);
        on_host.tier = Some(KvCacheTier::Host as i32);
        let mut from_host = removed_event(1);
        from_host.tier = Some(KvCacheTier::Host as i32);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_stored(&on_host, w1, &indexer, &mut wb);
        assert_eq!(
            wb.copies[&SequenceHash::from(1i64)],
            Copies { device: 2, host: 1 }
        );

        // A host removal consumes the host copy only.
        KvEventMonitor::apply_removed(&from_host, w1, &indexer, &mut wb);
        assert!(routable(&indexer, w1, &hashes));
        // A second host removal has nothing to take and evicts nothing.
        KvEventMonitor::apply_removed(&from_host, w1, &indexer, &mut wb);
        assert!(routable(&indexer, w1, &hashes));
        assert_eq!(wb.counters.unknown_copy, 1);

        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(routable(&indexer, w1, &hashes), "one device copy left");
        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(!routable(&indexer, w1, &hashes));
    }

    #[test]
    fn clearing_forgets_copy_counts() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_cleared(w1, &indexer, &mut wb);
        assert!(wb.copies.is_empty());

        KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(
            !routable(&indexer, w1, &hashes),
            "no count survives a clear"
        );
    }

    #[test]
    fn copy_counts_are_capped() {
        let indexer = KvIndex::positional(64);
        let w1 = indexer.intern_worker("http://w1:8000").unwrap();
        let mut wb = WorkerIndexState::default();
        let hashes = compute_request_content_hashes(&TOKENS, 4);

        // vLLM's `kv_cache_report_mode: full` re-announces a hit chain on
        // every lookup without removals; the count stops at the cap.
        for _ in 0..20 {
            KvEventMonitor::apply_stored(&stored_event(1, &TOKENS), w1, &indexer, &mut wb);
        }
        assert_eq!(wb.copies[&SequenceHash::from(1i64)].device, COPIES_CAP);
        assert_eq!(wb.counters.capped_copies, 20 - u64::from(COPIES_CAP));

        for _ in 1..COPIES_CAP {
            KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        }
        assert!(routable(&indexer, w1, &hashes));
        KvEventMonitor::apply_removed(&removed_event(1), w1, &indexer, &mut wb);
        assert!(
            !routable(&indexer, w1, &hashes),
            "the cap bounds the extra removals"
        );
    }

    // -----------------------------------------------------------------------
    // Recovery: synthetic streams against the reference indexer
    // -----------------------------------------------------------------------

    use std::collections::BTreeSet;

    use kv_index::ReferenceIndexer;

    use super::super::kv_event_recovery::{Cursor, RESTART_WINDOW};

    /// Token ids for engine block `id`: distinct content per id.
    fn tokens_for(id: i64) -> Vec<u32> {
        (0..4u32).map(|i| (id as u32) * 16 + i).collect()
    }

    fn kv_block(id: i64) -> KvBlock {
        KvBlock {
            block_hash: id,
            token_ids: tokens_for(id),
            block_size: 4,
            ..Default::default()
        }
    }

    fn stored(parent: Option<i64>, ids: &[i64]) -> KvCacheEvent {
        KvCacheEvent {
            event_id: 0,
            data: Some(kv_cache_event::Data::Stored(KvBlocksStored {
                blocks: ids.iter().map(|&id| kv_block(id)).collect(),
                parent_block_hash: parent,
                ..Default::default()
            })),
        }
    }

    fn removed(ids: &[i64]) -> KvCacheEvent {
        KvCacheEvent {
            event_id: 0,
            data: Some(kv_cache_event::Data::Removed(KvBlocksRemoved {
                block_hashes: ids.to_vec(),
                ..Default::default()
            })),
        }
    }

    fn cleared() -> KvCacheEvent {
        KvCacheEvent {
            event_id: 0,
            data: Some(kv_cache_event::Data::Cleared(KvCacheCleared::default())),
        }
    }

    fn batch(seq: u64, rank: Option<i32>, events: Vec<KvCacheEvent>) -> KvEventBatch {
        KvEventBatch {
            sequence_number: seq,
            timestamp: 0.0,
            events,
            dp_rank: rank,
            snapshot: None,
        }
    }

    /// `smg_kv_index_blocks{worker}` is set where applied batches are counted,
    /// from the index's own per-worker counter: it follows stores, removals and
    /// a clear, and costs the lookup path nothing.
    #[test]
    fn index_block_gauge_follows_stores_removals_and_a_clear() {
        use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

        fn index_blocks(handle: &PrometheusHandle) -> Option<f64> {
            handle
                .render()
                .lines()
                .find(|line| line.starts_with("smg_kv_index_blocks{worker=\"grpc://w1:9000\"}"))
                .and_then(|line| line.rsplit(' ').next())
                .and_then(|value| value.parse().ok())
        }

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let mut sim = Sim::new();
            assert_eq!(index_blocks(&handle), None, "nothing applied yet");

            assert_eq!(
                sim.feed(&batch(1, None, vec![stored(None, &[1, 2, 3])])),
                BatchOutcome::Applied
            );
            assert_eq!(sim.indexer.worker_block_count(sim.worker), 3);
            assert_eq!(index_blocks(&handle), Some(3.0), "three blocks stored");

            assert_eq!(
                sim.feed(&batch(2, None, vec![removed(&[3])])),
                BatchOutcome::Applied
            );
            assert_eq!(index_blocks(&handle), Some(2.0), "one removed");

            assert_eq!(
                sim.feed(&batch(3, None, vec![stored(Some(2), &[4, 5])])),
                BatchOutcome::Applied
            );
            assert_eq!(index_blocks(&handle), Some(4.0), "two more stored");

            assert_eq!(
                sim.feed(&batch(4, None, vec![cleared()])),
                BatchOutcome::Applied
            );
            assert_eq!(sim.indexer.worker_block_count(sim.worker), 0);
            assert_eq!(index_blocks(&handle), Some(0.0), "cleared");
        });
    }

    /// The production subscriber's per-worker state next to a reference that
    /// sees the stream the subscriber *should* have applied.
    struct Sim {
        indexer: KvIndex,
        worker: u32,
        state: WorkerStreamState,
        reference: ReferenceIndexer,
    }

    impl Sim {
        fn new() -> Self {
            let indexer = KvIndex::positional(8);
            let worker = indexer.intern_worker("grpc://w1:9000").unwrap();
            Self {
                indexer,
                worker,
                state: WorkerStreamState::default(),
                reference: ReferenceIndexer::new(),
            }
        }

        /// Feed a batch through the real admission path.
        fn feed(&mut self, b: &KvEventBatch) -> BatchOutcome {
            KvEventMonitor::admit_batch(
                b,
                "grpc://w1:9000",
                self.worker,
                &self.indexer,
                &mut self.state,
                &mut |_: &KvEventBatch| {},
            )
        }

        /// Apply a batch to the reference with the subscriber's semantics
        /// (a store whose parent is unknown starts a new chain).
        fn reference_apply(&mut self, b: &KvEventBatch) {
            let seed = namespace_seed(None, None);
            for event in &b.events {
                match event.data.as_ref().unwrap() {
                    kv_cache_event::Data::Stored(st) => {
                        let blocks: Vec<StoredBlock> = st
                            .blocks
                            .iter()
                            .map(|block| convert_kv_block(block, seed))
                            .collect();
                        let parent = st.parent_block_hash.map(SequenceHash::from);
                        if self
                            .reference
                            .apply_stored(self.worker, &blocks, parent)
                            .is_err()
                        {
                            self.reference
                                .apply_stored(self.worker, &blocks, None)
                                .unwrap();
                        }
                    }
                    kv_cache_event::Data::Removed(rm) => {
                        let hashes: Vec<SequenceHash> = rm
                            .block_hashes
                            .iter()
                            .map(|&h| SequenceHash::from(h))
                            .collect();
                        self.reference.apply_removed(self.worker, &hashes);
                    }
                    kv_cache_event::Data::Cleared(_) => self.reference.apply_cleared(self.worker),
                }
            }
        }

        /// Feed to both: what a correctly delivered batch does.
        fn deliver(&mut self, b: &KvEventBatch) -> BatchOutcome {
            self.reference_apply(b);
            self.feed(b)
        }

        /// Apply a batch straight into the worker's index state, bypassing
        /// admission (how a snapshot or a buffered tail lands).
        fn apply_direct(&mut self, b: &KvEventBatch) {
            for event in &b.events {
                KvEventMonitor::apply_event(
                    event,
                    self.worker,
                    &self.indexer,
                    &mut self.state.index,
                );
            }
        }

        fn assert_matches_reference(&self) {
            let production: BTreeSet<(u32, usize, ContentHash, SequenceHash)> =
                self.indexer.debug_blocks().into_iter().collect();
            assert_eq!(production, self.reference.blocks(), "index content");
            for query in self.queries() {
                let scores = self.indexer.find_matches(&query, false).scores;
                let expected = self.reference.find_matches(&query);
                let got: Vec<(u32, u32)> = scores.into_iter().collect();
                let want: Vec<(u32, u32)> = expected.into_iter().filter(|(_, s)| *s > 0).collect();
                assert_eq!(got, want, "scores for {query:?}");
            }
        }

        /// Lookups: every stored chain, plus a mutated copy of each.
        fn queries(&self) -> Vec<Vec<ContentHash>> {
            let mut chains: Vec<Vec<ContentHash>> = Vec::new();
            let blocks = self.reference.blocks();
            let max_pos = blocks.iter().map(|b| b.1).max().unwrap_or(0);
            // Rebuild chains by walking positions from the reference content.
            let mut by_pos: Vec<Vec<ContentHash>> = vec![Vec::new(); max_pos + 1];
            for (_, pos, content, _) in &blocks {
                if !by_pos[*pos].contains(content) {
                    by_pos[*pos].push(*content);
                }
            }
            let mut chain = Vec::new();
            for level in &by_pos {
                if let Some(c) = level.first() {
                    chain.push(*c);
                    chains.push(chain.clone());
                }
            }
            if let Some(full) = chains.last().cloned() {
                let mut mutated = full.clone();
                if mutated.len() > 1 {
                    mutated[1] = ContentHash(0xDEAD_BEEF);
                    chains.push(mutated);
                }
            }
            chains.push(vec![ContentHash(1), ContentHash(2)]);
            chains
        }
    }

    #[test]
    fn recovery_in_order_stream_matches_reference() {
        let mut sim = Sim::new();
        assert_eq!(
            sim.deliver(&batch(1, None, vec![stored(None, &[1, 2, 3])])),
            BatchOutcome::Applied
        );
        assert_eq!(
            sim.deliver(&batch(2, None, vec![stored(Some(3), &[4, 5])])),
            BatchOutcome::Applied
        );
        assert_eq!(
            sim.deliver(&batch(3, None, vec![removed(&[5])])),
            BatchOutcome::Applied
        );
        assert_eq!(
            sim.deliver(&batch(4, None, vec![stored(Some(4), &[6])])),
            BatchOutcome::Applied
        );
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_stream_numbered_from_zero_matches_reference() {
        // vLLM and SGLang publishers count from 0; 0 must not be treated as
        // "no cursor" once a batch carried it.
        let mut sim = Sim::new();
        assert_eq!(
            sim.deliver(&batch(0, None, vec![cleared(), stored(None, &[1, 2])])),
            BatchOutcome::Applied
        );
        assert_eq!(
            sim.deliver(&batch(1, None, vec![stored(Some(2), &[3])])),
            BatchOutcome::Applied
        );
        assert_eq!(
            sim.feed(&batch(1, None, vec![removed(&[1])])),
            BatchOutcome::Skipped
        );
        assert_eq!(sim.state.resume_sequence(), 1);
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_gap_filled_by_replay_matches_reference() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        // Batch 3 is lost on the wire; 4 arrives: one replay is asked for.
        let b3 = batch(3, None, vec![removed(&[3])]);
        let b4 = batch(4, None, vec![stored(Some(2), &[7])]);
        assert_eq!(
            sim.feed(&b4),
            BatchOutcome::Gap {
                expected: 3,
                received: 4
            }
        );
        assert_eq!(sim.state.resume_sequence(), 2);
        // Resuming after 2, the server replays 3 and continues live.
        assert_eq!(sim.deliver(&b3), BatchOutcome::Applied);
        assert_eq!(sim.deliver(&b4), BatchOutcome::Applied);
        assert_eq!(
            sim.deliver(&batch(5, None, vec![stored(Some(7), &[8])])),
            BatchOutcome::Applied
        );
        assert!(!sim.state.ranks[&0].is_degraded());
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_gap_without_replay_keeps_state_and_continues() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        // Batches 3 and 4 are lost; the server has no history (the Rust
        // relay): after the replay request it streams 5 again.
        let b5 = batch(5, None, vec![stored(Some(3), &[9])]);
        assert_eq!(
            sim.feed(&b5),
            BatchOutcome::Gap {
                expected: 3,
                received: 5
            }
        );
        assert_eq!(sim.deliver(&b5), BatchOutcome::Applied);
        assert!(sim.state.ranks[&0].is_degraded());
        // Everything after keeps applying: the old code reconnected forever.
        assert_eq!(
            sim.deliver(&batch(6, None, vec![stored(Some(9), &[10])])),
            BatchOutcome::Applied
        );
        // The reference saw the same stream minus the lost batches.
        sim.assert_matches_reference();
        // A later gap starts a fresh single replay attempt.
        assert_eq!(
            sim.feed(&batch(8, None, vec![])),
            BatchOutcome::Gap {
                expected: 7,
                received: 8
            }
        );
    }

    #[test]
    fn recovery_duplicates_and_out_of_order_batches_are_skipped() {
        let mut sim = Sim::new();
        let b1 = batch(1, None, vec![stored(None, &[1, 2])]);
        let b2 = batch(2, None, vec![stored(Some(2), &[3])]);
        let b3 = batch(3, None, vec![removed(&[3])]);
        let b4 = batch(4, None, vec![stored(Some(2), &[4])]);
        sim.deliver(&b1);
        sim.deliver(&b2);
        sim.deliver(&b3);
        // Replayed overlap after a reconnect: already applied, must not re-apply.
        // (A duplicate of sequence 1 would be taken as a new publisher: see
        // `a_counter_at_its_start_below_the_cursor_is_a_restart`.)
        assert_eq!(sim.feed(&b2), BatchOutcome::Skipped);
        assert_eq!(sim.feed(&b3), BatchOutcome::Skipped);
        sim.deliver(&b4);
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_publisher_restart_clears_the_rank() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2, 3])]));
        for seq in 2..=(RESTART_WINDOW + 5) {
            sim.deliver(&batch(seq, None, vec![]));
        }
        // The engine restarts: its cache is empty and it counts from 0 again.
        let fresh = batch(0, None, vec![stored(None, &[21, 22])]);
        sim.reference.apply_cleared(sim.worker);
        sim.reference_apply(&fresh);
        assert_eq!(sim.feed(&fresh), BatchOutcome::Applied);
        assert_eq!(
            sim.deliver(&batch(1, None, vec![stored(Some(22), &[23])])),
            BatchOutcome::Applied
        );
        assert!(!sim.state.ranks[&0].is_degraded());
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_restart_seen_on_a_fresh_connection_clears_the_rank() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        // The worker died and came back: the stream reconnected and the new
        // publisher counts from 1 with an empty cache (the relay and the mock
        // engine stream live; neither can replay).
        sim.state.ranks.get_mut(&0).unwrap().reconnected();
        let fresh = batch(1, None, vec![stored(None, &[7])]);
        sim.reference.apply_cleared(sim.worker);
        sim.reference_apply(&fresh);
        assert_eq!(sim.feed(&fresh), BatchOutcome::Applied);
        sim.assert_matches_reference();
        assert_eq!(sim.state.resume_sequence(), 1);
    }

    #[test]
    fn recovery_clear_below_the_cursor_is_a_restart() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        // SGLang's first batch after a restart carries AllBlocksCleared and
        // its counter starts over, with the servicer's stream still up.
        let first = batch(0, None, vec![cleared(), stored(None, &[9])]);
        sim.reference.apply_cleared(sim.worker);
        sim.reference_apply(&first);
        assert_eq!(sim.feed(&first), BatchOutcome::Applied);
        sim.assert_matches_reference();
        // A residency agent's clear below the cursor is just a duplicate.
        let agent = KvCacheEvent {
            event_id: 0,
            data: Some(kv_cache_event::Data::Cleared(KvCacheCleared {
                ownership: Some("kvcr".to_string()),
            })),
        };
        assert_eq!(
            sim.feed(&batch(0, None, vec![agent])),
            BatchOutcome::Skipped
        );
        sim.assert_matches_reference();
    }

    #[test]
    fn recovery_cleared_event_matches_reference() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2, 3])]));
        sim.deliver(&batch(2, None, vec![cleared()]));
        sim.deliver(&batch(3, None, vec![stored(None, &[4])]));
        sim.assert_matches_reference();
        assert_eq!(sim.reference.worker_block_count(sim.worker), 1);
    }

    #[test]
    fn recovery_dp_ranks_keep_independent_cursors() {
        let mut sim = Sim::new();
        // Two publishers, each numbering from 1, interleaved on one stream.
        sim.deliver(&batch(1, Some(0), vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(1, Some(1), vec![stored(None, &[101, 102])]));
        sim.deliver(&batch(2, Some(1), vec![stored(Some(102), &[103])]));
        sim.deliver(&batch(2, Some(0), vec![stored(Some(2), &[3])]));
        assert_eq!(
            sim.deliver(&batch(3, Some(0), vec![removed(&[3])])),
            BatchOutcome::Applied
        );
        // Each rank dedups against its own cursor.
        assert_eq!(
            sim.feed(&batch(2, Some(1), vec![stored(None, &[200])])),
            BatchOutcome::Skipped
        );
        assert_eq!(
            sim.deliver(&batch(3, Some(1), vec![stored(Some(103), &[104])])),
            BatchOutcome::Applied
        );
        assert_eq!(sim.state.ranks.len(), 2);
        assert_eq!(sim.state.degraded_ranks(), 0);
        assert_eq!(sim.state.resume_sequence(), 3);
        sim.assert_matches_reference();
        // Rank 1's publisher restarts: the worker's pooled index state is
        // cleared and rank 0's cursor starts over, so its next batch is taken
        // as a first one rather than clearing the index again.
        sim.state.reconnected();
        let fresh = batch(1, Some(1), vec![stored(None, &[111])]);
        sim.reference.apply_cleared(sim.worker);
        sim.reference_apply(&fresh);
        assert_eq!(sim.feed(&fresh), BatchOutcome::Applied);
        assert_eq!(
            sim.deliver(&batch(4, Some(0), vec![stored(None, &[5])])),
            BatchOutcome::Applied
        );
        sim.assert_matches_reference();
        assert_eq!(sim.state.resume_sequence(), 4);
    }

    #[test]
    fn recovery_resync_forgets_copy_counts() {
        // Copy counts live in the pooled index state and cursors in the rank
        // state; a restart clears the counts with the blocks, so no stale
        // host copy keeps a block routable afterwards.
        let mut sim = Sim::new();
        let mut on_host = stored(None, &[1, 2]);
        if let Some(kv_cache_event::Data::Stored(st)) = on_host.data.as_mut() {
            st.tier = Some(KvCacheTier::Host as i32);
        }
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![on_host]));
        sim.state.reconnected();
        let fresh = batch(1, None, vec![stored(None, &[1, 2])]);
        sim.reference.apply_cleared(sim.worker);
        sim.reference_apply(&fresh);
        assert_eq!(sim.feed(&fresh), BatchOutcome::Applied);
        assert_eq!(
            sim.deliver(&batch(2, None, vec![removed(&[2])])),
            BatchOutcome::Applied
        );
        sim.assert_matches_reference();
        assert_eq!(sim.reference.worker_block_count(sim.worker), 1);
    }

    /// The chunks a relay sends for a live set, stamped up to `through`:
    /// chunk 0 begins with the clear, every chunk is marked.
    fn snapshot_chunks(
        through: u64,
        rank: Option<i32>,
        stores_per_chunk: &[Vec<KvCacheEvent>],
        blocks: u64,
    ) -> Vec<KvEventBatch> {
        let count = stores_per_chunk.len() as u32;
        stores_per_chunk
            .iter()
            .enumerate()
            .map(|(index, stores)| {
                let mut events = Vec::new();
                if index == 0 {
                    events.push(cleared());
                }
                events.extend(stores.iter().cloned());
                KvEventBatch {
                    sequence_number: through + 1 - u64::from(count) + index as u64,
                    timestamp: 0.0,
                    events,
                    dp_rank: rank,
                    snapshot: Some(KvSnapshotChunk {
                        index: index as u32,
                        count,
                        blocks,
                        unknown_before: 0,
                    }),
                }
            })
            .collect()
    }

    fn index_blocks(sim: &Sim) -> BTreeSet<(u32, usize, ContentHash, SequenceHash)> {
        sim.indexer.debug_blocks().into_iter().collect()
    }

    /// A gateway that starts after the relay's history rolled receives the
    /// live set as a snapshot: applied as one `snapshot` resync, it leaves
    /// the index equal to the one that saw the whole stream (and to the
    /// reference), and the live stream continues from the cut with nothing
    /// skipped or repeated.
    #[test]
    fn a_relay_snapshot_is_applied_as_a_resync_and_rebuilds_the_live_set() {
        let mut seen = Sim::new();
        seen.deliver(&batch(1, None, vec![stored(None, &[1, 2, 3])]));
        seen.deliver(&batch(2, None, vec![stored(Some(3), &[4, 5])]));
        seen.deliver(&batch(3, None, vec![stored(None, &[10])]));
        seen.deliver(&batch(4, None, vec![removed(&[5, 10])]));
        seen.deliver(&batch(5, None, vec![stored(Some(2), &[6])]));
        seen.assert_matches_reference();

        // Live set at the cut (sequence 5): 1, 2, 3, 4 and the branch 6.
        let chunks = snapshot_chunks(
            5,
            None,
            &[
                vec![stored(None, &[1, 2, 3])],
                vec![stored(Some(3), &[4]), stored(Some(2), &[6])],
            ],
            5,
        );
        let mut fresh = Sim::new();
        for chunk in &chunks {
            assert_eq!(fresh.feed(chunk), BatchOutcome::Applied);
        }
        assert!(fresh.state.snapshot.is_none(), "both chunks arrived");
        assert_eq!(fresh.state.ranks[&0].cursor(), Cursor::Live(5));
        assert_eq!(fresh.state.resume_sequence(), 5);
        assert_eq!(
            index_blocks(&fresh),
            index_blocks(&seen),
            "index after the snapshot"
        );
        assert_eq!(
            index_blocks(&fresh),
            seen.reference.blocks(),
            "against the reference"
        );

        // The cut's own sequence is behind the cursor; the next one applies.
        assert_eq!(
            fresh.feed(&batch(5, None, vec![stored(None, &[99])])),
            BatchOutcome::Skipped
        );
        let live = batch(6, None, vec![stored(Some(6), &[7]), removed(&[4])]);
        assert_eq!(fresh.feed(&live), BatchOutcome::Applied);
        assert_eq!(seen.deliver(&live), BatchOutcome::Applied);
        seen.assert_matches_reference();
        assert_eq!(
            index_blocks(&fresh),
            index_blocks(&seen),
            "index after the live batch"
        );
        assert_eq!(seen.reference.worker_block_count(seen.worker), 5);
    }

    /// The snapshot lists a block once per physical copy the engine still
    /// holds, so the copy counts after it match a gateway that saw every
    /// store and removal: the same removals evict the same blocks.
    #[test]
    fn a_relay_snapshot_carries_the_copies_the_engine_still_holds() {
        let mut seen = Sim::new();
        seen.feed(&batch(1, None, vec![stored(None, &[1, 2])]));
        // A second copy of 1 and of 2, one copy of 1 removed since.
        seen.feed(&batch(
            2,
            None,
            vec![stored(None, &[1]), stored(Some(1), &[2])],
        ));
        seen.feed(&batch(3, None, vec![removed(&[1])]));
        let chunks = snapshot_chunks(
            3,
            None,
            &[vec![stored(None, &[1, 2]), stored(Some(1), &[2])]],
            3,
        );
        let mut fresh = Sim::new();
        assert_eq!(fresh.feed(&chunks[0]), BatchOutcome::Applied);
        assert_eq!(index_blocks(&fresh), index_blocks(&seen));
        for (seq, hashes) in [(4, &[1][..]), (5, &[2][..]), (6, &[2][..])] {
            let removal = batch(seq, None, vec![removed(hashes)]);
            assert_eq!(fresh.feed(&removal), BatchOutcome::Applied);
            assert_eq!(seen.feed(&removal), BatchOutcome::Applied);
            assert_eq!(index_blocks(&fresh), index_blocks(&seen), "after {seq}");
        }
        assert!(index_blocks(&fresh).is_empty(), "every copy is gone");
    }

    /// A snapshot chunk is a resync wherever the rank's cursor stands: the
    /// gap and duplicate rules do not apply to it, the worker's old blocks
    /// go, the cursor moves to the chunk's stamp.
    #[test]
    fn a_snapshot_replaces_whatever_the_rank_held_and_moves_its_cursor() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        assert_eq!(
            sim.feed(&batch(9, None, vec![stored(None, &[9])])),
            BatchOutcome::Gap {
                expected: 3,
                received: 9
            }
        );
        // The relay's answer to the resubscription: its window had rolled.
        let chunks = snapshot_chunks(40, None, &[vec![stored(None, &[7, 8])]], 2);
        assert_eq!(sim.feed(&chunks[0]), BatchOutcome::Applied);
        sim.reference_apply(&chunks[0]);
        sim.assert_matches_reference();
        assert_eq!(sim.state.ranks[&0].cursor(), Cursor::Live(40));
        assert!(sim.state.ranks[&0].replay_pending().is_none());
        assert!(!sim.state.ranks[&0].is_degraded());
        assert_eq!(sim.reference.worker_block_count(sim.worker), 2);
        let next = batch(41, None, vec![removed(&[7])]);
        assert_eq!(sim.deliver(&next), BatchOutcome::Applied);
        sim.assert_matches_reference();
        assert_eq!(sim.reference.worker_block_count(sim.worker), 1);
    }

    /// A snapshot whose relay joined the publisher late and could not replay
    /// the start leaves the rank degraded, as an unrecovered gap would; a
    /// whole one lifts it.
    #[test]
    fn a_snapshot_that_starts_late_marks_the_rank_degraded() {
        let mut sim = Sim::new();
        let mut late = snapshot_chunks(9, None, &[vec![stored(None, &[1])]], 1);
        late[0].snapshot.as_mut().unwrap().unknown_before = 40;
        assert_eq!(sim.feed(&late[0]), BatchOutcome::Applied);
        assert!(sim.state.ranks[&0].is_degraded());
        assert_eq!(sim.state.degraded_ranks(), 1);
        assert_eq!(
            sim.feed(&batch(10, None, vec![stored(Some(1), &[2])])),
            BatchOutcome::Applied
        );
        assert!(sim.state.ranks[&0].is_degraded(), "until the next resync");
        let whole = snapshot_chunks(12, None, &[vec![stored(None, &[1])]], 1);
        assert_eq!(sim.feed(&whole[0]), BatchOutcome::Applied);
        assert!(!sim.state.ranks[&0].is_degraded());
        assert_eq!(sim.state.degraded_ranks(), 0);
    }

    /// A stream that ends with chunks still owed leaves a partial live set:
    /// the cursors are forgotten so the next subscription asks from zero and
    /// gets a whole snapshot; a complete one leaves nothing to abandon.
    #[test]
    fn a_stream_that_ends_mid_snapshot_starts_the_next_subscription_from_zero() {
        let mut sim = Sim::new();
        let chunks = snapshot_chunks(
            10,
            None,
            &[vec![stored(None, &[1])], vec![stored(Some(1), &[2])]],
            2,
        );
        assert_eq!(sim.feed(&chunks[0]), BatchOutcome::Applied);
        assert_eq!(
            sim.state.snapshot,
            Some(SnapshotProgress {
                count: 2,
                applied: 1,
                blocks: 2
            })
        );
        assert_eq!(sim.state.resume_sequence(), 9);
        assert!(sim.state.abandon_snapshot());
        assert_eq!(sim.state.resume_sequence(), 0);
        assert!(!sim.state.abandon_snapshot());
        for chunk in &chunks {
            assert_eq!(sim.feed(chunk), BatchOutcome::Applied);
        }
        assert!(sim.state.snapshot.is_none());
        assert_eq!(sim.state.resume_sequence(), 10);
        assert!(!sim.state.abandon_snapshot());
    }

    #[test]
    fn recovery_snapshot_resync_applies_the_tail_in_order() {
        let mut sim = Sim::new();
        sim.deliver(&batch(1, None, vec![stored(None, &[1, 2])]));
        sim.deliver(&batch(2, None, vec![stored(Some(2), &[3])]));
        // Lost history: the subscriber asks for a snapshot out of band and
        // holds live batches meanwhile.
        sim.state.ranks.get_mut(&0).unwrap().begin_snapshot();
        let live3 = batch(3, None, vec![removed(&[3])]);
        let live4 = batch(4, None, vec![stored(Some(2), &[5])]);
        assert_eq!(sim.feed(&live3), BatchOutcome::Skipped);
        assert_eq!(sim.feed(&live4), BatchOutcome::Skipped);
        assert_eq!(sim.state.ranks[&0].tail_len(), 2);
        // The snapshot (engine state through sequence 2) replaces the rank.
        let snapshot = batch(
            2,
            None,
            vec![cleared(), stored(None, &[1, 2]), stored(Some(2), &[3])],
        );
        KvEventMonitor::apply_cleared(sim.worker, &sim.indexer, &mut sim.state.index);
        sim.apply_direct(&snapshot);
        let tail = sim
            .state
            .ranks
            .get_mut(&0)
            .unwrap()
            .finish_snapshot(2)
            .expect("tail intact");
        assert_eq!(tail.len(), 2);
        for b in &tail {
            sim.apply_direct(b);
        }
        sim.reference_apply(&live3);
        sim.reference_apply(&live4);
        assert_eq!(
            sim.deliver(&batch(5, None, vec![stored(Some(5), &[6])])),
            BatchOutcome::Applied
        );
        assert!(!sim.state.ranks[&0].is_degraded());
        sim.assert_matches_reference();
    }
}
