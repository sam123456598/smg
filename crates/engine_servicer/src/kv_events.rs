//! `SubscribeKvEvents`: an engine's ZMQ KV-cache event publisher relayed into
//! gRPC streams through one [`KvEventRelay`] per engine.
//!
//! The relay subscribes to the publisher once, for the servicer's lifetime,
//! and keeps what it relays in a bounded [`History`] (the last
//! `SMG_KV_EVENT_HISTORY_BATCHES` batches, 10,000 by default like the
//! engines' own `buffer_steps`, within `SMG_KV_EVENT_HISTORY_BYTES`, 256 MiB
//! by default). A `SubscribeKvEvents` call is served from it:
//!
//! - `start_sequence_number` is the last sequence the gateway applied. Inside
//!   the window, the batches after it come first, then live events; below
//!   the window (or beyond the newest sequence, which means another publisher
//!   incarnation) the call fails with `OUT_OF_RANGE`, and the gateway clears
//!   and resubscribes from zero. A relay that started after the publisher
//!   holds no history from before its start, so a cursor from before it is
//!   `OUT_OF_RANGE` too.
//! - zero means no cursor. When the window holds every batch the publisher
//!   ever numbered (it starts at 0, nothing lost or evicted) the subscriber
//!   gets all of it, which is the publisher's whole state; otherwise live
//!   events only.
//!
//! The publisher's own sequence numbers are kept. A sequence the relay did
//! not receive is asked of the engine's replay socket (vLLM's and SGLang's
//! ROUTER, the mock engine's too); what the replay cannot give leaves a hole
//! in the window, counted, which a subscriber skips the way the live stream
//! did. A payload that does not decode relays as an empty batch under its
//! sequence, so nobody sees a gap for it.
//!
//! A publisher restart is read the same way on every wire, from three signs
//! (vLLM and SGLang count from 0 per process; SGLang's first batch after a
//! start carries `AllBlocksCleared`, vLLM's does not): the sequence goes
//! backwards on the same socket; the engine's startup clear arrives under a
//! sequence the relay already passed; the counter is back at 0 or 1 after a
//! cursor above them. Each starts a new incarnation: the history is cleared,
//! live subscribers end with `DATA_LOSS`, and the gateway clears and
//! resubscribes from zero, where the new incarnation's complete history is
//! waiting for it. A repeated sequence without a clear is a duplicate.
//!
//! `SMG_KV_EVENT_HASH_CHECK=sglang|vllm-sha256-cbor` turns on the relay's
//! engine-hash verification ([`crate::engine_hash`]); mismatches are counted,
//! never dropped. The relay's counters ([`RelayCounts`]) are logged on every
//! gap, restart and refusal.
//!
//! Framing (`ZmqEventPublisher` in both engines): one PUB multipart message
//! per scheduler step, `[topic, sequence as u64 big-endian, msgpack batch]`.
//! The wire format and the normalization each event goes through live in
//! [`crate::kv_wire`].

use std::{
    collections::VecDeque,
    ops::RangeInclusive,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use engine_zmq_client::codec::TrailingTolerant;
use futures::stream;
use smg_grpc_client::common_proto::{self as common};
use tokio::sync::broadcast;
use tonic::Status;
use tracing::{debug, info, warn};
use zeromq::{
    prelude::{Socket, SocketRecv, SocketSend},
    DealerSocket, SocketOptions, SubSocket, ZmqError, ZmqMessage,
};

use crate::{
    kv_history::{History, Window},
    kv_wire::{low64_big_endian, Normalizer, WireBatch, WireEvent},
    BoxStream,
};

/// The Python vLLM servicer's refusal when vLLM runs without a ZMQ publisher.
pub(crate) const VLLM_DISABLED_MESSAGE: &str = "KV cache events not enabled. Start vLLM with \
     --kv-events-config '{\"enable_kv_cache_events\": true, \"publisher\": \"zmq\"}'";

/// The Python SGLang servicer's refusal when SGLang runs without a ZMQ publisher.
pub(crate) const SGLANG_DISABLED_MESSAGE: &str = "KV cache events not enabled. Start SGLang \
     with --kv-events-config '{\"publisher\": \"zmq\"}'";

/// The Python TokenSpeed servicer's refusal without a publisher.
pub(crate) const TOKENSPEED_DISABLED_MESSAGE: &str = "KV cache events not enabled. Start \
     TokenSpeed with --kv-events-config '{\"enable_kv_cache_events\": true, \"publisher\": \
     \"zmq\"}'";

/// How many relayed batches the history keeps (the engines' `buffer_steps`).
pub const HISTORY_BATCHES_ENV: &str = "SMG_KV_EVENT_HISTORY_BATCHES";
/// The history's byte budget over the encoded batches.
pub const HISTORY_BYTES_ENV: &str = "SMG_KV_EVENT_HISTORY_BYTES";
pub const DEFAULT_HISTORY_BATCHES: usize = 10_000;
pub const DEFAULT_HISTORY_BYTES: usize = 256 << 20;
const DEFAULT_REPLAY_TIMEOUT: Duration = Duration::from_secs(5);
/// Live batches a slow subscriber may fall behind before it is refilled from
/// the history.
const LIVE_CHANNEL: usize = 4_096;
/// The replay socket's end marker, eight 0xff bytes on both wires.
const END_SEQUENCE: [u8; 8] = [0xff; 8];

/// One publisher's relay settings.
#[derive(Clone, Debug)]
pub struct RelayConfig {
    /// The connectable SUB endpoint of rank 0's publisher.
    pub endpoint: String,
    /// Its replay ROUTER, when the engine runs one.
    pub replay_endpoint: Option<String>,
    pub topic: String,
    pub history_batches: usize,
    pub history_bytes: usize,
    /// How long a replay reply may take before the rest of a gap is lost.
    pub replay_timeout: Duration,
}

impl RelayConfig {
    /// Rank 0 of the publisher at `kv_events_endpoint` (bind wildcards
    /// resolved), with the history caps from the environment.
    pub fn for_publisher(
        kv_events_endpoint: &str,
        replay_endpoint: Option<&str>,
        topic: &str,
    ) -> Self {
        Self {
            endpoint: endpoint_for_rank(kv_events_endpoint, 0),
            replay_endpoint: replay_endpoint
                .filter(|endpoint| !endpoint.is_empty())
                .map(|endpoint| endpoint_for_rank(endpoint, 0)),
            topic: topic.to_string(),
            history_batches: env_usize(HISTORY_BATCHES_ENV, DEFAULT_HISTORY_BATCHES),
            history_bytes: env_usize(HISTORY_BYTES_ENV, DEFAULT_HISTORY_BYTES),
            replay_timeout: DEFAULT_REPLAY_TIMEOUT,
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => match value.trim().parse::<usize>() {
            Ok(parsed) => parsed,
            Err(_) => {
                warn!(%value, "{name} is not a number; using {default}");
                default
            }
        },
        _ => default,
    }
}

/// Resolve a KV-events PUB endpoint to a connectable SUB address: bind
/// wildcards become loopback, and under data parallelism rank `dp_rank`
/// publishes on `base_port + dp_rank` (tcp only; ipc/inproc get no port
/// arithmetic).
pub(crate) fn endpoint_for_rank(endpoint: &str, dp_rank: u32) -> String {
    let resolved = endpoint
        .replace('*', "127.0.0.1")
        .replace("0.0.0.0", "127.0.0.1");
    if dp_rank == 0 || !resolved.starts_with("tcp://") {
        return resolved;
    }
    let Some((host, port)) = resolved.rsplit_once(':') else {
        return resolved;
    };
    if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
        return resolved;
    }
    match port.parse::<u64>() {
        Ok(port) => format!("{host}:{}", port.saturating_add(u64::from(dp_rank))),
        Err(_) => resolved,
    }
}

/// What one relay has done since it started.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayCounts {
    /// Batches relayed under the publisher's sequence (live or replayed).
    pub relayed: u64,
    /// Payloads that did not decode and were relayed as empty batches.
    pub undecodable_batches: u64,
    /// Subscriptions whose first batches came from the history.
    pub served_from_history: u64,
    /// Subscriptions refused because their cursor was outside the window.
    pub out_of_range: u64,
    /// Sequence gaps seen on the publisher's socket.
    pub publisher_gaps: u64,
    /// Batches of those gaps the engine's replay gave back.
    pub gap_batches_recovered: u64,
    /// Batches of those gaps nobody had: holes in the window.
    pub gap_batches_lost: u64,
    /// Publisher incarnations after the first.
    pub publisher_restarts: u64,
    /// Subscribers that fell behind the live channel and were refilled.
    pub subscribers_lagged: u64,
}

/// The state the publisher task and the subscribers share.
struct Shared {
    history: History,
    /// The last sequence relayed in this incarnation, or none yet.
    cursor: Option<u64>,
    /// Counts the publisher's restarts; sequences compare within one.
    generation: u64,
    counts: RelayCounts,
    /// Why the publisher task gave up, when it did.
    failed: Option<String>,
}

/// How the publisher task classified a sequence against the cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
    Accept,
    Duplicate,
    Gap { from: u64, to: u64 },
    Restart { reason: RestartReason, last: u64 },
}

/// What showed that the publisher started over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestartReason {
    /// The sequence went backwards on the same socket.
    SequenceRegression,
    /// The engine's startup `AllBlocksCleared` arrived under a sequence the
    /// relay had already passed (SGLang's first batch after a start).
    StartupClear,
    /// The counter is back at 0 or 1 after a cursor above them (the mock
    /// engine's publisher restart, a vLLM process restart).
    CounterRestarted,
}

impl RestartReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SequenceRegression => "sequence_regression",
            Self::StartupClear => "startup_clear",
            Self::CounterRestarted => "counter_restarted",
        }
    }
}

impl Shared {
    /// Classify `seq` against the cursor; `startup_clear` says the batch
    /// begins with the engine's `AllBlocksCleared`.
    fn admit(&self, seq: u64, startup_clear: bool) -> Admission {
        let Some(last) = self.cursor else {
            return Admission::Accept;
        };
        if seq == last + 1 {
            return Admission::Accept;
        }
        if seq > last + 1 {
            return Admission::Gap {
                from: last + 1,
                to: seq - 1,
            };
        }
        let restart = |reason| Admission::Restart { reason, last };
        if seq <= 1 && last >= 2 {
            return restart(RestartReason::CounterRestarted);
        }
        if seq < last {
            return restart(RestartReason::SequenceRegression);
        }
        if startup_clear {
            return restart(RestartReason::StartupClear);
        }
        Admission::Duplicate
    }

    /// Start a new incarnation: the window is the old publisher's.
    fn restart(&mut self) -> u64 {
        self.generation += 1;
        self.history.clear();
        self.cursor = None;
        self.counts.publisher_restarts += 1;
        self.generation
    }
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the live channel carries to subscribers.
#[derive(Clone)]
enum Live {
    Batch(Arc<common::KvEventBatch>),
    Restart { generation: u64 },
}

/// One engine's KV-event relay: the publisher subscription, its history and
/// the live channel its subscribers read.
pub struct KvEventRelay {
    config: RelayConfig,
    shared: Arc<Mutex<Shared>>,
    live: broadcast::Sender<Live>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for KvEventRelay {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
        let counts = self.counts();
        let shared = lock(&self.shared);
        info!(
            endpoint = %self.config.endpoint,
            ?counts,
            window = shared.history.len(),
            holes = shared.history.holes(),
            bytes = shared.history.bytes(),
            "KV event relay closed"
        );
    }
}

impl KvEventRelay {
    pub fn new(config: RelayConfig) -> Arc<Self> {
        let (live, _) = broadcast::channel(LIVE_CHANNEL);
        Arc::new(Self {
            shared: Arc::new(Mutex::new(Shared {
                history: History::new(config.history_batches, config.history_bytes),
                cursor: None,
                generation: 0,
                counts: RelayCounts::default(),
                failed: None,
            })),
            config,
            live,
            task: Mutex::new(None),
        })
    }

    /// The relay for an engine's publisher, or `None` when events are off
    /// (an empty endpoint).
    pub fn for_publisher(
        kv_events_endpoint: &str,
        replay_endpoint: Option<&str>,
        topic: &str,
    ) -> Option<Arc<Self>> {
        (!kv_events_endpoint.is_empty()).then(|| {
            Self::new(RelayConfig::for_publisher(
                kv_events_endpoint,
                replay_endpoint,
                topic,
            ))
        })
    }

    pub fn counts(&self) -> RelayCounts {
        lock(&self.shared).counts.clone()
    }

    /// Subscribe to the publisher if not yet subscribed. Called by
    /// [`Self::subscribe`]; needs a Tokio runtime.
    pub fn start(&self) {
        let mut task = self.task.lock().unwrap_or_else(PoisonError::into_inner);
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let config = self.config.clone();
        let shared = Arc::clone(&self.shared);
        let live = self.live.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "the publisher subscription outlives any one RPC; aborted when the relay drops"
        )]
        let handle = tokio::spawn(run(config, shared, live));
        *task = Some(handle);
    }

    /// Handle one `SubscribeKvEvents` call: the history after the cursor,
    /// then live events; see the module docs for the cursor rules.
    pub fn subscribe(
        &self,
        request: common::SubscribeKvEventsRequest,
    ) -> Result<BoxStream<common::KvEventBatch>, Status> {
        self.start();
        // Subscribed before the history is read, so nothing published between
        // the two is missed; the subscriber skips what it already replayed.
        let rx = self.live.subscribe();
        let cursor = request.start_sequence_number;
        let (replay, last_sent) = {
            let mut shared = lock(&self.shared);
            if let Some(error) = &shared.failed {
                return Err(Status::internal(format!(
                    "SubscribeKvEvents: the relay for {} is down: {error}",
                    self.config.endpoint
                )));
            }
            if cursor == 0 {
                if shared.history.complete_from_start() {
                    shared.counts.served_from_history += 1;
                    (shared.history.all(), None)
                } else {
                    (Vec::new(), None)
                }
            } else {
                match shared.history.after(cursor) {
                    Ok(batches) => {
                        shared.counts.served_from_history += 1;
                        (batches, Some(cursor))
                    }
                    Err(window) => {
                        shared.counts.out_of_range += 1;
                        let endpoint = &self.config.endpoint;
                        let message = match window {
                            Window::Empty => format!(
                                "SubscribeKvEvents: the relay for {endpoint} holds no history \
                                 yet (it started after the publisher); resubscribe from zero"
                            ),
                            Window::Behind { oldest } => format!(
                                "SubscribeKvEvents: the relay for {endpoint} keeps history from \
                                 sequence {oldest}, after cursor {cursor}; resubscribe from zero"
                            ),
                            Window::Ahead { newest } => format!(
                                "SubscribeKvEvents: cursor {cursor} is beyond the publisher's \
                                 last sequence {newest} at {endpoint}: the publisher restarted; \
                                 resubscribe from zero"
                            ),
                        };
                        info!(
                            counts = ?shared.counts,
                            window = shared.history.len(),
                            holes = shared.history.holes(),
                            "{message}"
                        );
                        return Err(Status::out_of_range(message));
                    }
                }
            }
        };
        if !replay.is_empty() {
            debug!(
                endpoint = %self.config.endpoint,
                cursor,
                batches = replay.len(),
                "SubscribeKvEvents: serving from history"
            );
        }
        let subscriber = Subscriber {
            replay: replay.into(),
            rx,
            last_sent,
            shared: Arc::clone(&self.shared),
            endpoint: self.config.endpoint.clone(),
            done: false,
        };
        Ok(Box::pin(stream::unfold(
            subscriber,
            |mut subscriber| async move { subscriber.next().await.map(|item| (item, subscriber)) },
        )))
    }
}

type Item = Result<common::KvEventBatch, Status>;

/// One `SubscribeKvEvents` stream: what it still owes from the history, then
/// the live channel, deduplicated by sequence.
struct Subscriber {
    replay: VecDeque<Arc<common::KvEventBatch>>,
    rx: broadcast::Receiver<Live>,
    last_sent: Option<u64>,
    shared: Arc<Mutex<Shared>>,
    endpoint: String,
    done: bool,
}

impl Subscriber {
    async fn next(&mut self) -> Option<Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some(batch) = self.replay.pop_front() {
                self.last_sent = Some(batch.sequence_number);
                return Some(Ok((*batch).clone()));
            }
            match self.rx.recv().await {
                Ok(Live::Batch(batch)) => {
                    if self
                        .last_sent
                        .is_some_and(|last| batch.sequence_number <= last)
                    {
                        continue;
                    }
                    self.last_sent = Some(batch.sequence_number);
                    return Some(Ok((*batch).clone()));
                }
                Ok(Live::Restart { generation }) => {
                    self.done = true;
                    return Some(Err(Status::data_loss(format!(
                        "SubscribeKvEvents: publisher at {} restarted (incarnation \
                         {generation}); resubscribe from zero",
                        self.endpoint
                    ))));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let refill = {
                        let mut shared = lock(&self.shared);
                        shared.counts.subscribers_lagged += 1;
                        match self.last_sent {
                            Some(last) => shared.history.after(last),
                            None => Err(Window::Empty),
                        }
                    };
                    match refill {
                        Ok(batches) => {
                            debug!(
                                endpoint = %self.endpoint,
                                skipped,
                                refilled = batches.len(),
                                "SubscribeKvEvents: subscriber fell behind; refilled from history"
                            );
                            self.replay = batches.into();
                        }
                        Err(_) => {
                            self.done = true;
                            return Some(Err(Status::data_loss(format!(
                                "SubscribeKvEvents: subscriber of {} fell {skipped} batches \
                                 behind and the history no longer reaches back; resubscribe \
                                 from zero",
                                self.endpoint
                            ))));
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

/// A publisher payload as the relay read it.
enum Decoded {
    Batch(WireBatch),
    Undecodable,
}

fn decode(payload: &[u8], sequence: u64) -> Decoded {
    match rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(payload) {
        Ok(batch) => Decoded::Batch(batch.0),
        Err(error) => {
            warn!(%error, sequence, "Failed to decode KV event batch; relaying it empty");
            Decoded::Undecodable
        }
    }
}

/// The publisher task: one SUB socket for the relay's lifetime.
async fn run(config: RelayConfig, shared: Arc<Mutex<Shared>>, live: broadcast::Sender<Live>) {
    let mut socket = match connect(&config.endpoint, &config.topic).await {
        Ok(socket) => socket,
        Err(status) => {
            lock(&shared).failed = Some(status.message().to_string());
            return;
        }
    };
    let mut relay = Relaying {
        config: &config,
        shared: &shared,
        live: &live,
        normalizer: Normalizer::from_env(),
        event_id: 0,
    };
    loop {
        let message = match socket.recv().await {
            Ok(message) => message,
            Err(error) => {
                warn!(endpoint = %config.endpoint, %error, "KV event receive failed; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Some((sequence, payload)) = split_frames(&message) else {
            continue;
        };
        let decoded = decode(payload, sequence);
        let startup_clear = matches!(
            &decoded,
            Decoded::Batch(batch)
                if matches!(batch.events.first(), Some(WireEvent::AllBlocksCleared { .. }))
        );
        let mut admission = lock(&shared).admit(sequence, startup_clear);
        if let Admission::Gap { from, to } = admission {
            relay.recover_gap(from..=to).await;
            // The replay may have run past the live sequence.
            admission = lock(&shared).admit(sequence, startup_clear);
        }
        match admission {
            Admission::Accept => relay.relay(sequence, decoded),
            Admission::Gap { .. } | Admission::Duplicate => {}
            Admission::Restart { reason, last } => {
                let (generation, counts) = {
                    let mut shared = lock(&shared);
                    let generation = shared.restart();
                    (generation, shared.counts.clone())
                };
                warn!(
                    endpoint = %config.endpoint,
                    sequence,
                    last,
                    generation,
                    reason = reason.as_str(),
                    ?counts,
                    "KV event publisher restarted; live subscribers end with DATA_LOSS"
                );
                let _ = live.send(Live::Restart { generation });
                relay.normalizer = Normalizer::from_env();
                relay.relay(sequence, decoded);
            }
        }
    }
}

/// The publisher task's per-batch work.
struct Relaying<'a> {
    config: &'a RelayConfig,
    shared: &'a Mutex<Shared>,
    live: &'a broadcast::Sender<Live>,
    normalizer: Normalizer,
    event_id: u64,
}

impl Relaying<'_> {
    /// Normalize, remember and broadcast one batch under `sequence`.
    fn relay(&mut self, sequence: u64, decoded: Decoded) {
        let (batch, undecodable) = match decoded {
            Decoded::Batch(batch) => (
                self.normalizer
                    .normalize_batch(batch, sequence, &mut self.event_id),
                false,
            ),
            Decoded::Undecodable => (
                common::KvEventBatch {
                    sequence_number: sequence,
                    ..common::KvEventBatch::default()
                },
                true,
            ),
        };
        let batch = Arc::new(batch);
        {
            let mut shared = lock(self.shared);
            shared.history.push(sequence, Arc::clone(&batch));
            shared.cursor = Some(sequence);
            shared.counts.relayed += 1;
            if undecodable {
                shared.counts.undecodable_batches += 1;
            }
        }
        let _ = self.live.send(Live::Batch(batch));
    }

    /// Fill `gap` from the engine's replay socket; what it does not give
    /// becomes holes.
    async fn recover_gap(&mut self, gap: RangeInclusive<u64>) {
        let (from, to) = (*gap.start(), *gap.end());
        lock(self.shared).counts.publisher_gaps += 1;
        let replies = match &self.config.replay_endpoint {
            Some(endpoint) => match replay(endpoint, from, self.config.replay_timeout).await {
                Ok(replies) => replies,
                Err(error) => {
                    warn!(
                        endpoint = %self.config.endpoint,
                        %error,
                        "KV event replay failed; the gap stays"
                    );
                    Vec::new()
                }
            },
            None => Vec::new(),
        };
        let mut expected = from;
        let mut recovered = 0u64;
        for (sequence, payload) in replies {
            if sequence < expected {
                continue;
            }
            self.lose(expected..sequence);
            self.relay(sequence, decode(&payload, sequence));
            recovered += 1;
            expected = sequence + 1;
        }
        if expected <= to {
            self.lose(expected..=to);
        }
        let counts = {
            let mut shared = lock(self.shared);
            shared.counts.gap_batches_recovered += recovered;
            shared.counts.clone()
        };
        warn!(
            endpoint = %self.config.endpoint,
            from,
            to,
            recovered,
            lost = (to - from + 1).saturating_sub(recovered.min(to - from + 1)),
            ?counts,
            "KV event publisher skipped sequences"
        );
    }

    fn lose(&mut self, sequences: impl Iterator<Item = u64>) {
        let mut shared = lock(self.shared);
        for sequence in sequences {
            shared.history.push_lost(sequence);
            shared.cursor = Some(sequence);
            shared.counts.gap_batches_lost += 1;
        }
    }
}

/// Ask a publisher's replay ROUTER for its buffered batches from `from`:
/// `[b"", from as 8 bytes big-endian]` on a DEALER; replies are
/// `[b"", topic, seq, payload]` (vLLM) or `[b"", seq, payload]` (SGLang),
/// ending with the all-ones sequence. Returned in arrival order; a stop
/// before the end marker returns what arrived.
async fn replay(
    endpoint: &str,
    from: u64,
    timeout: Duration,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    let mut dealer = DealerSocket::new();
    dealer
        .connect(endpoint)
        .await
        .map_err(|error| format!("could not connect to replay {endpoint}: {error}"))?;
    let mut request = ZmqMessage::from(Vec::new());
    request.push_back(from.to_be_bytes().to_vec().into());
    dealer
        .send(request)
        .await
        .map_err(|error| format!("could not send the replay request to {endpoint}: {error}"))?;
    let mut replies = Vec::new();
    loop {
        let reply = match tokio::time::timeout(timeout, dealer.recv()).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => return Err(format!("replay from {endpoint} failed: {error}")),
            Err(_) => {
                warn!(
                    endpoint,
                    from,
                    received = replies.len(),
                    "KV event replay timed out"
                );
                return Ok(replies);
            }
        };
        let (sequence, payload) = match reply.len() {
            3 => (reply.get(1), reply.get(2)),
            4 => (reply.get(2), reply.get(3)),
            frames => {
                return Err(format!(
                    "malformed replay reply from {endpoint}: {frames} frames"
                ))
            }
        };
        let (Some(sequence), Some(payload)) = (sequence, payload) else {
            return Err(format!("malformed replay reply from {endpoint}"));
        };
        if sequence.as_ref() == &END_SEQUENCE[..] {
            return Ok(replies);
        }
        if sequence.len() != 8 {
            return Err(format!("malformed replay sequence from {endpoint}"));
        }
        replies.push((low64_big_endian(sequence.as_ref()), payload.to_vec()));
    }
}

/// A SUB socket subscribed to `topic` and connected to `endpoint`. The
/// subscription is recorded first and sent on connect (and on the crate's
/// reconnects), as libzmq does; a refused publisher is retried until the
/// socket is dropped, as libzmq's background connect would.
async fn connect(endpoint: &str, topic: &str) -> Result<SubSocket, Status> {
    let mut options = SocketOptions::default();
    options.no_connect_timeout();
    let mut socket = SubSocket::with_options(options);
    let failed = |step: &str, error: ZmqError| {
        Status::internal(format!("SubscribeKvEvents: {step} {endpoint}: {error}"))
    };
    socket
        .subscribe(topic)
        .await
        .map_err(|error| failed("could not subscribe to", error))?;
    socket
        .connect(endpoint)
        .await
        .map_err(|error| failed("could not connect to", error))?;
    info!(%endpoint, "SubscribeKvEvents: connected to ZMQ endpoint");
    Ok(socket)
}

/// A publisher message's `[topic, sequence, payload, ...]` as the sequence
/// number and payload, or `None` for fewer than three frames.
fn split_frames(message: &ZmqMessage) -> Option<(u64, &[u8])> {
    if message.len() < 3 {
        return None;
    }
    let sequence = message.get(1)?;
    let payload = message.get(2)?;
    Some((low64_big_endian(sequence), payload.as_ref()))
}

/// Golden publisher payloads encoded by vLLM 0.30.1rc1 (msgspec 0.22) with
/// `crates/engine_servicer/scripts/generate_kv_events_golden.py`, which also
/// prints the Python relay's conversion of them (the expected protos below).
#[cfg(test)]
pub(crate) mod golden {
    use zeromq::ZmqMessage;

    /// `KVEventBatch(ts=1700000000.5, data_parallel_rank=None)` with a
    /// `BlockStored` of two sha256-byte hashes (`00..00 80 00..00`,
    /// `00..00 ff..fe`), parent 7, tokens 1..=8, block size 4, medium GPU; a
    /// `BlockStored` of int hash 0x1234, no parent, tokens [9, 10], block
    /// size 2, lora_id 3, group_idx 0, kv_cache_spec_kind full_attention; an
    /// unaligned `BlockStored` (one hash, block size 4, three tokens); a
    /// `BlockRemoved` of [0x1234, ff..ff]; an `AllBlocksCleared`.
    pub(crate) const BATCH1: &str = "93cb41d954fc402000009588a474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657392c4200000000000000000000000000000000000000000000000008000000000000000c420000000000000000000000000000000000000000000000000fffffffffffffffeb1706172656e745f626c6f636b5f6861736807a9746f6b656e5f696473980102030405060708aa626c6f636b5f73697a6504a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c08aa474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391cd1234b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647392090aaa626c6f636b5f73697a6502a76c6f72615f696403a66d656469756dc0a96c6f72615f6e616d65c0a967726f75705f69647800b26b765f63616368655f737065635f6b696e64ae66756c6c5f617474656e74696f6e88a474797065ab426c6f636b53746f726564ac626c6f636b5f6861736865739105b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647393010203aa626c6f636b5f73697a6504a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c083a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657392cd1234c420ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffa66d656469756da347505581a474797065b0416c6c426c6f636b73436c6561726564c0";

    /// `KVEventBatch(ts=1700000001.0, data_parallel_rank=1)` with one
    /// `BlockStored`: int hash 42, parent 41, tokens [100, 101], block size 2.
    pub(crate) const BATCH2: &str = "93cb41d954fc404000009188a474797065ab426c6f636b53746f726564ac626c6f636b5f686173686573912ab1706172656e745f626c6f636b5f6861736829a9746f6b656e5f696473926465aa626c6f636b5f73697a6502a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c001";

    pub(crate) fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
            .collect()
    }

    /// A publisher message: `[topic, sequence (u64 big-endian), payload]`.
    pub(crate) fn frame(topic: &[u8], sequence: u64, payload: &[u8]) -> ZmqMessage {
        let mut message = ZmqMessage::from(topic.to_vec());
        message.push_back(sequence.to_be_bytes().to_vec().into());
        message.push_back(payload.to_vec().into());
        message
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use smg_grpc_client::common_proto::{kv_cache_event, KvCacheLocality, KvCacheTier};
    use tokio::time::timeout;
    use zeromq::{PubSocket, RouterSocket, SocketEvent};

    use super::{golden, *};
    use crate::kv_wire::WireBatch;

    fn decode(hex: &str) -> WireBatch {
        rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(&golden::bytes(hex))
            .expect("golden batch decodes")
            .0
    }

    fn convert_batch(
        batch: WireBatch,
        sequence_number: u64,
        event_id: &mut u64,
    ) -> common::KvEventBatch {
        Normalizer::new().normalize_batch(batch, sequence_number, event_id)
    }

    fn stored(event: &common::KvCacheEvent) -> &common::KvBlocksStored {
        match &event.data {
            Some(kv_cache_event::Data::Stored(stored)) => stored,
            other => panic!("expected a stored event, got {other:?}"),
        }
    }

    fn block(block_hash: i64, token_ids: Vec<u32>, lora_id: Option<i64>) -> common::KvBlock {
        common::KvBlock {
            block_hash,
            block_size: i32::try_from(token_ids.len()).unwrap(),
            token_ids,
            lora_id,
            cache_level: None,
            ..Default::default()
        }
    }

    #[test]
    fn endpoint_for_rank_mirrors_the_python_helper() {
        assert_eq!(endpoint_for_rank("tcp://*:5557", 0), "tcp://127.0.0.1:5557");
        assert_eq!(
            endpoint_for_rank("tcp://0.0.0.0:5557", 0),
            "tcp://127.0.0.1:5557"
        );
        assert_eq!(endpoint_for_rank("tcp://*:5557", 2), "tcp://127.0.0.1:5559");
        assert_eq!(
            endpoint_for_rank("tcp://10.0.0.1:5557", 1),
            "tcp://10.0.0.1:5558"
        );
        assert_eq!(endpoint_for_rank("tcp://host:port", 1), "tcp://host:port");
        assert_eq!(endpoint_for_rank("ipc:///tmp/kv", 1), "ipc:///tmp/kv");
    }

    /// The golden batches convert to exactly what the Python relay produced
    /// for them: sha256 hashes reduced to their low 64 bits, an unaligned
    /// store skipped but its event id consumed, `lora_id` and the parent
    /// carried, `dp_rank` set only when the publisher set it.
    #[test]
    fn golden_batches_convert_like_the_python_relay() {
        let mut event_id = 0;
        let batch = convert_batch(decode(golden::BATCH1), 9, &mut event_id);
        assert_eq!(event_id, 5);
        assert_eq!(batch.sequence_number, 9);
        assert!((batch.timestamp - 1_700_000_000.5).abs() < f64::EPSILON);
        assert_eq!(batch.dp_rank, None);
        assert_eq!(
            batch
                .events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 4, 5]
        );
        let first = stored(&batch.events[0]);
        assert_eq!(first.parent_block_hash, Some(7));
        assert_eq!(
            first.blocks,
            vec![
                block(i64::MIN, vec![1, 2, 3, 4], None),
                block(-2, vec![5, 6, 7, 8], None),
            ]
        );
        let second = stored(&batch.events[1]);
        assert_eq!(second.parent_block_hash, None);
        assert_eq!(second.blocks, vec![block(0x1234, vec![9, 10], Some(3))]);
        assert_eq!(
            batch.events[2].data,
            Some(kv_cache_event::Data::Removed(common::KvBlocksRemoved {
                block_hashes: vec![0x1234, -1],
                cache_level: None,
                tier: Some(KvCacheTier::Device as i32),
                medium: Some("GPU".to_string()),
                locality: Some(KvCacheLocality::Local as i32),
                ..Default::default()
            }))
        );
        assert_eq!(
            batch.events[3].data,
            Some(kv_cache_event::Data::Cleared(
                common::KvCacheCleared::default()
            ))
        );

        let batch = convert_batch(decode(golden::BATCH2), 10, &mut event_id);
        assert_eq!(event_id, 6);
        assert_eq!(batch.sequence_number, 10);
        assert_eq!(batch.dp_rank, Some(1));
        assert_eq!(batch.events[0].event_id, 6);
        let only = stored(&batch.events[0]);
        assert_eq!(only.parent_block_hash, Some(41));
        assert_eq!(only.blocks, vec![block(42, vec![100, 101], None)]);
    }

    /// The batch array may omit the trailing rank and may grow new fields;
    /// an event of a type this relay does not convert is skipped on its own
    /// (consuming its event id), as the Python relay skips unknown types.
    #[test]
    fn batch_layout_tolerates_an_omitted_rank_and_trailing_fields() {
        let short = rmp_serde::to_vec(&(1.5f64, Vec::<u8>::new())).unwrap();
        let batch = rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(&short)
            .unwrap()
            .0;
        assert!(batch.events.is_empty());
        assert_eq!(batch.dp_rank, None);

        let long = rmp_serde::to_vec(&(1.5f64, Vec::<u8>::new(), 2i32, "future")).unwrap();
        let batch = rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(&long)
            .unwrap()
            .0;
        assert_eq!(batch.dp_rank, Some(2));

        let unknown = rmp_serde::to_vec(&serde_json::json!([
            1.5,
            [{"type": "Mystery", "x": 1}, {"type": "AllBlocksCleared"}]
        ]))
        .unwrap();
        let batch = rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(&unknown)
            .unwrap()
            .0;
        let mut event_id = 4;
        let converted = convert_batch(batch, 7, &mut event_id);
        assert_eq!(event_id, 6, "the skipped event still consumed an id");
        assert_eq!(converted.events.len(), 1);
        assert_eq!(converted.events[0].event_id, 6);
        assert!(matches!(
            converted.events[0].data,
            Some(kv_cache_event::Data::Cleared(_))
        ));
    }

    #[test]
    fn frames_are_split_like_the_python_relay() {
        let payload = golden::bytes(golden::BATCH2);
        let message = golden::frame(b"kv", 5, &payload);
        let (sequence, body) = split_frames(&message).expect("three frames");
        assert_eq!(sequence, 5);
        assert_eq!(body, payload.as_slice());

        let mut short = ZmqMessage::from(b"kv".to_vec());
        short.push_back(5u64.to_be_bytes().to_vec().into());
        assert!(split_frames(&short).is_none());

        // A shorter sequence frame zero-extends, as `int.from_bytes` does.
        let mut narrow = ZmqMessage::from(b"kv".to_vec());
        narrow.push_back(vec![1, 2].into());
        narrow.push_back(payload.into());
        assert_eq!(
            split_frames(&narrow).map(|(sequence, _)| sequence),
            Some(0x0102)
        );
    }

    struct Lab {
        publisher: PubSocket,
        router: Option<RouterSocket>,
        relay: Arc<KvEventRelay>,
    }

    /// A local publisher (and replay ROUTER when asked) with a started relay.
    async fn start_lab(history_batches: usize, with_replay: bool) -> Lab {
        let mut publisher = PubSocket::new();
        let endpoint = publisher
            .bind("tcp://127.0.0.1:0")
            .await
            .expect("publisher binds")
            .to_string();
        let (router, replay_endpoint) = if with_replay {
            let mut router = RouterSocket::new();
            let endpoint = router
                .bind("tcp://127.0.0.1:0")
                .await
                .expect("router binds")
                .to_string();
            (Some(router), Some(endpoint))
        } else {
            (None, None)
        };
        let relay = KvEventRelay::new(RelayConfig {
            endpoint,
            replay_endpoint,
            topic: "kv".to_string(),
            history_batches,
            history_bytes: 64 << 20,
            replay_timeout: Duration::from_secs(2),
        });
        relay.start();
        Lab {
            publisher,
            router,
            relay,
        }
    }

    impl Lab {
        async fn publish(&mut self, sequence: u64, payload: &[u8]) {
            self.publisher
                .send(golden::frame(b"kv", sequence, payload))
                .await
                .expect("publish");
        }

        /// The subscription reaches the publisher a moment after the
        /// connect; publish sequence 0 until the relay has it.
        async fn prime(&mut self) {
            let batch1 = golden::bytes(golden::BATCH1);
            for _ in 0..250 {
                self.publish(0, &batch1).await;
                if self.relay.counts().relayed >= 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("the relay never saw sequence 0");
        }

        async fn wait_relayed(&self, count: u64) {
            for _ in 0..250 {
                if self.relay.counts().relayed >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!(
                "the relay relayed {} batches, not {count}",
                self.relay.counts().relayed
            );
        }

        fn subscribe(&self, cursor: u64) -> Result<BoxStream<common::KvEventBatch>, Status> {
            self.relay.subscribe(common::SubscribeKvEventsRequest {
                start_sequence_number: cursor,
            })
        }

        /// Answer the next replay request as vLLM's ROUTER does, with
        /// `replies` and the end marker; the request must ask from `start`.
        async fn answer_replay(&mut self, start: u64, replies: &[(u64, Vec<u8>)]) {
            let router = self.router.as_mut().expect("a replay socket");
            let request = timeout(Duration::from_secs(5), router.recv())
                .await
                .expect("a replay request in time")
                .expect("request");
            let frames: Vec<Vec<u8>> = request.iter().map(|frame| frame.to_vec()).collect();
            assert_eq!(frames.len(), 3, "[identity, empty, start]");
            assert_eq!(frames[1], b"");
            assert_eq!(frames[2], start.to_be_bytes());
            for (sequence, payload) in replies {
                let mut reply = ZmqMessage::from(frames[0].clone());
                reply.push_back(Vec::new().into());
                reply.push_back(b"kv".to_vec().into());
                reply.push_back(sequence.to_be_bytes().to_vec().into());
                reply.push_back(payload.clone().into());
                router.send(reply).await.expect("reply");
            }
            let mut end = ZmqMessage::from(frames[0].clone());
            end.push_back(Vec::new().into());
            end.push_back(Vec::new().into());
            end.push_back(END_SEQUENCE.to_vec().into());
            end.push_back(Vec::new().into());
            router.send(end).await.expect("end marker");
        }
    }

    fn refused(result: Result<BoxStream<common::KvEventBatch>, Status>) -> Status {
        match result {
            Err(status) => status,
            Ok(_) => panic!("the subscription was accepted"),
        }
    }

    async fn read(stream: &mut BoxStream<common::KvEventBatch>) -> common::KvEventBatch {
        timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a batch in time")
            .expect("stream open")
            .expect("a batch")
    }

    async fn read_error(stream: &mut BoxStream<common::KvEventBatch>) -> Status {
        timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("an item in time")
            .expect("stream open")
            .expect_err("an error")
    }

    async fn read_end(stream: &mut BoxStream<common::KvEventBatch>) {
        assert!(timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the end in time")
            .is_none());
    }

    #[tokio::test]
    async fn history_serves_a_cursor_inside_the_window_then_live() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        for sequence in 1..=5 {
            lab.publish(sequence, &batch2).await;
        }
        lab.wait_relayed(6).await;

        let mut stream = lab.subscribe(2).expect("inside the window");
        for expected in 3..=5 {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }
        lab.publish(6, &batch2).await;
        assert_eq!(read(&mut stream).await.sequence_number, 6);

        // The newest sequence as the cursor: nothing owed, live from here.
        let mut caught_up = lab.subscribe(6).expect("at the newest");
        lab.publish(7, &batch2).await;
        assert_eq!(read(&mut caught_up).await.sequence_number, 7);
        assert_eq!(read(&mut stream).await.sequence_number, 7);
        assert_eq!(lab.relay.counts().served_from_history, 2);
    }

    #[tokio::test]
    async fn cursors_outside_the_window_are_out_of_range() {
        let fresh = start_lab(10, false).await;
        let status = refused(fresh.subscribe(1));
        assert_eq!(status.code(), tonic::Code::OutOfRange);
        assert!(status.message().contains("holds no history yet"));

        let mut lab = start_lab(3, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        for sequence in 1..=9 {
            lab.publish(sequence, &batch2).await;
        }
        lab.wait_relayed(10).await;
        // The window is 7..=9: cursor 6 wants 7, served; cursor 5 wants 6, gone.
        let mut stream = lab.subscribe(6).expect("the oldest batch is wanted");
        for expected in 7..=9 {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }
        let behind = refused(lab.subscribe(5));
        assert_eq!(behind.code(), tonic::Code::OutOfRange);
        assert!(behind.message().contains("from sequence 7"));
        let ahead = refused(lab.subscribe(20));
        assert_eq!(ahead.code(), tonic::Code::OutOfRange);
        assert!(ahead.message().contains("publisher restarted"));
        assert_eq!(lab.relay.counts().out_of_range, 2);
    }

    #[tokio::test]
    async fn a_subscriber_without_a_cursor_gets_the_whole_history_or_live_only() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        lab.publish(1, &batch2).await;
        lab.publish(2, &batch2).await;
        lab.wait_relayed(3).await;
        // Everything since the publisher's first batch is here: hand it over.
        let mut stream = lab.subscribe(0).expect("live");
        for expected in 0..=2 {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }
        lab.publish(3, &batch2).await;
        assert_eq!(read(&mut stream).await.sequence_number, 3);

        // A relay that joined after the publisher's first batches has an
        // incomplete window: live only.
        let mut late = start_lab(100, false).await;
        for _ in 0..250 {
            late.publish(5, &batch2).await;
            if late.relay.counts().relayed >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(late.relay.counts().relayed, 1, "the relay saw sequence 5");
        let mut live = late.subscribe(0).expect("live");
        late.publish(6, &batch2).await;
        assert_eq!(
            read(&mut live).await.sequence_number,
            6,
            "nothing from before"
        );
    }

    #[tokio::test]
    async fn a_publisher_gap_is_filled_from_the_engines_replay() {
        let mut lab = start_lab(100, true).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        lab.publish(1, &batch2).await;
        lab.wait_relayed(2).await;
        let mut stream = lab.subscribe(1).expect("caught up");

        // 2 and 3 never reach the SUB; 4 reveals the gap.
        lab.publish(4, &batch2).await;
        lab.answer_replay(
            2,
            &[
                (2, batch2.clone()),
                (3, batch2.clone()),
                (4, batch2.clone()),
            ],
        )
        .await;
        for expected in 2..=4 {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }
        lab.publish(5, &batch2).await;
        assert_eq!(read(&mut stream).await.sequence_number, 5);
        let counts = lab.relay.counts();
        assert_eq!(
            (
                counts.publisher_gaps,
                counts.gap_batches_recovered,
                counts.gap_batches_lost
            ),
            (1, 3, 0)
        );
        assert_eq!(
            counts.relayed, 6,
            "the replay's copy of 4 was relayed, the live one skipped"
        );
        // The window is complete: a cursor inside it is served across the
        // recovered stretch.
        let mut resumed = lab.subscribe(1).expect("served");
        for expected in 2..=5 {
            assert_eq!(read(&mut resumed).await.sequence_number, expected);
        }
    }

    #[tokio::test]
    async fn a_gap_the_engine_cannot_fill_leaves_a_hole_not_a_dead_end() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        lab.publish(1, &batch2).await;
        lab.wait_relayed(2).await;
        let mut stream = lab.subscribe(1).expect("caught up");
        lab.publish(4, &batch2).await;
        assert_eq!(
            read(&mut stream).await.sequence_number,
            4,
            "the live stream jumps"
        );
        let counts = lab.relay.counts();
        assert_eq!(
            (
                counts.publisher_gaps,
                counts.gap_batches_recovered,
                counts.gap_batches_lost
            ),
            (1, 0, 2)
        );
        // A resume from inside the hole or before it skips it, as the gateway
        // then settles the gap itself instead of looping on OUT_OF_RANGE.
        let mut from_before = lab.subscribe(1).expect("served");
        assert_eq!(read(&mut from_before).await.sequence_number, 4);
        let mut from_inside = lab.subscribe(2).expect("served");
        assert_eq!(read(&mut from_inside).await.sequence_number, 4);
        let mut no_cursor = lab.subscribe(0).expect("live only: the window has a hole");
        lab.publish(5, &batch2).await;
        assert_eq!(read(&mut no_cursor).await.sequence_number, 5);
    }

    #[tokio::test]
    async fn a_partial_replay_recovers_what_it_can_and_loses_the_rest() {
        let mut lab = start_lab(100, true).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        let mut stream = lab.subscribe(0).expect("live");
        assert_eq!(read(&mut stream).await.sequence_number, 0);
        lab.publish(5, &batch2).await;
        // The engine's buffer starts at 3: 1 and 2 are gone for good.
        lab.answer_replay(1, &[(3, batch2.clone()), (4, batch2.clone())])
            .await;
        for expected in [3, 4, 5] {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }
        let counts = lab.relay.counts();
        assert_eq!(
            (counts.gap_batches_recovered, counts.gap_batches_lost),
            (2, 2)
        );
    }

    #[tokio::test]
    async fn undecodable_payloads_relay_as_empty_batches() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let mut stream = lab.subscribe(0).expect("live");
        assert_eq!(read(&mut stream).await.events.len(), 4);
        lab.publish(1, b"not msgpack").await;
        let empty = read(&mut stream).await;
        assert_eq!((empty.sequence_number, empty.events.len()), (1, 0));
        let mut short = ZmqMessage::from(b"kv".to_vec());
        short.push_back(2u64.to_be_bytes().to_vec().into());
        lab.publisher.send(short).await.expect("publish");
        lab.publish(2, &golden::bytes(golden::BATCH2)).await;
        assert_eq!(
            read(&mut stream).await.sequence_number,
            2,
            "a short frame is nothing"
        );
        assert_eq!(lab.relay.counts().undecodable_batches, 1);
    }

    #[tokio::test]
    async fn a_sequence_regression_ends_live_streams_and_clears_the_history() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        for sequence in 1..=3 {
            lab.publish(sequence, &batch2).await;
        }
        lab.wait_relayed(4).await;
        let mut stream = lab.subscribe(0).expect("whole history");
        for expected in 0..=3 {
            assert_eq!(read(&mut stream).await.sequence_number, expected);
        }

        // The publisher restarts and counts from 0 again.
        lab.publish(0, &batch2).await;
        let status = read_error(&mut stream).await;
        assert_eq!(status.code(), tonic::Code::DataLoss);
        assert!(status.message().contains("restarted"));
        read_end(&mut stream).await;
        assert_eq!(lab.relay.counts().publisher_restarts, 1);

        // The old incarnation's cursor is refused; the new one's complete
        // history is handed to a fresh subscriber.
        let stale = refused(lab.subscribe(3));
        assert_eq!(stale.code(), tonic::Code::OutOfRange);
        let mut fresh = lab
            .subscribe(0)
            .expect("the new incarnation from its start");
        assert_eq!(read(&mut fresh).await.sequence_number, 0);
        lab.publish(1, &batch2).await;
        assert_eq!(read(&mut fresh).await.sequence_number, 1);
    }

    fn cleared_payload() -> Vec<u8> {
        let batch = serde_json::json!([1700000002.0, [{"type": "AllBlocksCleared"}], 0]);
        rmp_serde::to_vec_named(&batch).expect("encodes")
    }

    #[test]
    fn restart_rules_read_the_same_on_every_wire() {
        let mut shared = Shared {
            history: History::new(10, usize::MAX),
            cursor: Some(500),
            generation: 0,
            counts: RelayCounts::default(),
            failed: None,
        };
        let restart = |reason| Admission::Restart { reason, last: 500 };
        assert_eq!(shared.admit(501, false), Admission::Accept);
        assert_eq!(
            shared.admit(501, true),
            Admission::Accept,
            "a flush continues the sequence"
        );
        assert_eq!(
            shared.admit(503, false),
            Admission::Gap { from: 501, to: 502 }
        );
        assert_eq!(shared.admit(500, false), Admission::Duplicate);
        assert_eq!(
            shared.admit(500, true),
            restart(RestartReason::StartupClear)
        );
        assert_eq!(
            shared.admit(499, false),
            restart(RestartReason::SequenceRegression)
        );
        assert_eq!(
            shared.admit(0, false),
            restart(RestartReason::CounterRestarted)
        );
        assert_eq!(
            shared.admit(1, true),
            restart(RestartReason::CounterRestarted)
        );
        // Under a cursor of 1, a repeated 1 is a duplicate unless it clears.
        shared.cursor = Some(1);
        assert_eq!(shared.admit(1, false), Admission::Duplicate);
        assert_eq!(
            shared.admit(1, true),
            Admission::Restart {
                reason: RestartReason::StartupClear,
                last: 1
            }
        );
        assert_eq!(
            shared.admit(0, false),
            Admission::Restart {
                reason: RestartReason::SequenceRegression,
                last: 1
            }
        );
        shared.cursor = None;
        assert_eq!(
            shared.admit(7, true),
            Admission::Accept,
            "no cursor yet: anything goes"
        );
    }

    /// SGLang's first batch after a start carries `AllBlocksCleared`; under a
    /// sequence the relay already passed it is a restart, not a duplicate.
    #[tokio::test]
    async fn the_engines_startup_clear_under_a_passed_cursor_is_a_restart() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        for sequence in 1..=3 {
            lab.publish(sequence, &batch2).await;
        }
        lab.wait_relayed(4).await;
        let mut stream = lab.subscribe(3).expect("caught up");
        // A repeated sequence without a clear is a duplicate.
        lab.publish(3, &batch2).await;
        lab.publish(4, &batch2).await;
        assert_eq!(read(&mut stream).await.sequence_number, 4);

        // The engine comes back and its startup clear lands on a sequence
        // the relay already passed.
        lab.publish(2, &cleared_payload()).await;
        let status = read_error(&mut stream).await;
        assert_eq!(status.code(), tonic::Code::DataLoss);
        assert_eq!(lab.relay.counts().publisher_restarts, 1);
        let mut fresh = lab.subscribe(0).expect("live");
        lab.publish(3, &batch2).await;
        assert_eq!(read(&mut fresh).await.sequence_number, 3);
    }

    /// A counter back at 0 or 1 after a cursor above them is a restart even
    /// when nothing else says so (the mock engine's restart-publisher hook,
    /// a vLLM process restart: no clear on that wire).
    #[tokio::test]
    async fn a_counter_back_at_its_start_is_a_restart() {
        let mut lab = start_lab(100, false).await;
        lab.prime().await;
        let batch2 = golden::bytes(golden::BATCH2);
        for sequence in 1..=5 {
            lab.publish(sequence, &batch2).await;
        }
        lab.wait_relayed(6).await;
        let mut stream = lab.subscribe(5).expect("caught up");
        lab.publish(1, &batch2).await;
        let status = read_error(&mut stream).await;
        assert_eq!(status.code(), tonic::Code::DataLoss);
        read_end(&mut stream).await;
        // The new incarnation started at 1, not 0: its window is not complete
        // from the publisher's first batch, so a fresh subscriber goes live.
        let mut fresh = lab.subscribe(0).expect("live");
        lab.publish(2, &batch2).await;
        assert_eq!(read(&mut fresh).await.sequence_number, 2);
        let mut resumed = lab.subscribe(1).expect("inside the new window");
        assert_eq!(read(&mut resumed).await.sequence_number, 2);
    }

    #[tokio::test]
    async fn dropping_the_relay_closes_the_publisher_subscription() {
        let mut lab = start_lab(10, false).await;
        let mut monitor = lab.publisher.monitor();
        lab.prime().await;
        let mut stream = lab.subscribe(0).expect("live");
        assert_eq!(read(&mut stream).await.sequence_number, 0);
        drop(lab.relay);
        read_end(&mut stream).await;
        let disconnected = timeout(Duration::from_secs(5), async {
            while let Some(event) = monitor.next().await {
                if matches!(event, SocketEvent::Disconnected(_)) {
                    return true;
                }
            }
            false
        })
        .await
        .expect("the publisher notices in time");
        assert!(disconnected);
    }
}
