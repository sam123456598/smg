//! Open-loop replay of a Mooncake indexer corpus, by Dynamo's method, against this crate's
//! indexers.
//!
//! The corpus is the schedule Dynamo's `mooncake_bench` prepares (after trace duplication, deadline
//! sort and id assignment), exported with its `--export-corpus` option; see `README.md` next to
//! this file for the format. Replaying it here reproduces their measurement without their
//! repository at measurement time:
//!
//! - queries go to `worker_id % query_lanes` lanes, each an OS thread that services lookups
//!   inline; events go to `(worker, dp_rank)`-pinned event lanes assigned round-robin on first
//!   sight, each an OS thread draining an unbounded queue;
//! - one query issuer and `--issuer-threads` event issuers (events sharded by contiguous worker
//!   ranges) issue at absolute deadlines (`clock_nanosleep` + a spin) and never wait for earlier
//!   operations; at equal deadlines queries are published before events;
//! - timing ends at the last completion (drain included); a trial is `generator_valid` when every
//!   operation was issued within 1.01× the window and `kept_up` when replay plus drain fit in
//!   1.10×; a block op is a requested, stored or removed block hash;
//! - lookup `query_service` is the time inside the indexer, `query_scheduled_to_finished` includes
//!   queueing; percentiles use the nearest-rank method.
//!
//! Differences from Dynamo's harness, stated so parity numbers can be read correctly: lanes are
//! OS threads parked on `std::thread::park` rather than tokio tasks on a `Notify`; event queues are
//! `std::sync::mpsc` (unbounded, one consumer) rather than `flume`. With `--mirror-dynamo-costs`
//! (default) the lanes pay what Dynamo's harness charges every backend: each event arrives as an
//! owned payload in Dynamo's block layout (40 bytes per block, allocated before the trial) that the
//! lane converts into this crate's 16-byte blocks and frees after the apply, and each lookup copies
//! its hashes into this crate's hash type, as the SMG adapter does inside Dynamo's binary. The
//! binary links mimalloc, as Dynamo's bench binary does.
#![expect(clippy::expect_used, clippy::print_stdout, clippy::print_stderr)]
// The harness pins threads and sleeps to absolute monotonic deadlines through libc, which the
// standard library does not expose; the five calls are wrapped in small checked helpers below.
#![expect(unsafe_code)]
#![recursion_limit = "256"]

use std::{
    collections::BTreeMap,
    hint::black_box,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Mutex,
    },
    thread,
    time::Instant,
};

use clap::{Parser, ValueEnum};
use kv_index::{
    ContentHash, PositionalIndexer, ReferenceIndexer, RunBlockMap, RunIndex, SequenceHash,
    StoredBlock, WorkerBlockMap,
};
use rustc_hash::FxHashMap;
use serde_json::json;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const MAGIC: &[u8; 8] = b"SMGMCK01";
const EMPTY_OPERATION_ID: u32 = u32::MAX;
const WARMUP_QUERIES: usize = 128;

// ---------------------------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
struct Totals {
    requests: u64,
    stored_events: u64,
    removed_events: u64,
    cleared_events: u64,
    request_blocks: u64,
    stored_blocks: u64,
    removed_blocks: u64,
}

impl Totals {
    fn events(&self) -> u64 {
        self.stored_events + self.removed_events + self.cleared_events
    }
    fn block_ops(&self) -> u64 {
        self.request_blocks + self.stored_blocks + self.removed_blocks
    }
}

#[derive(Clone, Copy, Debug)]
enum OpKind {
    Query {
        start: u64,
        len: u32,
    },
    Stored {
        dp_rank: u32,
        parent: Option<u64>,
        start: u64,
        len: u32,
    },
    Removed {
        dp_rank: u32,
        start: u64,
        len: u32,
    },
    Cleared {
        dp_rank: u32,
    },
}

#[derive(Clone, Copy, Debug)]
struct Op {
    id: u32,
    deadline_ns: u64,
    worker: u64,
    kind: OpKind,
}

impl Op {
    fn is_query(&self) -> bool {
        matches!(self.kind, OpKind::Query { .. })
    }
    fn dp_rank(&self) -> u32 {
        match self.kind {
            OpKind::Query { .. } => 0,
            OpKind::Stored { dp_rank, .. }
            | OpKind::Removed { dp_rank, .. }
            | OpKind::Cleared { dp_rank } => dp_rank,
        }
    }
}

struct Corpus {
    block_size: u32,
    reference_window_ns: u64,
    trace_duplication_factor: u64,
    trace_length_factor: u64,
    inference_worker_duplication_factor: u64,
    logical_workers: u64,
    totals: Totals,
    trace_path: String,
    /// blake3 of the corpus file, recorded in the result's provenance.
    file_blake3: String,
    hashes: Box<[ContentHash]>,
    blocks: Box<[StoredBlock]>,
    removed: Box<[SequenceHash]>,
    ops: Vec<Op>,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| anyhow::anyhow!("corpus truncated at byte {}", self.at))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }
    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> anyhow::Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> anyhow::Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

fn load_corpus(path: &str) -> anyhow::Result<Corpus> {
    let bytes = std::fs::read(path)?;
    let file_blake3 = blake3::hash(&bytes).to_hex().to_string();
    let mut c = Cursor {
        bytes: &bytes,
        at: 0,
    };
    anyhow::ensure!(c.take(8)? == MAGIC, "not an SMG Mooncake corpus: {path}");
    let version = c.u32()?;
    anyhow::ensure!(version == 1, "unsupported corpus format version {version}");
    let block_size = c.u32()?;
    let reference_window_ns = c.u64()?;
    let trace_duplication_factor = c.u64()?;
    let trace_length_factor = c.u64()?;
    let inference_worker_duplication_factor = c.u64()?;
    let logical_workers = c.u64()?;
    let totals = Totals {
        requests: c.u64()?,
        stored_events: c.u64()?,
        removed_events: c.u64()?,
        cleared_events: c.u64()?,
        request_blocks: c.u64()?,
        stored_blocks: c.u64()?,
        removed_blocks: c.u64()?,
    };
    let path_len = c.u64()? as usize;
    let trace_path = String::from_utf8_lossy(c.take(path_len)?).into_owned();
    let n = c.u64()? as usize;
    let mut hashes = Vec::with_capacity(n);
    for _ in 0..n {
        hashes.push(ContentHash(c.u64()?));
    }
    let n = c.u64()? as usize;
    let mut blocks = Vec::with_capacity(n);
    for _ in 0..n {
        let seq = c.u64()?;
        let content = c.u64()?;
        blocks.push(StoredBlock {
            seq_hash: SequenceHash(seq),
            content_hash: ContentHash(content),
        });
    }
    let n = c.u64()? as usize;
    let mut removed = Vec::with_capacity(n);
    for _ in 0..n {
        removed.push(SequenceHash(c.u64()?));
    }
    let n = c.u64()? as usize;
    let mut ops = Vec::with_capacity(n);
    for _ in 0..n {
        let id = c.u32()?;
        let deadline_ns = c.u64()?;
        let worker = c.u64()?;
        let kind = match c.u8()? {
            0 => OpKind::Query {
                start: c.u64()?,
                len: c.u32()?,
            },
            1 => {
                let dp_rank = c.u32()?;
                let _event_id = c.u64()?;
                let has_parent = c.u8()? != 0;
                let parent = c.u64()?;
                let _has_start = c.u8()?;
                let _start_position = c.u32()?;
                OpKind::Stored {
                    dp_rank,
                    parent: has_parent.then_some(parent),
                    start: c.u64()?,
                    len: c.u32()?,
                }
            }
            2 => {
                let dp_rank = c.u32()?;
                let _event_id = c.u64()?;
                OpKind::Removed {
                    dp_rank,
                    start: c.u64()?,
                    len: c.u32()?,
                }
            }
            3 => {
                let dp_rank = c.u32()?;
                let _event_id = c.u64()?;
                OpKind::Cleared { dp_rank }
            }
            other => anyhow::bail!("unknown operation kind {other}"),
        };
        ops.push(Op {
            id,
            deadline_ns,
            worker,
            kind,
        });
    }
    anyhow::ensure!(c.at == bytes.len(), "trailing bytes in corpus");
    // The ids are the sorted positions; verify the invariants the replay relies on.
    for (expected, op) in ops.iter().enumerate() {
        anyhow::ensure!(op.id as usize == expected, "operation ids are not dense");
    }
    anyhow::ensure!(
        ops.windows(2).all(|w| w[0].deadline_ns <= w[1].deadline_ns),
        "operations are not deadline-sorted"
    );
    let mut recount = Totals::default();
    for op in &ops {
        match op.kind {
            OpKind::Query { len, .. } => {
                recount.requests += 1;
                recount.request_blocks += u64::from(len);
            }
            OpKind::Stored { len, .. } => {
                recount.stored_events += 1;
                recount.stored_blocks += u64::from(len);
            }
            OpKind::Removed { len, .. } => {
                recount.removed_events += 1;
                recount.removed_blocks += u64::from(len);
            }
            OpKind::Cleared { .. } => recount.cleared_events += 1,
        }
    }
    anyhow::ensure!(
        recount.block_ops() == totals.block_ops() && recount.events() == totals.events(),
        "corpus totals disagree with its operations"
    );
    Ok(Corpus {
        block_size,
        reference_window_ns,
        trace_duplication_factor,
        trace_length_factor,
        inference_worker_duplication_factor,
        logical_workers,
        totals,
        trace_path,
        file_blake3,
        hashes: hashes.into_boxed_slice(),
        blocks: blocks.into_boxed_slice(),
        removed: removed.into_boxed_slice(),
        ops,
    })
}

// ---------------------------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------------------------

/// What a backend must offer the replay. `Lane` is the per-event-lane state (SMG keeps a block
/// map per worker in the lane that owns the worker, like the gateway's event monitor). Workers
/// are `(worker_id, dp_rank)` pairs as Dynamo keys them; a backend interns them as it likes.
trait ReplayBackend: Send + Sync + 'static {
    type Lane: Send;
    fn name(&self) -> &'static str;
    fn new_lane(&self) -> Self::Lane;
    /// Apply one stored event; `false` when the backend rejected it (counted, as Dynamo's lanes
    /// count rejected events).
    fn apply_stored(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        blocks: &[StoredBlock],
        parent: Option<SequenceHash>,
    ) -> bool;
    fn apply_removed(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        hashes: &[SequenceHash],
    ) -> bool;
    fn apply_cleared(&self, lane: &mut Self::Lane, worker: (u64, u32)) -> bool;
    /// Answer one lookup; the return value is only consumed by `black_box`.
    fn lookup(&self, hashes: &[ContentHash]) -> usize;
}

/// One stored block as Dynamo's lanes receive it: two hashes and an always-empty multimodal slot.
struct WireBlock {
    block_hash: u64,
    tokens_hash: u64,
    /// Never set by the Mooncake trace; present so the record has Dynamo's size.
    #[expect(dead_code)]
    mm_extra_info: Option<Vec<u64>>,
}
const _: () = assert!(size_of::<WireBlock>() == 40);

/// The owned event payload an issuer moves to a lane under `--mirror-dynamo-costs`; `None` when
/// the lane reads the event from the corpus slabs instead.
enum Payload {
    None,
    Stored(Vec<WireBlock>),
    Removed(Vec<u64>),
}

struct EventMsg {
    id: u32,
    payload: Payload,
}

struct Positional {
    inner: PositionalIndexer,
}

struct PositionalLane {
    workers: FxHashMap<(u64, u32), (u32, WorkerBlockMap)>,
}

impl ReplayBackend for Positional {
    type Lane = PositionalLane;

    fn name(&self) -> &'static str {
        "smg-positional"
    }

    fn new_lane(&self) -> Self::Lane {
        PositionalLane {
            workers: FxHashMap::default(),
        }
    }

    fn apply_stored(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        blocks: &[StoredBlock],
        parent: Option<SequenceHash>,
    ) -> bool {
        let (smg_id, blocks_map) = lane.workers.entry(worker).or_insert_with(|| {
            let id = self
                .inner
                .intern_worker(&format!("{}:{}", worker.0, worker.1))
                .expect("worker id space");
            (id, WorkerBlockMap::default())
        });
        self.inner
            .apply_stored(*smg_id, blocks, parent, blocks_map)
            .is_ok()
    }

    fn apply_removed(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        hashes: &[SequenceHash],
    ) -> bool {
        let Some((smg_id, blocks_map)) = lane.workers.get_mut(&worker) else {
            return false;
        };
        self.inner.apply_removed(*smg_id, hashes, blocks_map);
        true
    }

    fn apply_cleared(&self, lane: &mut Self::Lane, worker: (u64, u32)) -> bool {
        if let Some((smg_id, blocks_map)) = lane.workers.get_mut(&worker) {
            self.inner.apply_cleared(*smg_id, blocks_map);
        }
        true
    }

    fn lookup(&self, hashes: &[ContentHash]) -> usize {
        self.inner.find_matches(hashes, false).scores.len()
    }
}

struct Run {
    inner: RunIndex,
}

struct RunLane {
    workers: FxHashMap<(u64, u32), (u32, RunBlockMap)>,
}

impl ReplayBackend for Run {
    type Lane = RunLane;

    fn name(&self) -> &'static str {
        "smg-run"
    }

    fn new_lane(&self) -> Self::Lane {
        RunLane {
            workers: FxHashMap::default(),
        }
    }

    fn apply_stored(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        blocks: &[StoredBlock],
        parent: Option<SequenceHash>,
    ) -> bool {
        let (smg_id, blocks_map) = lane.workers.entry(worker).or_insert_with(|| {
            let id = self
                .inner
                .intern_worker(&format!("{}:{}", worker.0, worker.1))
                .expect("worker slots; raise --max-workers");
            (id, RunBlockMap::default())
        });
        self.inner
            .apply_stored(*smg_id, blocks, parent, blocks_map)
            .is_ok()
    }

    fn apply_removed(
        &self,
        lane: &mut Self::Lane,
        worker: (u64, u32),
        hashes: &[SequenceHash],
    ) -> bool {
        let Some((smg_id, blocks_map)) = lane.workers.get_mut(&worker) else {
            return false;
        };
        self.inner.apply_removed(*smg_id, hashes, blocks_map);
        true
    }

    fn apply_cleared(&self, lane: &mut Self::Lane, worker: (u64, u32)) -> bool {
        if let Some((smg_id, blocks_map)) = lane.workers.get_mut(&worker) {
            self.inner.apply_cleared(*smg_id, blocks_map);
        }
        true
    }

    fn lookup(&self, hashes: &[ContentHash]) -> usize {
        let mut scored = 0usize;
        self.inner
            .score_into(hashes, |content| content.0, false, |_, _| scored += 1);
        scored
    }
}

struct Reference {
    inner: Mutex<ReferenceIndexer>,
    ids: Mutex<FxHashMap<(u64, u32), u32>>,
}

impl Reference {
    fn id(&self, key: (u64, u32)) -> u32 {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        let next = ids.len() as u32;
        *ids.entry(key).or_insert(next)
    }
}

impl ReplayBackend for Reference {
    type Lane = ();

    fn name(&self) -> &'static str {
        "smg-reference"
    }

    fn new_lane(&self) -> Self::Lane {}

    fn apply_stored(
        &self,
        _lane: &mut Self::Lane,
        worker: (u64, u32),
        blocks: &[StoredBlock],
        parent: Option<SequenceHash>,
    ) -> bool {
        let worker = self.id(worker);
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply_stored(worker, blocks, parent)
            .is_ok()
    }

    fn apply_removed(
        &self,
        _lane: &mut Self::Lane,
        worker: (u64, u32),
        hashes: &[SequenceHash],
    ) -> bool {
        let worker = self.id(worker);
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply_removed(worker, hashes);
        true
    }

    fn apply_cleared(&self, _lane: &mut Self::Lane, worker: (u64, u32)) -> bool {
        let worker = self.id(worker);
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply_cleared(worker);
        true
    }

    fn lookup(&self, hashes: &[ContentHash]) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .find_matches(hashes)
            .len()
    }
}

/// Discards events and answers lookups with the sequence length: measures the harness alone
/// (issuers, queues, lanes, timing) with no indexer behind it, which bounds what any backend can
/// be measured at on a given layout.
struct Null;

impl ReplayBackend for Null {
    type Lane = ();

    fn name(&self) -> &'static str {
        "null"
    }

    fn new_lane(&self) -> Self::Lane {}

    fn apply_stored(
        &self,
        _lane: &mut Self::Lane,
        _worker: (u64, u32),
        _blocks: &[StoredBlock],
        _parent: Option<SequenceHash>,
    ) -> bool {
        true
    }

    fn apply_removed(
        &self,
        _lane: &mut Self::Lane,
        _worker: (u64, u32),
        _hashes: &[SequenceHash],
    ) -> bool {
        true
    }

    fn apply_cleared(&self, _lane: &mut Self::Lane, _worker: (u64, u32)) -> bool {
        true
    }

    fn lookup(&self, hashes: &[ContentHash]) -> usize {
        hashes.len()
    }
}

// ---------------------------------------------------------------------------------------------
// Lanes
// ---------------------------------------------------------------------------------------------

struct QueryLane {
    slots: Box<[AtomicU32]>,
    published: AtomicUsize,
    closed: AtomicBool,
    consumer: Mutex<Option<thread::Thread>>,
}

impl QueryLane {
    fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity)
                .map(|_| AtomicU32::new(EMPTY_OPERATION_ID))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            published: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            consumer: Mutex::new(None),
        }
    }
    fn wake(&self) {
        if let Some(consumer) = self
            .consumer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            consumer.unpark();
        }
    }
    fn publish(&self, count: usize) {
        self.published.store(count, Ordering::Release);
        self.wake();
    }
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake();
    }
}

#[derive(Clone, Copy, Default)]
struct QueryCompletion {
    id: u32,
    started_ns: u64,
    finished_ns: u64,
}

fn query_lane_worker<B: ReplayBackend>(
    backend: Arc<B>,
    lane: Arc<QueryLane>,
    corpus: Arc<Corpus>,
    epoch: Instant,
    cpus: Arc<[usize]>,
    mirror: bool,
) -> (Vec<QueryCompletion>, Option<&'static str>, u64) {
    let _ = pin_current_thread(&cpus);
    let cpu_started = thread_cpu_time_ns();
    *lane.consumer.lock().unwrap_or_else(|e| e.into_inner()) = Some(thread::current());
    let mut completions = Vec::with_capacity(lane.slots.len());
    let mut consumed = 0usize;
    let mut failure = None;
    loop {
        let published = lane.published.load(Ordering::Acquire);
        while consumed < published {
            let id = lane.slots[consumed].load(Ordering::Relaxed);
            if id == EMPTY_OPERATION_ID {
                failure = Some("query_lane_missing_published_id");
                break;
            }
            let OpKind::Query { start, len } = corpus.ops[id as usize].kind else {
                failure = Some("query_lane_non_query_id");
                break;
            };
            let hashes = &corpus.hashes[start as usize..start as usize + len as usize];
            let started_ns = elapsed_ns(epoch);
            if mirror {
                // The adapter's copy into this crate's hash type, inside the timed lookup.
                let owned: Vec<ContentHash> = hashes.to_vec();
                black_box(backend.lookup(&owned));
            } else {
                black_box(backend.lookup(hashes));
            }
            let finished_ns = elapsed_ns(epoch);
            completions.push(QueryCompletion {
                id,
                started_ns,
                finished_ns,
            });
            consumed += 1;
        }
        if failure.is_some() {
            break;
        }
        if lane.closed.load(Ordering::Acquire) && consumed == lane.published.load(Ordering::Acquire)
        {
            break;
        }
        if consumed == lane.published.load(Ordering::Acquire)
            && !lane.closed.load(Ordering::Acquire)
        {
            thread::park();
        }
    }
    (
        completions,
        failure,
        thread_cpu_time_ns().saturating_sub(cpu_started),
    )
}

#[derive(Clone, Copy, Default)]
struct EventCompletion {
    id: u32,
    finished_ns: u64,
    ok: bool,
}

fn event_lane_worker<B: ReplayBackend>(
    backend: Arc<B>,
    receiver: mpsc::Receiver<EventMsg>,
    corpus: Arc<Corpus>,
    epoch: Instant,
    cpus: Arc<[usize]>,
    expected: usize,
) -> (Vec<EventCompletion>, u64) {
    let _ = pin_current_thread(&cpus);
    let cpu_started = thread_cpu_time_ns();
    let mut lane = backend.new_lane();
    let mut completions = Vec::with_capacity(expected);
    while let Ok(EventMsg { id, payload }) = receiver.recv() {
        let op = &corpus.ops[id as usize];
        let worker = (op.worker, op.dp_rank());
        let ok = match (&op.kind, payload) {
            (&OpKind::Stored { parent, .. }, Payload::Stored(wire)) => {
                // The adapter's copy, Dynamo's records into this crate's blocks; the payload is
                // freed after the apply, as Dynamo's lanes free the event they were handed.
                let owned: Vec<StoredBlock> = wire
                    .iter()
                    .map(|block| StoredBlock {
                        seq_hash: SequenceHash(block.block_hash),
                        content_hash: ContentHash(block.tokens_hash),
                    })
                    .collect();
                let ok = backend.apply_stored(&mut lane, worker, &owned, parent.map(SequenceHash));
                drop(wire);
                ok
            }
            (
                &OpKind::Stored {
                    parent, start, len, ..
                },
                Payload::None,
            ) => backend.apply_stored(
                &mut lane,
                worker,
                &corpus.blocks[start as usize..start as usize + len as usize],
                parent.map(SequenceHash),
            ),
            (&OpKind::Removed { .. }, Payload::Removed(wire)) => {
                let owned: Vec<SequenceHash> =
                    wire.iter().map(|&hash| SequenceHash(hash)).collect();
                let ok = backend.apply_removed(&mut lane, worker, &owned);
                drop(wire);
                ok
            }
            (&OpKind::Removed { start, len, .. }, Payload::None) => backend.apply_removed(
                &mut lane,
                worker,
                &corpus.removed[start as usize..start as usize + len as usize],
            ),
            (&OpKind::Cleared { .. }, _) => backend.apply_cleared(&mut lane, worker),
            _ => false,
        };
        completions.push(EventCompletion {
            id,
            finished_ns: elapsed_ns(epoch),
            ok,
        });
    }
    (
        completions,
        thread_cpu_time_ns().saturating_sub(cpu_started),
    )
}

// ---------------------------------------------------------------------------------------------
// Issuers
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct IssueRecord {
    scheduled_ns: u64,
    accepted_ns: u64,
    is_query: bool,
    accepted: bool,
}

struct Clock {
    epoch: Instant,
    monotonic_epoch_ns: u64,
    spin_ns: u64,
}

impl Clock {
    fn new(spin_ns: u64) -> anyhow::Result<Self> {
        Ok(Self {
            epoch: Instant::now(),
            monotonic_epoch_ns: monotonic_now_ns()?,
            spin_ns,
        })
    }
    fn now_ns(&self) -> u64 {
        elapsed_ns(self.epoch)
    }
    fn wait_until(&self, target_ns: u64) {
        let sleep_target = target_ns.saturating_sub(self.spin_ns);
        if sleep_target > self.now_ns() {
            sleep_until_monotonic(self.monotonic_epoch_ns.saturating_add(sleep_target));
        }
        while self.now_ns() < target_ns {
            std::hint::spin_loop();
        }
    }
}

fn elapsed_ns(epoch: Instant) -> u64 {
    epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn monotonic_now_ns() -> anyhow::Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes a timespec it is given a valid pointer to.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    anyhow::ensure!(rc == 0, "clock_gettime failed");
    Ok((ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64))
}

fn sleep_until_monotonic(target_ns: u64) {
    let request = libc::timespec {
        tv_sec: (target_ns / 1_000_000_000) as libc::time_t,
        tv_nsec: (target_ns % 1_000_000_000) as libc::c_long,
    };
    loop {
        // SAFETY: an absolute sleep on CLOCK_MONOTONIC with a valid timespec and no remainder.
        let rc = unsafe {
            libc::clock_nanosleep(
                libc::CLOCK_MONOTONIC,
                libc::TIMER_ABSTIME,
                &request,
                std::ptr::null_mut(),
            )
        };
        if rc != libc::EINTR {
            return;
        }
    }
}

fn pin_current_thread(cpus: &[usize]) -> std::io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    // SAFETY: a zeroed cpu_set_t is a valid empty set; CPU_SET/CPU_ZERO only touch that set;
    // sched_setaffinity(0) applies it to the calling thread.
    unsafe {
        let mut set = std::mem::zeroed::<libc::cpu_set_t>();
        libc::CPU_ZERO(&mut set);
        for &cpu in cpus {
            libc::CPU_SET(cpu, &mut set);
        }
        let rc = libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set);
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn thread_cpu_time_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: as in monotonic_now_ns.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

fn parse_cpu_list(value: &str) -> anyhow::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((a, b)) = part.split_once('-') {
            let (a, b): (usize, usize) = (a.parse()?, b.parse()?);
            anyhow::ensure!(a <= b, "descending CPU range {part}");
            cpus.extend(a..=b);
        } else {
            cpus.push(part.parse()?);
        }
    }
    Ok(cpus)
}

/// Dynamo's `contiguous_worker_issuer`: contiguous worker ranges per event issuer.
fn event_issuer_for(worker: u64, logical_workers: u64, issuers: usize) -> usize {
    let per = (logical_workers as usize).div_ceil(issuers).max(1);
    ((worker as usize) / per).min(issuers - 1)
}

struct Shared {
    clock: Clock,
    start_ns: AtomicUsize,
    /// Queries of each deadline group not yet published; its events wait for zero.
    deadline_pending: Box<[AtomicU32]>,
    peer_failed: AtomicBool,
}

fn issue_queries(
    shared: &Shared,
    corpus: &Corpus,
    dispatch: &[(u32, u32, u16)], // (id, deadline_group, lane)
    lanes: &[Arc<QueryLane>],
    records: &mut Vec<(u32, IssueRecord)>,
) -> (u64, Option<&'static str>) {
    let cpu_started = thread_cpu_time_ns();
    let start_ns = shared.start_ns.load(Ordering::Acquire) as u64;
    let mut cursors = vec![0usize; lanes.len()];
    let mut touched = Vec::with_capacity(lanes.len());
    let mut touched_flags = vec![false; lanes.len()];
    let mut i = 0usize;
    let mut failure = None;
    while i < dispatch.len() && failure.is_none() {
        let deadline_ns = corpus.ops[dispatch[i].0 as usize].deadline_ns;
        let group = dispatch[i].1 as usize;
        shared
            .clock
            .wait_until(start_ns.saturating_add(deadline_ns));
        touched.clear();
        let group_start = i;
        while i < dispatch.len() && corpus.ops[dispatch[i].0 as usize].deadline_ns == deadline_ns {
            let (id, _, lane) = dispatch[i];
            let lane = lane as usize;
            let Some(slot) = lanes[lane].slots.get(cursors[lane]) else {
                failure = Some("issuer_query_lane_overflow");
                break;
            };
            slot.store(id, Ordering::Relaxed);
            cursors[lane] += 1;
            if !touched_flags[lane] {
                touched_flags[lane] = true;
                touched.push(lane);
            }
            records.push((
                id,
                IssueRecord {
                    scheduled_ns: start_ns.saturating_add(deadline_ns),
                    accepted_ns: shared.clock.now_ns(),
                    is_query: true,
                    accepted: true,
                },
            ));
            i += 1;
        }
        for &lane in &touched {
            lanes[lane].publish(cursors[lane]);
            touched_flags[lane] = false;
        }
        if failure.is_some() {
            break;
        }
        shared.deadline_pending[group].fetch_sub((i - group_start) as u32, Ordering::Release);
    }
    if failure.is_some() {
        shared.peer_failed.store(true, Ordering::Release);
    }
    (thread_cpu_time_ns().saturating_sub(cpu_started), failure)
}

/// One event in an issuer's dispatch: its operation, deadline group, lane and (when mirroring
/// Dynamo's costs) the owned payload the lane receives.
struct EventDispatch {
    id: u32,
    group: u32,
    lane: u16,
    payload: Payload,
}

fn issue_events(
    shared: &Shared,
    corpus: &Corpus,
    dispatch: Vec<EventDispatch>,
    senders: &[mpsc::Sender<EventMsg>],
    records: &mut Vec<(u32, IssueRecord)>,
) -> (u64, Option<&'static str>) {
    let cpu_started = thread_cpu_time_ns();
    let start_ns = shared.start_ns.load(Ordering::Acquire) as u64;
    let mut entries = dispatch.into_iter().peekable();
    let mut failure = None;
    while let Some(head) = entries.peek() {
        let deadline_ns = corpus.ops[head.id as usize].deadline_ns;
        let group = head.group as usize;
        shared
            .clock
            .wait_until(start_ns.saturating_add(deadline_ns));
        // Queries of this deadline are published before its events.
        while shared.deadline_pending[group].load(Ordering::Acquire) != 0 {
            if shared.peer_failed.load(Ordering::Acquire) {
                failure = Some("issuer_peer_failed");
                break;
            }
            std::hint::spin_loop();
        }
        if failure.is_some() {
            break;
        }
        while entries
            .peek()
            .is_some_and(|entry| corpus.ops[entry.id as usize].deadline_ns == deadline_ns)
        {
            let Some(entry) = entries.next() else {
                break;
            };
            let message = EventMsg {
                id: entry.id,
                payload: entry.payload,
            };
            if senders[entry.lane as usize].send(message).is_err() {
                failure = Some("issuer_event_lane_offline");
                break;
            }
            records.push((
                entry.id,
                IssueRecord {
                    scheduled_ns: start_ns.saturating_add(deadline_ns),
                    accepted_ns: shared.clock.now_ns(),
                    is_query: false,
                    accepted: true,
                },
            ));
        }
        if failure.is_some() {
            break;
        }
    }
    if failure.is_some() {
        shared.peer_failed.store(true, Ordering::Release);
    }
    (thread_cpu_time_ns().saturating_sub(cpu_started), failure)
}

// ---------------------------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BackendKind {
    /// This crate's event-driven PositionalIndexer.
    Positional,
    /// The single-threaded reference indexer (small corpora only).
    Reference,
    /// This crate's run-compressed RunIndex (worker slots from `--max-workers`).
    Run,
    /// No indexer: the harness's own ceiling on this layout.
    Null,
}

#[derive(Parser, Debug)]
#[command(about = "Open-loop replay of an exported Mooncake indexer corpus, by Dynamo's method")]
struct Args {
    /// Corpus file written by Dynamo's `mooncake_bench --export-corpus`.
    corpus: String,
    #[arg(long, value_enum, default_value = "positional")]
    backend: BackendKind,
    /// Jump size of the positional indexer's lookup.
    #[arg(long, default_value = "8")]
    jump_size: usize,
    /// Worker slots of the run index (one coverage bit per slot per run, at most 1024).
    #[arg(long, default_value = "256")]
    max_workers: usize,
    /// Replay window in milliseconds; deadlines are rescaled linearly from the corpus's
    /// reference window when they differ.
    #[arg(long, conflicts_with = "offered_block_ops_per_sec")]
    benchmark_duration_ms: Option<u64>,
    /// Offered rate in block ops per second: sets the window from the corpus's block-op total
    /// (window = total / rate), the knob a sustained-throughput threshold search moves.
    #[arg(long)]
    offered_block_ops_per_sec: Option<f64>,
    #[arg(long, default_value = "128")]
    query_lanes: usize,
    /// Event lanes (OS threads applying events), Dynamo's `--num-event-workers`.
    #[arg(long, default_value = "64")]
    event_lanes: usize,
    /// Event issuer threads; events are sharded by contiguous worker ranges.
    #[arg(long, default_value = "4")]
    issuer_threads: usize,
    /// CPUs for the event issuers (one per issuer thread when given).
    #[arg(long)]
    issuer_cpus: Option<String>,
    /// Query issuer threads; query lanes are sharded over them in contiguous ranges (Dynamo
    /// issues all queries from one thread, which caps its generator near 1.5B block ops/s here).
    #[arg(long, default_value = "1")]
    query_issuer_threads: usize,
    /// CPUs for the query issuers (one per thread when given); `--query-issuer-cpu` is an alias.
    #[arg(long, alias = "query-issuer-cpu")]
    query_issuer_cpus: Option<String>,
    /// CPUs for query and event lanes.
    #[arg(long)]
    backend_cpus: Option<String>,
    #[arg(long, default_value = "100")]
    issuer_spin_us: u64,
    #[arg(long, default_value = "250")]
    issue_lag_diagnostic_threshold_us: u64,
    #[arg(long, default_value = "5000")]
    pre_run_quiescence_ms: u64,
    /// Pin each event lane to one backend CPU (round robin) instead of letting it float over
    /// the set; a diagnostic for scheduler effects, not Dynamo's method.
    #[arg(long)]
    pin_event_lanes: bool,
    /// Give each worker's events to the issuer whose lane range holds the worker's lane (issuer k
    /// feeds lanes [k * lanes / issuers, (k + 1) * lanes / issuers)) instead of Dynamo's contiguous
    /// worker-id ranges; with `--pin-event-lanes` and the issuer CPUs listed in lane order every
    /// issuer then sits on the socket of the lanes it feeds. A two-socket diagnostic, not Dynamo's
    /// method.
    #[arg(long)]
    issuer_by_lane: bool,
    /// Charge what Dynamo's harness charges every backend: each event arrives as an owned payload
    /// in Dynamo's block layout (40 bytes per block, allocated before the trial) that the lane
    /// converts into this crate's blocks and frees after the apply, and each lookup copies its
    /// hashes into this crate's hash type. Off: lanes read the corpus slabs and copy nothing.
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    mirror_dynamo_costs: bool,
    #[arg(long, default_value = "mooncake_replay_result.json")]
    result_json_output: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let corpus = load_corpus(&args.corpus)?;
    let window_ns = match (args.benchmark_duration_ms, args.offered_block_ops_per_sec) {
        (Some(ms), _) => ms * 1_000_000,
        (None, Some(rate)) => {
            anyhow::ensure!(rate > 0.0, "--offered-block-ops-per-sec must be positive");
            (corpus.totals.block_ops() as f64 / rate * 1e9).round() as u64
        }
        (None, None) => corpus.reference_window_ns,
    };
    match args.backend {
        BackendKind::Positional => run(
            &args,
            corpus,
            window_ns,
            Arc::new(Positional {
                inner: PositionalIndexer::new(args.jump_size),
            }),
        ),
        BackendKind::Reference => {
            if corpus.ops.len() > 2_000_000 {
                eprintln!(
                    "warning: the reference backend is single-threaded; this corpus is large"
                );
            }
            run(
                &args,
                corpus,
                window_ns,
                Arc::new(Reference {
                    inner: Mutex::new(ReferenceIndexer::new()),
                    ids: Mutex::new(FxHashMap::default()),
                }),
            )
        }
        BackendKind::Run => run(
            &args,
            corpus,
            window_ns,
            Arc::new(Run {
                inner: RunIndex::with_max_workers(args.max_workers),
            }),
        ),
        BackendKind::Null => run(&args, corpus, window_ns, Arc::new(Null)),
    }
}

fn run<B: ReplayBackend>(
    args: &Args,
    mut corpus: Corpus,
    window_ns: u64,
    backend: Arc<B>,
) -> anyhow::Result<()> {
    anyhow::ensure!(args.query_lanes > 0 && args.event_lanes > 0 && args.issuer_threads > 0);
    let backend_cpus: Arc<[usize]> = args
        .backend_cpus
        .as_deref()
        .map(parse_cpu_list)
        .transpose()?
        .unwrap_or_default()
        .into();
    let issuer_cpus = args
        .issuer_cpus
        .as_deref()
        .map(parse_cpu_list)
        .transpose()?
        .unwrap_or_default();
    let issuer_threads = if issuer_cpus.is_empty() {
        args.issuer_threads
    } else {
        issuer_cpus.len()
    };
    let query_cpus = args
        .query_issuer_cpus
        .as_deref()
        .map(parse_cpu_list)
        .transpose()?
        .unwrap_or_default();
    let query_issuers = if query_cpus.is_empty() {
        args.query_issuer_threads.max(1)
    } else {
        query_cpus.len()
    };
    // Issuers and lanes must not share cores: a lane on an issuer's core eats the issue schedule
    // and the trial measures the layout mistake, not the indexer (Dynamo's runner refuses too).
    if !backend_cpus.is_empty() {
        let overlap: Vec<usize> = issuer_cpus
            .iter()
            .chain(query_cpus.iter())
            .copied()
            .filter(|cpu| backend_cpus.contains(cpu))
            .collect();
        anyhow::ensure!(
            overlap.is_empty(),
            "issuer CPUs {overlap:?} overlap the backend CPU set; give lanes their own cores"
        );
    }
    // The layout, first line of every run log, so the provenance shows it at a glance.
    println!(
        "layout: event issuers {} on {:?}, query issuers {} on {:?}, lanes {} event + {} query on {:?} ({} cores)",
        issuer_threads,
        issuer_cpus,
        query_issuers,
        query_cpus,
        args.event_lanes,
        args.query_lanes,
        backend_cpus,
        backend_cpus.len(),
    );
    if window_ns != corpus.reference_window_ns {
        let reference = corpus.reference_window_ns.max(1) as u128;
        for op in &mut corpus.ops {
            op.deadline_ns = ((op.deadline_ns as u128 * window_ns as u128) / reference) as u64;
        }
    }
    // Lane assignment and deadline groups, as Dynamo's prepare_open_loop_trial.
    let mirror = args.mirror_dynamo_costs;
    let mut lane_capacities = vec![0usize; args.query_lanes];
    let mut deadline_query_counts: Vec<u32> = Vec::new();
    let mut previous_deadline = None;
    let mut event_lane_of: FxHashMap<(u64, u32), u16> = FxHashMap::default();
    let mut event_lane_expected = vec![0usize; args.event_lanes];
    let mut query_dispatch: Vec<Vec<(u32, u32, u16)>> = vec![Vec::new(); query_issuers];
    let mut event_dispatch: Vec<Vec<EventDispatch>> =
        (0..issuer_threads).map(|_| Vec::new()).collect();
    for op in &corpus.ops {
        if previous_deadline != Some(op.deadline_ns) {
            previous_deadline = Some(op.deadline_ns);
            deadline_query_counts.push(0);
        }
        let group = (deadline_query_counts.len() - 1) as u32;
        if op.is_query() {
            let lane = (op.worker as usize) % args.query_lanes;
            lane_capacities[lane] += 1;
            deadline_query_counts[group as usize] += 1;
            query_dispatch[lane * query_issuers / args.query_lanes].push((
                op.id,
                group,
                lane as u16,
            ));
        } else {
            let next = event_lane_of.len();
            let lane = *event_lane_of
                .entry((op.worker, op.dp_rank()))
                .or_insert((next % args.event_lanes) as u16);
            event_lane_expected[lane as usize] += 1;
            let shard = if args.issuer_by_lane {
                (lane as usize) * issuer_threads / args.event_lanes
            } else {
                event_issuer_for(op.worker, corpus.logical_workers, issuer_threads)
            };
            // Under --mirror-dynamo-costs the payload is built here, before the trial, as Dynamo's
            // preparation builds the owned events its issuers move to the lanes.
            let payload = match op.kind {
                OpKind::Stored { start, len, .. } if mirror => Payload::Stored(
                    corpus.blocks[start as usize..start as usize + len as usize]
                        .iter()
                        .map(|block| WireBlock {
                            block_hash: block.seq_hash.0,
                            tokens_hash: block.content_hash.0,
                            mm_extra_info: None,
                        })
                        .collect(),
                ),
                OpKind::Removed { start, len, .. } if mirror => Payload::Removed(
                    corpus.removed[start as usize..start as usize + len as usize]
                        .iter()
                        .map(|hash| hash.0)
                        .collect(),
                ),
                _ => Payload::None,
            };
            event_dispatch[shard].push(EventDispatch {
                id: op.id,
                group,
                lane,
                payload,
            });
        }
    }
    let deadline_pending: Box<[AtomicU32]> = deadline_query_counts
        .iter()
        .map(|&count| AtomicU32::new(count))
        .collect::<Vec<_>>()
        .into_boxed_slice();

    // Quiescence, as Dynamo: return preparation pages and let the allocator settle.
    // SAFETY: malloc_trim takes an integer pad and has no other preconditions.
    unsafe {
        libc::malloc_trim(0);
    }
    if args.pre_run_quiescence_ms > 0 {
        thread::sleep(std::time::Duration::from_millis(args.pre_run_quiescence_ms));
    }
    let corpus = Arc::new(corpus);
    // Page-touch the corpus once.
    let mut checksum = 0u64;
    for hash in &*corpus.hashes {
        checksum ^= hash.0;
    }
    for block in &*corpus.blocks {
        checksum ^= block.seq_hash.0 ^ block.content_hash.0;
    }
    for op in &corpus.ops {
        checksum ^= op.deadline_ns ^ op.worker ^ u64::from(op.id);
    }
    black_box(checksum);

    pin_current_thread(&backend_cpus)?;
    let clock = Clock::new(args.issuer_spin_us.saturating_mul(1_000))?;
    let epoch = clock.epoch;
    // Fixed lookup warm-up.
    for op in corpus
        .ops
        .iter()
        .filter(|op| op.is_query())
        .take(WARMUP_QUERIES)
    {
        if let OpKind::Query { start, len } = op.kind {
            black_box(
                backend.lookup(&corpus.hashes[start as usize..start as usize + len as usize]),
            );
        }
    }

    // Lanes.
    let lanes: Vec<Arc<QueryLane>> = lane_capacities
        .iter()
        .map(|&capacity| Arc::new(QueryLane::new(capacity)))
        .collect();
    let mut query_threads = Vec::with_capacity(lanes.len());
    for lane in &lanes {
        let (backend, lane, corpus, cpus) = (
            Arc::clone(&backend),
            Arc::clone(lane),
            Arc::clone(&corpus),
            Arc::clone(&backend_cpus),
        );
        query_threads.push(thread::spawn(move || {
            query_lane_worker(backend, lane, corpus, epoch, cpus, mirror)
        }));
    }
    // Wait until every query lane has registered its parker.
    for lane in &lanes {
        while lane
            .consumer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            thread::yield_now();
        }
    }
    let mut senders = Vec::with_capacity(args.event_lanes);
    let mut event_threads = Vec::with_capacity(args.event_lanes);
    for (idx, &expected) in event_lane_expected.iter().enumerate() {
        let (tx, rx) = mpsc::channel::<EventMsg>();
        senders.push(tx);
        let cpus: Arc<[usize]> = if args.pin_event_lanes && !backend_cpus.is_empty() {
            Arc::from(vec![backend_cpus[idx % backend_cpus.len()]])
        } else {
            Arc::clone(&backend_cpus)
        };
        let (backend, corpus) = (Arc::clone(&backend), Arc::clone(&corpus));
        event_threads.push(thread::spawn(move || {
            event_lane_worker(backend, rx, corpus, epoch, cpus, expected)
        }));
    }

    let shared = Shared {
        clock,
        start_ns: AtomicUsize::new(0),
        deadline_pending,
        peer_failed: AtomicBool::new(false),
    };
    let ready = Barrier::new(issuer_threads + query_issuers + 1);
    let start = Barrier::new(issuer_threads + query_issuers + 1);
    let (start_ns, outputs) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(issuer_threads + query_issuers);
        let (shared_ref, corpus_ref, lanes_ref, ready_ref, start_ref) =
            (&shared, &corpus, &lanes, &ready, &start);
        for (idx, dispatch) in query_dispatch.iter().enumerate() {
            let cpu = query_cpus.get(idx).copied();
            handles.push(scope.spawn(move || {
                let pin = cpu.map_or(Ok(()), |cpu| pin_current_thread(&[cpu]));
                let mut failure = pin.err().map(|_| "issuer_affinity");
                if failure.is_some() {
                    shared_ref.peer_failed.store(true, Ordering::Release);
                }
                ready_ref.wait();
                start_ref.wait();
                let mut records = Vec::with_capacity(dispatch.len());
                let (cpu_ns, f) = if failure.is_none() {
                    issue_queries(shared_ref, corpus_ref, dispatch, lanes_ref, &mut records)
                } else {
                    (0, None)
                };
                failure = failure.or(f);
                (records, cpu_ns, failure)
            }));
        }
        for (idx, dispatch) in event_dispatch.into_iter().enumerate() {
            let cpu = issuer_cpus.get(idx).copied();
            let senders = &senders;
            handles.push(scope.spawn(move || {
                let pin = cpu.map_or(Ok(()), |cpu| pin_current_thread(&[cpu]));
                let mut failure = pin.err().map(|_| "issuer_affinity");
                if failure.is_some() {
                    shared_ref.peer_failed.store(true, Ordering::Release);
                }
                ready_ref.wait();
                start_ref.wait();
                let mut records = Vec::with_capacity(dispatch.len());
                let (cpu_ns, f) = if failure.is_none() {
                    issue_events(shared_ref, corpus_ref, dispatch, senders, &mut records)
                } else {
                    (0, None)
                };
                failure = failure.or(f);
                (records, cpu_ns, failure)
            }));
        }
        ready.wait();
        let start_ns = shared.clock.now_ns().saturating_add(20_000_000);
        shared.start_ns.store(start_ns as usize, Ordering::Release);
        start.wait();
        let outputs: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("issuer thread panicked"))
            .collect();
        (start_ns, outputs)
    });
    let producer_stop_ns = shared.clock.now_ns();
    drop(senders);
    for lane in &lanes {
        lane.close();
    }
    let mut failure_reasons: Vec<String> = Vec::new();
    let mut query_results = Vec::with_capacity(lanes.len());
    let mut query_lane_cpu_ns = 0u64;
    for handle in query_threads {
        let (completions, failure, cpu_ns) = handle.join().expect("query lane panicked");
        if let Some(failure) = failure {
            failure_reasons.push(failure.to_string());
        }
        query_lane_cpu_ns += cpu_ns;
        query_results.push(completions);
    }
    let (event_results, event_lane_cpu_ns): (Vec<Vec<EventCompletion>>, Vec<u64>) = event_threads
        .into_iter()
        .map(|h| h.join().expect("event lane panicked"))
        .unzip();

    // Merge issue records.
    let n = corpus.ops.len();
    let mut records = vec![IssueRecord::default(); n];
    let mut issuer_cpu_ns = 0u64;
    for (local, cpu_ns, failure) in outputs {
        issuer_cpu_ns += cpu_ns;
        if let Some(failure) = failure {
            failure_reasons.push(failure.to_string());
        }
        for (id, record) in local {
            if records[id as usize].accepted {
                failure_reasons.push("duplicate_issue_record".to_string());
            }
            records[id as usize] = record;
        }
    }
    // Completions by id, with the order checks Dynamo makes.
    let mut query_done: Vec<Option<QueryCompletion>> = vec![None; n];
    for (lane_idx, completions) in query_results.iter().enumerate() {
        let expected: Vec<u32> = query_dispatch
            .iter()
            .flatten()
            .filter(|(_, _, lane)| *lane as usize == lane_idx)
            .map(|(id, _, _)| *id)
            .collect();
        let actual: Vec<u32> = completions.iter().map(|c| c.id).collect();
        if expected != actual {
            failure_reasons.push(format!("query_lane_order_{lane_idx}"));
        }
        for c in completions {
            if query_done[c.id as usize].replace(*c).is_some() {
                failure_reasons.push("duplicate_query_completion".to_string());
            }
        }
    }
    let mut event_done: Vec<Option<EventCompletion>> = vec![None; n];
    let mut actual_by_worker: BTreeMap<(u64, u32), Vec<u32>> = BTreeMap::new();
    let mut failed_events = 0usize;
    for completions in &event_results {
        for c in completions {
            let op = &corpus.ops[c.id as usize];
            actual_by_worker
                .entry((op.worker, op.dp_rank()))
                .or_default()
                .push(c.id);
            if !c.ok {
                failed_events += 1;
            }
            if event_done[c.id as usize].replace(*c).is_some() {
                failure_reasons.push("duplicate_event_completion".to_string());
            }
        }
    }
    let mut expected_by_worker: BTreeMap<(u64, u32), Vec<u32>> = BTreeMap::new();
    for op in corpus.ops.iter().filter(|op| !op.is_query()) {
        expected_by_worker
            .entry((op.worker, op.dp_rank()))
            .or_default()
            .push(op.id);
    }
    let mut fifo_violations = 0usize;
    for (worker, expected) in &expected_by_worker {
        if actual_by_worker.get(worker) != Some(expected) {
            fifo_violations += 1;
        }
    }
    if fifo_violations > 0 {
        failure_reasons.push(format!("event_worker_fifo_{fifo_violations}"));
    }

    let tolerance_ns = args.issue_lag_diagnostic_threshold_us.saturating_mul(1_000);
    let (mut read_lag, mut update_lag, mut queue_wait, mut service, mut query_e2e) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut update_acc_fin, mut update_e2e) = (Vec::new(), Vec::new());
    let (mut delayed_reads, mut delayed_updates, mut races) = (0usize, 0usize, 0usize);
    let mut query_edges = Vec::new();
    let mut update_edges = Vec::new();
    let mut last_completion = 0u64;
    let mut unissued = 0usize;
    let (mut queued_queries_at_stop, mut outstanding_updates_at_stop) = (0usize, 0usize);
    for (id, record) in records.iter().enumerate() {
        if !record.accepted {
            unissued += 1;
            continue;
        }
        let lag = record.accepted_ns.saturating_sub(record.scheduled_ns);
        if record.is_query {
            read_lag.push(lag);
            delayed_reads += usize::from(lag > tolerance_ns);
            let Some(c) = query_done[id] else {
                failure_reasons.push(format!("missing_query_completion_{id}"));
                continue;
            };
            last_completion = last_completion.max(c.finished_ns);
            queued_queries_at_stop += usize::from(
                record.accepted_ns <= producer_stop_ns && c.started_ns > producer_stop_ns,
            );
            queue_wait.push(c.started_ns.saturating_sub(record.accepted_ns));
            service.push(c.finished_ns.saturating_sub(c.started_ns));
            query_e2e.push(c.finished_ns.saturating_sub(record.scheduled_ns));
            query_edges.push((record.accepted_ns, 1i8));
            query_edges.push((c.started_ns.max(record.accepted_ns), -1i8));
        } else {
            update_lag.push(lag);
            delayed_updates += usize::from(lag > tolerance_ns);
            let Some(c) = event_done[id] else {
                failure_reasons.push(format!("missing_event_completion_{id}"));
                continue;
            };
            last_completion = last_completion.max(c.finished_ns);
            outstanding_updates_at_stop += usize::from(c.finished_ns > producer_stop_ns);
            if c.finished_ns < record.accepted_ns {
                races += 1;
            }
            update_acc_fin.push(c.finished_ns.saturating_sub(record.accepted_ns));
            update_e2e.push(c.finished_ns.saturating_sub(record.scheduled_ns));
            update_edges.push((record.accepted_ns, 1i8));
            update_edges.push((c.finished_ns.max(record.accepted_ns), -1i8));
        }
    }
    if unissued > 0 {
        failure_reasons.push(format!("unissued_operations_{unissued}"));
    }
    if failure_reasons.len() > 32 {
        failure_reasons.truncate(32);
        failure_reasons.push("...".to_string());
    }
    let end_ns = if last_completion > 0 {
        last_completion
    } else {
        producer_stop_ns
    };
    let issue_span_ns = records
        .iter()
        .filter(|r| r.accepted)
        .map(|r| r.accepted_ns)
        .max()
        .unwrap_or(start_ns)
        .saturating_sub(start_ns);
    let drain_ns = end_ns.saturating_sub(producer_stop_ns);
    let elapsed = end_ns.saturating_sub(start_ns).max(1);
    let totals = corpus.totals;
    let total_logical_ops = totals.requests + totals.events();
    let total_block_ops = totals.block_ops();
    let offered_s = window_ns.max(1) as f64 / 1e9;
    let achieved_s = elapsed as f64 / 1e9;
    let issue_s = issue_span_ns.max(1) as f64 / 1e9;
    let issue_span_valid = issue_span_ns <= window_ns.saturating_mul(101) / 100;
    let generator_valid = failure_reasons.is_empty() && issue_span_valid;
    let kept_up = generator_valid && elapsed <= window_ns.saturating_mul(110) / 100;

    // Per-lane diagnostics (not in Dynamo's result): CPU time, event count and the time of the
    // last completion of each event lane, which tell scheduler starvation from slower work.
    let event_lane_cpu_ms: Vec<f64> = event_lane_cpu_ns
        .iter()
        .map(|&ns| ns as f64 / 1e6)
        .collect();
    let event_lane_events: Vec<usize> = event_results.iter().map(Vec::len).collect();
    let event_lane_last_finished_ms: Vec<f64> = event_results
        .iter()
        .map(|c| {
            c.last()
                .map_or(0.0, |c| c.finished_ns.saturating_sub(start_ns) as f64 / 1e6)
        })
        .collect();
    let result = json!({
        "schema_version": 3,
        "harness": "smg-mooncake-replay",
        "backend": backend.name(),
        "corpus": args.corpus,
        "mirror_dynamo_costs": args.mirror_dynamo_costs,
        "provenance": {
            "argv": std::env::args().collect::<Vec<_>>(),
            "binary": std::env::current_exe().ok().map(|p| p.display().to_string()),
            "binary_blake3": std::env::current_exe()
                .ok()
                .and_then(|p| std::fs::read(p).ok())
                .map(|b| blake3::hash(&b).to_hex().to_string()),
            "corpus_blake3": corpus.file_blake3,
            "corpus_reference_window_ns": corpus.reference_window_ns,
            "trace_path": corpus.trace_path,
            "trace_block_size": corpus.block_size,
            "trace_duplication_factor": corpus.trace_duplication_factor,
            "trace_length_factor": corpus.trace_length_factor,
            "inference_worker_duplication_factor": corpus.inference_worker_duplication_factor,
            "num_unique_inference_workers": corpus.logical_workers,
            "jump_size": args.jump_size,
            "issuer_spin_us": args.issuer_spin_us,
            "issue_lag_diagnostic_threshold_us": args.issue_lag_diagnostic_threshold_us,
        },
        "timer": "clock_nanosleep_monotonic_absolute",
        "benchmark_duration_ms": window_ns / 1_000_000,
        "block_size": corpus.block_size,
        "pre_run_quiescence_ms": args.pre_run_quiescence_ms,
        "query_lanes": args.query_lanes,
        "issuer_threads": issuer_threads,
        "event_workers": args.event_lanes,
        "issuer_cpus": issuer_cpus,
        "query_issuer_threads": query_issuers,
        "query_issuer_cpus": query_cpus,
        "backend_cpus": backend_cpus.to_vec(),
        "total_requests": totals.requests,
        "total_events": totals.events(),
        "total_stored_events": totals.stored_events,
        "total_removed_events": totals.removed_events,
        "total_cleared_events": totals.cleared_events,
        "total_request_blocks": totals.request_blocks,
        "total_stored_blocks": totals.stored_blocks,
        "total_removed_blocks": totals.removed_blocks,
        "total_logical_ops": total_logical_ops,
        "total_block_ops": total_block_ops,
        "offered_logical_ops_per_sec": total_logical_ops as f64 / offered_s,
        "actual_issue_logical_ops_per_sec": total_logical_ops as f64 / issue_s,
        "achieved_logical_ops_per_sec": total_logical_ops as f64 / achieved_s,
        "offered_block_ops_per_sec": total_block_ops as f64 / offered_s,
        "actual_issue_block_ops_per_sec": total_block_ops as f64 / issue_s,
        "achieved_block_ops_per_sec": total_block_ops as f64 / achieved_s,
        "read_issue_lag": distribution(read_lag),
        "update_issue_lag": distribution(update_lag),
        "generator_gate": "issue_span_exact_completion",
        "query_queue_wait": distribution(queue_wait),
        "query_service": distribution(service),
        "query_scheduled_to_finished": distribution(query_e2e),
        "update_accepted_to_finished": distribution(update_acc_fin),
        "update_scheduled_to_finished": distribution(update_e2e),
        "delayed_reads": delayed_reads,
        "delayed_updates": delayed_updates,
        "maximum_query_queue_depth": maximum_depth(&mut query_edges),
        "maximum_outstanding_updates": maximum_depth(&mut update_edges),
        "queued_queries_at_stop": queued_queries_at_stop,
        "outstanding_updates_at_stop": outstanding_updates_at_stop,
        "post_acceptance_completion_races": races,
        "rejected_events": failed_events,
        "issuer_cpu_ns": issuer_cpu_ns,
        "pin_event_lanes": args.pin_event_lanes,
        "issuer_by_lane": args.issuer_by_lane,
        "event_lane_cpu_ms": event_lane_cpu_ms,
        "event_lane_events": event_lane_events,
        "event_lane_last_finished_ms": event_lane_last_finished_ms,
        "query_lane_cpu_ms_total": query_lane_cpu_ns as f64 / 1e6,
        "issue_span_ns": issue_span_ns,
        "drain_ns": drain_ns,
        "generator_valid": generator_valid,
        "kept_up": kept_up,
        "failure_reasons": failure_reasons,
    });
    println!(
        "{} window {} ms: offered {:.1}M achieved {:.1}M block ops/s, kept_up {}, valid {}, \
         lookup service p50 {:.2} us p99 {:.2} us, scheduled->finished p99 {:.1} us, drain {:.1} ms",
        backend.name(),
        window_ns / 1_000_000,
        result["offered_block_ops_per_sec"].as_f64().unwrap_or(0.0) / 1e6,
        result["achieved_block_ops_per_sec"].as_f64().unwrap_or(0.0) / 1e6,
        kept_up,
        generator_valid,
        result["query_service"]["p50_ns"].as_u64().unwrap_or(0) as f64 / 1e3,
        result["query_service"]["p99_ns"].as_u64().unwrap_or(0) as f64 / 1e3,
        result["query_scheduled_to_finished"]["p99_ns"].as_u64().unwrap_or(0) as f64 / 1e3,
        drain_ns as f64 / 1e6,
    );
    if !result["failure_reasons"]
        .as_array()
        .is_some_and(Vec::is_empty)
    {
        println!("failure reasons: {}", result["failure_reasons"]);
    }
    std::fs::write(
        &args.result_json_output,
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(())
}

fn maximum_depth(edges: &mut [(u64, i8)]) -> usize {
    edges.sort_unstable_by(|l, r| l.0.cmp(&r.0).then_with(|| r.1.cmp(&l.1)));
    let (mut depth, mut maximum) = (0isize, 0isize);
    for &(_, delta) in edges.iter() {
        depth += delta as isize;
        maximum = maximum.max(depth);
    }
    maximum.max(0) as usize
}

fn distribution(mut values: Vec<u64>) -> serde_json::Value {
    if values.is_empty() {
        return json!({"p50_ns": 0, "p99_ns": 0, "p999_ns": 0, "max_ns": 0});
    }
    values.sort_unstable();
    let rank = |num: usize, den: usize| {
        let r = values.len().saturating_mul(num).div_ceil(den).max(1);
        values[r.saturating_sub(1).min(values.len() - 1)]
    };
    json!({
        "p50_ns": rank(50, 100),
        "p99_ns": rank(99, 100),
        "p999_ns": rank(999, 1000),
        "max_ns": values[values.len() - 1],
    })
}
