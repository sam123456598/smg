//! A run-compressed KV index: the event-driven prefix index as a tree of runs with per-run
//! worker coverage bitsets, lock-free and allocation-free for readers, bounded in memory.
//!
//! The index answers the same question as [`PositionalIndexer`](crate::PositionalIndexer): for a
//! request given as its per-block content hashes, how many leading blocks does each worker hold,
//! where "holds" means the worker stored, at every position up to there, the block that sits on
//! the request's chain. It is fed by the same engine events (stored / removed / cleared, keyed by
//! the engine's block hashes) and keeps the same per-worker block map the gateway's event monitor
//! owns, so it drops into the same call sites. Its results are checked against
//! [`ReferenceIndexer`](crate::ReferenceIndexer) by `tests/exactness_chain.rs` (including evictions
//! that leave holes in a chain) and under concurrent lanes by `tests/concurrency_chain.rs`.
//!
//! Shape:
//! - A **run** is a maximal stretch of consecutive positions on one chain whose set of holding
//!   workers is the same at every position. It stores one content hash per position and one
//!   coverage bit per worker. Runs form a tree: a run's children continue it with different next
//!   blocks. A store appends to a run or adds a child; a divergence or a coverage change splits a
//!   run into a prefix and a suffix; positions never move except by a split, which forwards them.
//!   Because coverage is uniform within a run, a worker that evicts a block in the middle of a
//!   chain simply stops covering the piece that holds it, and a lookup stops there for that
//!   worker and nowhere else: holes are exact.
//! - A **lookup** walks from the root, comparing the request's content hashes against each run on
//!   the path (one compare loop per run, not one probe per block) and ANDing the alive set with
//!   the run's coverage; a worker that drops out scores the position where it dropped. Readers
//!   take no locks, allocate nothing but the result map and write to no memory at all: every run
//!   header, hash array and child table lives in an arena addressed by integer ids, a run's
//!   window `(hash array, base, length, children)` is read under a seqlock version that is
//!   checked again after the run's hashes, coverage and child entry have been read, so a split,
//!   a growth, an unlink or a reuse of the run is atomic to a reader.
//! - **Writers** (the event lanes, one per engine worker) lock one run at a time, plus its parent
//!   for the moment it takes to unlink an empty run. A decode extension of the worker's own leaf
//!   appends in place: one lock, no allocation. A split shares the hash array between prefix and
//!   suffix (no copy) and leaves a forwarding record so map entries written before it still
//!   resolve, which keeps other lanes' maps untouched. The lane's own map takes an engine hash to
//!   `(run, offset)`.
//! - **Memory is recycled.** Run headers, hash arrays (reference-counted across the runs a split
//!   leaves sharing one) and child tables return to free lists when they die; a run id carries a
//!   generation so a stale child entry or forwarding record to a reused id is recognised.
//!   Children are an open-addressing table (linear probing, tombstones, rebuilt at 3/4 load), so
//!   a node with many children, the root above all, inserts in constant time.
//!
//! Engine hashes: the index trusts the engine's parent pointers and block identities, as the
//! positional indexer does. Nothing is shared between workers through the maps, so one engine
//! reusing a hash cannot corrupt another worker's view. The index carries one engine hash per
//! distinct block, the one its first holder stored, in an array parallel to the content hashes.
//! The engine hash is a chain hash (a hash of the parent's hash and the block's content), which
//! is what lets a store walk match a run by one engine hash at the end of its window instead of
//! a content hash per block (`match_run`); the content hash at the landing is still checked, and
//! a store that fails it (`landing_mismatches`) is placed by its content, block by block. A
//! worker whose engine names the same content by other hashes (`engine_conflicts` in the stats)
//! is matched by content too; its own hashes key its lane map, so nothing else changes for it.
//! The index therefore assumes one engine hash per worker per (parent, content) position. A
//! second name for a position the worker holds (a twin) is filed onto that position and
//! counted, not stored twice: the worker still scores the position, but once the first name is
//! removed the position goes with it while the second name is still in the lane map, and a
//! store under that name cuts the run at the parent and goes on after it (`store_in_run`). The
//! reference indexer keeps a position until its last name goes, so under twins the index holds
//! at most what the reference holds and never scores a worker above it (the exactness suite's
//! twins test); the normalizer guarantees one name per position for vLLM, whose hash is the
//! chain hash, so the count stays zero in the gateway, and a reading of sliding-window events
//! against the wrong tokens is what produced thousands of them in an offline capture.
//! A lane-map slot carries its key: a probe is settled in the map itself, with no read into the
//! index per block (key-less 8-byte slots checked through the index cost 30-40% of lane CPU).
//!
//! Memory: 16 bytes per distinct block on a chain (its content hash and its engine hash, shared
//! by every worker that holds it, with about 12% slack for growth) plus a 64-byte run header, the
//! coverage words and a child table per branching run, against the 16-byte lane-map slot each
//! lane keeps per held block for removals.

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{fence, AtomicIsize, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use dashmap::{mapref::entry::Entry, DashMap};
use parking_lot::{Mutex, MutexGuard};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use crate::event_tree::{
    chain_prefix_hash, ApplyError, ContentHash, OverlapScores, SequenceHash, StoredBlock,
    WorkerIdExhausted,
};

/// The virtual root: position 0's parent, holds no blocks, never dies.
const ROOT: u32 = 0;
/// "No table" / "no array": word 0 of the arena is never handed out.
const NONE: u32 = 0;
/// Forwarding target of blocks whose last holder evicted them: nowhere.
const GONE: u32 = u32::MAX;
/// A table slot whose child was unlinked.
const TOMB: u64 = u64::MAX;
/// Runs per slab chunk (1024) and chunks in the directory (64 Mi runs in all).
const RUN_CHUNK_BITS: u32 = 10;
const RUN_CHUNK: usize = 1 << RUN_CHUNK_BITS;
const RUN_DIR: usize = 1 << 16;
/// Words per arena chunk (1 Mi, 8 MiB) and chunks in the directory (4 Gi words in all).
const WORD_CHUNK_BITS: u32 = 20;
const WORD_CHUNK: usize = 1 << WORD_CHUNK_BITS;
const WORD_DIR: usize = 1 << 12;
/// Hash array capacities: multiples of 8 up to 128, then powers of two.
const SMALL_ARRAY_CLASSES: usize = 16;
const ARRAY_CLASSES: usize = SMALL_ARRAY_CLASSES + 4 * 13;
/// Classes above the wanted one an allocation may take a freed array from (one octave).
const ARRAY_FIT_SPAN: usize = 4;
/// Child table slot counts: powers of two from 2.
const MIN_TABLE_SLOTS: usize = 2;
const TABLE_CLASSES: usize = 27;
/// Coverage words per run at most: 1024 workers.
const MAX_WORDS: usize = 16;
/// Partial holders a lookup can buffer per run: every worker at most.
const MAX_PARTIAL: usize = MAX_WORDS * 64;
/// Prefix holders a run carries before it is split at their median cutoff: a lookup reads every
/// entry of a run it walks, so a hot chain held to a hundred different depths costs a hundred
/// entries per lookup as one run and a few bitset words as a handful; a split here turns the
/// holders at or past the median into whole holders of the prefix. Sixteen left the A2 pairs
/// unchanged at one and two shards where thirty-two still cost 0.1 us at two. Splits for this reason are
/// bounded by holders, not by requests, so they do not accumulate the way splits at every
/// divergence did.
const PARTIAL_CAP: usize = 16;

thread_local! {
    /// The lookup's buffer for one run's partial-holder entries, per thread: filled and read
    /// within one walk, never cleared, so a lookup does not zero 8 KB of stack first.
    static PARTIAL_BUFFER: std::cell::RefCell<[u64; MAX_PARTIAL]> =
        const { std::cell::RefCell::new([0; MAX_PARTIAL]) };
}

/// One step of [`ChainIndex::hop`] along a block's forwarding chain.
enum Hop {
    /// The place forwards on: the next place and the generation its run must have.
    Next(BlockRef, u32),
    /// The place stands; the run's generation.
    Here(u32),
    /// Nobody holds the block any more.
    Gone,
}

/// Where one of a worker's blocks lives: the run and the offset of the block within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockRef {
    pub run: u32,
    pub offset: u32,
}

pub use crate::lane_map::ChainBlockMap;

/// Hash array capacity class: `8, 16, .., 128` in steps of 8, then four classes per octave
/// (`160, 192, 224, 256, 320, ..`), so an array wastes at most a quarter of its words to the
/// class, not half; a run of 140 blocks with its slack lands in 160, not 256.
fn array_class(capacity: usize) -> usize {
    if capacity <= 8 * SMALL_ARRAY_CLASSES {
        capacity.div_ceil(8).max(1) - 1
    } else {
        // `capacity` in `(low, 2 * low]` with `low` a power of two from 128 up.
        let bits = usize::BITS - (capacity - 1).leading_zeros();
        let low = 1usize << (bits - 1);
        let quarter = (capacity - low).div_ceil(low / 4).max(1) - 1;
        SMALL_ARRAY_CLASSES + 4 * (bits as usize - 8) + quarter
    }
}

fn class_capacity(class: usize) -> usize {
    if class < SMALL_ARRAY_CLASSES {
        8 * (class + 1)
    } else {
        let above = class - SMALL_ARRAY_CLASSES;
        let low = 1usize << (7 + above / 4);
        low + (above % 4 + 1) * (low / 4)
    }
}

/// Room for `len` hashes plus a little for decode extensions.
fn capacity_for(len: usize) -> usize {
    len + (len / 8).max(2)
}

fn table_class(slots: usize) -> usize {
    (slots.trailing_zeros() as usize).saturating_sub(MIN_TABLE_SLOTS.trailing_zeros() as usize)
}

/// Append-only storage of 64-bit words in fixed chunks with free lists per size class. Holds
/// hash arrays (`used | capacity << 32`, `refs`, then the hashes) and child tables
/// (`slots | used << 32`, `live`, then `(head hash, run id | generation << 32)` slots). An
/// allocation never crosses a chunk, so any array is one slice.
struct WordArena {
    dir: Box<[OnceLock<Box<[AtomicU64]>>]>,
    next: AtomicU64,
    free_arrays: Vec<SegQueue<u32>>,
    free_tables: Vec<SegQueue<u32>>,
    free_partials: Vec<SegQueue<u32>>,
}

impl WordArena {
    fn new() -> Self {
        Self {
            dir: (0..WORD_DIR).map(|_| OnceLock::new()).collect(),
            next: AtomicU64::new(1),
            free_arrays: (0..ARRAY_CLASSES).map(|_| SegQueue::new()).collect(),
            free_tables: (0..TABLE_CLASSES).map(|_| SegQueue::new()).collect(),
            free_partials: (0..TABLE_CLASSES).map(|_| SegQueue::new()).collect(),
        }
    }

    #[inline]
    fn chunk(&self, index: usize) -> &[AtomicU64] {
        self.dir[index].get_or_init(|| (0..WORD_CHUNK).map(|_| AtomicU64::new(0)).collect())
    }

    /// `count` consecutive words starting at `start` (all within one chunk, as allocated). A
    /// lock-free reader may compute `count` from a header that is being recycled under it; it
    /// confirms the run's version afterwards and discards what it read, so the slice is clamped
    /// to the chunk rather than trusted.
    #[inline]
    fn words(&self, start: u32, count: usize) -> &[AtomicU64] {
        let start = start as usize;
        let offset = start & (WORD_CHUNK - 1);
        let end = offset.saturating_add(count).min(WORD_CHUNK);
        &self.chunk(start >> WORD_CHUNK_BITS)[offset..end]
    }

    #[inline]
    fn word(&self, at: u32) -> &AtomicU64 {
        &self.words(at, 1)[0]
    }

    /// Fresh words inside one chunk.
    fn bump(&self, count: usize) -> u32 {
        debug_assert!(0 < count && count <= WORD_CHUNK);
        loop {
            let current = self.next.load(Ordering::Relaxed);
            let mut start = current as usize;
            if start >> WORD_CHUNK_BITS != (start + count - 1) >> WORD_CHUNK_BITS {
                start = ((start >> WORD_CHUNK_BITS) + 1) << WORD_CHUNK_BITS;
            }
            let end = start + count;
            assert!(
                end <= WORD_DIR * WORD_CHUNK,
                "chain index arena exhausted: more than 2^32 hash words"
            );
            if self
                .next
                .compare_exchange_weak(current, end as u64, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.chunk(start >> WORD_CHUNK_BITS);
                return start as u32;
            }
        }
    }

    fn used(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// Bytes of chunks taken from the process allocator: what the arena costs in memory.
    fn chunk_bytes(&self) -> usize {
        let chunks = self
            .dir
            .iter()
            .filter(|chunk| chunk.get().is_some())
            .count();
        chunks * WORD_CHUNK * size_of::<AtomicU64>()
    }

    /// Words sitting in free lists.
    fn free_words(&self) -> usize {
        let arrays: usize = self
            .free_arrays
            .iter()
            .enumerate()
            .map(|(class, list)| list.len() * (class_capacity(class) + 2))
            .sum();
        let tables: usize = self
            .free_tables
            .iter()
            .enumerate()
            .map(|(class, list)| list.len() * (2 + 2 * (MIN_TABLE_SLOTS << class)))
            .sum();
        let partials: usize = self
            .free_partials
            .iter()
            .enumerate()
            .map(|(class, list)| list.len() * (2 + (MIN_TABLE_SLOTS << class)))
            .sum();
        arrays + tables + partials
    }

    // ---- hash arrays: [used | capacity << 32][refs][hash; capacity], data = start + 2 ----

    /// A hash array holding `contents` with room for at least `capacity`; returns the data start.
    fn alloc_array(&self, contents: &[u64], capacity: usize) -> u32 {
        let wanted = array_class(capacity.max(contents.len()).max(8));
        // A freed array of the wanted class, else of one up to an octave larger: fresh words
        // are bumped only when nothing in that range is free, so the free lists of neighbouring
        // classes do not fill while others bump (churn moves arrays between classes as runs
        // grow and die).
        let (class, recycled) = (wanted..ARRAY_CLASSES.min(wanted + ARRAY_FIT_SPAN + 1))
            .find_map(|class| self.free_arrays[class].pop().map(|start| (class, start)))
            .unwrap_or((wanted, 0));
        let capacity = class_capacity(class);
        let start = if recycled == 0 {
            self.bump(capacity + 2)
        } else {
            recycled
        };
        let data = start + 2;
        for (slot, &hash) in self.words(data, contents.len()).iter().zip(contents) {
            slot.store(hash, Ordering::Relaxed);
        }
        self.word(start + 1).store(1, Ordering::Relaxed);
        self.word(start).store(
            pack(contents.len() as u32, capacity as u32),
            Ordering::Release,
        );
        data
    }

    #[inline]
    fn array_header(&self, data: u32) -> &AtomicU64 {
        self.word(data - 2)
    }

    /// Words an array can hold: the class it was given, which may be larger than asked for.
    fn array_capacity(&self, data: u32) -> usize {
        unpack(self.array_header(data).load(Ordering::Relaxed)).1 as usize
    }

    /// One more run shares this array.
    fn array_retain(&self, data: u32) {
        self.word(data - 1).fetch_add(1, Ordering::Relaxed);
    }

    /// One run fewer uses this array; the last one frees it.
    fn array_release(&self, data: u32) {
        if data == NONE {
            return;
        }
        if self.word(data - 1).fetch_sub(1, Ordering::AcqRel) == 1 {
            let (_, capacity) = unpack(self.array_header(data).load(Ordering::Relaxed));
            self.free_arrays[array_class(capacity as usize)].push(data - 2);
        }
    }

    // ---- child tables: [slots | used << 32][live][(head, run | gen << 32); slots] ----

    fn alloc_table(&self, slots: usize) -> u32 {
        let class = table_class(slots);
        let recycled = self.free_tables[class].pop();
        let table = recycled.unwrap_or_else(|| self.bump(2 + 2 * slots));
        for word in self.words(table + 2, 2 * slots) {
            word.store(0, Ordering::Relaxed);
        }
        self.word(table + 1).store(0, Ordering::Relaxed);
        self.word(table).store(slots as u64, Ordering::Release);
        table
    }

    fn free_table(&self, table: u32) {
        if table == NONE {
            return;
        }
        let slots = self.table_slots(table);
        self.free_tables[table_class(slots)].push(table);
    }

    #[inline]
    fn table_slots(&self, table: u32) -> usize {
        self.word(table).load(Ordering::Relaxed) as u32 as usize
    }

    /// The child continuing with `head`, as `(run, generation)`.
    #[inline]
    /// Live entries of a child table (`NONE` has none).
    fn table_live(&self, table: u32) -> usize {
        if table == NONE {
            0
        } else {
            self.word(table + 1).load(Ordering::Relaxed) as usize
        }
    }

    fn table_find(&self, table: u32, head: u64) -> Option<(u32, u32)> {
        if table == NONE {
            return None;
        }
        let slots = self.table_slots(table);
        if slots == 0 || !slots.is_power_of_two() {
            // A header being recycled under a lock-free reader; the version check discards this.
            return None;
        }
        let words = self.words(table + 2, 2 * slots);
        let mask = slots - 1;
        let mut index = head as usize & mask;
        for _ in 0..slots {
            let entry = words.get(2 * index + 1)?.load(Ordering::Acquire);
            if entry == 0 {
                return None;
            }
            if entry != TOMB && words.get(2 * index)?.load(Ordering::Relaxed) == head {
                return Some((entry as u32, (entry >> 32) as u32));
            }
            index = (index + 1) & mask;
        }
        None
    }

    /// Claim a slot for `key` without a lock: the head word is taken by compare-and-swap, then the
    /// run word is published. A reader stops at an empty run word, so an entry in flight is simply
    /// not there yet for it; a writer that meets the same head in flight waits for it.
    fn table_claim(&self, table: u32, key: u64, child: u32, generation: u32) -> Claim {
        if table == NONE {
            return Claim::Full;
        }
        let slots = self.table_slots(table);
        if slots == 0 || !slots.is_power_of_two() {
            return Claim::Full;
        }
        let words = self.words(table + 2, 2 * slots);
        let mask = slots - 1;
        let mut index = key as usize & mask;
        for _ in 0..slots {
            let (Some(head_word), Some(run_word)) =
                (words.get(2 * index), words.get(2 * index + 1))
            else {
                return Claim::Full;
            };
            let mut head = head_word.load(Ordering::Acquire);
            if head == 0 {
                match head_word.compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        run_word.store(
                            u64::from(child) | (u64::from(generation) << 32),
                            Ordering::Release,
                        );
                        self.word(table).fetch_add(1 << 32, Ordering::Relaxed);
                        self.word(table + 1).fetch_add(1, Ordering::Relaxed);
                        return Claim::Inserted;
                    }
                    Err(taken) => head = taken,
                }
            }
            if head == key {
                loop {
                    let run = run_word.load(Ordering::Acquire);
                    if run == 0 {
                        std::hint::spin_loop();
                        continue;
                    }
                    if run == TOMB {
                        break;
                    }
                    return Claim::Exists(run as u32, (run >> 32) as u32);
                }
            }
            index = (index + 1) & mask;
        }
        Claim::Full
    }

    /// Live `(head, run, generation)` entries of a table.
    fn table_entries(&self, table: u32) -> Vec<(u64, u32, u32)> {
        if table == NONE {
            return Vec::new();
        }
        let slots = self.table_slots(table);
        let words = self.words(table + 2, 2 * slots);
        (0..slots)
            .filter_map(|index| {
                let entry = words[2 * index + 1].load(Ordering::Acquire);
                (entry != 0 && entry != TOMB).then(|| {
                    (
                        words[2 * index].load(Ordering::Relaxed),
                        entry as u32,
                        (entry >> 32) as u32,
                    )
                })
            })
            .collect()
    }

    /// Write an entry into a free or tombstoned slot of a table that has room (writer side).
    fn table_put(&self, table: u32, head: u64, run: u32, generation: u32) {
        let slots = self.table_slots(table);
        let words = self.words(table + 2, 2 * slots);
        let mask = slots - 1;
        let mut index = head as usize & mask;
        loop {
            let entry = words[2 * index + 1].load(Ordering::Relaxed);
            if entry == 0 || entry == TOMB {
                words[2 * index].store(head, Ordering::Relaxed);
                words[2 * index + 1].store(
                    u64::from(run) | (u64::from(generation) << 32),
                    Ordering::Release,
                );
                let header = self.word(table);
                let used = (header.load(Ordering::Relaxed) >> 32) + u64::from(entry == 0);
                header.store(slots as u64 | (used << 32), Ordering::Relaxed);
                self.word(table + 1).fetch_add(1, Ordering::Relaxed);
                return;
            }
            index = (index + 1) & mask;
        }
    }

    /// Tombstone the entry of `run`; returns the live count left.
    fn table_take(&self, table: u32, run: u32) -> u64 {
        let slots = self.table_slots(table);
        let words = self.words(table + 2, 2 * slots);
        for index in 0..slots {
            let entry = words[2 * index + 1].load(Ordering::Relaxed);
            if entry != 0 && entry != TOMB && entry as u32 == run {
                words[2 * index + 1].store(TOMB, Ordering::Release);
                return self.word(table + 1).fetch_sub(1, Ordering::Relaxed) - 1;
            }
        }
        self.word(table + 1).load(Ordering::Relaxed)
    }

    /// A new table holding the live entries of `table` plus room for one more.
    fn table_grown(&self, table: u32) -> u32 {
        let entries = self.table_entries(table);
        let needed = entries.len() + 1;
        let mut slots = MIN_TABLE_SLOTS;
        while needed * 4 > slots * 3 {
            slots *= 2;
        }
        let grown = self.alloc_table(slots);
        for (head, run, generation) in entries {
            self.table_put(grown, head, run, generation);
        }
        grown
    }

    /// A table holding `entries` (`(key, run, generation)`) with room for one more; `NONE` for
    /// none.
    fn table_from(&self, entries: &[(u64, u32, u32)]) -> u32 {
        if entries.is_empty() {
            return NONE;
        }
        let needed = entries.len() + 1;
        let mut slots = MIN_TABLE_SLOTS;
        while needed * 4 > slots * 3 {
            slots *= 2;
        }
        let table = self.alloc_table(slots);
        for &(key, run, generation) in entries {
            self.table_put(table, key, run, generation);
        }
        table
    }

    /// Tombstoned slots of a table: entries taken out since it was built.
    fn table_dead(&self, table: u32) -> usize {
        let header = self.word(table).load(Ordering::Relaxed);
        ((header >> 32) as usize)
            .saturating_sub(self.word(table + 1).load(Ordering::Relaxed) as usize)
    }
}

impl WordArena {
    // ---- partial holders: [slots | used << 32][live][(worker | cutoff << 32); slots] ----
    // Entries are appended in slot order; a removed entry is a tombstone. A worker listed here
    // holds the run's blocks `[0, cutoff)` with `0 < cutoff < len`.

    fn alloc_partials(&self, slots: usize) -> u32 {
        let class = table_class(slots);
        let recycled = self.free_partials[class].pop();
        let table = recycled.unwrap_or_else(|| self.bump(2 + slots));
        for word in self.words(table + 2, slots) {
            word.store(0, Ordering::Relaxed);
        }
        self.word(table + 1).store(0, Ordering::Relaxed);
        self.word(table).store(slots as u64, Ordering::Release);
        table
    }

    fn free_partials(&self, table: u32) {
        if table == NONE {
            return;
        }
        let slots = self.word(table).load(Ordering::Relaxed) as u32 as usize;
        self.free_partials[table_class(slots)].push(table);
    }

    /// `(slots, used)` of a partial table.
    #[inline]
    fn partials_shape(&self, table: u32) -> (usize, usize) {
        let header = self.word(table).load(Ordering::Relaxed);
        (header as u32 as usize, (header >> 32) as usize)
    }

    fn partials_live(&self, table: u32) -> usize {
        if table == NONE {
            0
        } else {
            self.word(table + 1).load(Ordering::Relaxed) as usize
        }
    }

    /// Live `(worker, cutoff)` entries, in slot order.
    fn partial_entries(&self, table: u32) -> Vec<(u32, u32)> {
        if table == NONE {
            return Vec::new();
        }
        let (_, used) = self.partials_shape(table);
        self.words(table + 2, used)
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .filter(|entry| *entry != 0 && *entry != TOMB)
            .map(|entry| (entry as u32, (entry >> 32) as u32))
            .collect()
    }

    /// The slot and cutoff of `worker`, if it is a partial holder.
    fn partial_find(&self, table: u32, worker: u32) -> Option<(usize, u32)> {
        if table == NONE {
            return None;
        }
        let (_, used) = self.partials_shape(table);
        self.words(table + 2, used)
            .iter()
            .enumerate()
            .find_map(|(index, slot)| {
                let entry = slot.load(Ordering::Relaxed);
                (entry != 0 && entry != TOMB && entry as u32 == worker)
                    .then_some((index, (entry >> 32) as u32))
            })
    }

    /// The largest cutoff among the partial holders (0 when there are none).
    fn partial_max(&self, table: u32) -> usize {
        if table == NONE {
            return 0;
        }
        let (_, used) = self.partials_shape(table);
        self.words(table + 2, used)
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .filter(|entry| *entry != 0 && *entry != TOMB)
            .map(|entry| (entry >> 32) as usize)
            .max()
            .unwrap_or(0)
    }

    fn partial_set(&self, table: u32, slot: usize, worker: u32, cutoff: u32) {
        self.word(table + 2 + slot as u32).store(
            u64::from(worker) | (u64::from(cutoff) << 32),
            Ordering::Release,
        );
    }

    /// Append an entry; `false` when the table is full.
    fn partial_put(&self, table: u32, worker: u32, cutoff: u32) -> bool {
        if table == NONE {
            return false;
        }
        let (slots, used) = self.partials_shape(table);
        if used >= slots {
            return false;
        }
        self.partial_set(table, used, worker, cutoff);
        self.word(table)
            .store(slots as u64 | ((used as u64 + 1) << 32), Ordering::Relaxed);
        self.word(table + 1).fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Tombstone a slot; returns the live count left.
    fn partial_remove(&self, table: u32, slot: usize) -> usize {
        self.word(table + 2 + slot as u32)
            .store(TOMB, Ordering::Release);
        self.word(table + 1).fetch_sub(1, Ordering::Relaxed) as usize - 1
    }

    // ---- forwarding records: [slots | count << 32][0][(at | suffix << 32), generation; slots] ----
    // Appended under the run's lock, read without it: `count` is published with a release store
    // after the record, and a reader checks the run's version around the read. Same word count as
    // a child table of the same slot count, so the two share free lists.

    fn alloc_forwards(&self, slots: usize) -> u32 {
        let class = table_class(slots);
        let recycled = self.free_tables[class].pop();
        let table = recycled.unwrap_or_else(|| self.bump(2 + 2 * slots));
        self.word(table + 1).store(0, Ordering::Relaxed);
        self.word(table).store(slots as u64, Ordering::Release);
        table
    }

    #[inline]
    fn forwards_shape(&self, table: u32) -> (usize, usize) {
        let header = self.word(table).load(Ordering::Acquire);
        (header as u32 as usize, (header >> 32) as usize)
    }

    /// The records of a table, oldest first.
    fn forward_records(&self, table: u32) -> Vec<(u32, u32, u32)> {
        if table == NONE {
            return Vec::new();
        }
        let (_, count) = self.forwards_shape(table);
        let words = self.words(table + 2, 2 * count);
        (0..count)
            .map(|index| {
                let first = words[2 * index].load(Ordering::Relaxed);
                (
                    first as u32,
                    (first >> 32) as u32,
                    words[2 * index + 1].load(Ordering::Relaxed) as u32,
                )
            })
            .collect()
    }

    /// Where a block at `offset` of the run went: the oldest record whose split point is at or
    /// before it (later splits cut the shorter prefix).
    #[inline]
    fn forwards_find(&self, table: u32, offset: u32) -> Option<(BlockRef, u32)> {
        if table == NONE {
            return None;
        }
        let (_, count) = self.forwards_shape(table);
        let words = self.words(table + 2, 2 * count);
        (0..count).find_map(|index| {
            let first = words.get(2 * index)?.load(Ordering::Relaxed);
            let at = first as u32;
            if at > offset {
                return None;
            }
            Some((
                BlockRef {
                    run: (first >> 32) as u32,
                    offset: offset - at,
                },
                words.get(2 * index + 1)?.load(Ordering::Relaxed) as u32,
            ))
        })
    }

    /// Append a record; `false` when the table is full.
    fn forwards_push(&self, table: u32, at: u32, suffix: u32, generation: u32) -> bool {
        if table == NONE {
            return false;
        }
        let (slots, count) = self.forwards_shape(table);
        if count >= slots {
            return false;
        }
        let words = self.words(table + 2, 2 * slots);
        words[2 * count].store(u64::from(at) | (u64::from(suffix) << 32), Ordering::Relaxed);
        words[2 * count + 1].store(u64::from(generation), Ordering::Relaxed);
        self.word(table)
            .store(slots as u64 | ((count as u64 + 1) << 32), Ordering::Release);
        true
    }

    /// A new table with the records of `table` and room for more.
    fn forwards_grown(&self, table: u32) -> u32 {
        let records = self.forward_records(table);
        let mut slots = MIN_TABLE_SLOTS;
        while slots < 2 * (records.len() + 1) {
            slots *= 2;
        }
        let grown = self.alloc_forwards(slots);
        for (at, suffix, generation) in records {
            self.forwards_push(grown, at, suffix, generation);
        }
        grown
    }

    /// A partial table holding `entries`; `NONE` for none.
    fn partials_from(&self, entries: &[(u32, u32)]) -> u32 {
        if entries.is_empty() {
            return NONE;
        }
        let mut slots = MIN_TABLE_SLOTS;
        while slots < 2 * entries.len() {
            slots *= 2;
        }
        let table = self.alloc_partials(slots);
        for &(worker, cutoff) in entries {
            self.partial_put(table, worker, cutoff);
        }
        table
    }

    /// A new table with the live entries of `table` and room for `extra` more.
    fn partials_grown(&self, table: u32, extra: usize) -> u32 {
        let entries = self.partial_entries(table);
        let needed = entries.len() + extra;
        let mut slots = MIN_TABLE_SLOTS;
        while slots < 2 * needed {
            slots *= 2;
        }
        let grown = self.alloc_partials(slots);
        for (worker, cutoff) in entries {
            self.partial_put(grown, worker, cutoff);
        }
        grown
    }
}

#[inline]
fn pack(used: u32, capacity: u32) -> u64 {
    (u64::from(capacity) << 32) | u64::from(used)
}

#[inline]
fn unpack(header: u64) -> (u32, u32) {
    (header as u32, (header >> 32) as u32)
}

/// Writer-side bookkeeping of a run, under its lock.
#[derive(Default)]
struct RunMeta {
    /// Unlinked from the tree; its id may be reused (with the next generation).
    dead: bool,
}

/// A reader's consistent view of a run's window.
#[derive(Clone, Copy)]
struct Window {
    /// Data start of the hash array, or `NONE`.
    block: u32,
    /// Offset of the run's first hash within the array.
    base: u32,
    /// Data start of the engine-hash array, or `NONE`.
    engine: u32,
    len: u32,
    /// Child table, or `NONE`.
    children: u32,
    /// Partial-holder table, or `NONE`.
    partials: u32,
    /// Forwarding records of the splits this run has undergone, or `NONE`: a block that sat at
    /// `offset >= at` before a split lives in that split's suffix at `offset - at` (and may have
    /// been forwarded again from there); a `GONE` suffix means nobody holds it any more. Split
    /// points decrease along the records: a run never grows after a split.
    forwards: u32,
}

struct Run {
    /// Absolute position of the run's first block.
    start: AtomicU32,
    /// Run id of the parent (the root's parent is itself).
    parent: AtomicU32,
    /// Generation in the high half, seqlock in the low half: odd while an update is in flight.
    version: AtomicU64,
    block: AtomicU32,
    base: AtomicU32,
    /// Data start of the engine-hash array, parallel to `block` (same base, same length, shared
    /// and released with it), or `NONE`. One engine hash per distinct block, the first holder's.
    engine: AtomicU32,
    len: AtomicU32,
    children: AtomicU32,
    /// Workers holding only a prefix of the run, with how much: `(worker, cutoff)` entries in the
    /// arena. A worker is either in the coverage bitset (whole run) or here, never both.
    partials: AtomicU32,
    forwards: AtomicU32,
    /// Lock-free child inserts in progress on this run; a writer that replaces the child table
    /// or hands it to a suffix waits for this to drain inside its version step.
    inflight: AtomicU32,
    meta: Mutex<RunMeta>,
}

impl Run {
    fn blank() -> Self {
        Self {
            start: AtomicU32::new(0),
            parent: AtomicU32::new(ROOT),
            version: AtomicU64::new(0),
            block: AtomicU32::new(NONE),
            base: AtomicU32::new(0),
            engine: AtomicU32::new(NONE),
            len: AtomicU32::new(0),
            children: AtomicU32::new(NONE),
            partials: AtomicU32::new(NONE),
            forwards: AtomicU32::new(NONE),
            inflight: AtomicU32::new(0),
            meta: Mutex::new(RunMeta::default()),
        }
    }

    #[inline]
    fn start(&self) -> usize {
        self.start.load(Ordering::Relaxed) as usize
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire) as usize
    }

    #[inline]
    fn generation(&self) -> u32 {
        (self.version.load(Ordering::Relaxed) >> 32) as u32
    }

    /// The window as a reader sees it, with the version to confirm afterwards. An in-place
    /// append only grows `len`, published with a release store after its hashes, and needs no
    /// version; everything else that changes the window goes through `begin_update`.
    #[inline]
    fn snapshot(&self) -> (Window, u64) {
        loop {
            let before = self.version.load(Ordering::Acquire);
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let window = Window {
                block: self.block.load(Ordering::Relaxed),
                base: self.base.load(Ordering::Relaxed),
                engine: self.engine.load(Ordering::Relaxed),
                len: self.len.load(Ordering::Acquire),
                children: self.children.load(Ordering::Relaxed),
                partials: self.partials.load(Ordering::Relaxed),
                forwards: self.forwards.load(Ordering::Relaxed),
            };
            fence(Ordering::Acquire);
            if self.version.load(Ordering::Relaxed) == before {
                return (window, before);
            }
        }
    }

    /// Whether everything read since the snapshot belongs to it.
    #[inline]
    fn confirm(&self, version: u64) -> bool {
        fence(Ordering::Acquire);
        self.version.load(Ordering::Relaxed) == version
    }

    fn begin_update(&self) {
        // SeqCst against the inserters' `inflight` increment: either they see the odd version and
        // back off, or the writer sees their count and waits (see `wait_inflight`).
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    /// Wait for lock-free child inserts to finish; called inside a version step, so no new one
    /// starts meanwhile.
    fn wait_inflight(&self) {
        while self.inflight.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
        }
    }

    fn end_update(&self) {
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Start a new life of this header: next generation, fields reset.
    fn reincarnate(&self, start: usize, parent: u32, window: Window) {
        self.version.fetch_add((1 << 32) | 1, Ordering::Acquire);
        self.start.store(start as u32, Ordering::Relaxed);
        self.parent.store(parent, Ordering::Relaxed);
        self.block.store(window.block, Ordering::Relaxed);
        self.base.store(window.base, Ordering::Relaxed);
        self.engine.store(window.engine, Ordering::Relaxed);
        self.len.store(window.len, Ordering::Relaxed);
        self.children.store(window.children, Ordering::Relaxed);
        self.partials.store(window.partials, Ordering::Relaxed);
        self.forwards.store(window.forwards, Ordering::Relaxed);
        self.end_update();
    }
}

struct RunChunk {
    runs: Box<[Run]>,
    /// `words` coverage words per run, run-major.
    coverage: Box<[AtomicU64]>,
}

/// Run storage: a directory of fixed-size chunks created on first use, with a free list of dead
/// ids.
struct RunSlab {
    dir: Box<[OnceLock<RunChunk>]>,
    next: AtomicU32,
    free: SegQueue<u32>,
    words: usize,
}

impl RunSlab {
    fn new(words: usize) -> Self {
        Self {
            dir: (0..RUN_DIR).map(|_| OnceLock::new()).collect(),
            next: AtomicU32::new(0),
            free: SegQueue::new(),
            words,
        }
    }

    #[inline]
    fn chunk(&self, index: usize) -> &RunChunk {
        self.dir[index].get_or_init(|| RunChunk {
            runs: (0..RUN_CHUNK).map(|_| Run::blank()).collect(),
            coverage: (0..RUN_CHUNK * self.words)
                .map(|_| AtomicU64::new(0))
                .collect(),
        })
    }

    #[inline]
    fn run(&self, id: u32) -> &Run {
        let id = id as usize;
        &self.chunk(id >> RUN_CHUNK_BITS).runs[id & (RUN_CHUNK - 1)]
    }

    #[inline]
    fn coverage(&self, id: u32) -> &[AtomicU64] {
        let id = id as usize;
        let chunk = self.chunk(id >> RUN_CHUNK_BITS);
        let first = (id & (RUN_CHUNK - 1)) * self.words;
        &chunk.coverage[first..first + self.words]
    }

    /// A run that is not yet reachable from the tree: a dead header given its next life, or a
    /// fresh one.
    fn alloc(&self, start: usize, parent: u32, window: Window) -> u32 {
        if let Some(id) = self.free.pop() {
            let run = self.run(id);
            let mut meta = run.meta.lock();
            debug_assert!(meta.dead && coverage_is_empty(self.coverage(id)));
            meta.dead = false;
            run.reincarnate(start, parent, window);
            return id;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        assert!(
            (id as usize) < RUN_DIR * RUN_CHUNK,
            "chain index slab exhausted: more than 2^26 runs"
        );
        let run = self.run(id);
        run.start.store(start as u32, Ordering::Relaxed);
        run.parent.store(parent, Ordering::Relaxed);
        run.block.store(window.block, Ordering::Relaxed);
        run.base.store(window.base, Ordering::Relaxed);
        run.engine.store(window.engine, Ordering::Relaxed);
        run.len.store(window.len, Ordering::Relaxed);
        run.children.store(window.children, Ordering::Relaxed);
        run.partials.store(window.partials, Ordering::Relaxed);
        run.forwards.store(window.forwards, Ordering::Relaxed);
        id
    }

    fn allocated(&self) -> usize {
        self.next.load(Ordering::Relaxed) as usize
    }

    /// Bytes of chunks taken from the process allocator: headers and coverage words of every
    /// slot, used or not.
    fn chunk_bytes(&self) -> usize {
        let chunks = self
            .dir
            .iter()
            .filter(|chunk| chunk.get().is_some())
            .count();
        chunks * RUN_CHUNK * (size_of::<Run>() + self.words * size_of::<AtomicU64>())
    }
}

#[inline]
fn has(coverage: &[AtomicU64], worker: u32) -> bool {
    coverage[(worker / 64) as usize].load(Ordering::Relaxed) & (1u64 << (worker % 64)) != 0
}

fn set(coverage: &[AtomicU64], worker: u32) {
    coverage[(worker / 64) as usize].fetch_or(1u64 << (worker % 64), Ordering::Relaxed);
}

fn clear(coverage: &[AtomicU64], worker: u32) {
    coverage[(worker / 64) as usize].fetch_and(!(1u64 << (worker % 64)), Ordering::Relaxed);
}

fn coverage_is_empty(coverage: &[AtomicU64]) -> bool {
    coverage
        .iter()
        .all(|word| word.load(Ordering::Relaxed) == 0)
}

/// Exactly `worker` and nobody else.
fn covered_only_by(coverage: &[AtomicU64], worker: u32) -> bool {
    let word = (worker / 64) as usize;
    let bit = 1u64 << (worker % 64);
    coverage.iter().enumerate().all(|(index, slot)| {
        let value = slot.load(Ordering::Relaxed);
        if index == word {
            value == bit
        } else {
            value == 0
        }
    })
}

fn workers(coverage: &[AtomicU64]) -> Vec<u32> {
    coverage
        .iter()
        .enumerate()
        .flat_map(|(index, word)| {
            let value = word.load(Ordering::Relaxed);
            (0..64)
                .filter(move |bit| value & (1u64 << bit) != 0)
                .map(move |bit| (index * 64 + bit) as u32)
        })
        .collect()
}

/// Memory and shape counters, for the scoreboard.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChainIndexStats {
    /// Run headers ever created (resident).
    pub runs_allocated: usize,
    /// Dead headers waiting for reuse.
    pub runs_free: usize,
    /// Runs linked in the tree.
    pub runs_live: usize,
    /// Content hashes held by live runs.
    pub blocks_live: usize,
    /// Bytes of the word arena handed out so far (hash arrays, child tables, free lists
    /// included).
    pub arena_bytes: usize,
    /// Bytes of the word arena sitting in free lists.
    pub arena_free_bytes: usize,
    /// Bytes the word arena holds from the process allocator: `arena_bytes` rounded up to whole
    /// chunks (8 MiB each).
    pub arena_chunk_bytes: usize,
    /// Bytes of run headers, coverage words included (all allocated runs).
    pub header_bytes: usize,
    /// Bytes the run slab holds from the process allocator: whole chunks of headers and coverage
    /// words, used or not.
    pub slab_bytes: usize,
    /// Stored blocks whose engine hash differed from the one the index carries for the block
    /// (an engine fleet that does not agree on hashes); such a block is matched by content and
    /// keyed by the worker's own hash in its lane map.
    pub engine_conflicts: usize,
    /// Stores whose blocks carried the engine hashes the index holds for other content: what
    /// the relay's hash check refuses at the engine, seen from the index. Such a store is
    /// placed by its content, so the index stays exact; the count names a broken engine.
    pub landing_mismatches: usize,
    /// Blocks a store moved to another place for their worker (an engine hash stored again at
    /// a different position, as after a store without its parent); the old membership is
    /// released so a hash is held at one place per worker.
    pub moved_hashes: usize,
    /// Runs split because a stored chain diverged inside them: content structure, bounded by
    /// the distinct branch points of what the engines hold.
    pub splits_by_branch: usize,
    /// Runs split because a removal left a worker holding blocks on both sides of a hole.
    pub splits_by_hole: usize,
    /// Runs split because a store entered them under a parent the worker did not hold up to
    /// (a stale parent, or a worker re-entering a chain it had lost the head of).
    pub splits_by_mid_run_store: usize,
    /// Runs split at the median cutoff of their prefix holders once more than `PARTIAL_CAP`
    /// held a prefix of them (bounded by holders, never by requests).
    pub splits_by_prefix_holders: usize,
    /// Runs unlinked from the tree since the index was created (their headers go back to the
    /// slab's free list).
    pub runs_died: usize,
    /// Prefix-holder entries over the live runs (a lookup reads a run's entries when a holder it
    /// follows is not a whole holder of the run), and the most one run carries.
    pub partial_entries: usize,
    pub max_partials: usize,
    /// Child-table entries over the live runs, live and tombstoned.
    pub child_entries: usize,
    pub child_tombstones: usize,
}

/// Where a writer waited: the root, a leaf only this worker holds, or a run others hold too.
#[derive(Clone, Copy)]
enum LockKind {
    /// Constructed by the lane-stats lock accounting; a divergence no longer splits a run, so
    /// no split is charged to the root kind any more.
    #[cfg_attr(not(feature = "lane-stats"), expect(dead_code))]
    Root = 0,
    Own = 1,
    Shared = 2,
}

/// Lane-side counters (feature `lane-stats`; zero otherwise): lock acquisitions and the time spent
/// waiting for contended ones by run kind, store walks restarted, and splits by cause.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LaneStats {
    pub locks: [u64; 3],
    pub contended: [u64; 3],
    pub wait_ns: [u64; 3],
    pub restarts: u64,
    pub splits_divergence: u64,
    pub splits_hole: u64,
    pub splits_stale_parent: u64,
    /// Children linked without the parent's lock.
    pub inserts: u64,
    /// Store events and the blocks they carried.
    pub stores: u64,
    pub store_blocks: u64,
    /// Time in stores: resolving the parent block, walking the runs (lock-free and locked), and
    /// writing the lane map.
    pub store_ns: [u64; 3],
    /// Removal events and the blocks they named.
    pub removes: u64,
    pub remove_blocks: u64,
    /// Time in removals: taking the hashes out of the lane map, grouping them by run, and the
    /// per-run work (locks, splits, holdings).
    pub remove_ns: [u64; 3],
}

#[cfg(feature = "lane-stats")]
#[derive(Default)]
struct LaneCounters {
    locks: [CachePadded<AtomicU64>; 3],
    contended: [CachePadded<AtomicU64>; 3],
    wait_ns: [CachePadded<AtomicU64>; 3],
    restarts: CachePadded<AtomicU64>,
    inserts: CachePadded<AtomicU64>,
    stores: CachePadded<AtomicU64>,
    store_blocks: CachePadded<AtomicU64>,
    store_ns: [CachePadded<AtomicU64>; 3],
    removes: CachePadded<AtomicU64>,
    remove_blocks: CachePadded<AtomicU64>,
    remove_ns: [CachePadded<AtomicU64>; 3],
}

/// A phase timer that costs nothing without `lane-stats`.
#[cfg(feature = "lane-stats")]
type Tick = std::time::Instant;
/// Stands in for the timer when the feature is off.
#[cfg(not(feature = "lane-stats"))]
#[derive(Clone, Copy)]
struct Tick;

#[inline]
fn tick() -> Tick {
    #[cfg(feature = "lane-stats")]
    {
        std::time::Instant::now()
    }
    #[cfg(not(feature = "lane-stats"))]
    {
        Tick
    }
}

#[cfg(feature = "lane-stats")]
type LaneCounterSlot = CachePadded<AtomicU64>;
/// Stands in for a counter when the feature is off.
#[cfg(not(feature = "lane-stats"))]
struct LaneCounterSlot;

#[cfg(not(feature = "lane-stats"))]
static NO_COUNTER: LaneCounterSlot = LaneCounterSlot;

#[inline]
fn lap(counter: &LaneCounterSlot, since: Tick) {
    #[cfg(feature = "lane-stats")]
    counter.fetch_add(since.elapsed().as_nanos() as u64, Ordering::Relaxed);
    #[cfg(not(feature = "lane-stats"))]
    let _ = (counter, since);
}

/// Worker slots: a slot is in use from `intern_worker` until `remove_worker`, after which it is
/// handed out again (every coverage bit of a removed worker is clear by then).
#[derive(Default)]
struct WorkerRegistry {
    names: Vec<Option<Arc<str>>>,
    free: Vec<u32>,
}

/// The run-compressed index. Worker ids are interned `u32`s, as in the positional indexer.
pub struct ChainIndex {
    slab: RunSlab,
    arena: WordArena,
    words: usize,
    max_workers: usize,
    worker_to_id: DashMap<Arc<str>, u32, FxBuildHasher>,
    registry: Mutex<WorkerRegistry>,
    /// Blocks held per worker, one cache line each: the lanes write these on every event.
    worker_blocks: Box<[CachePadded<AtomicUsize>]>,
    /// Per-worker signed contributions to the distinct-block count, summed on demand.
    distinct_blocks: Box<[CachePadded<AtomicIsize>]>,
    /// Stored blocks whose engine hash was not the one the index carries.
    engine_conflicts: AtomicUsize,
    /// Stores whose blocks carried the engine hashes the index holds for other content.
    landing_mismatches: AtomicUsize,
    /// Blocks a store moved to another place for their worker (the old membership released).
    moved_hashes: AtomicUsize,
    /// Splits by cause and runs unlinked, always counted (one relaxed increment per split or
    /// death): the fragmentation figures a churn harness and the gateway's gauges read.
    splits_branch: AtomicUsize,
    splits_hole: AtomicUsize,
    splits_mid_run: AtomicUsize,
    splits_prefix_holders: AtomicUsize,
    runs_died: AtomicUsize,
    #[cfg(feature = "lane-stats")]
    counters: LaneCounters,
}

impl Default for ChainIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// How a worker holds a run.
enum Holding {
    Full,
    /// The first `cutoff` blocks only.
    Partial(usize),
    None,
}

/// Outcome of one attempt to walk a store into the tree.
enum Walk {
    Done,
    /// The walk met a run another lane unlinked meanwhile; start over from the parent block.
    Restart,
    /// The parent block is not held after all (its run died since the map was written).
    NoParent,
}

/// What the lock-free look at a run during a store walk decided.
enum Plan {
    /// Nothing changes here: `matched` blocks are already held; carry on after them. With them,
    /// how many of the matched blocks carry an engine hash that is not the one the index holds.
    Skip(usize, usize),
    /// Continue in the child `(id, generation)` from its first block.
    Descend(u32, u32),
    /// Open a new child for the blocks in hand at this offset of the run (its end, or a
    /// divergence inside it), without the run's lock if the table has room.
    InsertAt(usize),
    /// The run must change: take its lock and re-read it.
    Lock,
}

/// Outcome of claiming a child-table slot.
enum Claim {
    Inserted,
    /// Another writer already linked a child with this head.
    Exists(u32, u32),
    /// No free slot (or no table): grow under the lock.
    Full,
    /// The run changed since the plan was made (a split moved its end): plan again.
    Changed,
}

/// What [`ChainIndex::store_in_run`] found.
enum InRun<'b> {
    /// Every block is placed.
    Done,
    /// The blocks still to place start right after the run's last block.
    Continue(&'b [StoredBlock]),
    /// The blocks still to place diverge from the run at this offset: they continue it from
    /// there as a child.
    ContinueAt(&'b [StoredBlock], usize),
    /// Carry on in this run (id, generation) from its first block.
    MoveTo(u32, u32),
}

/// Blocks a store walk placed: `count` blocks from `start` in the event sit in `run` from
/// `offset`. Recorded under the run lock, written into the lane map after it.
struct Placed {
    run: u32,
    offset: u32,
    start: usize,
    count: usize,
    /// Blocks of the range whose engine hash differs from the one the index carries for the
    /// block (counted in the stats; the worker's own hash keys its lane map either way).
    conflicts: usize,
}

/// How many of the stored blocks carry an engine hash that is not the one the index holds
/// (`engines` runs parallel to `stored`); zero in a fleet whose engines agree.
fn engine_conflicts(stored: &[StoredBlock], engines: &[AtomicU64]) -> usize {
    stored
        .iter()
        .zip(engines)
        .filter(|(block, slot)| block.seq_hash.0 != slot.load(Ordering::Relaxed))
        .count()
}

/// A batch of a worker's offsets in one run, with the generation the run must still have.
struct Removal {
    run: u32,
    generation: Option<u32>,
    offsets: Vec<u32>,
}

impl ChainIndex {
    /// An index for up to 256 workers.
    pub fn new() -> Self {
        Self::with_max_workers(256)
    }

    /// An index for up to `max_workers` interned workers (at most 1024); coverage costs one bit
    /// per worker per run, rounded up to 64.
    pub fn with_max_workers(max_workers: usize) -> Self {
        let max_workers = max_workers.clamp(1, MAX_WORDS * 64);
        let words = max_workers.div_ceil(64);
        let slab = RunSlab::new(words);
        // The root is run 0: position 0's parent, no blocks, no coverage.
        let root = slab.alloc(
            0,
            ROOT,
            Window {
                block: NONE,
                base: 0,
                engine: NONE,
                len: 0,
                children: NONE,
                partials: NONE,
                forwards: NONE,
            },
        );
        debug_assert_eq!(root, ROOT);
        Self {
            slab,
            arena: WordArena::new(),
            words,
            max_workers,
            worker_to_id: DashMap::with_hasher(FxBuildHasher),
            registry: Mutex::new(WorkerRegistry::default()),
            worker_blocks: (0..max_workers)
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect(),
            distinct_blocks: (0..max_workers)
                .map(|_| CachePadded::new(AtomicIsize::new(0)))
                .collect(),
            engine_conflicts: AtomicUsize::new(0),
            landing_mismatches: AtomicUsize::new(0),
            moved_hashes: AtomicUsize::new(0),
            splits_branch: AtomicUsize::new(0),
            splits_hole: AtomicUsize::new(0),
            splits_mid_run: AtomicUsize::new(0),
            splits_prefix_holders: AtomicUsize::new(0),
            runs_died: AtomicUsize::new(0),
            #[cfg(feature = "lane-stats")]
            counters: LaneCounters::default(),
        }
    }

    /// The lane-side counters (all zero unless the crate is built with `lane-stats`).
    pub fn lane_stats(&self) -> LaneStats {
        #[cfg(feature = "lane-stats")]
        {
            let load = |slots: &[CachePadded<AtomicU64>; 3]| {
                [
                    slots[0].load(Ordering::Relaxed),
                    slots[1].load(Ordering::Relaxed),
                    slots[2].load(Ordering::Relaxed),
                ]
            };
            LaneStats {
                locks: load(&self.counters.locks),
                contended: load(&self.counters.contended),
                wait_ns: load(&self.counters.wait_ns),
                restarts: self.counters.restarts.load(Ordering::Relaxed),
                splits_divergence: self.splits_branch.load(Ordering::Relaxed) as u64,
                splits_hole: self.splits_hole.load(Ordering::Relaxed) as u64,
                splits_stale_parent: self.splits_mid_run.load(Ordering::Relaxed) as u64,
                inserts: self.counters.inserts.load(Ordering::Relaxed),
                stores: self.counters.stores.load(Ordering::Relaxed),
                store_blocks: self.counters.store_blocks.load(Ordering::Relaxed),
                store_ns: load(&self.counters.store_ns),
                removes: self.counters.removes.load(Ordering::Relaxed),
                remove_blocks: self.counters.remove_blocks.load(Ordering::Relaxed),
                remove_ns: load(&self.counters.remove_ns),
            }
        }
        #[cfg(not(feature = "lane-stats"))]
        {
            LaneStats {
                splits_divergence: self.splits_branch.load(Ordering::Relaxed) as u64,
                splits_hole: self.splits_hole.load(Ordering::Relaxed) as u64,
                splits_stale_parent: self.splits_mid_run.load(Ordering::Relaxed) as u64,
                ..LaneStats::default()
            }
        }
    }

    /// Lock a run's writer state, charging contended waits to the kind of run (root, a leaf only
    /// `worker` holds, or a shared run) when `lane-stats` is on.
    #[inline]
    fn lock_run(&self, run_id: u32, worker: u32) -> MutexGuard<'_, RunMeta> {
        let run = self.slab.run(run_id);
        #[cfg(not(feature = "lane-stats"))]
        {
            let _ = worker;
            run.meta.lock()
        }
        #[cfg(feature = "lane-stats")]
        {
            let (guard, waited) = match run.meta.try_lock() {
                Some(guard) => (guard, 0u64),
                None => {
                    let started = std::time::Instant::now();
                    let guard = run.meta.lock();
                    (guard, started.elapsed().as_nanos() as u64)
                }
            };
            let kind = if run_id == ROOT {
                LockKind::Root
            } else if run.children.load(Ordering::Relaxed) == NONE
                && run.partials.load(Ordering::Relaxed) == NONE
                && covered_only_by(self.slab.coverage(run_id), worker)
            {
                LockKind::Own
            } else {
                LockKind::Shared
            } as usize;
            self.counters.locks[kind].fetch_add(1, Ordering::Relaxed);
            if waited > 0 {
                self.counters.contended[kind].fetch_add(1, Ordering::Relaxed);
                self.counters.wait_ns[kind].fetch_add(waited, Ordering::Relaxed);
            }
            guard
        }
    }

    /// Count a split by its cause: a divergence inside the run (`Root`), a hole a removal left
    /// (`Own`), or a store under a parent the worker did not hold up to (`Shared`).
    #[inline]
    fn count_split(&self, cause: LockKind) {
        let counter = match cause {
            LockKind::Root => &self.splits_branch,
            LockKind::Own => &self.splits_hole,
            LockKind::Shared => &self.splits_mid_run,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Intern a worker name; the same name maps to the same id until the worker is removed.
    pub fn intern_worker(&self, worker: &str) -> Result<u32, WorkerIdExhausted> {
        if let Some(entry) = self.worker_to_id.get(worker) {
            return Ok(*entry.value());
        }
        let name: Arc<str> = Arc::from(worker);
        match self.worker_to_id.entry(name.clone()) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let mut registry = self.registry.lock();
                let id = match registry.free.pop() {
                    Some(id) => id,
                    None if registry.names.len() < self.max_workers => {
                        registry.names.push(None);
                        (registry.names.len() - 1) as u32
                    }
                    None => return Err(WorkerIdExhausted),
                };
                registry.names[id as usize] = Some(name);
                entry.insert(id);
                Ok(id)
            }
        }
    }

    /// Hand a removed worker's slot back once nothing refers to it any more.
    fn release_worker(&self, worker: u32) {
        // Lock order is name map shard, then registry (as in `intern_worker`): never hold the
        // registry while touching the name map, or an intern and a release deadlock each other.
        let name = {
            let mut registry = self.registry.lock();
            registry
                .names
                .get_mut(worker as usize)
                .and_then(Option::take)
        };
        let Some(name) = name else {
            return;
        };
        self.worker_to_id.remove(&*name);
        self.registry.lock().free.push(worker);
    }

    pub fn worker_id(&self, worker: &str) -> Option<u32> {
        self.worker_to_id.get(worker).map(|entry| *entry.value())
    }

    /// Whether no worker holds any block: the root has no live child, read under the root's
    /// version. O(1), for a caller that asks before every lookup (`current_size` reads a line
    /// per worker slot).
    pub fn is_empty(&self) -> bool {
        let root = self.slab.run(ROOT);
        loop {
            let (window, version) = root.snapshot();
            let empty = window.children == NONE || self.arena.table_live(window.children) == 0;
            if root.confirm(version) {
                return empty;
            }
        }
    }

    /// Blocks held across all workers (a block two workers hold counts twice).
    pub fn current_size(&self) -> usize {
        self.worker_blocks
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum()
    }

    /// Distinct blocks held by at least one worker.
    pub fn entry_count(&self) -> usize {
        let total: isize = self
            .distinct_blocks
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum();
        total.max(0) as usize
    }

    pub fn worker_block_count(&self, worker: u32) -> usize {
        self.worker_blocks
            .get(worker as usize)
            .map_or(0, |count| count.load(Ordering::Relaxed))
    }

    fn credit(&self, worker: u32, blocks: usize) {
        if blocks == 0 {
            return;
        }
        self.worker_blocks[worker as usize].fetch_add(blocks, Ordering::Relaxed);
    }

    fn debit(&self, worker: u32, blocks: usize) {
        if blocks == 0 {
            return;
        }
        self.worker_blocks[worker as usize].fetch_sub(blocks, Ordering::Relaxed);
    }

    /// `worker` became the first holder of `blocks` distinct blocks.
    fn distinct_add(&self, worker: u32, blocks: usize) {
        if blocks != 0 {
            self.distinct_blocks[worker as usize].fetch_add(blocks as isize, Ordering::Relaxed);
        }
    }

    /// `worker` was the last holder of `blocks` distinct blocks.
    fn distinct_sub(&self, worker: u32, blocks: usize) {
        if blocks != 0 {
            self.distinct_blocks[worker as usize].fetch_sub(blocks as isize, Ordering::Relaxed);
        }
    }

    /// The hash at `offset` of a run, from the writer's side (the run is locked).
    #[inline]
    fn hash_at(&self, run: &Run, offset: usize) -> u64 {
        let data = run.block.load(Ordering::Relaxed) + run.base.load(Ordering::Relaxed);
        self.arena
            .word(data + offset as u32)
            .load(Ordering::Relaxed)
    }

    /// The child-table key of a child continuing a run from `offset` with the content hash
    /// `head`: a child may hang off any offset of its parent (a divergence inside a run does not
    /// split the run), so the key carries the offset beside the hash. Never zero, which a table
    /// slot reads as empty.
    #[inline]
    fn child_key(offset: usize, head: u64) -> u64 {
        let mixed = head
            ^ (offset as u64 + 1)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .rotate_left(23);
        if mixed == 0 {
            1
        } else {
            mixed
        }
    }

    /// The content hash of a run's first block, read under its version (the run is not locked).
    fn child_head(&self, run_id: u32) -> u64 {
        let run = self.slab.run(run_id);
        loop {
            let (window, version) = run.snapshot();
            let head = self
                .arena
                .word(window.block + window.base)
                .load(Ordering::Relaxed);
            if run.confirm(version) {
                return head;
            }
        }
    }

    /// One step along a block's forwarding chain, read under the run's version: the next place
    /// and the generation it must have, the place standing (with the run's generation), or the
    /// block gone (a dead or reused run, a `GONE` suffix). A split publishes its shorter length
    /// and its forwarding record under one lock but in two steps, so a reader that meets an
    /// offset at or past the length without a record waits for the lock once and reads again;
    /// after that the record is there, or the reference was stale.
    fn hop(&self, at: BlockRef, expected: Option<u32>) -> Hop {
        let mut waited = false;
        loop {
            if at.run == GONE {
                return Hop::Gone;
            }
            let run = self.slab.run(at.run);
            let (window, version) = run.snapshot();
            let generation = (version >> 32) as u32;
            if expected.is_some_and(|wanted| wanted != generation)
                || (at.run != ROOT && window.len == 0)
            {
                return Hop::Gone;
            }
            let hop = self.arena.forwards_find(window.forwards, at.offset);
            if !run.confirm(version) {
                continue;
            }
            match hop {
                Some((next, next_generation)) => return Hop::Next(next, next_generation),
                None if at.run != ROOT && at.offset >= window.len => {
                    if waited {
                        return Hop::Gone;
                    }
                    drop(run.meta.lock());
                    waited = true;
                }
                None => return Hop::Here(generation),
            }
        }
    }

    /// Where a block recorded at `at` lives now, with the generation of the run: the end of its
    /// forwarding chain. `None` when nobody holds it any more.
    fn resolve(&self, mut at: BlockRef) -> Option<(BlockRef, u32)> {
        let mut expected: Option<u32> = None;
        loop {
            match self.hop(at, expected) {
                Hop::Next(next, generation) => {
                    at = next;
                    expected = Some(generation);
                }
                Hop::Here(generation) => return Some((at, generation)),
                Hop::Gone => return None,
            }
        }
    }

    /// Whether `to` is `from` or a place `from` has forwarded to since: the places one block has
    /// had. Forwarding records only accumulate while their runs live (a prefix outlives its
    /// suffix, being its parent), so the answer does not depend on when it is read, unlike the
    /// ends of two chains resolved one after the other.
    fn forwards_to(&self, from: BlockRef, to: BlockRef) -> bool {
        let mut at = from;
        let mut expected: Option<u32> = None;
        loop {
            if at == to {
                return true;
            }
            match self.hop(at, expected) {
                Hop::Next(next, generation) => {
                    at = next;
                    expected = Some(generation);
                }
                Hop::Here(_) | Hop::Gone => return false,
            }
        }
    }

    /// Whether `worker`'s lane map holds the block with engine hash `key`.
    pub fn is_held(&self, map: &ChainBlockMap, key: SequenceHash) -> bool {
        let _ = self;
        map.contains_key(key)
    }

    /// Add `child` to the run's table (the run is locked): claims a slot like a lock-free
    /// inserter, growing the table under a version step that first drains inserters in flight.
    /// `Some` when another writer linked a child with this head meanwhile: the caller descends
    /// into that one instead.
    fn link_child(&self, run: &Run, offset: usize, head: u64, child: u32) -> Option<(u32, u32)> {
        let generation = self.slab.run(child).generation();
        let key = Self::child_key(offset, head);
        loop {
            let table = run.children.load(Ordering::Acquire);
            match self.arena.table_claim(table, key, child, generation) {
                Claim::Inserted => return None,
                Claim::Exists(other, other_generation) => return Some((other, other_generation)),
                Claim::Full | Claim::Changed => {}
            }
            run.begin_update();
            run.wait_inflight();
            let table = run.children.load(Ordering::Relaxed);
            let grown = if table == NONE {
                self.arena.alloc_table(MIN_TABLE_SLOTS)
            } else {
                self.arena.table_grown(table)
            };
            run.children.store(grown, Ordering::Relaxed);
            run.end_update();
            self.arena.free_table(table);
        }
    }

    /// Take `child` out of the run's table (the run is locked); an emptied table goes away once
    /// no insert is in flight on it.
    fn unlink_child(&self, run: &Run, child: u32) {
        let table = run.children.load(Ordering::Relaxed);
        if table == NONE {
            return;
        }
        let live = self.arena.table_take(table, child);
        if live == 0 {
            run.begin_update();
            run.wait_inflight();
            if self.arena.word(table + 1).load(Ordering::Relaxed) == 0 {
                run.children.store(NONE, Ordering::Relaxed);
                run.end_update();
                self.arena.free_table(table);
            } else {
                run.end_update();
            }
        } else if self.arena.table_dead(table) > self.arena.table_slots(table) / 2 {
            // Children come and go at every offset of a long-lived run (a decode tail per prompt
            // end): a table more than half tombstones is rebuilt from its live entries, so a
            // probe never walks the dead keys of a thousand finished requests.
            run.begin_update();
            run.wait_inflight();
            let fresh = self.arena.table_from(&self.arena.table_entries(table));
            run.children.store(fresh, Ordering::Relaxed);
            run.end_update();
            self.arena.free_table(table);
        }
    }

    /// A lock-free attempt to link `child` (prepared, unpublished) under `run` for the blocks
    /// after its last one, as seen in the snapshot with `planned` version: holds `inflight`
    /// across the claim so a split or table growth cannot move the table under the insert, and
    /// gives up with `Claim::Changed` if the run moved on since the plan (its end is elsewhere
    /// now). `Claim::Full` means the locked path must do it.
    fn insert_child(
        &self,
        run_id: u32,
        child: u32,
        offset: usize,
        head: u64,
        planned: u64,
    ) -> Claim {
        let run = self.slab.run(run_id);
        loop {
            run.inflight.fetch_add(1, Ordering::SeqCst);
            let version = run.version.load(Ordering::SeqCst);
            if version & 1 == 1 {
                run.inflight.fetch_sub(1, Ordering::SeqCst);
                std::hint::spin_loop();
                continue;
            }
            if version != planned {
                run.inflight.fetch_sub(1, Ordering::SeqCst);
                return Claim::Changed;
            }
            let table = run.children.load(Ordering::Acquire);
            let len = run.len();
            if run.version.load(Ordering::SeqCst) != version {
                run.inflight.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
            if offset > len {
                run.inflight.fetch_sub(1, Ordering::SeqCst);
                return Claim::Changed;
            }
            // The child continues this run from `offset`: its end, or a divergence inside it.
            let new_run = self.slab.run(child);
            new_run
                .start
                .store((run.start() + offset) as u32, Ordering::Relaxed);
            new_run.parent.store(run_id, Ordering::Release);
            let claim = self.arena.table_claim(
                table,
                Self::child_key(offset, head),
                child,
                new_run.generation(),
            );
            run.inflight.fetch_sub(1, Ordering::SeqCst);
            return claim;
        }
    }

    /// A run prepared for a store that another lane beat to the slot: back to the slab.
    fn discard_run(&self, run_id: u32, worker: u32, freed: &mut Vec<u32>) {
        let mut meta = self.slab.run(run_id).meta.lock();
        clear(self.slab.coverage(run_id), worker);
        self.kill(run_id, &mut meta, freed);
    }

    /// Retire a run that is unlinked (locked by the caller): its array reference goes, its
    /// window empties, and its id is queued for reuse once the caller has dropped the lock.
    fn kill(&self, run_id: u32, meta: &mut RunMeta, freed: &mut Vec<u32>) {
        let run = self.slab.run(run_id);
        let block = run.block.load(Ordering::Relaxed);
        let engine = run.engine.load(Ordering::Relaxed);
        let partials = run.partials.load(Ordering::Relaxed);
        let forwards = run.forwards.load(Ordering::Relaxed);
        run.begin_update();
        run.block.store(NONE, Ordering::Relaxed);
        run.base.store(0, Ordering::Relaxed);
        run.engine.store(NONE, Ordering::Relaxed);
        run.len.store(0, Ordering::Relaxed);
        run.children.store(NONE, Ordering::Relaxed);
        run.partials.store(NONE, Ordering::Relaxed);
        run.forwards.store(NONE, Ordering::Relaxed);
        run.end_update();
        self.arena.array_release(block);
        self.arena.array_release(engine);
        self.arena.free_partials(partials);
        self.arena.free_table(forwards);
        meta.dead = true;
        self.runs_died.fetch_add(1, Ordering::Relaxed);
        freed.push(run_id);
    }

    /// Dead ids go back to the slab only after their locks are released, so a thread holding a
    /// live run's lock and reviving a dead id never waits on a thread that holds the dead id's
    /// lock and wants the live run.
    fn recycle(&self, freed: &mut Vec<u32>) {
        for id in freed.drain(..) {
            self.slab.free.push(id);
        }
    }

    /// How `worker` holds a run: all of it, a prefix of it, or nothing.
    fn holding(&self, run_id: u32, worker: u32) -> Holding {
        if has(self.slab.coverage(run_id), worker) {
            return Holding::Full;
        }
        let table = self.slab.run(run_id).partials.load(Ordering::Relaxed);
        match self.arena.partial_find(table, worker) {
            Some((_, cutoff)) => Holding::Partial(cutoff as usize),
            None => Holding::None,
        }
    }

    /// Blocks of a run that `worker` holds.
    fn held_by(&self, run_id: u32, worker: u32) -> usize {
        match self.holding(run_id, worker) {
            Holding::Full => self.slab.run(run_id).len(),
            Holding::Partial(cutoff) => cutoff,
            Holding::None => 0,
        }
    }

    /// Blocks of a run held by at least one worker: all of them while anybody holds the whole
    /// run, otherwise the longest partial prefix.
    fn held_len(&self, run_id: u32) -> usize {
        let run = self.slab.run(run_id);
        if coverage_is_empty(self.slab.coverage(run_id)) {
            self.arena.partial_max(run.partials.load(Ordering::Relaxed))
        } else {
            run.len()
        }
    }

    fn has_holders(&self, run_id: u32) -> bool {
        !coverage_is_empty(self.slab.coverage(run_id))
            || self
                .arena
                .partials_live(self.slab.run(run_id).partials.load(Ordering::Relaxed))
                > 0
    }

    /// Distinct-block accounting around a change to `run_id` (locked by the caller): the delta
    /// of blocks held by anybody, attributed to `worker`. A split changes nothing in total (the
    /// prefix and the suffix hold between them what the run held), so callers take `before`
    /// after any split; the suffix is published by then and other lanes account for their own
    /// changes to it.
    fn settle_distinct(&self, worker: u32, before: usize, run_id: u32) {
        let after = self.held_len(run_id);
        if after > before {
            self.distinct_add(worker, after - before);
        } else {
            self.distinct_sub(worker, before - after);
        }
    }

    /// Make `worker` hold exactly `[0, cutoff)` of the run (the run is locked): the whole run when
    /// `cutoff` reaches its length, nothing when 0. Readers treat the coverage bit as the truth
    /// when both forms are visible, so a worker gains its bit before its partial entry goes and
    /// gains a partial entry before its bit goes.
    fn set_holding(&self, run_id: u32, worker: u32, cutoff: usize) {
        let run = self.slab.run(run_id);
        let coverage = self.slab.coverage(run_id);
        let len = run.len();
        let was_full = has(coverage, worker);
        let table = run.partials.load(Ordering::Relaxed);
        let entry = if was_full {
            None
        } else {
            self.arena.partial_find(table, worker)
        };
        if cutoff >= len {
            if !was_full {
                set(coverage, worker);
            }
            if let Some((slot, _)) = entry {
                self.drop_partial(run, table, slot);
            }
        } else if cutoff == 0 {
            if was_full {
                clear(coverage, worker);
            }
            if let Some((slot, _)) = entry {
                self.drop_partial(run, table, slot);
            }
        } else {
            match entry {
                Some((slot, old)) => {
                    if old as usize != cutoff {
                        self.arena.partial_set(table, slot, worker, cutoff as u32);
                    }
                }
                None if was_full => {
                    // A reader that saw neither the entry nor the bit would score nothing for a
                    // worker that holds a prefix: keep the two writes inside one version step.
                    run.begin_update();
                    self.add_partial(run, table, worker, cutoff as u32);
                    clear(coverage, worker);
                    run.end_update();
                }
                None => self.add_partial(run, table, worker, cutoff as u32),
            }
        }
    }

    /// Append a partial entry, growing (and republishing) the table when it is full.
    fn add_partial(&self, run: &Run, table: u32, worker: u32, cutoff: u32) {
        if self.arena.partial_put(table, worker, cutoff) {
            return;
        }
        let grown = if table == NONE {
            self.arena.alloc_partials(MIN_TABLE_SLOTS)
        } else {
            self.arena.partials_grown(table, 1)
        };
        let placed = self.arena.partial_put(grown, worker, cutoff);
        debug_assert!(placed);
        let nested = run.version.load(Ordering::Relaxed) & 1 == 1;
        if !nested {
            run.begin_update();
        }
        run.partials.store(grown, Ordering::Relaxed);
        if !nested {
            run.end_update();
        }
        self.arena.free_partials(table);
    }

    /// Tombstone a partial entry; an emptied table goes away.
    fn drop_partial(&self, run: &Run, table: u32, slot: usize) {
        if self.arena.partial_remove(table, slot) == 0 {
            run.begin_update();
            run.partials.store(NONE, Ordering::Relaxed);
            run.end_update();
            self.arena.free_partials(table);
        }
    }

    /// Split `run` (locked by the caller, whose guard is `_meta`) at `at`: the run keeps `[0, at)`; a new suffix run takes
    /// `[at, len)` on the same hash array, with the run's children, its full holders and the
    /// partial holders reaching past `at`; partial holders reaching `at` become full holders of
    /// the prefix. A suffix nobody would hold is not created when the run has no children: the
    /// forwarding record says those blocks are gone. No worker's holdings change in total.
    /// Split the run (locked by the caller) at the median cutoff of its prefix holders when more
    /// than `PARTIAL_CAP` of them hold a prefix of it: the holders at or past the median become
    /// whole holders of the prefix, the rest keep their entries on one side or the other.
    fn cap_prefix_holders(&self, run_id: u32, meta: &mut RunMeta) {
        let run = self.slab.run(run_id);
        let table = run.partials.load(Ordering::Relaxed);
        if table == NONE || self.arena.partials_live(table) <= PARTIAL_CAP {
            return;
        }
        let mut cutoffs: Vec<u32> = self
            .arena
            .partial_entries(table)
            .iter()
            .map(|&(_, cutoff)| cutoff)
            .collect();
        if cutoffs.len() <= PARTIAL_CAP {
            return;
        }
        cutoffs.sort_unstable();
        let at = cutoffs[cutoffs.len() / 2] as usize;
        if at == 0 || at >= run.len() {
            return;
        }
        self.splits_prefix_holders.fetch_add(1, Ordering::Relaxed);
        self.split_locked(run_id, meta, at);
    }

    fn split_locked(&self, run_id: u32, _meta: &mut RunMeta, at: usize) -> u32 {
        let run = self.slab.run(run_id);
        let coverage = self.slab.coverage(run_id);
        let len = run.len();
        debug_assert!(at > 0 && at < len, "split inside the run: 0 < {at} < {len}");
        let block = run.block.load(Ordering::Relaxed);
        let engine = run.engine.load(Ordering::Relaxed);
        let base = run.base.load(Ordering::Relaxed);
        let children = run.children.load(Ordering::Relaxed);
        let partials = self
            .arena
            .partial_entries(run.partials.load(Ordering::Relaxed));
        let beyond: Vec<(u32, u32)> = partials
            .iter()
            .filter(|(_, cutoff)| *cutoff as usize > at)
            .map(|&(worker, cutoff)| (worker, cutoff - at as u32))
            .collect();
        let suffix_held = !coverage_is_empty(coverage) || !beyond.is_empty();
        // Children hang off any offset of the run: those past `at` move to the suffix, keyed by
        // their offset within it; the rest stay. Decided under the version step, after
        // the inserts in flight have landed, so none is missed.
        run.begin_update();
        run.wait_inflight();
        let start = run.start();
        let mut kept: Vec<(u64, u32, u32)> = Vec::new();
        let mut moved: Vec<(u64, u32, u32)> = Vec::new();
        for (key, child, generation) in self.arena.table_entries(children) {
            let child_offset = self.slab.run(child).start().saturating_sub(start);
            // A child at `at` itself stays an end child of the prefix (a walk never descends
            // from offset zero of a run, so the suffix must not carry one there).
            if child_offset <= at {
                kept.push((key, child, generation));
            } else {
                moved.push((
                    Self::child_key(child_offset - at, self.child_head(child)),
                    child,
                    generation,
                ));
            }
        }
        let suffix_id = if !suffix_held && moved.is_empty() {
            run.len.store(at as u32, Ordering::Relaxed);
            run.end_update();
            GONE
        } else {
            self.arena.array_retain(block);
            self.arena.array_retain(engine);
            let suffix_partials = self.arena.partials_from(&beyond);
            let suffix_children = self.arena.table_from(&moved);
            let suffix_id = self.slab.alloc(
                start + at,
                run_id,
                Window {
                    block,
                    base: base + at as u32,
                    engine,
                    len: (len - at) as u32,
                    children: suffix_children,
                    partials: suffix_partials,
                    forwards: NONE,
                },
            );
            let suffix = self.slab.run(suffix_id);
            for (slot, word) in self.slab.coverage(suffix_id).iter().zip(coverage) {
                slot.store(word.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            for &(_, child, _) in &moved {
                self.slab
                    .run(child)
                    .parent
                    .store(suffix_id, Ordering::Release);
            }
            kept.push((
                Self::child_key(at, self.hash_at(run, at)),
                suffix_id,
                suffix.generation(),
            ));
            let table = self.arena.table_from(&kept);
            run.len.store(at as u32, Ordering::Relaxed);
            run.children.store(table, Ordering::Relaxed);
            run.end_update();
            self.arena.free_table(children);
            suffix_id
        };
        // The prefix is `[0, at)` now: a partial holder that reached it holds all of it. Done
        // after the truncation so no reader sees a bit for the whole old run.
        for (worker, cutoff) in partials {
            if cutoff as usize >= at {
                self.set_holding(run_id, worker, at);
            }
        }
        let generation = if suffix_id == GONE {
            0
        } else {
            self.slab.run(suffix_id).generation()
        };
        self.add_forward(run, at as u32, suffix_id, generation);
        suffix_id
    }

    /// Record a split on the run (locked by the caller); a full table is replaced under a version
    /// step so a lock-free reader never follows a recycled one.
    fn add_forward(&self, run: &Run, at: u32, suffix: u32, generation: u32) {
        let table = run.forwards.load(Ordering::Relaxed);
        if self.arena.forwards_push(table, at, suffix, generation) {
            return;
        }
        let grown = if table == NONE {
            self.arena.alloc_forwards(MIN_TABLE_SLOTS)
        } else {
            self.arena.forwards_grown(table)
        };
        let placed = self.arena.forwards_push(grown, at, suffix, generation);
        debug_assert!(placed);
        run.begin_update();
        run.forwards.store(grown, Ordering::Relaxed);
        run.end_update();
        self.arena.free_table(table);
    }

    /// Unlink `run` (locked by the caller, known to be an uncovered leaf) from its parent and
    /// retire it, then the parent if that leaves it an uncovered leaf too.
    fn unlink_locked(&self, run_id: u32, meta: &mut RunMeta, freed: &mut Vec<u32>) {
        if run_id == ROOT || meta.dead {
            return;
        }
        let run = self.slab.run(run_id);
        loop {
            let parent_id = run.parent.load(Ordering::Acquire);
            let parent = self.slab.run(parent_id);
            // Child-then-parent is the only order in which two run locks are ever held. A split
            // of the parent may have re-parented this run while we waited; check and retry.
            let mut parent_meta = parent.meta.lock();
            if run.parent.load(Ordering::Acquire) != parent_id {
                continue;
            }
            self.unlink_child(parent, run_id);
            self.kill(run_id, meta, freed);
            if parent_id != ROOT
                && parent.children.load(Ordering::Relaxed) == NONE
                && !self.has_holders(parent_id)
            {
                self.unlink_locked(parent_id, &mut parent_meta, freed);
            }
            return;
        }
    }

    /// Store `blocks` for `worker` after `parent` (position 0 when `None`).
    pub fn apply_stored(
        &self,
        worker: u32,
        blocks: &[StoredBlock],
        parent: Option<SequenceHash>,
        map: &mut ChainBlockMap,
    ) -> Result<(), ApplyError> {
        if blocks.is_empty() {
            return Ok(());
        }
        #[cfg(feature = "lane-stats")]
        {
            self.counters.stores.fetch_add(1, Ordering::Relaxed);
            self.counters
                .store_blocks
                .fetch_add(blocks.len() as u64, Ordering::Relaxed);
        }
        let origin = match parent {
            None => None,
            Some(hash) => {
                if map.is_empty() {
                    return Err(ApplyError::WorkerNotTracked);
                }
                match map.get(hash) {
                    Some(at) => Some((hash, at)),
                    None => return Err(ApplyError::ParentBlockNotFound),
                }
            }
        };
        loop {
            match self.store_walk(worker, blocks, origin, map) {
                Walk::Done => return Ok(()),
                Walk::Restart => {}
                Walk::NoParent => return Err(ApplyError::ParentBlockNotFound),
            }
        }
    }

    /// One attempt to place a store, from the parent block (or the root) down the tree. Map
    /// entries for the placed blocks are written after the run locks are released: the lane map
    /// is private, and a split meanwhile is covered by the forwarding records.
    fn store_walk(
        &self,
        worker: u32,
        blocks: &[StoredBlock],
        origin: Option<(SequenceHash, BlockRef)>,
        map: &mut ChainBlockMap,
    ) -> Walk {
        let mut pending: Vec<Placed> = Vec::new();
        let walk = tick();
        let outcome = self.store_walk_locked(worker, blocks, origin, map, &mut pending);
        lap(self.counter_slot(1), walk);
        self.write_placements(worker, blocks, pending, map);
        outcome
    }

    /// The lane map writes of one store, after the run locks are released. A block the engine
    /// names by a hash this worker already holds elsewhere (a store that moves the hash, as a
    /// store without its parent followed by the whole chain does) keeps one place per hash: the
    /// old membership is released. A re-store of the same block at the same place is not a
    /// move, and "the same place" is read through the forwarding records: the placement was
    /// recorded under the run's lock, and splits by other lanes land freely between that and
    /// this write, so the recorded place and the map's old entry may both be behind the block's
    /// current place by any number of splits. A block that did not move has its new place on
    /// the forwarding chain from its old one, and that stays true however many splits follow
    /// (`forwards_to`); comparing the two places resolved to their ends instead raced with a
    /// split between the two reads, took the block for a moved hash, and released the worker's
    /// holding at its real place while the map kept the entry (lookups then scored the worker
    /// short, and a store under the block as parent found no parent).
    fn write_placements(
        &self,
        worker: u32,
        blocks: &[StoredBlock],
        pending: Vec<Placed>,
        map: &mut ChainBlockMap,
    ) {
        let writes = tick();
        let mut moved: Vec<BlockRef> = Vec::new();
        for placed in pending {
            let first = BlockRef {
                run: placed.run,
                offset: placed.offset,
            };
            let range = &blocks[placed.start..placed.start + placed.count];
            if placed.conflicts > 0 {
                self.engine_conflicts
                    .fetch_add(placed.conflicts, Ordering::Relaxed);
            }
            map.insert_run(
                range.iter().map(|stored| stored.seq_hash),
                first,
                |old, new| {
                    if !self.forwards_to(old, new) {
                        moved.push(old);
                    }
                },
            );
        }
        lap(self.counter_slot(2), writes);
        if !moved.is_empty() {
            self.moved_hashes.fetch_add(moved.len(), Ordering::Relaxed);
            let work = group_by_run(moved);
            self.apply_removals(worker, work);
        }
    }

    /// The timer slot for a phase: stores 0-2 (resolve, walk, map), removals 3-5 (map, group,
    /// runs).
    #[inline]
    fn counter_slot(&self, phase: usize) -> &LaneCounterSlot {
        #[cfg(feature = "lane-stats")]
        {
            if phase < 3 {
                &self.counters.store_ns[phase]
            } else {
                &self.counters.remove_ns[phase - 3]
            }
        }
        #[cfg(not(feature = "lane-stats"))]
        {
            let _ = (self, phase);
            &NO_COUNTER
        }
    }

    fn store_walk_locked(
        &self,
        worker: u32,
        blocks: &[StoredBlock],
        origin: Option<(SequenceHash, BlockRef)>,
        map: &mut ChainBlockMap,
        pending: &mut Vec<Placed>,
    ) -> Walk {
        let resolving = tick();
        let (mut run_id, mut offset, mut expected) = match origin {
            None => (ROOT, 0usize, self.slab.run(ROOT).generation()),
            Some((hash, at)) => {
                let Some((at, generation)) = self.resolve(at) else {
                    map.remove(hash);
                    return Walk::NoParent;
                };
                map.insert(hash, at);
                (at.run, at.offset as usize + 1, generation)
            }
        };
        lap(self.counter_slot(0), resolving);
        let mut remaining = blocks;
        // Set when a lock-free insert found the child table full: the next look at the same run
        // takes its lock, whose path grows the table.
        let mut force_lock = false;
        loop {
            // Look at the run under its version first: a run this worker already holds up to
            // the blocks in hand, or a run whose child continues them, is passed without its
            // lock. Only a run that must change is locked, and re-read under the lock.
            let run = self.slab.run(run_id);
            let (window, version) = run.snapshot();
            if (version >> 32) as u32 != expected || (run_id != ROOT && window.len == 0) {
                return self.restart();
            }
            let len = window.len as usize;
            if offset > len {
                // A split moved the blocks after the parent into a suffix: resolve again.
                return self.restart();
            }
            let block_start = blocks.len() - remaining.len();
            let plan = if force_lock {
                force_lock = false;
                Plan::Lock
            } else if offset < len {
                let held = self.held_in(run_id, &window, worker);
                if held < offset {
                    Plan::Lock
                } else {
                    let data = window.block + window.base + offset as u32;
                    let hashes = self.arena.words(data, len - offset);
                    let engines = self
                        .arena
                        .words(window.engine + window.base + offset as u32, len - offset);
                    let (matched, conflicts) = self.match_run(hashes, engines, remaining, false);
                    if matched > 0 && offset + matched <= held {
                        // Blocks already held: step past them. A divergence after them is the
                        // next round's business, at the offset where it starts.
                        Plan::Skip(matched, conflicts)
                    } else if matched == 0 && offset > 0 {
                        // The blocks in hand leave the run's content right here: they continue
                        // it as a child hanging off this offset, found or opened without the
                        // lock. The run itself does not change.
                        self.plan_child(&window, offset, remaining[0].content_hash.0)
                    } else {
                        Plan::Lock
                    }
                }
            } else {
                self.plan_child(&window, len, remaining[0].content_hash.0)
            };
            if !run.confirm(version) {
                continue;
            }
            match plan {
                Plan::InsertAt(at) => {
                    let head = remaining[0].content_hash.0;
                    let contents: Vec<u64> = remaining
                        .iter()
                        .map(|stored| stored.content_hash.0)
                        .collect();
                    let engines: Vec<u64> =
                        remaining.iter().map(|stored| stored.seq_hash.0).collect();
                    let block = self
                        .arena
                        .alloc_array(&contents, capacity_for(contents.len()));
                    let engine = self
                        .arena
                        .alloc_array(&engines, self.arena.array_capacity(block));
                    let new_id = self.slab.alloc(
                        0,
                        run_id,
                        Window {
                            block,
                            base: 0,
                            engine,
                            len: contents.len() as u32,
                            children: NONE,
                            partials: NONE,
                            forwards: NONE,
                        },
                    );
                    set(self.slab.coverage(new_id), worker);
                    match self.insert_child(run_id, new_id, at, head, version) {
                        Claim::Inserted => {
                            #[cfg(feature = "lane-stats")]
                            self.counters.inserts.fetch_add(1, Ordering::Relaxed);
                            pending.push(Placed {
                                run: new_id,
                                offset: 0,
                                start: block_start,
                                count: remaining.len(),
                                conflicts: 0,
                            });
                            self.credit(worker, contents.len());
                            self.distinct_add(worker, contents.len());
                            return Walk::Done;
                        }
                        Claim::Exists(child, generation) => {
                            let mut freed = Vec::new();
                            self.discard_run(new_id, worker, &mut freed);
                            self.recycle(&mut freed);
                            run_id = child;
                            expected = generation;
                            offset = 0;
                        }
                        Claim::Full => {
                            let mut freed = Vec::new();
                            self.discard_run(new_id, worker, &mut freed);
                            self.recycle(&mut freed);
                            force_lock = true;
                        }
                        Claim::Changed => {
                            let mut freed = Vec::new();
                            self.discard_run(new_id, worker, &mut freed);
                            self.recycle(&mut freed);
                            return self.restart();
                        }
                    }
                }
                Plan::Skip(matched, conflicts) => {
                    pending.push(Placed {
                        run: run_id,
                        offset: offset as u32,
                        start: block_start,
                        count: matched,
                        conflicts,
                    });
                    if matched == remaining.len() {
                        return Walk::Done;
                    }
                    remaining = &remaining[matched..];
                    offset += matched;
                }
                Plan::Descend(child, generation) => {
                    run_id = child;
                    expected = generation;
                    offset = 0;
                }
                Plan::Lock => {
                    let mut meta = self.lock_run(run_id, worker);
                    if meta.dead || run.generation() != expected || offset > run.len() {
                        return self.restart();
                    }
                    let next = match self.store_in_run(
                        worker,
                        run_id,
                        &mut meta,
                        offset,
                        remaining,
                        block_start,
                        pending,
                    ) {
                        InRun::Done => return Walk::Done,
                        InRun::Continue(rest) => {
                            remaining = rest;
                            let end = run.len();
                            self.store_at(
                                worker,
                                run_id,
                                &meta,
                                end,
                                remaining,
                                blocks.len() - remaining.len(),
                                pending,
                            )
                        }
                        InRun::ContinueAt(rest, at) => {
                            remaining = rest;
                            self.store_at(
                                worker,
                                run_id,
                                &meta,
                                at,
                                remaining,
                                blocks.len() - remaining.len(),
                                pending,
                            )
                        }
                        InRun::MoveTo(child, generation) => Some((child, generation)),
                    };
                    drop(meta);
                    match next {
                        Some((child, generation)) => {
                            run_id = child;
                            expected = generation;
                            offset = 0;
                        }
                        None => return Walk::Done,
                    }
                }
            }
        }
    }

    /// How far `remaining` continues a run whose hashes from the position in hand are `contents`
    /// and `engines`: the matched count and how many of the matched blocks carry an engine hash
    /// that is not the one the index holds.
    ///
    /// An engine hash is a chain hash: an engine names a block by a hash of its parent's hash and
    /// the block's own content, so two chains that agree at a position agree at every position
    /// before it. The match is therefore decided by the engine hash at the end of the window,
    /// and on a mismatch by bisection to the first differing position, instead of a content
    /// compare per block. Two checks keep it exact for a worker whose engine breaks the
    /// assumption, both by falling back to the compare per block: the content hash at the
    /// landing (a block named by the hash the index carries but holding other content, which
    /// is what the relay's hash check exists to catch; `landing_mismatches` counts it), and the
    /// content hash right after the match (an engine that hashes the same content differently
    /// matches by content where it cannot by engine hash, and is counted).
    ///
    /// `count` says whether a landing mismatch is counted: the lock-free look at a run finds it
    /// first and then takes the lock, where the locked look finds it again.
    fn match_run(
        &self,
        contents: &[AtomicU64],
        engines: &[AtomicU64],
        remaining: &[StoredBlock],
        count: bool,
    ) -> (usize, usize) {
        let window = remaining.len().min(contents.len()).min(engines.len());
        if window == 0 {
            return (0, 0);
        }
        let engine_eq = |at: usize| remaining[at].seq_hash.0 == engines[at].load(Ordering::Relaxed);
        let content_eq =
            |at: usize| remaining[at].content_hash.0 == contents[at].load(Ordering::Relaxed);
        let matched = if engine_eq(window - 1) {
            window
        } else {
            // The positions that agree are a prefix: find the first that does not.
            let (mut low, mut high) = (0usize, window - 1);
            while low < high {
                let mid = low + (high - low) / 2;
                if engine_eq(mid) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            low
        };
        let landing_holds = matched == 0 || content_eq(matched - 1);
        let same_content_after = matched < window && content_eq(matched);
        if landing_holds && !same_content_after {
            return (matched, 0);
        }
        if !landing_holds && count {
            self.landing_mismatches.fetch_add(1, Ordering::Relaxed);
        }
        let matched = remaining
            .iter()
            .zip(contents)
            .take_while(|(stored, slot)| stored.content_hash.0 == slot.load(Ordering::Relaxed))
            .count();
        (
            matched,
            engine_conflicts(&remaining[..matched], &engines[..matched]),
        )
    }

    /// Blocks of a run `worker` holds, read from a snapshot (no lock).
    #[inline]
    fn held_in(&self, run_id: u32, window: &Window, worker: u32) -> usize {
        if has(self.slab.coverage(run_id), worker) {
            window.len as usize
        } else {
            self.arena
                .partial_find(window.partials, worker)
                .map_or(0, |(_, cutoff)| cutoff as usize)
        }
    }

    /// A store walk that must start over from the parent block.
    #[inline]
    fn restart(&self) -> Walk {
        #[cfg(feature = "lane-stats")]
        self.counters.restarts.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(feature = "lane-stats"))]
        let _ = self;
        Walk::Restart
    }

    /// The plan where the blocks in hand leave the run's content, at its end or at a divergence
    /// at `offset`: descend into the child that continues the run there, open one without the
    /// lock when the run has a table, or lock a leaf (the worker's own to extend, or a shared one
    /// without a table yet).
    fn plan_child(&self, window: &Window, offset: usize, head: u64) -> Plan {
        match self
            .arena
            .table_find(window.children, Self::child_key(offset, head))
        {
            Some((child, generation)) => {
                // The child's header is the next line the walk reads: ask for it while the
                // version is confirmed.
                crate::prefetch::prefetch_read(std::ptr::from_ref(self.slab.run(child)));
                Plan::Descend(child, generation)
            }
            None if window.children != NONE => Plan::InsertAt(offset),
            None => Plan::Lock,
        }
    }

    /// Match `remaining` against the run from `offset`: join the run as far as it matches, record
    /// the matched blocks, and say how to go on (a divergence inside the run continues as a
    /// child hanging off it; the run is never split for one).
    #[expect(clippy::too_many_arguments)]
    fn store_in_run<'b>(
        &self,
        worker: u32,
        run_id: u32,
        meta: &mut RunMeta,
        offset: usize,
        remaining: &'b [StoredBlock],
        block_start: usize,
        pending: &mut Vec<Placed>,
    ) -> InRun<'b> {
        let run = self.slab.run(run_id);
        let len = run.len();
        if offset >= len {
            return InRun::Continue(remaining);
        }
        let held = self.held_by(run_id, worker);
        if held < offset {
            // The parent entry pointed past what this worker holds: the engine re-stored under
            // a stale parent, or named one position by two engine hashes (the same content under
            // the same parent) and removed the one the index filed the position under. Cut here
            // and join the suffix; when nobody holds anything past the cut and no child hangs
            // there, the split leaves nothing to join (`GONE`) and the run simply ends at the
            // cut: the blocks go after it, like any store past a run's end.
            self.count_split(LockKind::Shared);
            let suffix = self.split_locked(run_id, meta, offset);
            if suffix == GONE {
                return InRun::Continue(remaining);
            }
            return InRun::MoveTo(suffix, self.slab.run(suffix).generation());
        }
        let base = run.base.load(Ordering::Relaxed) + offset as u32;
        let data = run.block.load(Ordering::Relaxed) + base;
        let hashes = self.arena.words(data, len - offset);
        let engines = self
            .arena
            .words(run.engine.load(Ordering::Relaxed) + base, len - offset);
        let (matched, conflicts) = self.match_run(hashes, engines, remaining, true);
        let available = len - offset;
        let reach = offset + matched;
        // A divergence inside the run leaves the run whole: the blocks from the divergence on
        // continue it as a child hanging off `reach` (see `store_at`).
        let diverges = matched < available && matched < remaining.len();
        let before = self.held_len(run_id);
        if reach > held {
            self.set_holding(run_id, worker, reach);
            self.credit(worker, reach - held);
        }
        self.settle_distinct(worker, before, run_id);
        pending.push(Placed {
            run: run_id,
            offset: offset as u32,
            start: block_start,
            count: matched,
            conflicts,
        });
        if matched == remaining.len() {
            // The store ends in this run: a safe point to cap its prefix holders (a split here
            // moves nothing this store still has to place).
            self.cap_prefix_holders(run_id, meta);
            return InRun::Done;
        }
        if diverges {
            return InRun::ContinueAt(&remaining[matched..], reach);
        }
        InRun::Continue(&remaining[matched..])
    }

    /// Place `remaining` after `offset` blocks of the run (locked by the caller): in the child
    /// that already continues the run there, in place when the run is this worker's own leaf and
    /// `offset` is its end, or in a new child linked at `offset`. `Some` names a child that
    /// already existed, for the caller to carry on in.
    #[expect(clippy::too_many_arguments)]
    fn store_at(
        &self,
        worker: u32,
        run_id: u32,
        _meta: &RunMeta,
        offset: usize,
        remaining: &[StoredBlock],
        block_start: usize,
        pending: &mut Vec<Placed>,
    ) -> Option<(u32, u32)> {
        let run = self.slab.run(run_id);
        let coverage = self.slab.coverage(run_id);
        let head = remaining[0].content_hash.0;
        let children = run.children.load(Ordering::Relaxed);
        if let Some(found) = self
            .arena
            .table_find(children, Self::child_key(offset, head))
        {
            return Some(found);
        }
        let len = run.len();
        debug_assert!(
            offset <= len,
            "a child hangs off the run: {offset} <= {len}"
        );
        let contents: Vec<u64> = remaining
            .iter()
            .map(|stored| stored.content_hash.0)
            .collect();
        let engines: Vec<u64> = remaining.iter().map(|stored| stored.seq_hash.0).collect();
        let own_leaf = offset == len
            && run_id != ROOT
            && children == NONE
            && run.forwards.load(Ordering::Relaxed) == NONE
            && covered_only_by(coverage, worker);
        let (target, first) = if own_leaf {
            self.append(run, &contents, &engines);
            (run_id, len)
        } else {
            let block = self
                .arena
                .alloc_array(&contents, capacity_for(contents.len()));
            let engine = self
                .arena
                .alloc_array(&engines, self.arena.array_capacity(block));
            let new_id = self.slab.alloc(
                run.start() + offset,
                run_id,
                Window {
                    block,
                    base: 0,
                    engine,
                    len: contents.len() as u32,
                    children: NONE,
                    partials: NONE,
                    forwards: NONE,
                },
            );
            set(self.slab.coverage(new_id), worker);
            if let Some(existing) = self.link_child(run, offset, head, new_id) {
                let mut freed = Vec::new();
                self.discard_run(new_id, worker, &mut freed);
                self.recycle(&mut freed);
                return Some(existing);
            }
            (new_id, 0)
        };
        pending.push(Placed {
            run: target,
            offset: first as u32,
            start: block_start,
            count: remaining.len(),
            conflicts: 0,
        });
        self.credit(worker, contents.len());
        self.distinct_add(worker, contents.len());
        None
    }

    /// Extend a leaf in place when its window ends the hash array and the array has room;
    /// otherwise move it to a larger array.
    fn append(&self, run: &Run, contents: &[u64], engines: &[u64]) {
        let block = run.block.load(Ordering::Relaxed);
        let engine = run.engine.load(Ordering::Relaxed);
        let base = run.base.load(Ordering::Relaxed) as usize;
        let len = run.len();
        let header = self.arena.array_header(block);
        let (used, capacity) = unpack(header.load(Ordering::Acquire));
        let end = base + len;
        let claimed = end == used as usize
            && end + contents.len() <= capacity as usize
            && header
                .compare_exchange(
                    pack(used, capacity),
                    pack((end + contents.len()) as u32, capacity),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
        if claimed {
            let slots = self.arena.words(block + end as u32, contents.len());
            for (slot, &hash) in slots.iter().zip(contents) {
                slot.store(hash, Ordering::Relaxed);
            }
            let slots = self.arena.words(engine + end as u32, engines.len());
            for (slot, &hash) in slots.iter().zip(engines) {
                slot.store(hash, Ordering::Relaxed);
            }
            run.len
                .store((len + contents.len()) as u32, Ordering::Release);
            return;
        }
        let mut grown: Vec<u64> = self
            .arena
            .words(block + base as u32, len)
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .collect();
        grown.extend_from_slice(contents);
        let mut grown_engines: Vec<u64> = self
            .arena
            .words(engine + base as u32, len)
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .collect();
        grown_engines.extend_from_slice(engines);
        let new_block = self.arena.alloc_array(&grown, capacity_for(grown.len()));
        // The engine array must hold at least what the content array can: an in-place append
        // claims room from the content array's header and writes both.
        let new_engine = self
            .arena
            .alloc_array(&grown_engines, self.arena.array_capacity(new_block));
        run.begin_update();
        run.block.store(new_block, Ordering::Relaxed);
        run.base.store(0, Ordering::Relaxed);
        run.engine.store(new_engine, Ordering::Relaxed);
        run.len.store(grown.len() as u32, Ordering::Release);
        run.end_update();
        self.arena.array_release(block);
        self.arena.array_release(engine);
    }

    /// Forget the named blocks of `worker`; unknown hashes are ignored.
    pub fn apply_removed(&self, worker: u32, hashes: &[SequenceHash], map: &mut ChainBlockMap) {
        #[cfg(feature = "lane-stats")]
        {
            self.counters.removes.fetch_add(1, Ordering::Relaxed);
            self.counters
                .remove_blocks
                .fetch_add(hashes.len() as u64, Ordering::Relaxed);
        }
        let unmapping = tick();
        let mut refs: Vec<BlockRef> = Vec::with_capacity(hashes.len());
        map.remove_all(hashes, |at| refs.push(at));
        lap(self.counter_slot(3), unmapping);
        let grouping = tick();
        let work = group_by_run(refs);
        lap(self.counter_slot(4), grouping);
        let applying = tick();
        self.apply_removals(worker, work);
        lap(self.counter_slot(5), applying);
    }

    /// Drop `worker` from the grouped places, one run at a time, forwarding what a split moved.
    fn apply_removals(&self, worker: u32, mut work: Vec<Removal>) {
        // The runs' headers are the lines the locked work reads first: ask for all of them now
        // so their misses overlap instead of serialising one run after another.
        for removal in &work {
            if removal.run != GONE {
                crate::prefetch::prefetch_read(std::ptr::from_ref(self.slab.run(removal.run)));
            }
        }
        let mut freed = Vec::new();
        while let Some(removal) = work.pop() {
            self.remove_from_run(worker, removal, &mut work, &mut freed);
            self.recycle(&mut freed);
        }
    }

    /// Drop `worker` from the offsets of one run, forwarding offsets a split moved on.
    fn remove_from_run(
        &self,
        worker: u32,
        removal: Removal,
        work: &mut Vec<Removal>,
        freed: &mut Vec<u32>,
    ) {
        if removal.run == GONE {
            return;
        }
        let run = self.slab.run(removal.run);
        let mut meta = self.lock_run(removal.run, worker);
        if meta.dead
            || removal
                .generation
                .is_some_and(|generation| generation != run.generation())
        {
            return;
        }
        let offsets = reforward(
            &self.arena,
            run.forwards.load(Ordering::Relaxed),
            removal.offsets,
            work,
        );
        if offsets.is_empty() || self.held_by(removal.run, worker) == 0 {
            return;
        }
        self.remove_ranges(worker, removal.run, &mut meta, offsets);
        if !self.has_holders(removal.run) && run.children.load(Ordering::Relaxed) == NONE {
            self.unlink_locked(removal.run, &mut meta, freed);
        }
    }

    /// Drop `worker` from the given offsets of a run it covers, splitting the run so that the
    /// pieces it still covers keep their offsets.
    fn remove_ranges(&self, worker: u32, run_id: u32, meta: &mut RunMeta, mut offsets: Vec<u32>) {
        offsets.sort_unstable();
        offsets.dedup();
        let mut held = self.held_by(run_id, worker);
        offsets.retain(|&offset| (offset as usize) < held);
        // Contiguous ranges, highest first, so earlier ranges keep their offsets after the splits
        // a later range causes.
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for &offset in &offsets {
            let offset = offset as usize;
            match ranges.last_mut() {
                Some((_, high)) if *high + 1 == offset => *high = offset,
                _ => ranges.push((offset, offset)),
            }
        }
        for (low, high) in ranges.into_iter().rev() {
            // Blocks after the range stay held by this worker too: a hole, so the tail becomes
            // its own run. A range reaching the worker's end only lowers its cutoff.
            if high + 1 < held {
                self.count_split(LockKind::Own);
                self.split_locked(run_id, meta, high + 1);
            }
            let before = self.held_len(run_id);
            self.set_holding(run_id, worker, low);
            self.debit(worker, high + 1 - low);
            self.settle_distinct(worker, before, run_id);
            held = low;
        }
        // Every range is applied: cap the run's prefix holders (a tail eviction adds one).
        self.cap_prefix_holders(run_id, meta);
    }

    /// Forget every block of `worker` (the engine cleared its cache); the map is emptied.
    pub fn apply_cleared(&self, worker: u32, map: &mut ChainBlockMap) {
        let drained = std::mem::take(map);
        self.drop_worker(worker, drained);
    }

    /// Forget every block of `worker` (the worker left) and free its slot; the same name interns
    /// afresh afterwards.
    pub fn remove_worker(&self, worker: u32, map: ChainBlockMap) {
        self.drop_worker(worker, map);
        self.release_worker(worker);
    }

    fn drop_worker(&self, worker: u32, map: ChainBlockMap) {
        // Keyed by (run, generation): a forwarding record to a dead generation of an id must not
        // shadow the live run that reused the id.
        let mut seen: FxHashSet<(u32, Option<u32>)> = FxHashSet::default();
        let mut work: Vec<(u32, Option<u32>)> = Vec::new();
        for (_, at) in map {
            if seen.insert((at.run, None)) {
                work.push((at.run, None));
            }
        }
        let mut freed = Vec::new();
        while let Some((run_id, generation)) = work.pop() {
            if run_id == GONE {
                continue;
            }
            let run = self.slab.run(run_id);
            let mut meta = self.lock_run(run_id, worker);
            if meta.dead || generation.is_some_and(|generation| generation != run.generation()) {
                continue;
            }
            // Blocks of this worker may have moved into suffixes since the map was written.
            for (_, suffix, suffix_generation) in self
                .arena
                .forward_records(run.forwards.load(Ordering::Relaxed))
            {
                if suffix != GONE && seen.insert((suffix, Some(suffix_generation))) {
                    work.push((suffix, Some(suffix_generation)));
                }
            }
            let held = self.held_by(run_id, worker);
            if held > 0 {
                let before = self.held_len(run_id);
                self.set_holding(run_id, worker, 0);
                self.debit(worker, held);
                self.settle_distinct(worker, before, run_id);
                if !self.has_holders(run_id) && run.children.load(Ordering::Relaxed) == NONE {
                    self.unlink_locked(run_id, &mut meta, &mut freed);
                }
            }
            drop(meta);
            self.recycle(&mut freed);
        }
    }

    /// Score every worker by how many leading blocks of the request it holds. With `early_exit`,
    /// report the workers holding the first block, each scored 1.
    pub fn find_matches(&self, content_hashes: &[ContentHash], early_exit: bool) -> OverlapScores {
        let mut out = OverlapScores::default();
        self.score_into(
            content_hashes,
            |content| content.0,
            early_exit,
            |worker, score| {
                out.scores.insert(worker, score);
            },
        );
        out
    }

    /// The lookup behind [`find_matches`](Self::find_matches), for callers that keep their own
    /// hash type and result shape: `hash_of` reads a block's content hash, `report` receives
    /// every `(worker, score)` with a non-empty prefix (once each). Nothing is allocated.
    /// Returns the number of runs walked, a measure of how fragmented the matched path is.
    pub fn score_into<T>(
        &self,
        content_hashes: &[T],
        hash_of: impl Fn(&T) -> u64,
        early_exit: bool,
        report: impl FnMut(u32, u32),
    ) -> usize {
        let Some(first) = content_hashes.first() else {
            return 0;
        };
        match self.head_entry(hash_of(first)) {
            Some(entry) => self.score_from(entry, content_hashes, hash_of, early_exit, report),
            None => 0,
        }
    }

    /// The run under the root that starts with the content hash `first`, with its generation:
    /// whether any worker holds a chain starting there, read under the root's version. This is
    /// the lookup's first step, and what lets a sharded lookup pass over a shard that cannot
    /// hold the request at all.
    pub(crate) fn head_entry(&self, first: u64) -> Option<(u32, u32)> {
        let root = self.slab.run(ROOT);
        loop {
            let (window, version) = root.snapshot();
            let found = self
                .arena
                .table_find(window.children, Self::child_key(0, first));
            if root.confirm(version) {
                return found;
            }
        }
    }

    /// The lookup from the run under the root that [`head_entry`](Self::head_entry) found.
    pub(crate) fn score_from<T>(
        &self,
        entry: (u32, u32),
        content_hashes: &[T],
        hash_of: impl Fn(&T) -> u64,
        early_exit: bool,
        mut report: impl FnMut(u32, u32),
    ) -> usize {
        let (mut run_id, mut expected) = entry;
        let words = self.words;
        let mut alive = [0u64; MAX_WORDS];
        // The partial-holder entries of the run in hand live in a per-thread buffer: a walk
        // writes the prefix it then reads, so nothing is zeroed per lookup (the 8 KB this
        // buffer would cost on the stack was a sixth of a lookup). `report` must not look up.
        PARTIAL_BUFFER.with(|buffer| {
            let mut partial = buffer.borrow_mut();
            let mut position = 0usize;
            let mut walked = 0usize;
            loop {
                let run = self.slab.run(run_id);
                let (window, version) = run.snapshot();
                if (version >> 32) as u32 != expected {
                    break;
                }
                let len = window.len as usize;
                let available = len.min(content_hashes.len() - position);
                let hashes = self.arena.words(window.block + window.base, available);
                let matched = content_hashes[position..position + available]
                    .iter()
                    .zip(hashes)
                    .take_while(|(content, slot)| hash_of(content) == slot.load(Ordering::Relaxed))
                    .count();
                // Partial holders before the coverage words: a worker moving from a prefix to the
                // whole run gains its bit before it loses its entry, so this order never misses it.
                let mut partials = 0usize;
                if window.partials != NONE {
                    let (_, used) = self.arena.partials_shape(window.partials);
                    for slot in self.arena.words(window.partials + 2, used) {
                        let entry = slot.load(Ordering::Relaxed);
                        if entry != 0 && entry != TOMB && partials < MAX_PARTIAL {
                            partial[partials] = entry;
                            partials += 1;
                        }
                    }
                }
                let coverage = self.slab.coverage(run_id);
                let mut held = [0u64; MAX_WORDS];
                for (word, slot) in held[..words].iter_mut().zip(coverage) {
                    *word = slot.load(Ordering::Relaxed);
                }
                let next = if position + matched < content_hashes.len() {
                    // The request goes on past what matched here: a child may continue the run
                    // from this offset, at its end or at a divergence inside it.
                    self.arena.table_find(
                        window.children,
                        Self::child_key(matched, hash_of(&content_hashes[position + matched])),
                    )
                } else {
                    None
                };
                if !run.confirm(version) {
                    continue;
                }
                walked += 1;
                if matched == 0 {
                    break;
                }
                if position == 0 && early_exit {
                    alive = held;
                    for &entry in &partial[..partials] {
                        let worker = entry as u32;
                        alive[(worker / 64) as usize] |= 1u64 << (worker % 64);
                    }
                    emit(&alive[..words], 1, &mut report);
                    return walked;
                }
                // Prefix holders, in one pass: at the first run every entry is a holder, later
                // only those still alive. One that holds the whole matched stretch of a run the
                // request leaves inside goes on (a child hanging off the divergence may continue
                // it); the rest end here with what they hold. A whole holder is never in the
                // table, and a worker gaining its bit while its entry lingers counts as whole.
                let mut through = [0u64; MAX_WORDS];
                for &entry in &partial[..partials] {
                    let worker = entry as u32;
                    let (index, bit) = ((worker / 64) as usize, 1u64 << (worker % 64));
                    if held[index] & bit != 0 || (position > 0 && alive[index] & bit == 0) {
                        continue;
                    }
                    let cutoff = (entry >> 32) as usize;
                    if matched < len && cutoff >= matched {
                        through[index] |= bit;
                    } else {
                        report(worker, (position + cutoff.min(matched)) as u32);
                        // Reported here: not among the holders dropped below.
                        alive[index] &= !bit;
                    }
                }
                if position > 0 {
                    for (index, word) in alive[..words].iter_mut().enumerate() {
                        let dropped = *word & !held[index] & !through[index];
                        if dropped != 0 {
                            emit_word(index, dropped, position as u32, &mut report);
                        }
                    }
                    for (index, word) in alive[..words].iter_mut().enumerate() {
                        *word &= held[index] | through[index];
                    }
                } else {
                    for (index, word) in alive[..words].iter_mut().enumerate() {
                        *word = held[index] | through[index];
                    }
                }
                if alive[..words].iter().all(|word| *word == 0) {
                    return walked;
                }
                position += matched;
                match next {
                    Some((child, generation)) => {
                        run_id = child;
                        expected = generation;
                    }
                    None => break,
                }
            }
            emit(&alive[..words], position as u32, &mut report);
            walked
        })
    }

    /// Every block every worker holds, as `(worker, position, content hash, prefix hash)`;
    /// for tests and for comparing against the reference indexer. Not consistent under
    /// concurrent writes.
    #[doc(hidden)]
    pub fn debug_blocks(&self) -> BTreeSet<(u32, usize, ContentHash, SequenceHash)> {
        let mut out = BTreeSet::new();
        let mut stack: Vec<(u32, Option<SequenceHash>)> = Vec::new();
        let (root, _) = self.slab.run(ROOT).snapshot();
        for (_, child, _) in self.arena.table_entries(root.children) {
            stack.push((child, None));
        }
        while let Some((run_id, mut prefix)) = stack.pop() {
            let run = self.slab.run(run_id);
            let (window, _) = run.snapshot();
            let start = run.start();
            let holders = workers(self.slab.coverage(run_id));
            let partials = self.arena.partial_entries(window.partials);
            let hashes = self
                .arena
                .words(window.block + window.base, window.len as usize);
            // The prefix hash after each count of the run's blocks: a child hangs off any offset
            // and continues from the hash at its own.
            let mut prefixes: Vec<Option<SequenceHash>> = Vec::with_capacity(hashes.len() + 1);
            prefixes.push(prefix);
            for (offset, slot) in hashes.iter().enumerate() {
                let content = ContentHash(slot.load(Ordering::Relaxed));
                let next = match prefix {
                    Some(previous) => chain_prefix_hash(previous, content),
                    None => SequenceHash(content.0),
                };
                for &worker in &holders {
                    out.insert((worker, start + offset, content, next));
                }
                for &(worker, cutoff) in &partials {
                    if offset < cutoff as usize {
                        out.insert((worker, start + offset, content, next));
                    }
                }
                prefix = Some(next);
                prefixes.push(prefix);
            }
            for (_, child, _) in self.arena.table_entries(window.children) {
                let child_offset = self.slab.run(child).start().saturating_sub(start);
                stack.push((child, prefixes[child_offset.min(prefixes.len() - 1)]));
            }
        }
        out
    }

    /// Adjacent runs a compaction could fold: a run with exactly one child whose coverage equals
    /// its own and that has no prefix holders of its own (a prefix holder of the child that is
    /// not a whole holder of the parent holds a suffix, which one run could not express).
    /// Returns the pairs and the blocks the children hold. A diagnostic walk, not consistent
    /// under concurrent writes.
    #[doc(hidden)]
    pub fn debug_mergeable(&self) -> (usize, usize) {
        let (strict, _, blocks) = self.debug_mergeable_by_rule();
        (strict, blocks)
    }

    /// As [`debug_mergeable`](Self::debug_mergeable), but also counting the pairs a generalised
    /// merge could fold: the child's whole holders a subset of the parent's and every prefix
    /// holder of the child a whole holder of the parent (the parent's extra holders would become
    /// prefix holders of the merged run). Returns `(strict pairs, generalised pairs, blocks of the
    /// generalised pairs' children)`.
    #[doc(hidden)]
    pub fn debug_mergeable_by_rule(&self) -> (usize, usize, usize) {
        let (mut strict, mut general, mut blocks) = (0usize, 0usize, 0usize);
        let mut stack: Vec<u32> = Vec::new();
        let (root, _) = self.slab.run(ROOT).snapshot();
        for (_, child, _) in self.arena.table_entries(root.children) {
            stack.push(child);
        }
        while let Some(run_id) = stack.pop() {
            let (window, _) = self.slab.run(run_id).snapshot();
            let children: Vec<u32> = self
                .arena
                .table_entries(window.children)
                .into_iter()
                .map(|(_, child, _)| child)
                .collect();
            if let [only] = children[..] {
                let (child_window, _) = self.slab.run(only).snapshot();
                let parent_words: Vec<u64> = self
                    .slab
                    .coverage(run_id)
                    .iter()
                    .map(|w| w.load(Ordering::Relaxed))
                    .collect();
                let child_words: Vec<u64> = self
                    .slab
                    .coverage(only)
                    .iter()
                    .map(|w| w.load(Ordering::Relaxed))
                    .collect();
                let same_coverage = parent_words == child_words;
                let subset = parent_words
                    .iter()
                    .zip(&child_words)
                    .all(|(p, c)| c & !p == 0);
                let child_partials = self.arena.partial_entries(child_window.partials);
                let partials_held = child_partials
                    .iter()
                    .all(|&(worker, _)| has(self.slab.coverage(run_id), worker));
                if same_coverage && child_partials.is_empty() {
                    strict += 1;
                }
                if subset && partials_held {
                    general += 1;
                    blocks += child_window.len as usize;
                }
            }
            stack.extend(children);
        }
        (strict, general, blocks)
    }

    /// Shape and memory counters.
    pub fn stats(&self) -> ChainIndexStats {
        let allocated = self.slab.allocated();
        let mut stats = ChainIndexStats {
            runs_allocated: allocated,
            runs_free: self.slab.free.len(),
            arena_bytes: self.arena.used() as usize * size_of::<AtomicU64>(),
            arena_free_bytes: self.arena.free_words() * size_of::<AtomicU64>(),
            arena_chunk_bytes: self.arena.chunk_bytes(),
            header_bytes: allocated * (size_of::<Run>() + self.words * size_of::<AtomicU64>()),
            slab_bytes: self.slab.chunk_bytes(),
            engine_conflicts: self.engine_conflicts.load(Ordering::Relaxed),
            landing_mismatches: self.landing_mismatches.load(Ordering::Relaxed),
            moved_hashes: self.moved_hashes.load(Ordering::Relaxed),
            splits_by_branch: self.splits_branch.load(Ordering::Relaxed),
            splits_by_hole: self.splits_hole.load(Ordering::Relaxed),
            splits_by_mid_run_store: self.splits_mid_run.load(Ordering::Relaxed),
            splits_by_prefix_holders: self.splits_prefix_holders.load(Ordering::Relaxed),
            runs_died: self.runs_died.load(Ordering::Relaxed),
            ..ChainIndexStats::default()
        };
        for id in 1..allocated as u32 {
            let run = self.slab.run(id);
            if run.meta.lock().dead {
                continue;
            }
            stats.runs_live += 1;
            stats.blocks_live += run.len();
            let (window, _) = run.snapshot();
            let partials = self.arena.partial_entries(window.partials).len();
            stats.partial_entries += partials;
            stats.max_partials = stats.max_partials.max(partials);
            if window.children != NONE {
                let live = self.arena.table_live(window.children);
                stats.child_entries += live;
                stats.child_tombstones += self.arena.table_dead(window.children);
            }
        }
        stats
    }
}

/// Offsets taken from the map may have moved into suffixes since they were written: send those
/// on, tagged with the generation the suffix had at the split.
/// The places of one worker's blocks grouped by run (stored coordinates; the per-run work
/// forwards what a split moved).
fn group_by_run(mut refs: Vec<BlockRef>) -> Vec<Removal> {
    refs.sort_unstable_by_key(|at| at.run);
    let mut work: Vec<Removal> = Vec::new();
    let mut index = 0;
    while index < refs.len() {
        let run = refs[index].run;
        let end = refs[index..]
            .iter()
            .position(|at| at.run != run)
            .map_or(refs.len(), |count| index + count);
        work.push(Removal {
            run,
            generation: None,
            offsets: refs[index..end].iter().map(|at| at.offset).collect(),
        });
        index = end;
    }
    work
}

fn reforward(
    arena: &WordArena,
    forwards: u32,
    mut offsets: Vec<u32>,
    work: &mut Vec<Removal>,
) -> Vec<u32> {
    if forwards == NONE {
        return offsets;
    }
    let mut forwarded: FxHashMap<(u32, u32), Vec<u32>> = FxHashMap::default();
    offsets.retain(|&offset| match arena.forwards_find(forwards, offset) {
        Some((next, generation)) => {
            if next.run != GONE {
                forwarded
                    .entry((next.run, generation))
                    .or_default()
                    .push(next.offset);
            }
            false
        }
        None => true,
    });
    work.extend(
        forwarded
            .into_iter()
            .map(|((run, generation), offsets)| Removal {
                run,
                generation: Some(generation),
                offsets,
            }),
    );
    offsets
}

fn emit(alive: &[u64], score: u32, report: &mut impl FnMut(u32, u32)) {
    if score == 0 {
        return;
    }
    for (index, &word) in alive.iter().enumerate() {
        if word != 0 {
            emit_word(index, word, score, report);
        }
    }
}

#[inline]
fn emit_word(index: usize, mut word: u64, score: u32, report: &mut impl FnMut(u32, u32)) {
    while word != 0 {
        let bit = word.trailing_zeros();
        report((index * 64 + bit as usize) as u32, score);
        word &= word - 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::{request_prefix_hashes, ReferenceIndexer};

    fn content(stream: u64, position: usize) -> ContentHash {
        crate::compute_content_hash(&[stream as u32, (stream >> 32) as u32, position as u32])
    }

    fn blocks_of(contents: &[ContentHash]) -> Vec<StoredBlock> {
        contents
            .iter()
            .zip(request_prefix_hashes(contents))
            .map(|(&content_hash, seq_hash)| StoredBlock {
                seq_hash,
                content_hash,
            })
            .collect()
    }

    fn scores(index: &ChainIndex, query: &[ContentHash]) -> Vec<(u32, u32)> {
        let mut v: Vec<(u32, u32)> = index
            .find_matches(query, false)
            .scores
            .into_iter()
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn array_classes_round_up_and_back() {
        for capacity in [1usize, 8, 9, 16, 100, 128, 129, 256, 257, 1000, 4096, 5000] {
            let class = array_class(capacity);
            assert!(class_capacity(class) >= capacity, "capacity {capacity}");
            assert_eq!(array_class(class_capacity(class)), class);
        }
        assert_eq!(table_class(2), 0);
        assert_eq!(table_class(4), 1);
        assert_eq!(table_class(1024), 9);
    }

    #[test]
    fn store_lookup_and_divergence() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..10).map(|p| content(1, p)).collect();
        index
            .apply_stored(w, &blocks_of(&held), None, &mut map)
            .expect("store");
        assert_eq!(scores(&index, &held), vec![(w, 10)]);
        assert_eq!(scores(&index, &held[..4]), vec![(w, 4)]);
        let mut diverged = held.clone();
        diverged[5] = content(2, 0);
        assert_eq!(scores(&index, &diverged), vec![(w, 5)]);
        let mut extended = held.clone();
        extended.push(content(3, 0));
        assert_eq!(scores(&index, &extended), vec![(w, 10)]);
        assert_eq!(scores(&index, &[content(9, 0)]), vec![]);
        assert_eq!(index.current_size(), 10);
        assert_eq!(index.entry_count(), 10);
    }

    #[test]
    fn two_workers_share_a_prefix_and_split_at_the_fork() {
        let index = ChainIndex::with_max_workers(8);
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let base: Vec<ContentHash> = (0..6).map(|p| content(1, p)).collect();
        let mut fork = base[..3].to_vec();
        fork.extend((0..4).map(|p| content(2, p)));
        index
            .apply_stored(a, &blocks_of(&base), None, &mut ma)
            .expect("store a");
        index
            .apply_stored(b, &blocks_of(&fork), None, &mut mb)
            .expect("store b");
        assert_eq!(scores(&index, &base), vec![(a, 6), (b, 3)]);
        assert_eq!(scores(&index, &fork), vec![(a, 3), (b, 7)]);
        let mut early: Vec<(u32, u32)> =
            index.find_matches(&base, true).scores.into_iter().collect();
        early.sort_unstable();
        assert_eq!(early, vec![(a, 1), (b, 1)]);
        let mut reference = ReferenceIndexer::new();
        reference
            .apply_stored(a, &blocks_of(&base), None)
            .expect("ref a");
        reference
            .apply_stored(b, &blocks_of(&fork), None)
            .expect("ref b");
        assert_eq!(index.debug_blocks(), reference.blocks());
        assert_eq!(index.entry_count(), 10);
    }

    #[test]
    fn a_worker_holds_both_sides_of_its_own_divergence() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let base: Vec<ContentHash> = (0..6).map(|p| content(1, p)).collect();
        let mut fork = base[..3].to_vec();
        fork.extend((0..2).map(|p| content(2, p)));
        let blocks = blocks_of(&base);
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("base");
        let fork_blocks = blocks_of(&fork);
        index
            .apply_stored(w, &fork_blocks[3..], Some(blocks[2].seq_hash), &mut map)
            .expect("fork");
        assert_eq!(scores(&index, &base), vec![(w, 6)]);
        assert_eq!(scores(&index, &fork), vec![(w, 5)]);
        assert_eq!(index.current_size(), 8);
    }

    #[test]
    fn a_hole_stops_the_match_at_the_hole() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..8).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("store");
        index.apply_removed(w, &[blocks[3].seq_hash], &mut map);
        assert_eq!(scores(&index, &held), vec![(w, 3)]);
        assert_eq!(index.current_size(), 7);
        // Re-storing the missing block after its parent heals the hole.
        index
            .apply_stored(w, &blocks[3..4], Some(blocks[2].seq_hash), &mut map)
            .expect("heal");
        assert_eq!(scores(&index, &held), vec![(w, 8)]);
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(w, &blocks, None).expect("ref");
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    #[test]
    fn a_hole_in_a_shared_run_affects_only_the_evicting_worker() {
        let index = ChainIndex::with_max_workers(8);
        let v = index.intern_worker("v").expect("id");
        let w = index.intern_worker("w").expect("id");
        let (mut mv, mut mw) = (ChainBlockMap::default(), ChainBlockMap::default());
        let held: Vec<ContentHash> = (0..10).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index.apply_stored(v, &blocks, None, &mut mv).expect("v");
        index.apply_stored(w, &blocks, None, &mut mw).expect("w");
        index.apply_removed(w, &[blocks[5].seq_hash], &mut mw);
        assert_eq!(scores(&index, &held), vec![(v, 10), (w, 5)]);
        index.apply_removed(v, &[blocks[7].seq_hash, blocks[8].seq_hash], &mut mv);
        assert_eq!(scores(&index, &held), vec![(v, 7), (w, 5)]);
        index
            .apply_stored(w, &blocks[5..6], Some(blocks[4].seq_hash), &mut mw)
            .expect("heal");
        assert_eq!(scores(&index, &held), vec![(v, 7), (w, 10)]);
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(v, &blocks, None).expect("ref v");
        reference.apply_stored(w, &blocks, None).expect("ref w");
        reference.apply_removed(v, &[blocks[7].seq_hash, blocks[8].seq_hash]);
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    #[test]
    fn tail_removal_truncates_and_a_clear_empties() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..8).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("store");
        let tail: Vec<SequenceHash> = blocks[5..].iter().map(|b| b.seq_hash).collect();
        index.apply_removed(w, &tail, &mut map);
        assert_eq!(scores(&index, &held), vec![(w, 5)]);
        assert_eq!(map.len(), 5);
        assert_eq!(index.entry_count(), 5);
        index.apply_cleared(w, &mut map);
        assert!(map.is_empty());
        assert_eq!(scores(&index, &held), vec![]);
        assert_eq!(index.current_size(), 0);
        assert_eq!(index.entry_count(), 0);
        assert!(index.debug_blocks().is_empty());
        let stats = index.stats();
        assert_eq!(stats.runs_live, 0);
        assert_eq!(stats.runs_free, 1, "the dead run waits for reuse");
        assert_eq!(
            stats.arena_free_bytes,
            stats.arena_bytes - 8,
            "every array and table is back in a free list"
        );
    }

    #[test]
    fn dead_runs_and_arrays_are_reused() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        for round in 0..200u64 {
            let held: Vec<ContentHash> = (0..12).map(|p| content(10 + round, p)).collect();
            let blocks = blocks_of(&held);
            index
                .apply_stored(w, &blocks, None, &mut map)
                .expect("store");
            assert_eq!(scores(&index, &held), vec![(w, 12)]);
            let hashes: Vec<SequenceHash> = blocks.iter().map(|b| b.seq_hash).collect();
            index.apply_removed(w, &hashes, &mut map);
            assert_eq!(scores(&index, &held), vec![]);
        }
        let stats = index.stats();
        assert!(
            stats.runs_allocated <= 3,
            "runs were not recycled: {stats:?}"
        );
        assert!(
            stats.arena_bytes < 4096,
            "arena words were not recycled: {stats:?}"
        );
        assert_eq!(index.current_size(), 0);
    }

    #[test]
    fn appends_reuse_the_array_until_another_worker_joins() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let v = index.intern_worker("v").expect("id");
        let (mut mw, mut mv) = (ChainBlockMap::default(), ChainBlockMap::default());
        let held: Vec<ContentHash> = (0..40).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index
            .apply_stored(w, &blocks[..4], None, &mut mw)
            .expect("first");
        for step in 1..10 {
            let from = step * 4;
            index
                .apply_stored(
                    w,
                    &blocks[from..from + 4],
                    Some(blocks[from - 1].seq_hash),
                    &mut mw,
                )
                .expect("extend");
        }
        assert_eq!(
            index.stats().runs_live,
            1,
            "decode extensions stay in one run"
        );
        assert_eq!(scores(&index, &held), vec![(w, 40)]);
        index
            .apply_stored(v, &blocks[..20], None, &mut mv)
            .expect("join");
        assert_eq!(scores(&index, &held), vec![(w, 40), (v, 20)]);
        assert_eq!(
            index.stats().runs_live,
            1,
            "a prefix holder joins as a partial holder, no split"
        );
        let more: Vec<ContentHash> = (0..3).map(|p| content(2, p)).collect();
        let mut long = held.clone();
        long.extend(more);
        let long_blocks = blocks_of(&long);
        index
            .apply_stored(w, &long_blocks[40..], Some(blocks[39].seq_hash), &mut mw)
            .expect("extend after join");
        assert_eq!(scores(&index, &long), vec![(w, 43), (v, 20)]);
        assert_eq!(index.stats().runs_live, 1, "the run is still w's own leaf");
    }

    #[test]
    fn many_children_grow_the_table_and_stay_findable() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let prompt: Vec<ContentHash> = (0..3).map(|p| content(1, p)).collect();
        index
            .apply_stored(w, &blocks_of(&prompt), None, &mut map)
            .expect("prompt");
        let anchor = blocks_of(&prompt)[2].seq_hash;
        let mut chains = Vec::new();
        for branch in 0..300u64 {
            let mut chain = prompt.clone();
            chain.extend((0..2).map(|p| content(100 + branch, p)));
            index
                .apply_stored(w, &blocks_of(&chain)[3..], Some(anchor), &mut map)
                .expect("branch");
            chains.push(chain);
        }
        for chain in &chains {
            assert_eq!(scores(&index, chain), vec![(w, 5)]);
        }
        let mut unknown = prompt.clone();
        unknown.push(content(999, 0));
        assert_eq!(scores(&index, &unknown), vec![(w, 3)]);
        // Unlinking every other branch tombstones its slot; the rest stay findable.
        for chain in chains.iter().step_by(2) {
            let hashes: Vec<SequenceHash> =
                blocks_of(chain)[3..].iter().map(|b| b.seq_hash).collect();
            index.apply_removed(w, &hashes, &mut map);
        }
        for (branch, chain) in chains.iter().enumerate() {
            let expected = if branch % 2 == 0 { 3 } else { 5 };
            assert_eq!(
                scores(&index, chain),
                vec![(w, expected)],
                "branch {branch}"
            );
        }
        let mut reference = ReferenceIndexer::new();
        reference
            .apply_stored(w, &blocks_of(&prompt), None)
            .expect("ref prompt");
        for chain in chains.iter().skip(1).step_by(2) {
            reference
                .apply_stored(w, &blocks_of(chain)[3..], Some(anchor))
                .expect("ref branch");
        }
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    #[test]
    fn tail_evictions_and_regrowth_do_not_split() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let v = index.intern_worker("v").expect("id");
        let (mut mw, mut mv) = (ChainBlockMap::default(), ChainBlockMap::default());
        let held: Vec<ContentHash> = (0..40).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index.apply_stored(w, &blocks, None, &mut mw).expect("w");
        index.apply_stored(v, &blocks, None, &mut mv).expect("v");
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(w, &blocks, None).expect("ref w");
        reference.apply_stored(v, &blocks, None).expect("ref v");
        // v evicts its tail twice: a cutoff, not a split.
        for keep in [30usize, 12] {
            let gone: Vec<SequenceHash> = blocks[keep..].iter().map(|b| b.seq_hash).collect();
            index.apply_removed(v, &gone, &mut mv);
            reference.apply_removed(v, &gone);
            assert_eq!(scores(&index, &held), vec![(w, 40), (v, keep as u32)]);
            assert_eq!(index.stats().runs_live, 1, "tail eviction to {keep}");
            assert_eq!(index.worker_block_count(v), keep);
            assert_eq!(index.entry_count(), 40);
        }
        // A decode extends v back to the end of the run: full holder again, still one run.
        index
            .apply_stored(v, &blocks[12..], Some(blocks[11].seq_hash), &mut mv)
            .expect("regrow");
        reference
            .apply_stored(v, &blocks[12..], Some(blocks[11].seq_hash))
            .expect("ref regrow");
        assert_eq!(scores(&index, &held), vec![(w, 40), (v, 40)]);
        assert_eq!(index.stats().runs_live, 1);
        assert_eq!(index.debug_blocks(), reference.blocks());
        // w evicts everything, v keeps a prefix: the run survives with one partial holder.
        let all: Vec<SequenceHash> = blocks.iter().map(|b| b.seq_hash).collect();
        index.apply_removed(w, &all, &mut mw);
        reference.apply_removed(w, &all);
        index.apply_removed(v, &all[25..], &mut mv);
        reference.apply_removed(v, &all[25..]);
        assert_eq!(scores(&index, &held), vec![(v, 25)]);
        assert_eq!(index.entry_count(), 25);
        assert_eq!(index.debug_blocks(), reference.blocks());
        let walked = index.score_into(&held, |c| c.0, false, |_, _| {});
        assert_eq!(walked, 1);
    }

    #[test]
    fn a_staircase_of_prefix_holders_is_a_few_runs() {
        let index = ChainIndex::with_max_workers(128);
        let held: Vec<ContentHash> = (0..64).map(|p| content(3, p)).collect();
        let blocks = blocks_of(&held);
        let mut maps: Vec<ChainBlockMap> = (0..64).map(|_| ChainBlockMap::default()).collect();
        let mut reference = ReferenceIndexer::new();
        for step in 0..64usize {
            assert_eq!(index.intern_worker(&format!("w{step}")), Ok(step as u32));
        }
        // The longest holder stores first (a full prefill); every shorter prefix then joins as a
        // partial holder, the way evictions and cache hits shape a shared prompt.
        for step in (0..64usize).rev() {
            index
                .apply_stored(step as u32, &blocks[..=step], None, &mut maps[step])
                .expect("store");
            reference
                .apply_stored(step as u32, &blocks[..=step], None)
                .expect("ref");
        }
        // A divergence never splits, but more than `PARTIAL_CAP` prefix holders on one run do,
        // at their median cutoff: 64 prefixes of one chain end up in a few runs with the longer
        // holders whole on the prefixes, never in one run per holder.
        let runs = index.stats().runs_live;
        assert!(
            (2..=2 * 64 / PARTIAL_CAP).contains(&runs),
            "64 prefixes of one chain under the prefix-holder cap of {PARTIAL_CAP}: {runs} runs"
        );
        assert!(index.stats().splits_by_prefix_holders >= 1);
        let expected: Vec<(u32, u32)> = (0..64u32).map(|w| (w, w + 1)).collect();
        assert_eq!(scores(&index, &held), expected);
        assert_eq!(index.debug_blocks(), reference.blocks());
        let walked = index.score_into(&held, |c| c.0, false, |_, _| {});
        assert_eq!(walked, runs, "the walk visits each run of the chain once");
        let mut early: Vec<(u32, u32)> =
            index.find_matches(&held, true).scores.into_iter().collect();
        early.sort_unstable();
        assert_eq!(early, (0..64u32).map(|w| (w, 1)).collect::<Vec<_>>());
        // A hole in the longest holder's prefix still splits, and only for it.
        index.apply_removed(63, &[blocks[40].seq_hash], &mut maps[63]);
        reference.apply_removed(63, &[blocks[40].seq_hash]);
        assert_eq!(scores(&index, &held)[63], (63, 40));
        assert!(index.stats().runs_live >= runs);
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    /// A lock-free reader may still hold a table id whose words were recycled and rewritten by
    /// the time it reads them; the version check discards the read, so the read itself must only
    /// return garbage, never panic. Rewrites the headers a recycled slot could carry: a child
    /// table that became a forwards table of another size, and a partial table whose used count
    /// outgrew its slots.
    #[test]
    fn stale_table_headers_are_read_without_panicking() {
        let arena = WordArena::new();
        let child_table = arena.alloc_table(MIN_TABLE_SLOTS);
        arena.table_put(child_table, 0xabcd, 7, 1);
        let partial_table = arena.alloc_partials(MIN_TABLE_SLOTS);
        arena.partial_put(partial_table, 3, 5);
        let forwards_table = arena.alloc_forwards(MIN_TABLE_SLOTS);
        arena.forwards_push(forwards_table, 4, 9, 2);
        // Garbage of every shape a recycled header might show: zero, not a power of two, huge.
        for garbage in [
            0u64,
            3,
            u64::MAX,
            (u64::MAX << 32) | 5,
            (7u64 << 32) | (1 << 31),
        ] {
            for table in [child_table, partial_table, forwards_table] {
                arena.word(table).store(garbage, Ordering::Relaxed);
                let _ = arena.table_find(table, 0xabcd);
                let _ = arena.forwards_find(table, 4);
                let _ = arena.partial_find(table, 3);
                let _ = arena.partial_max(table);
                let (_, used) = arena.partials_shape(table);
                let _ = arena.words(table + 2, used).len();
            }
        }
        // A whole walk over an index keeps working after the headers it reads are rewritten.
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..12).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("store");
        assert_eq!(scores(&index, &held), vec![(w, 12)]);
    }

    /// The gateway's parent-missing fallback: a worker holds b0..b3, evicts b2 and b3, the
    /// engine extends after b3 but the parent is unknown, so b4 and b5 are stored without a
    /// parent and land at positions 0 and 1 under the hashes of positions 4 and 5; when the
    /// engine then announces the whole chain, each hash is held at one place only, the old
    /// memberships are released, the counts read six blocks, a query for the mislaid pair
    /// scores nothing, and a worker interned later into the freed id inherits nothing.
    #[test]
    fn a_hash_stored_again_at_another_position_releases_its_old_place() {
        let index = ChainIndex::with_max_workers(8);
        let mut reference = ReferenceIndexer::new();
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let chain: Vec<ContentHash> = (0..6).map(|p| content(23, p)).collect();
        let blocks = blocks_of(&chain);
        index
            .apply_stored(w, &blocks[..4], None, &mut map)
            .expect("b0..b3");
        reference.apply_stored(w, &blocks[..4], None).expect("ref");
        let evicted = [blocks[2].seq_hash, blocks[3].seq_hash];
        index.apply_removed(w, &evicted, &mut map);
        reference.apply_removed(w, &evicted);
        // The fallback: b4 and b5 with no parent, so at positions 0 and 1.
        index
            .apply_stored(w, &blocks[4..], None, &mut map)
            .expect("fallback");
        reference
            .apply_stored(w, &blocks[4..], None)
            .expect("ref fallback");
        assert_eq!(scores(&index, &chain[4..]), vec![(w, 2)]);
        assert_eq!(index.worker_block_count(w), 4);
        // The engine announces the chain whole.
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("whole");
        reference.apply_stored(w, &blocks, None).expect("ref whole");
        assert_eq!(index.worker_block_count(w), 6);
        assert_eq!(index.current_size(), 6);
        assert_eq!(index.entry_count(), 6);
        assert_eq!(index.stats().moved_hashes, 2);
        assert_eq!(scores(&index, &chain), vec![(w, 6)]);
        assert_eq!(scores(&index, &chain[4..]), vec![]);
        assert_eq!(index.debug_blocks(), reference.blocks());
        assert_eq!(reference.find_matches(&chain[4..]).len(), 0);
        // The id is freed and reused: nothing is inherited.
        index.remove_worker(w, map);
        assert!(index.is_empty());
        let v = index.intern_worker("v").expect("id");
        assert_eq!(v, w);
        assert_eq!(scores(&index, &chain), vec![]);
        assert_eq!(index.worker_block_count(v), 0);
    }

    /// A worker that stores its chain again after another worker's divergence split the run
    /// re-stores every block at the same place in forwarded coordinates: nothing moves, nothing
    /// is released.
    #[test]
    fn a_re_store_across_a_split_moves_nothing() {
        let index = ChainIndex::with_max_workers(8);
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let chain: Vec<ContentHash> = (0..30).map(|p| content(24, p)).collect();
        let blocks = blocks_of(&chain);
        index.apply_stored(a, &blocks, None, &mut ma).expect("a");
        let mut fork = chain[..12].to_vec();
        fork.extend((12..20).map(|p| content(25, p)));
        index
            .apply_stored(b, &blocks_of(&fork), None, &mut mb)
            .expect("b");
        // a re-stores the whole chain and then a tail after its own parent.
        index
            .apply_stored(a, &blocks, None, &mut ma)
            .expect("a again");
        index
            .apply_stored(a, &blocks[20..], Some(blocks[19].seq_hash), &mut ma)
            .expect("a tail");
        assert_eq!(index.stats().moved_hashes, 0);
        assert_eq!(index.worker_block_count(a), 30);
        assert_eq!(scores(&index, &chain), vec![(a, 30), (b, 12)]);
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(a, &blocks, None).expect("ref a");
        reference
            .apply_stored(b, &blocks_of(&fork), None)
            .expect("ref b");
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    /// A split by another lane can land between a store's placement, recorded under the run's
    /// lock, and its lane-map write. The write then meets a recorded place that forwards to the
    /// suffix, exactly as the map's old entry for the block does: not a move, nothing released.
    /// Driven through the write phase directly, with the placement recorded before the split
    /// (a hole another holder opens; a divergence hangs a child and splits nothing).
    #[test]
    fn a_placement_recorded_before_a_split_is_not_a_move() {
        let index = ChainIndex::with_max_workers(8);
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let chain: Vec<ContentHash> = (0..8).map(|p| content(26, p)).collect();
        let blocks = blocks_of(&chain);
        index.apply_stored(a, &blocks, None, &mut ma).expect("a");
        index.apply_stored(b, &blocks, None, &mut mb).expect("b");
        let first = ma.get(blocks[0].seq_hash).expect("mapped");
        // b drops block 1: a hole, the run is split at 2. a's map keeps its entries in the
        // original run's coordinates, forwarded to the suffix that holds blocks 2..8 now.
        index.apply_removed(b, &[blocks[1].seq_hash], &mut mb);
        let (suffix, _) = index
            .resolve(BlockRef {
                run: first.run,
                offset: 2,
            })
            .expect("forwarded");
        assert_ne!(suffix.run, first.run);
        assert_eq!(suffix.offset, 0);
        // What a's re-store of the chain records at this point: blocks 0..2 in the prefix,
        // blocks 2..8 in the suffix.
        let pending = vec![
            Placed {
                run: first.run,
                offset: 0,
                start: 0,
                count: 2,
                conflicts: 0,
            },
            Placed {
                run: suffix.run,
                offset: 0,
                start: 2,
                count: 6,
                conflicts: 0,
            },
        ];
        // Before the write lands, b drops block 4: the suffix is split at 3.
        index.apply_removed(b, &[blocks[4].seq_hash], &mut mb);
        assert_ne!(
            index
                .resolve(BlockRef {
                    run: suffix.run,
                    offset: 3,
                })
                .expect("forwarded again")
                .0
                .run,
            suffix.run
        );
        index.write_placements(a, &blocks, pending, &mut ma);
        assert_eq!(index.stats().moved_hashes, 0);
        assert_eq!(index.worker_block_count(a), 8);
        assert_eq!(ma.len(), 8);
        assert_eq!(scores(&index, &chain), vec![(a, 8), (b, 1)]);
        // The entries resolve to places that credit a: a store under block 6 finds its parent.
        index
            .apply_stored(a, &blocks[7..], Some(blocks[6].seq_hash), &mut ma)
            .expect("a tail");
        assert_eq!(index.worker_block_count(a), 8);
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(a, &blocks, None).expect("ref a");
        reference.apply_stored(b, &blocks, None).expect("ref b");
        reference.apply_removed(b, &[blocks[1].seq_hash]);
        reference.apply_removed(b, &[blocks[4].seq_hash]);
        assert_eq!(index.debug_blocks(), reference.blocks());
    }

    /// An engine that names one position by two engine hashes (the same content under the same
    /// parent: a twin) has the second filed onto the first by content, and the lane map carries
    /// both names for one held position. Removing the first name takes the position with it;
    /// a store under the second then points past what the worker holds, which cuts the run at
    /// the parent and joins what lies beyond. When nothing lies beyond (nobody else holds the
    /// tail, no child hangs there) the cut used to hand back `GONE` as a run to join and the
    /// slab lookup panicked; now the run ends at the cut and the blocks go after it.
    #[test]
    fn a_store_under_a_twin_whose_first_name_was_removed_does_not_panic() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let chain: Vec<ContentHash> = (0..8).map(|p| content(29, p)).collect();
        let blocks = blocks_of(&chain);
        index
            .apply_stored(w, &blocks, None, &mut map)
            .expect("chain");
        // Twins of positions 4..8: the same content under the same parent, other engine hashes.
        let twins: Vec<StoredBlock> = blocks[4..]
            .iter()
            .enumerate()
            .map(|(i, b)| StoredBlock {
                seq_hash: SequenceHash(b.seq_hash.0 ^ (0x5151 << (i + 8))),
                content_hash: b.content_hash,
            })
            .collect();
        index
            .apply_stored(w, &twins, Some(blocks[3].seq_hash), &mut map)
            .expect("twins");
        assert_eq!(index.stats().engine_conflicts, 4);
        assert_eq!(index.worker_block_count(w), 8);
        assert_eq!(map.len(), 12);
        // The first names of positions 4..8 go: the positions go with them.
        let first_names: Vec<SequenceHash> = blocks[4..].iter().map(|b| b.seq_hash).collect();
        index.apply_removed(w, &first_names, &mut map);
        assert_eq!(index.worker_block_count(w), 4);
        assert_eq!(scores(&index, &chain), vec![(w, 4)]);
        // A store under the twin at position 5: the parent entry points past the holding.
        let tail = blocks_of(&[
            chain[0],
            chain[1],
            chain[2],
            chain[3],
            chain[4],
            chain[5],
            content(30, 6),
        ]);
        let stored = index.apply_stored(w, &tail[6..], Some(twins[1].seq_hash), &mut map);
        assert!(stored.is_ok(), "{stored:?}");
        assert_eq!(index.worker_block_count(w), 5);
        // Positions 0..4 and the new block at 6 are held; positions 4 and 5 are not (the
        // documented twin gap: one engine hash per held position).
        assert_eq!(scores(&index, &chain[..4]), vec![(w, 4)]);
        let query: Vec<ContentHash> = tail.iter().map(|b| b.content_hash).collect();
        assert_eq!(scores(&index, &query), vec![(w, 4)]);
        assert!(index
            .debug_blocks()
            .iter()
            .any(|b| b.0 == w && b.1 == 6 && b.2 == content(30, 6)));
        // Everything the worker holds is still reachable and consistent.
        assert_eq!(index.debug_blocks().len(), 5);
    }
    /// `is_empty` follows the blocks: false from the first store, true again once every block
    /// is removed, cleared or taken with its worker.
    #[test]
    fn emptiness_follows_the_blocks() {
        let index = ChainIndex::with_max_workers(8);
        assert!(index.is_empty());
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let chain: Vec<ContentHash> = (0..12).map(|p| content(21, p)).collect();
        let blocks = blocks_of(&chain);
        index.apply_stored(a, &blocks, None, &mut ma).expect("a");
        assert!(!index.is_empty());
        index
            .apply_stored(b, &blocks[..6], None, &mut mb)
            .expect("b");
        let hashes: Vec<SequenceHash> = blocks.iter().map(|block| block.seq_hash).collect();
        index.apply_removed(a, &hashes, &mut ma);
        assert!(!index.is_empty(), "b still holds a prefix");
        index.apply_cleared(b, &mut mb);
        assert!(index.is_empty());
        index
            .apply_stored(a, &blocks[..3], None, &mut ma)
            .expect("a again");
        assert!(!index.is_empty());
        index.remove_worker(a, ma);
        assert!(index.is_empty());
    }

    /// A content array recycled from a larger class gets an engine-hash twin of at least that
    /// capacity, so an in-place append (which claims room from the content header and writes
    /// both arrays) never runs past the twin into the words behind it.
    #[test]
    fn the_engine_twin_is_as_large_as_a_recycled_content_array() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut mw = ChainBlockMap::default();
        // One freed array two classes above what 140 blocks ask for: the content array takes
        // it, the twin must not come out smaller.
        let big = index.arena.alloc_array(&[0; 200], 200);
        index.arena.array_release(big);
        let chain: Vec<ContentHash> = (0..200).map(|p| content(9, p)).collect();
        let blocks = blocks_of(&chain);
        index
            .apply_stored(w, &blocks[..140], None, &mut mw)
            .expect("first store");
        let run_id = mw.get(blocks[0].seq_hash).expect("mapped").run;
        let run = index.slab.run(run_id);
        let (block, engine) = (
            run.block.load(Ordering::Relaxed),
            run.engine.load(Ordering::Relaxed),
        );
        assert_eq!(block, big, "the recycled array was taken");
        assert!(
            index.arena.array_capacity(engine) >= index.arena.array_capacity(block),
            "twin {} words, content {}",
            index.arena.array_capacity(engine),
            index.arena.array_capacity(block)
        );
        // Words bumped right behind the twin: an overflowing append would land here.
        let guard = index.arena.alloc_array(&[7; 8], 8);
        index
            .apply_stored(w, &blocks[140..], Some(blocks[139].seq_hash), &mut mw)
            .expect("append");
        assert_eq!(run.len(), 200, "appended in place");
        assert_eq!(
            run.block.load(Ordering::Relaxed),
            block,
            "same content array"
        );
        for (slot, word) in index.arena.words(guard, 8).iter().enumerate() {
            assert_eq!(
                word.load(Ordering::Relaxed),
                7,
                "guard word {slot} overwritten"
            );
        }
        for block in &blocks {
            assert!(index.is_held(&mw, block.seq_hash), "{:?}", block.seq_hash);
        }
    }

    /// A run is matched by its engine chain: a store that agrees to the end of the window is
    /// taken whole on one compare, one that diverges inside is split exactly where the bisection
    /// lands, one that stops short is a prefix, one that runs past the end extends; the
    /// reference agrees on every block and no counter moves.
    #[test]
    fn stores_match_a_run_by_its_engine_chain() {
        let index = ChainIndex::with_max_workers(8);
        let mut reference = ReferenceIndexer::new();
        let chain: Vec<ContentHash> = (0..100).map(|p| content(5, p)).collect();
        let mut forked = chain[..57].to_vec();
        forked.extend((57..100).map(|p| content(6, p)));
        let short = chain[..30].to_vec();
        let longer: Vec<ContentHash> = (0..140).map(|p| content(5, p)).collect();
        let mut early_fork = chain[..1].to_vec();
        early_fork.extend((1..40).map(|p| content(7, p)));
        let mut maps = Vec::new();
        for (name, contents) in [
            ("whole", &chain),
            ("forked", &forked),
            ("short", &short),
            ("longer", &longer),
            ("early", &early_fork),
        ] {
            let worker = index.intern_worker(name).expect("id");
            let mut map = ChainBlockMap::default();
            index
                .apply_stored(worker, &blocks_of(contents), None, &mut map)
                .expect("store");
            reference
                .apply_stored(worker, &blocks_of(contents), None)
                .expect("reference store");
            maps.push((worker, map));
        }
        for query in [&chain, &forked, &short, &longer, &early_fork] {
            let expected: Vec<(u32, u32)> = reference.find_matches(query).into_iter().collect();
            assert_eq!(scores(&index, query), expected);
        }
        assert_eq!(index.debug_blocks(), reference.blocks());
        // The run of the first 57 blocks is shared by four workers and the walk after the
        // divergence took the fork's own run: a decode extension of the fork appends to it.
        let (fork_worker, fork_map) = &mut maps[1];
        let more: Vec<ContentHash> = (100..110).map(|p| content(6, p)).collect();
        let mut extended = forked.clone();
        extended.extend(more.iter().copied());
        let blocks = blocks_of(&extended);
        index
            .apply_stored(
                *fork_worker,
                &blocks[100..],
                Some(blocks[99].seq_hash),
                fork_map,
            )
            .expect("extend");
        reference
            .apply_stored(*fork_worker, &blocks[100..], Some(blocks[99].seq_hash))
            .expect("reference extend");
        let expected: Vec<(u32, u32)> = reference.find_matches(&extended).into_iter().collect();
        assert_eq!(scores(&index, &extended), expected);
        let stats = index.stats();
        assert_eq!(stats.engine_conflicts, 0);
        assert_eq!(stats.landing_mismatches, 0);
    }

    /// A worker whose engine names the same content by other hashes cannot match by engine
    /// hash; the walk sees the content continue past the mismatch, compares block by block and
    /// counts the conflicts, and the worker's own hashes key its lane map: lookups, an extension
    /// after its own parent hash and removals by its hashes all stay exact.
    #[test]
    fn a_worker_with_other_engine_hashes_matches_by_content() {
        let index = ChainIndex::with_max_workers(8);
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let chain: Vec<ContentHash> = (0..50).map(|p| content(8, p)).collect();
        let blocks = blocks_of(&chain);
        let other: Vec<StoredBlock> = blocks
            .iter()
            .map(|block| StoredBlock {
                seq_hash: SequenceHash(block.seq_hash.0 ^ 0x5bd1_e995),
                content_hash: block.content_hash,
            })
            .collect();
        index
            .apply_stored(a, &blocks[..40], None, &mut ma)
            .expect("a");
        index
            .apply_stored(b, &other[..40], None, &mut mb)
            .expect("b");
        assert_eq!(scores(&index, &chain), vec![(a, 40), (b, 40)]);
        assert_eq!(index.stats().engine_conflicts, 40);
        index
            .apply_stored(b, &other[40..], Some(other[39].seq_hash), &mut mb)
            .expect("b extends");
        assert_eq!(scores(&index, &chain), vec![(a, 40), (b, 50)]);
        let tail: Vec<SequenceHash> = other[20..].iter().map(|block| block.seq_hash).collect();
        index.apply_removed(b, &tail, &mut mb);
        assert_eq!(scores(&index, &chain), vec![(a, 40), (b, 20)]);
        for block in &other[20..] {
            assert!(!index.is_held(&mb, block.seq_hash));
        }
        for block in &other[..20] {
            assert!(index.is_held(&mb, block.seq_hash));
        }
        assert_eq!(index.stats().landing_mismatches, 0);
    }

    /// A store that carries the engine hashes the index holds for other content (an engine
    /// whose hashes do not follow its content, which the relay's hash check refuses) is counted
    /// and placed by its content: the match ends where the content does.
    #[test]
    fn other_content_under_known_engine_hashes_is_counted_and_placed_by_content() {
        let index = ChainIndex::with_max_workers(8);
        let a = index.intern_worker("a").expect("id");
        let b = index.intern_worker("b").expect("id");
        let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
        let chain: Vec<ContentHash> = (0..20).map(|p| content(9, p)).collect();
        let blocks = blocks_of(&chain);
        let mut other = chain[..10].to_vec();
        other.extend((10..20).map(|p| content(10, p)));
        let impostor: Vec<StoredBlock> = blocks_of(&other)
            .into_iter()
            .zip(&blocks)
            .map(|(block, known)| StoredBlock {
                seq_hash: known.seq_hash,
                content_hash: block.content_hash,
            })
            .collect();
        index.apply_stored(a, &blocks, None, &mut ma).expect("a");
        index.apply_stored(b, &impostor, None, &mut mb).expect("b");
        assert_eq!(index.stats().landing_mismatches, 1);
        assert_eq!(scores(&index, &chain), vec![(a, 20), (b, 10)]);
        assert_eq!(scores(&index, &other), vec![(a, 10), (b, 20)]);
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(a, &blocks, None).expect("ref a");
        reference.apply_stored(b, &impostor, None).expect("ref b");
        assert_eq!(index.debug_blocks(), reference.blocks());
        let hashes: Vec<SequenceHash> = impostor.iter().map(|block| block.seq_hash).collect();
        index.apply_removed(b, &hashes, &mut mb);
        assert_eq!(scores(&index, &other), vec![(a, 10)]);
        assert!(mb.is_empty());
    }

    /// The race the release harness caught: a split of the parent between a store walk's plan
    /// and its lock-free claim must make the insert give up, not link the child after the new
    /// end. Replayed deterministically: plan (take the version), split, then try the insert.
    #[test]
    fn a_child_insert_planned_before_a_split_gives_up() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut mw = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..10).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        index.apply_stored(w, &blocks, None, &mut mw).expect("w");
        let parent = mw.get(blocks[9].seq_hash).expect("mapped").run;
        let (_, planned) = index.slab.run(parent).snapshot();
        // A divergence inside the run no longer moves its end (a fork hangs off its offset), and
        // neither does a tail eviction (the cutoff drops, the array stays for regrowth); a hole
        // does: w drops blocks 3 and 4, and the run is cut at 5.
        let hole: Vec<SequenceHash> = blocks[3..5].iter().map(|block| block.seq_hash).collect();
        index.apply_removed(w, &hole, &mut mw);
        assert_eq!(index.slab.run(parent).len(), 5);
        // The child prepared for blocks after the old end must not be linked after the new one.
        let contents = [content(3, 0).0, content(3, 1).0];
        let block = index.arena.alloc_array(&contents, capacity_for(2));
        let engine = index
            .arena
            .alloc_array(&[30, 31], index.arena.array_capacity(block));
        let child = index.slab.alloc(
            0,
            parent,
            Window {
                block,
                base: 0,
                engine,
                len: 2,
                children: NONE,
                partials: NONE,
                forwards: NONE,
            },
        );
        set(index.slab.coverage(child), w);
        assert!(matches!(
            index.insert_child(parent, child, 10, contents[0], planned),
            Claim::Changed
        ));
        let mut freed = Vec::new();
        index.discard_run(child, w, &mut freed);
        index.recycle(&mut freed);
        // The real path restarts from the parent block and lands the blocks after block 9, in
        // the suffix the hole split off; the lookup still stops at the hole.
        let mut longer = held.clone();
        longer.extend([content(3, 0), content(3, 1)]);
        let longer_blocks = blocks_of(&longer);
        index
            .apply_stored(w, &longer_blocks[10..], Some(blocks[9].seq_hash), &mut mw)
            .expect("extend");
        let mut reference = ReferenceIndexer::new();
        reference.apply_stored(w, &blocks, None).expect("ref w");
        reference.apply_removed(w, &hole);
        reference
            .apply_stored(w, &longer_blocks[10..], Some(blocks[9].seq_hash))
            .expect("ref extend");
        assert_eq!(index.debug_blocks(), reference.blocks());
        assert_eq!(scores(&index, &longer), vec![(w, 3)]);
    }

    /// Interning and releasing worker slots from many threads at once: an intern holds a name-map
    /// shard and then the registry, a release must not hold the registry while it takes a shard,
    /// or the two deadlock (this test hung within seconds before the order was fixed).
    #[test]
    fn worker_slots_churn_from_many_threads_without_deadlock() {
        let index = ChainIndex::with_max_workers(64);
        std::thread::scope(|scope| {
            for thread in 0..8u32 {
                let index = &index;
                scope.spawn(move || {
                    for round in 0..2_000u32 {
                        let name = format!("t{thread}-r{}", round % 5);
                        let id = index.intern_worker(&name).expect("slot");
                        let mut map = ChainBlockMap::default();
                        let held: Vec<ContentHash> =
                            (0..3).map(|p| content(u64::from(id) + 1, p)).collect();
                        index
                            .apply_stored(id, &blocks_of(&held), None, &mut map)
                            .expect("store");
                        index.remove_worker(id, map);
                    }
                });
            }
        });
        assert_eq!(index.current_size(), 0);
        assert_eq!(
            index.intern_worker("after"),
            Ok(index.intern_worker("after").expect("slot"))
        );
    }

    #[test]
    fn parent_errors_match_the_positional_indexer() {
        let index = ChainIndex::with_max_workers(8);
        let w = index.intern_worker("w").expect("id");
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..3).map(|p| content(1, p)).collect();
        let blocks = blocks_of(&held);
        assert!(matches!(
            index.apply_stored(w, &blocks[1..], Some(blocks[0].seq_hash), &mut map),
            Err(ApplyError::WorkerNotTracked)
        ));
        index
            .apply_stored(w, &blocks[..1], None, &mut map)
            .expect("store");
        assert!(matches!(
            index.apply_stored(w, &blocks[2..], Some(blocks[1].seq_hash), &mut map),
            Err(ApplyError::ParentBlockNotFound)
        ));
    }

    #[test]
    fn worker_slots_are_bounded_and_reused_after_removal() {
        let index = ChainIndex::with_max_workers(2);
        assert_eq!(index.intern_worker("a"), Ok(0));
        assert_eq!(index.intern_worker("b"), Ok(1));
        assert_eq!(index.intern_worker("a"), Ok(0));
        assert_eq!(index.intern_worker("c"), Err(WorkerIdExhausted));
        let mut map = ChainBlockMap::default();
        let held: Vec<ContentHash> = (0..4).map(|p| content(1, p)).collect();
        index
            .apply_stored(0, &blocks_of(&held), None, &mut map)
            .expect("store");
        index.remove_worker(0, map);
        assert_eq!(index.worker_id("a"), None);
        assert_eq!(
            index.intern_worker("c"),
            Ok(0),
            "the freed slot is handed out again"
        );
        assert_eq!(
            scores(&index, &held),
            vec![],
            "nothing of the old holder survives"
        );
        assert_eq!(index.intern_worker("a"), Err(WorkerIdExhausted));
    }
}
