//! The relay's state snapshot against the engines' recorded streams
//! (`tests/fixtures/captured`): the live set the snapshot emits equals a
//! reference replay of the normalized stream, copies and tiers included; a
//! live parent always precedes its children; the emitted chains re-verify
//! under the engine's own hash, which needs every parent's digest before its
//! child and so fails on any ordering slip; and the chunks are stamped the
//! way the gateway's cursor needs them.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::print_stderr
)]

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use engine_servicer::{
    engine_hash::EngineHash,
    kv_state::{LiveState, SnapshotChunks, CHUNK_BLOCKS},
    kv_wire::{
        BlockHash, Counts, EventTail, ExtraKey, Normalizer, WireBatch, WireEvent, WireStored,
        WireTokens,
    },
};
use engine_zmq_client::codec::TrailingTolerant;
use serde::Deserialize;
use smg_grpc_client::common_proto::{
    kv_block_extra_key, kv_cache_event, KvBlocksStored, KvEventBatch,
};

/// `(rank, tier, hash)` with its physical copies.
type LiveSet = BTreeMap<(Option<i32>, i32, i64), u32>;

#[derive(Deserialize)]
struct Line {
    seq: u64,
    payload_b64: String,
}

fn captured(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/captured")
        .join(name)
}

/// A capture's publisher payloads in sequence order, the replay copy of a
/// batch folded into its live one.
fn payloads(path: &Path) -> Vec<(u64, Vec<u8>)> {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let text = if path.extension().is_some_and(|ext| ext == "gz") {
        let mut text = String::new();
        flate2::read::GzDecoder::new(&bytes[..])
            .read_to_string(&mut text)
            .expect("a gzipped capture");
        text
    } else {
        String::from_utf8(bytes).expect("utf-8 capture")
    };
    let mut lines: Vec<(u64, Vec<u8>)> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let line: Line = serde_json::from_str(line).expect("a capture line");
            let payload = STANDARD.decode(line.payload_b64).expect("base64 payload");
            (line.seq, payload)
        })
        .collect();
    lines.sort_by_key(|(seq, _)| *seq);
    lines.dedup_by_key(|(seq, _)| *seq);
    lines
}

/// The capture decoded and normalized the way the relay forwards it.
fn normalized(path: &Path, normalizer: &mut Normalizer, event_id: &mut u64) -> Vec<KvEventBatch> {
    payloads(path)
        .into_iter()
        .map(|(seq, payload)| {
            let batch = rmp_serde::from_slice::<TrailingTolerant<WireBatch>>(&payload)
                .unwrap_or_else(|error| panic!("{}: batch {seq}: {error}", path.display()))
                .0;
            normalizer.normalize_batch(batch, seq, event_id)
        })
        .collect()
}

/// The live set a naive replay of the normalized stream leaves: stores add
/// a copy (capped like the gateway counts them), removals take one, a clear
/// empties the rank.
fn reference(batches: &[KvEventBatch]) -> LiveSet {
    let mut live = LiveSet::new();
    for batch in batches {
        for event in &batch.events {
            match &event.data {
                Some(kv_cache_event::Data::Stored(stored)) => {
                    let tier = stored.tier.expect("the relay sets the tier");
                    for block in &stored.blocks {
                        let copies = live
                            .entry((batch.dp_rank, tier, block.block_hash))
                            .or_insert(0);
                        if *copies < 8 {
                            *copies += 1;
                        }
                    }
                }
                Some(kv_cache_event::Data::Removed(removed)) => {
                    let tier = removed.tier.expect("the relay sets the tier");
                    for &hash in &removed.block_hashes {
                        let key = (batch.dp_rank, tier, hash);
                        if let Some(copies) = live.get_mut(&key) {
                            *copies -= 1;
                            if *copies == 0 {
                                live.remove(&key);
                            }
                        }
                    }
                }
                Some(kv_cache_event::Data::Cleared(_)) => {
                    live.retain(|(rank, _, _), _| *rank != batch.dp_rank);
                }
                None => {}
            }
        }
    }
    live
}

/// Every stored event of the chunks with the rank it came under.
fn stores(chunks: &[KvEventBatch]) -> Vec<(Option<i32>, &KvBlocksStored)> {
    chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .events
                .iter()
                .filter_map(move |event| match &event.data {
                    Some(kv_cache_event::Data::Stored(stored)) => Some((chunk.dp_rank, stored)),
                    _ => None,
                })
        })
        .collect()
}

fn emitted(chunks: &[KvEventBatch]) -> LiveSet {
    let mut live = LiveSet::new();
    for (rank, stored) in stores(chunks) {
        for block in &stored.blocks {
            *live
                .entry((rank, stored.tier.unwrap(), block.block_hash))
                .or_insert(0) += 1;
        }
    }
    live
}

/// A snapshot store as the publisher would have sent it, so the relay's
/// hash check can rehash the emitted chain.
fn wire_stored(stored: &KvBlocksStored) -> WireEvent {
    let first = &stored.blocks[0];
    WireEvent::BlockStored(WireStored {
        block_hashes: stored
            .blocks
            .iter()
            .map(|block| BlockHash(block.block_hash))
            .collect(),
        parent_block_hash: stored.parent_block_hash.map(BlockHash),
        token_ids: WireTokens::Ids(
            stored
                .blocks
                .iter()
                .flat_map(|block| block.token_ids.iter().copied())
                .collect(),
        ),
        block_size: i64::from(first.block_size),
        lora_id: first.lora_id,
        lora_name: stored.lora_name.clone(),
        cache_salt: stored.cache_salt.clone(),
        extra_keys: Some(
            stored
                .blocks
                .iter()
                .map(|block| {
                    (!block.extra_keys.is_empty()).then(|| {
                        block
                            .extra_keys
                            .iter()
                            .map(|key| match key.key.clone().expect("a key") {
                                kv_block_extra_key::Key::Text(text) => ExtraKey::Text(text),
                                kv_block_extra_key::Key::Number(number) => ExtraKey::Number(number),
                                kv_block_extra_key::Key::Blob(blob) => ExtraKey::Blob(blob),
                                kv_block_extra_key::Key::Multimodal(mm) => ExtraKey::Multimodal {
                                    identifier: mm.identifier,
                                    offset: mm.offset,
                                },
                            })
                            .collect()
                    })
                })
                .collect(),
        ),
        tail: EventTail {
            medium: stored.medium.clone(),
            group_idx: stored.group_idx,
            kv_cache_spec_kind: stored.kv_cache_spec_kind.clone(),
            kv_cache_spec_sliding_window: stored.kv_cache_spec_sliding_window,
            locality: None,
            ownership: stored.ownership.clone(),
            session_id: stored.session_id.clone(),
        },
    })
}

/// Rehash the emitted stores in order with `engine`'s algorithm.
fn rehash(chunks: &[KvEventBatch], engine: EngineHash) -> Counts {
    let mut checker = Normalizer::with_hash_check(engine);
    let mut event_id = 0;
    for (rank, stored) in stores(chunks) {
        event_id += 1;
        checker
            .normalize(wire_stored(stored), rank, event_id)
            .expect("a snapshot store is forwardable");
    }
    checker.counts().clone()
}

struct Case {
    name: &'static str,
    files: &'static [&'static str],
    engine: EngineHash,
}

const CASES: &[Case] = &[
    Case {
        name: "vllm",
        files: &["vllm/qwen3-0.6b-vllm0.31-capture.jsonl.gz"],
        engine: EngineHash::VllmSha256Cbor,
    },
    Case {
        name: "sglang",
        files: &["sglang/qwen3-0.6b-sglang0.5.21-capture.jsonl.gz"],
        engine: EngineHash::Sglang,
    },
    Case {
        name: "sglang-hicache",
        files: &["sglang-hicache/qwen3-0.6b-sglang0.5.21-hicache-capture.jsonl"],
        engine: EngineHash::Sglang,
    },
    Case {
        name: "sglang-dp2",
        files: &[
            "sglang-dp2/qwen3-0.6b-sglang0.5.21-dp2-rank0-capture.jsonl.gz",
            "sglang-dp2/qwen3-0.6b-sglang0.5.21-dp2-rank1-capture.jsonl.gz",
        ],
        engine: EngineHash::Sglang,
    },
];

/// The capture's normalized batches, every file's stream in turn (the dp2
/// ranks are two publishers feeding one state).
fn stream(case: &Case) -> (Vec<KvEventBatch>, Counts) {
    let mut normalizer = Normalizer::with_hash_check(case.engine);
    let mut event_id = 0;
    let mut batches = Vec::new();
    for file in case.files {
        batches.extend(normalized(&captured(file), &mut normalizer, &mut event_id));
    }
    (batches, normalizer.counts().clone())
}

#[test]
fn snapshots_of_the_captured_streams_equal_their_live_sets() {
    for case in CASES {
        let name = case.name;
        let (batches, stream_counts) = stream(case);
        assert!(!batches.is_empty(), "{name}: capture decodes");
        let mut state = LiveState::new();
        for batch in &batches {
            state.apply(batch);
        }
        let through = batches.iter().map(|b| b.sequence_number).max().unwrap();
        let chunks: Vec<KvEventBatch> =
            SnapshotChunks::new(state.snapshot(), through, 1.0, 0).collect();

        let want = reference(&batches);
        let got = emitted(&chunks);
        assert_eq!(got, want, "{name}: the emitted live set");
        assert_eq!(
            state.blocks(),
            want.values().map(|&copies| u64::from(copies)).sum::<u64>(),
            "{name}: live copies"
        );
        assert_eq!(state.entries(), want.len(), "{name}: live entries");

        // Framing: the clear first, every chunk marked, stamps up to `through`.
        assert!(matches!(
            chunks[0].events[0].data,
            Some(kv_cache_event::Data::Cleared(_))
        ));
        let count = chunks.len() as u32;
        for (index, chunk) in chunks.iter().enumerate() {
            let marker = chunk.snapshot.as_ref().expect("marked");
            assert_eq!(
                (marker.index, marker.count),
                (index as u32, count),
                "{name}"
            );
            assert_eq!(marker.blocks, state.blocks(), "{name}");
            assert_eq!(
                chunk.sequence_number,
                through + 1 - u64::from(count) + index as u64,
                "{name}: stamp of chunk {index}"
            );
            let blocks: usize = stores(std::slice::from_ref(chunk))
                .iter()
                .map(|(_, stored)| stored.blocks.len())
                .sum();
            assert!(
                blocks <= CHUNK_BLOCKS,
                "{name}: chunk {index} holds {blocks}"
            );
        }

        // Order: a live parent precedes its children, per rank.
        let live_hashes: HashSet<(Option<i32>, i64)> =
            want.keys().map(|(rank, _, hash)| (*rank, *hash)).collect();
        let mut seen: HashSet<(Option<i32>, i64)> = HashSet::new();
        for (rank, stored) in stores(&chunks) {
            if let Some(parent) = stored.parent_block_hash {
                assert!(
                    seen.contains(&(rank, parent)) || !live_hashes.contains(&(rank, parent)),
                    "{name}: parent {parent} of {} emitted after its child",
                    stored.blocks[0].block_hash
                );
            }
            for block in &stored.blocks {
                seen.insert((rank, block.block_hash));
            }
        }

        // The engine's own hash over the emitted chains: every block checked
        // with a known parent. SGLang's capture verifies exactly; the vLLM
        // capture was taken under PYTHONHASHSEED=0, which seeds NONE_HASH
        // differently from the default the check reproduces (its README),
        // so there the check proves the chain order, not the digests.
        let counts = rehash(&chunks, case.engine);
        let blocks: u64 = got.values().map(|&copies| u64::from(copies)).sum();
        assert_eq!(counts.hash_checked, blocks, "{name}: every block rehashed");
        assert_eq!(counts.hash_unverifiable, 0, "{name}: a parent was missing");
        if stream_counts.hash_mismatch == 0 {
            assert_eq!(counts.hash_mismatch, 0, "{name}: the chain rehashes");
        }
        eprintln!(
            "{name}: {} batches -> {} live entries, {} copies, {} chunks; stream hash check \
             {}/{} mismatched, snapshot {}/{}",
            batches.len(),
            state.entries(),
            state.blocks(),
            chunks.len(),
            stream_counts.hash_mismatch,
            stream_counts.hash_checked,
            counts.hash_mismatch,
            counts.hash_checked,
        );
    }
}

/// Cutting the stream anywhere and snapshotting there equals the reference
/// at that point: the state follows removals and clears, not just stores.
#[test]
fn snapshots_at_every_cut_of_the_vllm_stream_follow_the_reference() {
    let case = &CASES[0];
    let (batches, _) = stream(case);
    let mut state = LiveState::new();
    for (index, batch) in batches.iter().enumerate() {
        state.apply(batch);
        let want = reference(&batches[..=index]);
        let chunks: Vec<KvEventBatch> =
            SnapshotChunks::new(state.snapshot(), batch.sequence_number, 1.0, 0).collect();
        assert_eq!(
            emitted(&chunks),
            want,
            "after batch {}",
            batch.sequence_number
        );
    }
}
