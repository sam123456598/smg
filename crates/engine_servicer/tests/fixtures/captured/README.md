# Captured KV-event streams from real engines

Raw ZMQ KV-event batches recorded on devgpu044 (arm64 Grace, NVIDIA GB300, driver 580.126.20)
on 2026-10-05 with the scripts in `~/smg-perf/gpu/scripts` (see `crates/kv_index/docs/gpu-harness.md`).
Model: `Qwen/Qwen3-0.6B`, one engine per capture, data-parallel size 1 unless the section says otherwise.

## What is checked in

One gzipped capture per engine variant and nothing else. Every file is loaded by a test:
`tests/kv_event_snapshot.rs` (the relay's state snapshot against a reference replay of the stream),
`model_gateway/src/worker/kv_index_backend/exactness.rs` (both gateway index backends against the reference
indexer on the same streams) and, for the vLLM stream, `model_gateway/benches/kv_index_decision.rs`.

| file | engine |
|---|---|
| `vllm/qwen3-0.6b-vllm0.31-capture.jsonl.gz` | vLLM 0.31.0 |
| `sglang/qwen3-0.6b-sglang0.5.21-capture.jsonl.gz` | SGLang 0.5.21 |
| `sglang-dp2/qwen3-0.6b-sglang0.5.21-dp2-rank0-capture.jsonl.gz`, `...-rank1-capture.jsonl.gz` | SGLang 0.5.21, `--dp-size 2`, one stream per rank |
| `sglang-hicache/qwen3-0.6b-sglang0.5.21-hicache-capture.jsonl.gz` | SGLang 0.5.21, HiCache write-through |

The rest of what each capture run produced (the launch script, `GET /server_info`, the field census from
`summarize_kv_events.py`, the workload with the engine's own `prompt_tokens_details.cached_tokens` per request,
the HiCache per-hash transition list, ten-batch excerpts) is not in the tree; it lives beside the uncompressed
originals under `~/smg-perf/gpu/fixtures/{vllm,sglang,sglang-dp2,sglang-hicache}/` on the capture host. The facts
the tests and the harness notes rely on are quoted below.

## File format

One JSON object per line:

| key | meaning |
|---|---|
| `source` | `live` (SUB socket) or `replay` (fetched from the replay ROUTER with start seq 0 before subscribing) |
| `recv_ts` | host wall clock at receive |
| `topic` | first PUB frame as text (`kv-events`; the SGLang replay reply carries no topic frame, stored as `""`) |
| `seq` | second frame, 8-byte big-endian unsigned |
| `payload_len`, `payload_b64` | third frame: the msgpack `KVEventBatch` exactly as sent |

Decode with `msgpack.unpackb(base64.b64decode(payload_b64), raw=False)`: an array `[ts, events, dp_rank]`
whose events are tagged maps (`type` in `BlockStored`, `BlockRemoved`, `AllBlocksCleared`) with `omit_defaults`.

## Workload (identical for both engines)

Token-id prompts over `/v1/completions`, `max_tokens=1`, phases in order: `shared` (8 sequential prompts sharing a
512-token prefix, 128-token suffixes), `repeat` (same 8 again), `burst` (8 concurrent prompts on a new prefix),
`evict` (120 unique 1024-token prompts, 4 concurrent, through a cache of ~9k tokens), `recheck` (the shared 8
again), `reset` (`POST /reset_prefix_cache` on vLLM, `POST /flush_cache` on SGLang, then 2 shared prompts).

## vLLM 0.31.0 (`docker.io/vllm/vllm-openai:latest`, arm64, image created 2026-10-03)

Command: `run-vllm.sh vllm-smoke 0 8101 5557 Qwen/Qwen3-0.6B --max-model-len 4096 --max-num-seqs 16
--kv-cache-memory-bytes 1073741824 --enforce-eager --gpu-memory-utilization 0.3`; the script adds
`--prefix-caching-hash-algo sha256_cbor --block-size 16 --enable-prompt-tokens-details` (585 GPU blocks, 9,360 KV
tokens per the engine's `vllm:cache_config_info` line), `--kv-events-config` with the ZMQ publisher, topic
`kv-events`, replay endpoint on, `PYTHONHASHSEED=0` (note: vLLM seeds `NONE_HASH` with the verbatim value of
`PYTHONHASHSEED` when it is set, so the hashes in this capture chain from `sha256(cbor("0"))`, not from the default
`"vllm-none-hash"`; a reproduction must use the same seed), `VLLM_SERVER_DEV_MODE=1` (for `/reset_prefix_cache`).

Stream: 57 batches, seq 0..56 without gaps, 7547 events (154 `BlockStored`, 7392 `BlockRemoved`, 1 `AllBlocksCleared`),
envelope `[ts, events, 0]`, `ts` monotonic.
Fields seen: `BlockStored` = `type, block_hashes, parent_block_hash, token_ids, block_size, lora_id, medium,
lora_name, extra_keys, group_idx, kv_cache_spec_kind`; `BlockRemoved` = `type, block_hashes, medium, group_idx`;
`AllBlocksCleared` = `type` only. Values: `medium` always `"GPU"`, `group_idx` always 0, `kv_cache_spec_kind`
always `"full_attention"`, `block_size` 16, `lora_id` and `lora_name` null, `extra_keys` a list of nulls aligned
with `block_hashes`, `parent_block_hash` null on 124 of 154 stores. Absent on every event: `session_id`,
`locality`, `ownership`, `kv_cache_spec_sliding_window`, `cache_salt`. Hashes are unsigned 64-bit ints
(`VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES=1` default), none negative. `len(token_ids) == len(block_hashes) * 16`
in every store. One `BlockRemoved` per evicted hash (up to 256 per batch during the eviction phase).

Engine truth from the workload: `shared` = `[0, 512 x7]`, `repeat` = `[624 x8]` (640-token prompt: full hit minus the
recomputed last block), `burst` = `[0, 512 x7]` (in-flight prefill blocks are hittable), `evict` = all 0,
`recheck` = `[0, 512 x7]` (prefix evicted, first request repopulated it), `after_reset` = `[0, 512]`.

## SGLang 0.5.21 (`docker.io/lmsysorg/sglang:latest`, arm64)

Command: `run-sglang.sh sglang-smoke 1 8201 5567 Qwen/Qwen3-0.6B --mem-fraction-static 0.3 --max-total-tokens 9216
--max-running-requests 16 --context-length 4096 --attention-backend triton`; the script adds `--page-size 16
--enable-cache-report --enable-metrics` and
`--kv-events-config '{"publisher":"zmq","topic":"kv-events","endpoint":"tcp://*:5567","replay_endpoint":"tcp://*:5568"}'`.
`GET /server_info` advertised the publisher, port base, topic, block_size 16 and dp_size 1 in its `kv_events` block.

Stream: 83 batches (1 replayed, the startup `AllBlocksCleared`; live seq 1..82 without gaps), 179 events
(146 `BlockStored`, 31 `BlockRemoved`, 2 `AllBlocksCleared`), envelope `[ts, events, 0]`.
Fields seen: `BlockStored` = `type, block_hashes, parent_block_hash, token_ids, block_size, lora_id, medium`;
`BlockRemoved` = `type, block_hashes, medium`; no `group_idx`, `kv_cache_spec_kind`, `extra_keys`, `session_id`,
`cache_salt` or `locality` on any event. `medium` always `"GPU"` (no HiCache in this run), `block_size` 16,
`lora_id` null. Hashes are signed 64-bit ints (4000 of 8016 negative, as SGLang publishes the first 8 digest bytes
as i64). Stores are coalesced per node (up to 64 hashes per event); one `BlockRemoved` per evicted node listing all
its pages.

Engine truth from the workload: `shared` = `[absent, 512 x7]` (SGLang omits `prompt_tokens_details` when nothing
was cached), `repeat` = `[624 x8]`, `burst` = all absent (prompt KV is inserted only after prefill, so concurrent
sharers miss), `evict` = all absent, `recheck` = `[absent, 512 x7]`, `after_reset` = `[absent, 512]`.

## SGLang 0.5.21, data-parallel size 2 (`sglang-dp2/`, host venv, GPUs 2 and 3)

Command: `run-sglang-host.sh sgl-dp2 2,3 8201 5567 5577 --dp-size 2 --mem-fraction-static 0.15
--max-running-requests 16 --context-length 4096`; KV events `endpoint tcp://*:5567` (rank r publishes on 5567+r,
replay on 5577+r), captured as two streams (`rank0-*`, `rank1-*`) with the same scripted workload (40 unique prompts
in the eviction phase, which this configuration's larger pools did not evict). Each rank: its own sequence counter
from 0 (the startup `AllBlocksCleared` is batch 0, only in the replay buffer; live 1..28, no gaps), envelope
`[ts, events, attn_dp_rank]` with `attn_dp_rank` 0 on the first stream and 1 on the second, 29 `BlockStored`,
2 `AllBlocksCleared` (startup and `/flush_cache`), no removals. Same per-event keys as the single-rank capture.

## SGLang 0.5.21, HiCache write-through (`sglang-hicache/`, host venv, GPU 1)

Command: `run-sglang-host.sh sgl-hicache 1 8202 5587 5597 --mem-fraction-static 0.1 --max-total-tokens 4096
--max-running-requests 8 --context-length 2048 --enable-hierarchical-cache --hicache-ratio 8
--hicache-write-policy write_through` (a 4 k-token device pool under a 32 k-token pinned-host tier). Workload:
`shared` (8 sharers of a 512-token prefix), `repeat`, `evict` (12 unique 1024-token prompts, 2 concurrent), `recheck`
(the shared prompts again), `reset` (`/flush_cache`). Stream: 43 batches, seq 1..42 without gaps, 53 `BlockStored`
(31 `medium: "GPU"`, 22 `"CPU_PINNED"`), 11 `BlockRemoved` (all GPU), 2 `AllBlocksCleared`. Per-hash transitions
over the 864 distinct hashes: 640 x `Store GPU -> Store CPU_PINNED -> Remove GPU`, 128 x `Store GPU -> Store
CPU_PINNED`, 48 x `Store GPU -> Store CPU_PINNED -> Remove GPU -> Store GPU` and 48 x the same followed by a fresh
`Store GPU -> Store CPU_PINNED` after the flush. The 96 hashes of the shared prefix and its first suffixes therefore
show exactly `Store GPU -> Store CPU_PINNED -> Remove GPU -> Store GPU` (write-through backup, demotion under the
eviction phase with the host copy kept, load-back when the prefix was requested again; the recheck requests
reported `cached_tokens` 624, counting the host hit). A first run with `--hicache-ratio 2` (`run1-ratio2` beside the
original, not checked in) evicted the host copy too (`... -> Remove GPU -> Remove CPU_PINNED`), so the ratio
matters for reproducing the load-back.
