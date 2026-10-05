# GPU harness: real engines on devgpu044 (GB300)

Companion to the KV-router leap contract (`kv-router-leap.md`, 3.T4/T9 and 4.5). Everything here ran on
devgpu044 (arm64 Grace, 4x NVIDIA GB300 284 GB, driver 580.126.20, podman 5.8.5 rootless, no
nvidia-container-toolkit, no root). Scripts, fixtures and results live under `~/smg-perf/gpu/`; the
captured streams are checked in under `crates/engine_servicer/tests/fixtures/captured/`.

## 1. GPU access without root

No CDI spec exists on the host and nothing can be installed, so containers get the GPUs the manual way
(`scripts/podman-gpu.sh`, path (a) of the three candidates; it worked first time, so (b) a bare venv with the
aarch64 vLLM wheel and (c) the SGLang image were not needed as fallbacks):

- device nodes passed with `--device`: `/dev/nvidiactl`, `/dev/nvidia-uvm`, `/dev/nvidia-uvm-tools`,
  `/dev/nvidia-caps-imex-channels/channel0` and `/dev/nvidia<N>` per GPU (all `crw-rw-rw-`; the
  `/dev/nvidia-caps/*` nodes are root-only and are not needed);
- the driver's user-space libraries copied once from `/usr/lib64` into `~/smg-perf/gpu/nvlibs`
  (`scripts/stage-nvlibs.sh`: `libcuda`, `libnvidia-ml`, `libnvidia-ptxjitcompiler`, `libnvidia-nvvm`,
  `libnvidia-gpucomp`, `libnvidia-cfg`, `libnvidia-allocator`, `libcudadebugger`, `libnvidia-nscq`, with their
  SONAME symlinks, plus `nvidia-smi`) and bind-mounted read-only at `/usr/local/nvidia/lib64`, which the
  `nvidia/cuda` base images already have on `LD_LIBRARY_PATH`;
- `--cpuset-cpus=72-143` (cores 0-63 are the measurement partition of the other workstreams), `--network host`,
  `--ipc host` (do not combine with `--shm-size`), `--log-driver k8s-file` (the rootless default is journald,
  which the user cannot read).

Check: `podman-gpu.sh 0 --rm --entrypoint python3 docker.io/vllm/vllm-openai:latest -c 'import torch; print(torch.cuda.is_available())'`
gives torch 2.13.0+cu130, vLLM 0.31.0, `NVIDIA GB300`.

Pitfalls: `podman run` occasionally fails with `crun: sd-bus call: Remote peer disconnected` (rootless cgroup
setup through the user bus); re-running works. Pulls and pip need `http_proxy=http://fwdproxy:8080`.
`HF_HUB_OFFLINE=1` with the HF cache mounted at `/root/.cache/huggingface` keeps the engines from reaching out
(weights copied from other users' caches into `~/smg-perf/gpu/models/hub`).

## 2. Images and versions

| component | version |
|---|---|
| `docker.io/vllm/vllm-openai:latest` (arm64) | vLLM 0.31.0, torch 2.13.0+cu130, Python 3.12, grpcio 1.84, msgspec 0.22, pyzmq 27.2 |
| `docker.io/lmsysorg/sglang:latest` (arm64, 34 GB) | SGLang 0.5.21 |
| `localhost/smg-vllm:local` | the vLLM image plus `smg-grpc-proto`, `smg-grpc-servicer` and the `smg` binding wheel from this worktree (`image-ctx/Containerfile`, `scripts/build-image.sh`) |
| gateway | `smg 1.11.0` built from this branch into `~/.cargo/target-leap-gpu-harness/release/smg` |

The binding wheel is abi3 (`pyo3 abi3-py38`), built on the host with
`maturin build --release --features vendored-openssl --compatibility linux` (host glibc 2.34 is older than the
image's, so the wheel loads inside the container).

## 3. KV-event capture (deliverable 1)

`scripts/capture_kv_events.py` connects a SUB socket to the engine's publisher (optionally fetching the replay
buffer from seq 0 first) and appends one JSON line per batch with the three raw frames (`topic`, `seq`,
base64 msgpack payload). `scripts/summarize_kv_events.py` prints the field census; `scripts/workload_kv.py` drives
the scripted workload (token-id prompts, shared prefixes, concurrent sharers, 120 unique 1 k prompts through a
9 k-token cache, reset) and records the engine's own `cached_tokens` per request.

Engines were sized to evict: vLLM `--kv-cache-memory-bytes 1073741824` (585 blocks of 16), SGLang
`--max-total-tokens 9216 --page-size 16`. The reset is `POST /reset_prefix_cache` on vLLM (needs
`VLLM_SERVER_DEV_MODE=1`; there is no CLI flag for it in 0.31) and `POST /flush_cache` on SGLang. SGLang's
default attention backend on GB300 forces page size 64, so `--attention-backend triton` is used to keep
`--page-size 16` equal to vLLM's block size.

Commands:

```
scripts/run-vllm.sh vllm-smoke 0 8101 5557 Qwen/Qwen3-0.6B --max-model-len 4096 --max-num-seqs 16 \
    --kv-cache-memory-bytes 1073741824 --enforce-eager --gpu-memory-utilization 0.3
scripts/run-sglang.sh sglang-smoke 1 8201 5567 Qwen/Qwen3-0.6B --mem-fraction-static 0.3 \
    --max-total-tokens 9216 --max-running-requests 16 --context-length 4096 --attention-backend triton
venv/bin/python scripts/capture_kv_events.py --endpoint tcp://127.0.0.1:5557 --topic kv-events \
    --replay tcp://127.0.0.1:5558 --from-seq 0 --out fixtures/vllm/capture.jsonl --stop-file fixtures/vllm/.stop &
venv/bin/python scripts/workload_kv.py --base http://127.0.0.1:8101 --model Qwen/Qwen3-0.6B --out fixtures/vllm/workload.jsonl
touch fixtures/vllm/.stop; venv/bin/python scripts/summarize_kv_events.py fixtures/vllm/capture.jsonl
```

What the real streams contain (full census in the fixtures' `README.md` and `*-summary.json`):

| | vLLM 0.31.0 | SGLang 0.5.21 |
|---|---|---|
| envelope | `[ts, events, dp_rank=0]`, seq from 0, no gaps | same, seq from 0 (first batch = startup `AllBlocksCleared`, only in the replay buffer) |
| `BlockStored` keys | `block_hashes, parent_block_hash, token_ids, block_size, lora_id, medium, lora_name, extra_keys, group_idx, kv_cache_spec_kind` | `block_hashes, parent_block_hash, token_ids, block_size, lora_id, medium` |
| `BlockRemoved` keys | `block_hashes, medium, group_idx` | `block_hashes, medium` |
| `medium` | `"GPU"` always | `"GPU"` always (no HiCache) |
| `group_idx` / `kv_cache_spec_kind` | 0 / `full_attention` | absent |
| `locality`, `ownership`, `session_id`, `cache_salt`, `kv_cache_spec_sliding_window` | absent (omitted when None) | absent |
| `extra_keys` | list of nulls aligned with `block_hashes` | absent |
| hashes | u64, never negative | i64, half negative |
| removals | one event per hash, up to 256 per batch | one event per node, all its pages listed |
| `cached_tokens` | `prompt_tokens_details` always present; 624 for a fully cached 640-token prompt | `prompt_tokens_details` omitted when 0; concurrent sharers of a new prefix all miss |

## 4. Fleet through the Rust servicer (deliverable 2)

Image `localhost/smg-vllm:local` = the vLLM image plus `smg-grpc-proto` (built from `crates/grpc_client/python`;
its `smg_grpc_proto/proto` is a symlink to `../../proto`, which must be dereferenced in the build context),
`smg-grpc-servicer` (`grpc_servicer/`) and the `smg` binding wheel. Build the wheel on the host first
(`scripts/build-wheel.sh`), then `scripts/build-image.sh` (`podman build --network host` so pip reaches the proxy).

Eight workers, two per GPU (`scripts/launch-fleet.sh Qwen/Qwen3-0.6B 8 --max-model-len 4096 --max-num-seqs 64
--gpu-memory-utilization 0.4`; each is `scripts/run-vllm-grpc.sh`: `vllm serve <model> --grpc --port <p>
--servicer-impl rust --kv-events-config {...zmq...} --prefix-caching-hash-algo sha256_cbor --block-size 16
--enable-prompt-tokens-details`). Startup: ~2 min (torch.compile 37 s, CUDA graphs) until the servicer logs
`Engine connected; the servicer is SERVING` and `SubscribeKvEvents: connected to ZMQ endpoint`.

Gateway (`scripts/launch-gateway.sh <policy> 8 <tokenizer-dir>`): `smg launch --backend vllm --worker-urls
grpc://127.0.0.1:<p>... --policy cache_aware --model-path <tokenizer-dir> --port 30100 --prometheus-port 29100`.
The connection mode comes from the `grpc://` scheme; with `cache_aware` the gateway creates the KV event monitor
and subscribes to every gRPC worker by itself (log lines `Starting KV event subscription`, `KV event stream
connected worker_url=... start_seq=0`, one per worker; the model's block size is learned from the first event).
`GET /workers` lists the eight workers as healthy with `connection_mode: grpc`.

T4 ground truth, by hand (`scripts/workload_kv.py --text --stream` through the gateway at `--log-level debug`,
`scripts/t4_compare.py` joins the engine's `prompt_tokens_details.cached_tokens` with the gateway's per-request
decision lines; `~/smg-perf/gpu/results/t4-cache_aware-*`):

| phase | routing decision (gateway debug log) | engine `cached_tokens` | router-implied overlap |
|---|---|---|---|
| shared, request 0 (new 512-token prefix) | `no overlap, expected-wait fallback` -> worker 50051 | 0 | 0 |
| shared, requests 1-7 (same prefix, new suffixes) | `overlap match branch=event_hit` -> 50051 | 512 each | 512 |
| repeat (the same 8 prompts again) | `event_hit` -> 50051 | 624 each (640-token prompt, last block recomputed) | 624 |
| burst (8 concurrent prompts on a new prefix) | no overlap yet -> spread over 8 workers | 0 x7, 512 once (two landed on 50054, the second hit the in-flight prefill) | n/a |
| recheck (shared prompts again) | `event_hit` -> 50051 | 624 each | 624 |

Agreement 24/24 on the sequential phases: every routing decision that claimed overlap landed on a worker where
the engine then reported exactly the block-floored prefix. The gateway exposes the engine's count only on
streaming completions (`stream_options.include_usage`) and chat completions; non-streaming `/v1/completions`
responses omit `prompt_tokens_details` on this path (the servicer does send `cached_tokens`). There is no
per-request overlap number in metrics or headers for gRPC workers (`x-smg-routed-worker-id` is HTTP-only;
`smg_cache_aware_match_ratio` is recorded by the tree path, not the event path), so the comparison needs the
debug log.

Qwen3-8B fleet: four workers, one per GPU, run **from the host venv instead of podman** (`scripts/run-vllm-grpc-host.sh`;
podman wedged for good mid-session, see pitfalls). Results in `gpu-harness-8b-baseline.md`: warm run at 16 req/s,
cache_aware TTFT p50/p90/p99 58/90/369 ms and TPOT 4.5 ms with 15.1 req/s delivered, round robin 73 ms/5.0 s/9.8 s
with 12.6 req/s delivered; one plain 8B engine at one worker's share (4 req/s) 112/278/689 ms.

Hash-check drill (`gpu-harness-hash-drill.md`, `scripts/hash-drill.sh`): four 0.6B workers through the loop-head
servicer (f4dc134b wheel) with `SMG_KV_EVENT_HASH_CHECK=vllm-sha256-cbor`; the worker started with
`PYTHONHASHSEED=12345` logged `hash_checked 964, hash_mismatch 964`, the other three `962 / 0`, nothing dropped,
and the workload through the loop-head gateway completed 256/256 with 240 prefix hits.

SGLang per-rank relay and HiCache fixtures (`crates/engine_servicer/tests/fixtures/captured/sglang-dp2`,
`sglang-hicache`): `--dp-size 2` publishes one stream per rank (ports 5567+r, replay 5577+r, each sequence from 0,
`attn_dp_rank` 0/1 in the envelope); HiCache write-through with `--hicache-ratio 8` over a 4 k-token device pool
gives the `Store GPU -> Store CPU_PINNED -> Remove GPU -> Store GPU` cycle for the shared prefix (96 hashes),
while `--hicache-ratio 2` evicts the host copy as well. The replay-gap drill (engine restart mid-run) was left for
later as instructed.

### Container-free path (used for everything after the podman wedge)

`~/smg-perf/gpu/venv-vllm`: `pip install vllm==0.31.0 ninja` plus `crates/grpc_client/python`, `grpc_servicer/` and the
`smg` wheel. Two things the wheel needs on this host: `LD_PRELOAD=<venv>/nvidia/cu13/lib/libcublas.so.13` (the
aarch64 extension `vllm/_C_stable_libtorch.abi3.so` references `cublasHgemm` without a `DT_NEEDED` on cuBLAS, so the
import fails with an undefined symbol until it is preloaded) and `ninja` + `/usr/local/cuda/bin` on `PATH` for
FlashInfer's JIT (first start of each model takes 3-6 min of kernel builds, cached under `~/.cache/flashinfer`
afterwards; a cold engine also autotunes on the first benchmark, so quote the second run). GPUs are selected with
`CUDA_VISIBLE_DEVICES`; everything runs under `taskset -c 72-143`. `~/smg-perf/gpu/venv-sglang` holds
`sglang[all]==0.5.21` the same way (`scripts/run-sglang-host.sh`).

## 5. Baseline (deliverable 3)

Workload: prefix repetition, 16 prefixes x 32 prompts, 1024 prefix + 128 suffix tokens, 64 output tokens,
Poisson 16 req/s, concurrency 64, 512 requests (`scripts/run-bench.sh` = `vllm bench serve --dataset-name
prefix_repetition` from the vLLM image; `scripts/bench_prefix.py` = the same shape without a container).

| run | client | TTFT p50 / p90 / p99 (ms) | TPOT p50 / p99 (ms) | E2E p50 (ms) | req/s |
|---|---|---|---|---|---|
| gateway cache_aware (first run of the session) | vllm bench | 26.4 / 42.8 / 154.3 | 2.51 / 5.39 | 187 | 15.9 |
| gateway round_robin | vllm bench | 1324 / 2641 / 3084 | 0.00 / 3.03 | 1331 | 15.3 |
| gateway round_robin, repeat | vllm bench | 3733 / 5785 / 6403 | 0.00 / 4.69 | 3750 | 13.5 |
| gateway round_robin | bench_prefix.py | 1567 / 3291 / 4180 | 0.04 / 8.4 | 1617 | 16.2 |
| gateway random | bench_prefix.py | 1222 / 2730 / 3723 | 0.02 / 4.6 | 1352 | 14.2 (72 x HTTP 500) |
| gateway cache_aware (later run) | bench_prefix.py | 732 / 3082 / 3698 | 0.04 / 4.1 | 801 | 16.0 (28 x HTTP 500) |
| one plain vLLM HTTP instance, same 16 req/s | bench_prefix.py | 28.5 / 44.1 / 56.9 | 1.99 / 4.06 | 155 | 17.0 |
| gateway cache_aware, **warn** level, 4 workers w0-w3, cores 64-71 | bench_prefix.py | 18.4 / 22.4 / 28.1 | 1.49 / 1.93 | 113 | 17.0 |
| gateway round_robin, **warn** level, 4 workers | bench_prefix.py | 17.6 / 20.8 / 27.0 | 1.47 / 1.85 | 110 | 17.0 |
| gateway round_robin, info level, same 4 workers | bench_prefix.py | 333 / 910 / 1744 | 1.44 / 22.6 | 463 | 16.9 |
| one plain vLLM HTTP instance, all-unique prompts, 2 req/s | vllm bench | 23.5 / - / 33.5 | 1.68 / 2.08 | 129 | 2.0 |

Resolved the same day: the stall is the gateway's per-request info logging written to a file (warn level or stdout to /dev/null gives 18-31 ms TTFT and smooth streaming on the same workers; table in the anomaly note). The harness runs the gateway at `--log-level warn` for every measurement; the info-level rows above are kept as the record of that defect. Starvation was tested and ruled out first (gateway and clients on the idle cores 64-71, scheduler wait time 0 ms, 8 or 72 runtime workers, round robin and cache_aware, HTTP and gRPC workers all show the same stall; details and a syscall timeline in `~/smg-perf/gpu/results/gateway-stream-anomaly.md`). The gateway and the load clients now run on cores 64-71 for every GPU-side run (`launch-gateway.sh`, `GATEWAY_CPUS`), engines and builds on 72-143. Read with that note: the direct gRPC path to any of the eight
servicers answers a 2048-token prompt in 15-41 ms and streams at 1.4 ms/token, while the gateway's HTTP path
adds 30-600 ms and often delivers the whole response as one burst (`smg_router_request_duration_seconds` mean
3.6 ms vs `smg_http_request_duration_seconds` mean 140 ms over the same requests). The first cache_aware run is
the only gateway run that streamed normally; the round-robin runs never did. So the only numbers above that
qualify as an engine-truth reference for T9 are the direct-engine rows and the direct-servicer probes; the
gateway rows record the state of the HTTP path on this branch and must be re-taken once it is fixed. The HTTP
500s are `Tokenizer not found for model` during the first seconds after the gateway reports its workers
healthy: wait for the `Tokenizer '<model>' ... registered` log line before loading it.

## 6. Pitfalls collected

- Keep worker ports below 32768: `net.ipv4.ip_local_port_range` is 32768-65535 here, and two servicers failed
  with `failed to bind 0.0.0.0:50056: Address already in use` because an outgoing connection of another
  process already held the port. The fleet scripts default to 20051+i (gRPC), 21051+i (handshake), 5600+2i (ZMQ).
- `pkill -f` matching your own command line kills your shell; use a pid file (`launch-gateway.sh` + `gateway.pid`).
- Rootless podman wedges under host load: `podman ps`/`run`/`rm` hung for minutes while the containers kept
  running; `timeout` on an attached `podman run` leaves the container running. Run benchmarks detached with a
  log file, or without containers (`bench_prefix.py`, `probe_grpc.py` from the venv with `grpcio` and the local
  `smg-grpc-proto`).
- Each container has its own torch.compile cache (`/root/.cache/vllm`); a ninth instance recompiled for 285 s
  under load. Mount a shared cache directory if startup time matters.
- The gateway rejects token-id prompts (`prompt: data did not match any variant of untagged enum StringOrArray`);
  send text and size it with the tokenizer (`workload_kv.py --text`).
- SGLang omits `prompt_tokens_details` when nothing was cached; vLLM always sends it (0).
- `nvidia-smi topo -m` puts GPUs 0/1 on cores 0-17 and GPUs 2/3 on 18-33; our cpuset 72-143 is remote to all
  four, which did not matter for these runs (SM utilisation stayed near zero).
- Rootless podman can wedge for the rest of a session: a killed `podman stop`/`podman rm` leaves a zombie whose
  last thread sits in `ovl_sync_fs`/`wb_wait_for_completion` (unmounting the container's overlay on btrfs) while
  still owning `overlay-layers/layers.lock`; every later podman command blocks on that lock (`/proc/locks` shows
  the holder, `lslocks` does not). Nothing short of the writeback finishing clears it. Containers keep running;
  their processes can be killed by pid. This is why the second half of the session ran engines from the host venv.
- `pgrep -f`/`pkill -f` with a pattern that also appears in your own command line kills your shell (twice here);
  bracket one character of the pattern (`'vllm serve Qwen/Qwen3-0.6[B]'`) or use pid files.
- Do not set `PYTHONHASHSEED` on vLLM workers unless every worker and the relay's hash check agree on it: vLLM
  seeds `NONE_HASH` with its verbatim value, so a worker with a different value publishes different hashes for
  the same tokens (that is exactly what the hash-check drill exploits).
- A single-engine reference needs KV room for the benchmark's concurrency: at `--gpu-memory-utilization 0.1` an
  8B instance holds about 80 k tokens, which 64 in-flight requests of 1216 tokens exceed; its tails were queueing,
  not compute.
- `vllm serve` inside a container and from the host do not share the torch.compile / FlashInfer caches; warm the
  one you measure.
- Never let two vLLM engines profile on the same GPU at the same time: the profiler reads device-wide free memory and
  books another process's allocations made during its window as its own non-torch memory. Two engines started three
  seconds apart on GPU 0 came up with 170 k and 621 k-token pools instead of 676 k and 754 k. Start the engines of one GPU
  strictly one after the other (wait for `the servicer is SERVING`); different GPUs can start in parallel.
- Do not edit a shell script while a run of it is still executing: bash reads the file incrementally and the running
  instance picks up the shifted bytes (a series driver died with a syntax error after its last run). Copy, then edit.
- The gateway polls `GetLoads` every `--load-monitor-interval` seconds (10 by default); the `smg_engine_*` gauges and the
  expected-wait inputs are that stale. A per-second series must read the servicer directly (`fleet_series.py` does).
- The e69487f8 relay serves a cursor-0 subscriber the engine's whole state only while its history still starts at
  sequence 0 (10,000 batches / 256 MiB by default, two or three replays' worth); after that a fresh gateway on warm
  engines learns nothing the engines already hold and routes on `event_miss` (99 % of selections in the branch census
  of `gpu-harness-e69487f8.md` section 4). Run the fleet with `SMG_KV_EVENT_HISTORY_BATCHES=400000
  SMG_KV_EVENT_HISTORY_BYTES=4294967296` (`scripts/relaunch-8b.sh`) or relaunch the engines before each series, and
  check the servicer log for `serving from history cursor=0 batches=N` on every fresh gateway
  (`RUST_LOG=info,engine_servicer::kv_events=debug`).
- A host reboot takes every engine and gateway with it; the pid files stay behind. Check `kill -0 $(cat logs/host-*.pid)`
  before trusting section 10, and check `uptime`.

## 7. Loop-head recipe (what the publication run repeats)

Binaries: `smg` and `replay` built from the loop-head worktree (`/tmp/wt-leap-gpu-harness-head`, f4dc134b at the time of
writing; rebuild from the SHA the orchestrator names) into `~/.cargo/target-leap-gpu-harness-head/release/`, and the `smg`
binding wheel from the same tree (`scripts/build-head.sh`, maturin `--compatibility linux`) installed into both host venvs
(`venv-vllm`, `venv-sglang`) together with `crates/grpc_client/python` and `grpc_servicer/` from that tree.

1. Workers, cores 72-143, one GPU each, pid files under `logs/host-<name>.pid`:
   `scripts/run-vllm-grpc-host.sh 8b-w$i $i $((20061+i)) $((5730+2*i)) Qwen/Qwen3-8B --max-model-len 40960 --max-num-seqs 128 --gpu-memory-utilization 0.4`
   (0.3 on a GPU that also carries the reference instance); the servicer logs `Engine connected; the servicer is SERVING`
   (2-6 min on a cold FlashInfer cache, ~70 s warm). SGLang: `scripts/run-sglang-grpc-host.sh <name> <gpu> <port> <zmq> <replay> [flags]`
   (the servicer listens on `--port` = `<port>+1000`). Keep `PYTHONHASHSEED` unset. Never kill by name; `kill $(cat logs/host-<name>.pid)`.
2. Gateway, cores 64-71: `SMG_BIN=<head smg> WORKER_URLS="grpc://127.0.0.1:20061 ..." scripts/launch-gateway.sh <policy> 4 <tokenizer-dir> --reasoning-parser passthrough --health-check-timeout-secs 30 --health-check-interval-secs 20 --health-failure-threshold 5`
   (`GATEWAY_LOG_LEVEL=warn` default, never info to a file; `GATEWAY_BACKEND=sglang` for SGLang workers; `GATEWAY_PORT`/`PROM_PORT` to run a second one).
   Before any load, wait until `/workers` shows every worker healthy **and** a dozen chat requests have returned 200 (the
   tokenizer registration finishes after the health flip; early requests get `Tokenizer not found` 500s).
   `--reasoning-parser passthrough` because Qwen3 thinks by default and the replay counts only `delta.content`.
3. Replay (client on 64-71): `replay --trace ~/dynamo/lib/kv-router/traces/mooncake_trace.jsonl --gateway http://127.0.0.1:30100 --model Qwen/Qwen3-8B --speedup 3 --skip 0 --limit 2000 --out results/replay/<label> --label <label>`;
   three runs per policy, 10 s apart; `summary.json` carries the table columns. Results: `gpu-harness-replay-8b.md`
   (cache_aware mean of 3: goodput 14.48 req/s, within-SLO 87.8 %, TTFT p50 179 ms; round robin 13.88 req/s, 83.6 %, 213 ms;
   prefix reuse 0.39 under both because four 97 GB pools hold the whole working set). 50 rows over 40 960 tokens are rejected (`http-400`).
4. HiCache check against the loop-head gateway: `gpu-harness-hicache-routing.md` (recheck routed to the demoted worker with
   `event_hit`, `cached_tokens` 624, after its prefix blocks went `Remove GPU` with the host copy kept).

5. Engine calibration for the mock's `--timing` mode: `gpu-harness-calibration.md` / `.json` (`scripts/calibrate_engine.py`
   against one warm worker over gRPC: prefill TTFC minus the 1-token overhead at 128..16 384 tokens, steady-window decode
   ITL at 8..128 concurrent streams with 512/2048-token prompts, capacity 676 128 tokens at the fleet setting; prefill is a
   plateau-and-steps curve on this stack, so use the points, not only the quadratic).
6. Restricted pool (`--num-gpu-blocks-override 12000`, 192 k tokens per worker = fleet 7 % of the rows 0-1999 working set):
   `gpu-harness-replay-8b-kv12k.md`. Both policies lose half their goodput; cache_aware collapses run over run when the
   gateway is not restarted between runs (index credit outlives the engine's lazy block removals), round robin does not.

7. Loop head 2d37b25a (logging fix, hardened run index): restricted-pool replay `gpu-harness-replay-8b-kv12k-2d37b25a.md`
   (round robin goodput 12.8-14.2 req/s vs cache_aware 5.0-8.6 at the same prefix reuse: with pools at 7 % of the working
   set the credited worker queues while its blocks are already evicted; the full pool is where cache_aware leads), and the
   info-level check in `gpu-harness-stream-anomaly.md` (info logging to a file at 16 req/s: goodput 14.26 req/s, TTFT p50
   198 ms, same as warn, so the stall is fixed on hardware; `GATEWAY_LOG_LEVEL=warn` stays the harness default only for log size).
8. Mock calibration additions: `gpu-harness-scheduler-settings.json` (max_num_seqs 128, max_num_batched_tokens 16384,
   chunked prefill on, long_prefill_token_threshold 0, block 16, pools 42 258 / 29 669 / 12 000 blocks) and
   `gpu-harness-prefill-batch-sweep.json` (over the gRPC servicer a 4x1024 burst prefills in one ~78 ms pass, 8x1024 in
   ~113 ms, 16x1024 in ~200 ms, 8x4096 in two passes ~335 ms; the HTTP front end admits one request per engine step and
   must not be used to calibrate batching).

9. HiCache on 2d37b25a (`gpu-harness-hicache-routing-2d37b25a.md`): with the HiCache worker's prefix blocks demoted
   (`Remove GPU`, host copies kept) and a healthy empty worker added through `POST /workers` before the recheck, the gateway
   routed all eight recheck requests to the demoted worker with `event_hit` and the engine answered `cached_tokens` 624
   (load-back visible in its stream). When both workers hold the prefix, the event path prefers the device copy.
   Servicer parity: one cache_aware and one round-robin replay on the fleet running the 2d37b25a servicer wheel gave
   goodput 8.5 and 8.3 req/s at the restricted pool, inside the spread of the f4dc134b-servicer runs.

10. Loop head e69487f8 (`gpu-harness-e69487f8.md`): the vLLM servicer's new load fields confirmed live on the 8B fleet
   (`num_waiting_uncached_tokens` up to 472 k under a 96 x 8,192-token burst, `gen_throughput`, `cache_hit_rate`,
   `num_used_tokens`, `utilization`; fixture `captured/vllm-8b-getloads/`); the gateway reads them every
   `--load-monitor-interval` (10 s). Labelled replays: round robin unchanged (14.55 req/s, 87.1 %, 184/379/765 ms);
   `cache_aware` with the live fields forms the mock's hot worker (one fallback target at KV 1.00 with 17-38 waiting), and
   fresh gateways on warm engines went blind once the relay history window rolled (section 4). Clean series on fresh
   engines with a session-long history: `cache_aware` 12.67 req/s / 79.0 % / TTFT p50-p90-p99 192-2159-4660 ms at the
   default 10 s load poll, 14.73 / 90.9 % / 161-418-957 at `--load-monitor-interval 1` (round robin 14.3-14.6 / 86-87 %). Drills: an engine restart is detected at the first reconnect
   (`out_of_range` resync at +20 s, before the engine is serving) instead of at the first batch; a gateway restart gets the
   fleet's state from history in the first second with no resync and hit fraction 1.00 throughout.

## 10. What is running / where things are

As of the end of the e69487f8 round (after the 13:45 host reboot; everything relaunched on the e69487f8 servicer wheel from
`wheels-head5`, binaries in `~/.cargo/target-leap-gpu-harness-head/release/`): host processes `8b-w0..w3` (gRPC 20061-20064,
full 676 k / 474 k-token pools), `drill-w0..w3` (20071-20074, `RUST_LOG=info,engine_servicer::kv_events=debug`), `http-8b`
(:8104), `sgl-hicache` (:8202) and `sgl-dp2` (:8201, two visible GPUs); logs `logs/host-<name>.log`, pids
`logs/host-<name>.pid`; the last labelled gateway may still be up on :30100 (`logs/gateway.pid`), the drill gateway on
:30400 (`logs/gateway-drill.pid`). Results of the round: `gpu-harness-e69487f8.md`.

As of the end of round 4 (superseded by the reboot): host processes `8b-w0..w3` on the 2d37b25a servicer wheel (kept up for the policy comparison; 40 960 context, **12 000-block pools** for the restricted setting, drop `--num-gpu-blocks-override` for the full pool) (gRPC 20061-20064, logs `logs/host-8b-w*.log`, pids `logs/host-8b-w*.pid`), `http-8b` (:8104), `drill-w0..w3` (gRPC 20071-20074), `sgl-dp2` (:8201), `sgl-hicache` (:8202); the podman containers were killed by pid and podman itself is still wedged. `kill $(cat logs/host-*.pid)` stops the host processes.

- `~/smg-perf/gpu/scripts`: everything above; `fixtures/{vllm,sglang}`: raw captures and summaries;
  `results/`: bench JSONs, T4 table, anomaly note; `logs/`: container and gateway logs; `models/hub`: copies
  of Qwen3-0.6B and Qwen3-8B; `wheels/`: the binding wheel; `image-ctx/`: the image build context.
- Containers `vllm-w0..w7` (fleet), `vllm-http` (plain HTTP instance on GPU 0, port 8102) and the gateway
  (`logs/gateway.pid`) were left running for the next workstream; `podman rm -f vllm-w0 ... vllm-http` and
  `kill $(cat logs/gateway.pid)` stop them.
