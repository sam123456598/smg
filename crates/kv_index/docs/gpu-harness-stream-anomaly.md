# Gateway HTTP path adds latency and buffers streamed tokens (observed 2026-10-05, the GB300 host)

Setup: 8 x vLLM 0.31 Qwen3-0.6B workers behind the Rust servicer (`vllm serve --grpc --servicer-impl rust`),
gateway `smg 1.11.0` (branch leap/gpu-harness @ 53b9ad54) with `--worker-urls grpc://...`, host load 140-500
from other workstreams on cores 0-63, everything of ours pinned to cores 72-143 (6 % busy while probing).

Evidence (2048-token unique prompts, 16 output tokens, `ignore_eos`, streaming):

| path | client | TTFT | streaming |
|---|---|---|---|
| servicer gRPC `Generate` direct, any of the 8 workers | python grpc | 15-41 ms | 1.4 ms/token, no bursts |
| plain vLLM HTTP instance (`vllm serve`, port 8102) | aiohttp | 29-35 ms | 1.4 ms/token |
| gateway, policy cache_aware | aiohttp | 55-165 ms | 1 of 4 responses delivered as one burst at the end |
| gateway, policy cache_aware / round_robin / random | python requests | 80-670 ms | most responses delivered as one burst (all 16 chunks within 0.5 ms, e2e == TTFT) |
| gateway, round_robin, `vllm bench serve` 16 req/s | aiohttp | median 1.3 s and 3.7 s (two runs), p99 3-6 s | median TPOT 0.00 ms (bursts) |
| gateway, cache_aware, `vllm bench serve` 16 req/s (first run of the session) | aiohttp | median 26 ms, p99 154 ms | TPOT 2.5 ms, streamed |

Gateway metrics over 9 probe requests: `smg_router_request_duration_seconds` mean 3.6 ms,
`smg_router_generation_duration_seconds` mean 7.6 ms, `smg_http_request_duration_seconds` mean 140 ms,
`smg_tokio_global_queue_depth` 0. The router's own accounting does not see the time; it is spent in the HTTP
layer around it, and the response body reaches the client only when the generation is complete in the bursty
cases. Client headers (`Accept-Encoding: identity`, `Connection: close`) and prompt content (English vs
pseudo-words) do not change it. Keeping all workers busy with background streams does not change it.

Not root-caused here (gateway code is outside this workstream). Reproduce with
`scripts/probe_stream.py http://127.0.0.1:30100 <tokenizer-dir> 2048 16 <seed>` against the fleet and
`scripts/probe_grpc.py 127.0.0.1:50051 <tokenizer-dir> 2048 16 3` against a worker.
Until it is fixed, gateway TTFT/TPOT numbers from this harness are not an engine-truth baseline for T9; the
direct-engine and direct-servicer numbers above are.

## Follow-up (same day): starvation ruled out, stall localized to the gateway ingress

Steps taken on the orchestrator's request, gateway and clients pinned to cores 64-71 (other users' light
processes there, 28 % busy; builds and engines on 72-143; host load average 100-140):

| test | result |
|---|---|
| gateway re-pinned to 64-71 (`taskset -a -cp`), client on 64-71, 4 probes | 191-580 ms TTFT, 3 of 4 bursty |
| per-thread scheduler statistics (`/proc/<pid>/task/*/schedstat`) across a 166 ms probe | threads ran 9 ms and waited **0 ms** for a CPU; the gateway is asleep, not starved; nice 5 (sandbox scope), no cgroup quota, no throttling |
| restart with `--runtime-worker-threads 8` on 64-71 | 131-146 ms, still bursty |
| round robin on 64-71 at load 100 | 109-310 ms, 5 of 6 bursty |
| keep-alive session vs fresh connection per request | no difference (76-425 ms either way) |
| 16 req/s burst of 160 requests, round robin | gateway processed 5.3 req/s at ~0 % CPU, TTFT p50 84 ms, p90 28.8 s |
| second gateway in front of the plain **HTTP** vLLM worker (`--worker-urls http://127.0.0.1:8102`, round robin) | 149-1210 ms TTFT, 2 of 5 bursty; the same client direct to that worker: 31-32 ms |
| thread wait channels sampled every 50 ms during a probe | one tokio worker in `epoll_pwait`, all others parked in futex; only the allocator-stats and metrics threads in `nanosleep` |

Timeline of one request (`strace --seccomp-bpf -f -tt -yy` on the gateway, cache_aware, 2048-token prompt,
files `strace-gateway-one-request-seccomp.txt` / `strace-client-timeline-seccomp.txt`):

```
03:04:35.153  client sends the request (new connection)
03:04:35.18-37.16  gateway only exchanges the periodic 44+53-byte frames with the 8 workers (health/load checks)
03:04:37.488  gateway's first recvfrom on the client socket (24 B, then 5665 B = the whole request)   <- 2.3 s late
03:04:37.513  HEADERS+DATA to worker 50052 (Generate)      03:04:37.538 first token back (25 ms, engine time)
03:04:37.538-37.562  17 chunks relayed, one writev each, 1.5 ms apart; client sees them at the same times
```

So in the traced run the stall sits entirely between the kernel receiving the request and the gateway reading
it, while the runtime's I/O driver is parked in `epoll_pwait`; in other runs the same kind of stall lands on
the response side and the chunks come out as one burst. The gateway's own request log (`latency=` in the
logging middleware) shows a further 65-112 ms inside the pipeline on the slow runs and 9.6 ms on a fast one.
It is independent of policy (cache_aware, round_robin, random), worker type (gRPC servicer or HTTP vLLM),
client (requests, aiohttp, curl, vllm bench), connection reuse, runtime worker count (8 or 72) and CPU
placement (64-71 or 72-143). Rate limiting is disabled (`max_concurrent_requests = -1`); the scheduler
middleware is off by default. Suspects for the gateway-path investigation: the HTTP ingress / connection
handling layers shared by both routers (server.rs acceptor, logging / metrics / request-id layers, body limit),
or a task that is only re-polled by the ~1 Hz worker health/load timers. The first cache_aware baseline of the
session (26 ms median at 16 req/s, streamed) shows the fast path exists; everything since then has shown the stall.

## Resolution (same day): per-request info logging written to a file stalls the stream path

Second factor suggested by the policy workstream, tested on the idle cores 64-71 with the same four 0.6B
workers (w0-w3), fresh gateway per condition, `probe_stream.py` 2048-token prompts, 16 output tokens:

| gateway condition | TTFT of 5-6 probes | streaming |
|---|---|---|
| A `--log-level info`, stdout+stderr to a file on $HOME (btrfs) | 130-343 ms | all 6 bursty |
| B `--log-level warn`, file on $HOME | 27-31 ms | smooth, 1.4 ms/token |
| C `--log-level info`, file on /tmp (btrfs) | 44-184 ms | 3 of 5 bursty |
| D `--log-level info`, stdout+stderr to /dev/null | 28-31 ms | smooth |
| E `--log-level warn` again | 28-30 ms | smooth |

16 req/s prefix-repetition baseline (512 requests, 16 prefixes, 1024+128 in, 64 out) on the same four workers:

| gateway | TTFT p50 / p90 / p99 (ms) | TPOT p50 / p99 (ms) | E2E p50 (ms) | zero-gap ITL fraction |
|---|---|---|---|---|
| warn, cache_aware | 18.4 / 22.4 / 28.1 | 1.49 / 1.93 | 113 | 0.3 % |
| warn, round_robin | 17.6 / 20.8 / 27.0 | 1.47 / 1.85 | 110 | 0.4 % |
| info, round_robin | 333 / 910 / 1744 | 1.44 / 22.6 | 463 | 61 % |

Finding: the per-request info log lines (`smg::request` / `smg::response` from the logging middleware, two per
request) stall the stream path whenever they are written to a regular file; the same lines to /dev/null do
not, and warn level does not. Formatting is not the cost, the file write is (both btrfs filesystems on this
host show it). This matches the 2.3 s gap before the gateway read the client's request in the strace timeline
and the idle runtime (the writer blocks a worker, so ready connections wait). Everything in the earlier
sections was measured with info-level logging to a file; the first cache_aware run of the session was the
only one that escaped it. Hand-off: gateway-path fix for the logging layer (non-blocking / buffered writer,
or per-request lines at debug) together with the policy agent's syscall profile. The harness now runs the
gateway at `--log-level warn` for every measurement (`launch-gateway.sh` default).

## Confirmation on the pushed head 2d37b25a (logging fix): info level no longer stalls on hardware

Gateway built from 2d37b25a, `--log-level info` with stdout+stderr to a file on $HOME (4283 lines written during the
run), round robin over the same four 8B workers (12 000-block pools), Mooncake rows 0-1999 at speedup 3 (~16.6 req/s):
goodput 14.26 req/s, within-SLO 85.6 %, TTFT mean/p50/p90/p99 246/198/445/982 ms, mean-ITL p99 213 ms, e2e p50 624 ms.
The warn-level runs of the same configuration gave 14.24 / 12.77 req/s goodput and TTFT p50 200-245 ms (`replay-kv12k-2d37b25a`),
so info-level logging to a file is now free of the stall; the harness keeps `GATEWAY_LOG_LEVEL=warn` as the default
only to keep the logs small.
