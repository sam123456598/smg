# Per-worker load series on the full-pool 8B fleet (b4943d69 gateway in inference-gateway mode, workers labelled weight_version=grpc:<port>)

Mooncake rows 0-1999 at speedup 3, 4 x Qwen3-8B (676 k-token pools, 474 k on GPU 3), warm engines, fresh labelled gateway per run. Series = servicer GetLoads polled per worker about once per second (num_running_reqs, num_waiting_reqs, token_usage = KV usage fraction) plus the gateway's smg_engine_{running,waiting}_requests / smg_engine_token_usage gauges (series-*.csv). The headless engines behind the Rust servicer expose no vLLM /metrics, so vllm:request_queue_time_seconds, vllm:request_prefill_time_seconds and the preemption counter are not available on this path; waiting/running/KV usage are the engine's own scheduler stats as the servicer reports them. The first cache_aware run after the relaunch (cold engines, 8b-cache_aware-run1-cold: goodput 6.7 req/s) is kept but not quoted.

## cache_aware: goodput 15.31 req/s, within-SLO 92.5 %, TTFT p50/p90/p99 156/440/864 ms, reuse 0.388, balance 1.63, series samples per worker 118 over 123 s
| worker | requests | uncached prompt tokens | TTFT p50 / p90 / p99 ms | cached/prompt | waiting max / p90 / mean | s with waiting>=50 | s with waiting>=10 | running max / p90 | KV usage max / p90 |
|---|---|---|---|---|---|---|---|---|---|
| grpc:20061 | 797 | 960278 | 139 / 250 / 410 | 0.82 | 0 / 0 / 0.0 | 0 | 0 | 12 / 4 | 0.11 / 0.04 |
| grpc:20062 | 483 | 2193879 | 148 / 444 / 933 | 0.29 | 4 / 0 / 0.1 | 0 | 0 | 17 / 12 | 0.28 / 0.18 |
| grpc:20063 | 364 | 2903726 | 246 / 495 / 858 | 0.09 | 2 / 0 / 0.0 | 0 | 0 | 28 / 23 | 0.40 / 0.31 |
| grpc:20064 | 306 | 3040354 | 265 / 607 / 919 | 0.06 | 2 / 0 / 0.1 | 0 | 0 | 30 / 25 | 0.64 / 0.52 |

## round_robin: goodput 14.29 req/s, within-SLO 85.7 %, TTFT p50/p90/p99 197/444/973 ms, reuse 0.392, balance 1.00, series samples per worker 112 over 121 s
| worker | requests | uncached prompt tokens | TTFT p50 / p90 / p99 ms | cached/prompt | waiting max / p90 / mean | s with waiting>=50 | s with waiting>=10 | running max / p90 | KV usage max / p90 |
|---|---|---|---|---|---|---|---|---|---|
| grpc:20061 | 487 | 2343331 | 198 / 466 / 813 | 0.40 | 1 / 0 / 0.0 | 0 | 0 | 14 / 12 | 0.22 / 0.19 |
| grpc:20062 | 487 | 2247906 | 192 / 456 / 900 | 0.39 | 1 / 0 / 0.0 | 0 | 0 | 22 / 14 | 0.34 / 0.22 |
| grpc:20063 | 488 | 2191388 | 200 / 411 / 1100 | 0.40 | 3 / 0 / 0.0 | 0 | 0 | 17 / 13 | 0.33 / 0.19 |
| grpc:20064 | 488 | 2259660 | 187 / 372 / 780 | 0.38 | 1 / 0 / 0.0 | 0 | 0 | 22 / 16 | 0.64 / 0.31 |

Reading. The hardware fleet does not form the mock's queue. Under cache_aware the load does concentrate exactly as in the
mock: grpc:20061 takes 797 requests but only 0.96 M uncached prompt tokens (82 % of its prompt tokens cached), while the
expected-wait fallback targets grpc:20063/20064 take 306-364 requests carrying 2.9-3.0 M uncached tokens each (6-9 % cached),
and their TTFT p50 is 246-265 ms against 139 ms on the affine worker. But the hot worker's waiting queue never exceeds 4
requests in any one-second sample (p90 0, mean 0.1), and no sample of any worker reaches 10, let alone 50: vLLM admits the
long uncached prompts straight into the running batch (running up to 30 on the hot worker; chunked prefill with a 16 384-token
step budget and max_num_seqs 128) and pays for them as longer steps, which shows up as the hot worker's TTFT p90 of 607 ms and
the fleet's ITL tail, not as queue wait. KV usage stays far from the ceiling (max 0.64 on the 474 k-token GPU-3 worker, 0.11 on
the affine worker). Round robin spreads the uncached tokens evenly (2.2-2.3 M per worker, 38-40 % cached each) with the same
near-empty queues. So the mock's hot worker (queue wait p90 4.2 s, 63 preemptions, 50+ queued) over-serialises admission
relative to this engine: the fixes on the mock side are admission of several prefills per step up to the token budget (the
batched-prefill sweep: 4 x 1024 in one pass, 16 x 1024 in ~200 ms) and no preemption at these KV levels.

Setup notes. Labels: the gateway was started with no static workers and `--enable-igw`, and the four workers were registered
with `POST /workers {"url":..., "worker_type":"regular", "connection_mode":"grpc", "runtime_type":"vllm", "labels":{"weight_version":"grpc:<port>"}}`
(`scripts/launch-gateway-labeled.sh`); without `--enable-igw`, dynamically registered workers were healthy but requests
answered `model_not_found`. The replay reports the label through `system_fingerprint`, so `per_worker_requests` and
`balance_max_over_mean` are now real (1.63 cache_aware, 1.00 round robin). Files: `8b-<policy>/summary.json`, `requests.csv`,
`series-<policy>.csv`, `fleet_series_analyze.py` output above.
