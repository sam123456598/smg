# T9: the mock engine against the GB300 fleet (2026-10-05)

Goal T9 of the leap contract asks the simulation (the `mock-worker` realistic engine driven by `crates/replay`)
and the hardware runs to agree on direction and within 15% on magnitude for the T4/T5 metrics. This is the
first complete comparison: same input (Mooncake rows 0-1999), same replayer, same gateway family, both fleet
settings the hardware was measured in, three runs per policy on the mock against the hardware tables.

## Method

- Input: `~/dynamo/lib/kv-router/traces/mooncake_trace.jsonl` rows 0-1999 at `--speedup 3` (trace span 342 s,
  about 117 s per run), `crates/replay` with 480 words per block and 512 max output tokens, streaming chat
  completions with `include_usage`; 50 rows exceed the 40,960-token context and are rejected by the gateway in
  every run on both sides (1,950 served / 50 errors everywhere). SLO: 500 ms TTFT and 50 ms mean ITL per request.
- Gateway: loop head 35e06a3e plus the allocator-defaults commit (2684a14d on `perf/kv-router-leap`), inference-
  gateway mode, `--log-level warn`, default runtime threads, policy `cache_aware` or `round_robin`, four gRPC
  workers registered with `weight_version=grpc:<port>` so the per-worker split is real.
- Mock fleet (`mock-worker --engine realistic`): four workers, block 16, context 40,960, `--max-running 128`,
  `--max-batched-tokens 16384` (the fleet's vLLM scheduler settings: `max_num_seqs` 128, `max_num_batched_tokens`
  16,384, chunked prefill), reserve-full-input admission (vLLM's `scheduler_reserve_full_isl`, the mock's default
  since 35e06a3e), LIFO preemption, pass-end KV-event visibility, `--loads-like vllm` (the load report carries only
  what the vLLM servicer reports: running, waiting, KV share and maxima, so the gateway's expected-wait score
  falls back to its defaults exactly as it does on the fleet).
- Mock timing `--timing fit:~/smg-perf/replay/fit-gb300-v2.json`, calibrated on one warm Qwen3-8B worker over the
  servicer's gRPC (`~/smg-perf/gpu/results/calibration/`): prefill from the single-request time-to-first-chunk
  point table (1 to 16,384 tokens; 23.8 ms at 1 token, ~75 ms at 1,024, 105.7 at 8,192, 211 at 16,384,
  interpolated) with the batched pass form 36.1 ms + 0.0103 ms per token for passes that prefill several requests
  (a pass never costs less than the table value of its largest request); decode step 3.76 + 15.2u - 2.9u^2 ms
  over KV utilisation u; capacity 676,128 tokens per worker from the fleet's pool.
- Scenarios: **full pool** (676,128 KV tokens per worker) and **restricted pool** (`--kv-blocks 12000` x 16 =
  192,000 tokens per worker, 7% of the rows' working set).
- Hardware references: full pool = the labelled fleet-series runs on the b4943d69 gateway, one run per policy,
  `~/smg-perf/gpu/results/replay-8b-fleet-series/8b-cache_aware/summary.json` and
  `.../8b-round_robin/summary.json` (README there has the per-worker series); restricted pool = the means of 3
  on the 2d37b25a gateway in `~/smg-perf/gpu/results/replay-kv12k-2d37b25a/8b-replay-kv12k-2d37b25a-table.md`.
  Both fleets: 4 x vLLM 0.31 Qwen3-8B through the Rust servicer, KV events on, warm engines.
- Measurement: one run = fresh mock fleet and fresh gateway (`~/smg-perf/replay/run.sh`), cores 0-63 under
  `/tmp/leap-measure.lock` (one hold per run, owner note written), driver `~/smg-perf/replay/fit-compare-t9.sh`,
  tables `fit-table.py`, ratios `t9-ratios.py`. Two of the twelve runs could not get the lock within 15 minutes
  and ran unlocked on cores 64-71 (gateway and client) / 72-143 (fleet) shared with the running soak; they are
  labelled `unlocked` below and kept in the means (their values sit inside the spread of the locked runs).
- Ratio = mock mean of 3 / hardware; "within 15%" counts ratios in 0.85-1.15; the direction column is the sign of
  cache_aware minus round_robin on the mock and on the hardware.

## Full pool, 676,128 KV tokens per worker

| metric | cache_aware mock | hardware | ratio | round_robin mock | hardware | ratio | direction (ca-rr) mock / hw |
|---|---|---|---|---|---|---|---|
| req/s | 16.48 | 16.55 | 1.00 | 16.64 | 16.67 | 1.00 | - / - |
| goodput req/s | 14.32 | 15.31 | 0.94 | 13.81 | 14.29 | 0.97 | + / + |
| within SLO % | 86.8 | 92.5 | 0.94 | 83.0 | 85.7 | 0.97 | + / + |
| strict SLO % | 40.7 | 51.7 | 0.79 (!) | 25.6 | 39.8 | 0.64 (!) | + / + |
| prefix reuse | 0.42 | 0.39 | 1.08 | 0.39 | 0.39 | 1.00 | + / - (differs) |
| TTFT mean ms | 242.3 | 226.1 | 1.07 | 230.6 | 243.0 | 0.95 | + / - (differs) |
| TTFT p50 | 175.4 | 156.1 | 1.12 | 198.6 | 196.8 | 1.01 | - / - |
| TTFT p90 | 506.1 | 440.4 | 1.15 | 408.4 | 444.3 | 0.92 | + / - (differs) |
| TTFT p99 | 1074.5 | 864.0 | 1.24 (!) | 701.8 | 972.5 | 0.72 (!) | + / - (differs) |
| ITL mean ms | 15.6 | 11.2 | 1.39 (!) | 25.6 | 20.9 | 1.23 (!) | - / - |
| ITL p90 | 33.9 | 19.4 | 1.75 (!) | 59.6 | 34.1 | 1.75 (!) | - / - |
| ITL p99 | 72.9 | 51.9 | 1.41 (!) | 216.3 | 208.7 | 1.04 | - / - |
| e2e p50 ms | 445.1 | 512.3 | 0.87 | 645.0 | 621.0 | 1.04 | - / - |
| e2e p99 | 12235.8 | 9772.3 | 1.25 (!) | 7114.7 | 7919.3 | 0.90 | + / + |

Within 15%: 18 of 28 metrics (21 of 28 within 25%); direction agreement 10 of 14. Per run:

| run | ok / errors | req/s | goodput | within SLO | strict | reuse | TTFT mean / p50 / p90 / p99 (ms) | mean ITL mean / p90 / p99 (ms) | e2e p50 / p99 (ms) | hit/oracle |
|---|---|---|---|---|---|---|---|---|---|---|
| cache_aware r1 | 1950 / 50 | 16.56 | 14.59 | 88.1 % | 40.1 % | 0.428 | 220 / 167 / 465 / 872 | 14.5 / 34.3 / 67.0 | 512 / 8935 | 1.000 |
| cache_aware r2 | 1950 / 50 | 16.31 | 13.48 | 82.7 % | 38.1 % | 0.400 | 279 / 180 / 613 / 1486 | 19.0 / 43.1 / 86.8 | 350 / 18035 | 0.998 |
| cache_aware r3 | 1950 / 50 | 16.58 | 14.88 | 89.7 % | 43.9 % | 0.432 | 227 / 179 / 441 / 865 | 13.2 / 24.3 / 64.8 | 474 / 9737 | 0.997 |
| round_robin r1 | 1950 / 50 | 16.66 | 13.90 | 83.4 % | 23.8 % | 0.392 | 218 / 176 / 393 / 699 | 25.9 / 59.8 / 217.1 | 636 / 6921 | 0.883 |
| round_robin r2 (unlocked) | 1950 / 50 | 16.62 | 13.63 | 82.0 % | 27.7 % | 0.393 | 254 / 243 / 431 / 724 | 25.4 / 57.5 / 215.4 | 671 / 7616 | 0.880 |
| round_robin r3 | 1950 / 50 | 16.65 | 13.92 | 83.6 % | 25.3 % | 0.395 | 219 / 177 / 402 / 683 | 25.6 / 61.4 / 216.5 | 628 / 6807 | 0.888 |
| hardware cache_aware | 1950 / 50 | 16.55 | 15.31 | 92.5 % | 51.7 % | 0.388 | 226 / 156 / 440 / 864 | 11.2 / 19.4 / 51.9 | 512 / 9772 | n/a |
| hardware round_robin | 1950 / 50 | 16.67 | 14.29 | 85.7 % | 39.8 % | 0.392 | 243 / 197 / 444 / 973 | 20.9 / 34.1 / 208.7 | 621 / 7919 | n/a |

Reading. Throughput, goodput, within-SLO, TTFT p50/p90 and e2e p50 agree on both policies (0.87-1.15), and the
mock reproduces the hardware's ordering on goodput and within-SLO (cache_aware ahead by 0.5 req/s and 4 points
on the mock, 1.0 req/s and 7 points on the fleet). The misses are of two kinds. The mock's decode step is slower
and more variable than the engine's at these batch sizes (ITL mean 1.2-1.4x, ITL p90 1.75x on both policies), so
the strict SLO (per-request p99 ITL under 50 ms) is 0.64-0.79x: the decode fit was taken over KV utilisation
alone and the per-step cost of mixed prefill+decode passes is too high. The tails of `cache_aware` (TTFT p99
1.24x, e2e p99 1.25x) come from the mock's affine worker taking slightly more of the hot prefix traffic than the
fleet's (reuse 0.42 against 0.39, hit/oracle 0.998), and round robin's p99 is the opposite (0.72x: the fleet has
a few slow first tokens the mock does not model). The hot-worker cascade of the earlier, pre-`--loads-like vllm`
series (TTFT p90 3.1x) is gone: with the vLLM load fields the gateway spreads the fallback traffic as it does on
hardware.

## Restricted pool, 12,000 blocks x 16 = 192,000 KV tokens per worker

| metric | cache_aware mock | hardware | ratio | round_robin mock | hardware | ratio | direction (ca-rr) mock / hw |
|---|---|---|---|---|---|---|---|
| req/s | 13.48 | 14.80 | 0.91 | 15.44 | 16.60 | 0.93 | - / - |
| goodput req/s | 7.32 | 7.04 | 1.04 | 6.54 | 11.88 | 0.55 (!) | + / - (differs) |
| within SLO % | 54.4 | 47.8 | 1.14 | 42.4 | 71.5 | 0.59 (!) | + / - (differs) |
| strict SLO % | 35.1 | 33.0 | 1.06 | 13.2 | 33.0 | 0.40 (!) | + / 0 (differs) |
| prefix reuse | 0.37 | 0.37 | 1.02 | 0.31 | 0.36 | 0.84 (!) | + / + |
| TTFT mean ms | 4350.7 | 4065.0 | 1.07 | 1856.7 | 641.0 | 2.90 (!) | + / + |
| TTFT p50 | 292.1 | 821.0 | 0.36 (!) | 601.3 | 245.0 | 2.45 (!) | - / + (differs) |
| TTFT p90 | 15410.9 | 13217.0 | 1.17 (!) | 5603.8 | 1838.0 | 3.05 (!) | + / + |
| TTFT p99 | 24003.9 | 24479.0 | 0.98 | 8192.3 | 4787.0 | 1.71 (!) | + / + |
| ITL mean ms | 22.1 | 26.1 | 0.85 (!) | 31.1 | 26.9 | 1.16 (!) | - / - |
| ITL p90 | 32.6 | 66.1 | 0.49 (!) | 48.2 | 68.5 | 0.70 (!) | - / - |
| ITL p99 | 149.5 | 198.1 | 0.75 (!) | 182.5 | 223.0 | 0.82 (!) | - / - |
| e2e p50 ms | 1273.8 | 3599.0 | 0.35 (!) | 4032.0 | 1258.0 | 3.21 (!) | - / + (differs) |
| e2e p99 | 32759.0 | 30061.0 | 1.09 | 19320.0 | 10317.0 | 1.87 (!) | + / + |

Within 15%: 9 of 28 metrics (15 of 28 within 25%); direction agreement 9 of 14. Per run:

| run | ok / errors | req/s | goodput | within SLO | strict | reuse | TTFT mean / p50 / p90 / p99 (ms) | mean ITL mean / p90 / p99 (ms) | e2e p50 / p99 (ms) | hit/oracle |
|---|---|---|---|---|---|---|---|---|---|---|
| cache_aware r1 (unlocked) | 1950 / 50 | 13.97 | 6.47 | 46.3 % | 28.2 % | 0.366 | 3387 / 349 / 13140 / 19622 | 27.0 / 44.6 / 220.5 | 2541 / 29376 | 0.948 |
| cache_aware r2 | 1950 / 50 | 13.57 | 8.48 | 62.5 % | 37.9 % | 0.381 | 3101 / 234 / 12949 / 22480 | 21.3 / 27.4 / 169.9 | 705 / 30643 | 0.977 |
| cache_aware r3 | 1950 / 50 | 12.90 | 7.00 | 54.3 % | 39.1 % | 0.373 | 6563 / 294 / 20144 / 29909 | 17.9 / 25.8 / 58.2 | 575 / 38258 | 0.959 |
| round_robin r1 | 1950 / 50 | 15.44 | 6.83 | 44.3 % | 14.5 % | 0.310 | 1745 / 487 / 5246 / 8144 | 30.7 / 45.0 / 175.5 | 3944 / 18976 | 0.815 |
| round_robin r2 | 1950 / 50 | 15.48 | 6.12 | 39.5 % | 11.9 % | 0.300 | 1902 / 795 / 5399 / 8040 | 31.9 / 51.5 / 197.9 | 4100 / 19083 | 0.792 |
| round_robin r3 | 1950 / 50 | 15.40 | 6.67 | 43.3 % | 13.3 % | 0.306 | 1924 / 523 / 6166 / 8393 | 30.8 / 48.1 / 174.1 | 4052 / 19901 | 0.807 |
| hardware cache_aware (mean of 3) | 1950 / 50 | 14.80 | 7.04 | 47.8 % | 33.0 % | 0.366 | 4065 / 821 / 13217 / 24479 | 26.1 / 66.1 / 198.1 | 3599 / 30061 | n/a |
| hardware round_robin (mean of 3) | 1950 / 50 | 16.60 | 11.88 | 71.5 % | 33.0 % | 0.363 | 641 / 245 / 1838 / 4787 | 26.9 / 68.5 / 223.0 | 1258 / 10317 | n/a |

Reading. With the reserve-full-input admission the collapse of the earlier mock (round robin at 0.96 req/s with
about 1,000 preemptions per run) is gone, and `cache_aware` now tracks the fleet: goodput 1.04, within-SLO 1.14,
reuse 1.02, TTFT mean 1.07, p99 0.98, e2e p99 1.09; its p50s are lower on the mock (0.36 and 0.35) because the
mock serves the credited worker's hits without the fleet's queueing delay. Round robin does not agree: the mock
delivers 6.5 req/s at 42% within SLO where the fleet delivers 11.9 at 71.5%, with TTFT p50 2.45x and p90 3.05x,
and the ordering of the two policies is inverted (fleet: round robin ahead by 4.8 req/s; mock: cache_aware ahead
by 0.8). The engine truth points at eviction: at the same pool the mock's round robin keeps a prefix reuse of 0.305
against the fleet's 0.363, that is it evicts more of the blocks the next repeat would have hit, so every worker
runs more uncached prefill and queues. The suspect is the order in which a finished request's blocks enter the
free queue. vLLM frees a finished request's blocks tail-first (`free()` reverses the block list before appending
to the free queue, so the head of a prefix outlives its tail and stays hittable) and removes a cached block from
the free queue on a hit; the mock's `release()` unreferences a request's blocks head-first, giving the head of
the prefix the oldest LRU stamp, so the head goes first and the whole chain behind it becomes unhittable. That is
the next mock lever, one change with a restricted-pool re-run of both policies.

## Verdict for T9 so far

- Full pool: agreement within 15% on 18 of 28 metrics and on the policy ordering of goodput and within-SLO; the
  systematic miss is the decode step (ITL 1.2-1.75x), which a decode fit over batch composition (prefill tokens in
  the pass as a second axis) should close.
- Restricted pool: `cache_aware` within 15% on the goodput/within-SLO/reuse/tail metrics; round robin far off and
  the policy ordering inverted, attributed to the mock's eviction order. T9 does not yet hold at this pool.
- The scoreboard reads the per-metric mock/hardware ratio; the 15% goal is met at the full pool for the headline
  metrics (goodput 0.94/0.97, within-SLO 0.94/0.97, TTFT p50 1.12/1.01) and not yet for the tails or the
  restricted pool.

## Files

- Mock runs: `~/smg-perf/replay/results/t9f-{cache_aware,round_robin}-r{1,2,3}` and `t9k-...` (`summary.json`,
  `requests.csv`, `fleet.csv` per-second fleet series, `fleet.json`, `metrics.txt`, `gateway.log`, `mock.log`,
  `run.log`); `t9f-round_robin-r2-unlocked` and `t9k-cache_aware-r1-unlocked` are the unlocked runs.
- Driver log with both per-run tables: `~/smg-perf/replay/results/fit-compare-t9.log`.
- Scripts: `~/smg-perf/replay/{run.sh,fit-compare-t9.sh,fit-table.py,t9-ratios.py}`; calibration
  `~/smg-perf/replay/fit-gb300-v2.json` (sources `qwen3-8b-gb300-vllm0.31.json`, `prefill-batch-sweep.json`,
  `scheduler-settings.json` under `~/smg-perf/gpu/results/calibration/`).
