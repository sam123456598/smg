# Hardware policy table on loop head 673e0187 (8B fleet, full pool, one-second load poll)

Companion to `gpu-harness-e69487f8.md` (sections 3-6 explain the fleet, the labelled gateway, the per-second series, the
relay history window and the one-second poll). Fleet: `8b-w0..w3` (Qwen3-8B, 676,144-token pools, 474,720 on GPU 3) on the
673e0187 servicer wheel with `SMG_KV_EVENT_HISTORY_BATCHES=400000` / 4 GiB (every fresh gateway received each relay's
complete history), engines warm (one cold run discarded). Each row: three labelled runs (fresh `--enable-igw` gateway per
run, workers registered with `weight_version=grpc:<port>`), Mooncake rows 0-1999 at speedup 3, the per-second per-worker
series with the vLLM servicer's load fields. Scripts: `scripts/policy-table.sh`, `policy-table-report.py`, `trace_oracle.py`;
raw results `~/smg-perf/gpu/results/policy-table-673e0187/`.


`--worker-wedge-secs 600` disarms the liveness wedge veto, which false-fires on long prefills in this head (the fix lands in the next batch; the affected rows are re-run then). Balanced rows: `--policy cache_aware --selection-policy cache-aware-balanced (parameters at their 673e0187 defaults, balanced.rs: affinity_cap_tokens 16384, saturation_waiting_requests 8, saturation_kv_usage 0.8)`.

| configuration | policy / flags | runs | req/s | goodput req/s | within SLO | strict | TTFT p50 / p90 / p99 (ms) | engine reuse | hit/oracle | balance | hot-worker check: max waiting / most s waiting>=10 / most s KV>=0.95 / top uncached share |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rr | `round_robin` --load-monitor-interval 1 --worker-wedge-secs 600 | 3 | 16.66 | 14.49 | 87.0 % | 39.2 % | 188 / 375 / 767 | 0.391 | 0.773 | 1.00 | 3 / 0 / 0 / 0.27 -> ok |
| ca | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 | 3 | 16.67 | 14.50 | 87.0 % | 43.5 % | 184 / 392 / 804 | 0.445 | 0.880 | 1.06 | 4 / 0 / 0 / 0.31 -> ok |
| ll | `least_load` --load-monitor-interval 1 --worker-wedge-secs 600 | 3 | 16.59 | 14.03 | 84.6 % | 36.9 % | 202 / 394 / 817 | 0.391 | 0.773 | 1.10 | 5 / 0 / 0 / 0.29 -> ok |
| cab | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 --selection-policy cache-aware-balanced | 3 | 16.41 | 14.07 | 85.7 % | 40.8 % | 185 / 401 / 829 | 0.425 | 0.840 | 1.14 | 4 / 0 / 0 / 0.36 -> ok |
| ca+tu0.8 | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 --worker-overload-protection --worker-overload-token-usage 0.8 | 3 | 16.63 | 14.69 | 88.3 % | 43.6 % | 177 / 382 / 773 | 0.449 | 0.887 | 1.06 | 4 / 0 / 0 / 0.30 -> ok |
| ca+wq8 | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 --worker-overload-protection --worker-overload-waiting-requests 8 | 3 | 16.60 | 14.58 | 87.8 % | 43.0 % | 170 / 358 / 773 | 0.451 | 0.891 | 1.09 | 5 / 0 / 0 / 0.30 -> ok |
| cab+tu0.8 | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 --selection-policy cache-aware-balanced --worker-overload-protection --worker-overload-token-usage 0.8 | 3 | 16.48 | 14.28 | 86.6 % | 40.6 % | 172 / 380 / 789 | 0.434 | 0.857 | 1.13 | 3 / 0 / 0 / 0.33 -> ok |
| cab+wq8 | `cache_aware` --load-monitor-interval 1 --worker-wedge-secs 600 --selection-policy cache-aware-balanced --worker-overload-protection --worker-overload-waiting-requests 8 | 3 | 16.62 | 14.27 | 85.9 % | 41.4 % | 170 / 399 / 802 | 0.428 | 0.847 | 1.11 | 5 / 0 / 0 / 0.34 -> ok |

hit/oracle = engine-truth cached/prompt over the run's ok rows divided by the trace oracle over the same rows (longest already-seen hash_ids prefix, one unbounded shared cache, `scripts/trace_oracle.py`; the oracle over all 2000 rows is 0.4489). Hot-worker check: HOT when one worker shows >= 10 one-second samples with waiting >= 10 or with KV usage >= 0.95 in any run.

The 50 errors of 2,000 on every row are the trace rows above the 40,960-token context (HTTP 400 ContextLength), a prompt-length matter, not routing; goodput and SLO shares are over the 1,950 served rows.

## Per run

| run | ok / errors | req/s | goodput | within SLO | strict | TTFT p50 / p90 / p99 | engine reuse | hit/oracle | balance | hot check |
|---|---|---|---|---|---|---|---|---|---|---|
| rr run 1 | 1950 / 50 | 16.64 | 14.42 | 86.7 % | 38.8 % | 195 / 367 / 777 | 0.392 | 0.774 | 1.00 | 3 / 0 / 0 / 0.26 |
| rr run 2 | 1950 / 50 | 16.66 | 14.53 | 87.2 % | 39.7 % | 191 / 375 / 778 | 0.390 | 0.771 | 1.00 | 2 / 0 / 0 / 0.27 |
| rr run 3 | 1950 / 50 | 16.67 | 14.52 | 87.1 % | 39.1 % | 177 / 382 / 747 | 0.392 | 0.774 | 1.00 | 3 / 0 / 0 / 0.25 |
| ca run 1 | 1950 / 50 | 16.65 | 14.43 | 86.7 % | 44.0 % | 175 / 410 / 827 | 0.443 | 0.875 | 1.07 | 4 / 0 / 0 / 0.28 |
| ca run 2 | 1950 / 50 | 16.69 | 14.61 | 87.5 % | 43.4 % | 188 / 374 / 807 | 0.447 | 0.883 | 1.02 | 3 / 0 / 0 / 0.27 |
| ca run 3 | 1950 / 50 | 16.68 | 14.47 | 86.8 % | 43.0 % | 188 / 392 / 779 | 0.446 | 0.881 | 1.09 | 3 / 0 / 0 / 0.31 |
| ll run 1 | 1950 / 50 | 16.69 | 14.23 | 85.3 % | 37.2 % | 214 / 380 / 814 | 0.389 | 0.769 | 1.10 | 1 / 0 / 0 / 0.28 |
| ll run 2 | 1950 / 50 | 16.59 | 13.97 | 84.2 % | 35.7 % | 198 / 407 / 837 | 0.388 | 0.766 | 1.15 | 2 / 0 / 0 / 0.29 |
| ll run 3 | 1950 / 50 | 16.49 | 13.90 | 84.3 % | 37.8 % | 193 / 396 / 800 | 0.396 | 0.783 | 1.06 | 5 / 0 / 0 / 0.29 |
| cab run 1 | 1950 / 50 | 16.44 | 14.35 | 87.3 % | 41.2 % | 156 / 378 / 800 | 0.426 | 0.841 | 1.14 | 4 / 0 / 0 / 0.36 |
| cab run 2 | 1950 / 50 | 16.39 | 13.98 | 85.3 % | 41.0 % | 197 / 433 / 892 | 0.425 | 0.840 | 1.14 | 4 / 0 / 0 / 0.36 |
| cab run 3 | 1950 / 50 | 16.40 | 13.87 | 84.6 % | 40.3 % | 200 / 391 / 795 | 0.425 | 0.840 | 1.13 | 4 / 0 / 0 / 0.28 |
| ca+tu0.8 run 1 | 1950 / 50 | 16.71 | 14.64 | 87.6 % | 42.6 % | 174 / 377 / 776 | 0.442 | 0.873 | 1.03 | 3 / 0 / 0 / 0.30 |
| ca+tu0.8 run 2 | 1950 / 50 | 16.75 | 14.88 | 88.8 % | 44.7 % | 170 / 371 / 768 | 0.452 | 0.892 | 1.09 | 4 / 0 / 0 / 0.29 |
| ca+tu0.8 run 3 | 1950 / 50 | 16.43 | 14.55 | 88.6 % | 43.5 % | 186 / 397 / 775 | 0.453 | 0.895 | 1.07 | 4 / 0 / 0 / 0.28 |
| ca+wq8 run 1 | 1950 / 50 | 16.54 | 14.65 | 88.6 % | 43.0 % | 174 / 348 / 762 | 0.453 | 0.895 | 1.07 | 4 / 0 / 0 / 0.28 |
| ca+wq8 run 2 | 1950 / 50 | 16.54 | 14.50 | 87.6 % | 44.1 % | 168 / 361 / 775 | 0.450 | 0.888 | 1.09 | 2 / 0 / 0 / 0.27 |
| ca+wq8 run 3 | 1950 / 50 | 16.71 | 14.59 | 87.3 % | 42.0 % | 168 / 365 / 783 | 0.450 | 0.889 | 1.10 | 5 / 0 / 0 / 0.30 |
| cab+tu0.8 run 1 | 1950 / 50 | 16.53 | 14.14 | 85.5 % | 39.2 % | 180 / 383 / 756 | 0.429 | 0.847 | 1.07 | 3 / 0 / 0 / 0.29 |
| cab+tu0.8 run 2 | 1950 / 50 | 16.39 | 14.35 | 87.6 % | 41.8 % | 163 / 368 / 797 | 0.442 | 0.873 | 1.20 | 3 / 0 / 0 / 0.33 |
| cab+tu0.8 run 3 | 1950 / 50 | 16.53 | 14.34 | 86.7 % | 40.7 % | 172 / 389 / 814 | 0.430 | 0.850 | 1.12 | 3 / 0 / 0 / 0.31 |
| cab+wq8 run 1 | 1950 / 50 | 16.52 | 14.24 | 86.2 % | 41.3 % | 176 / 402 / 801 | 0.428 | 0.845 | 1.13 | 5 / 0 / 0 / 0.32 |
| cab+wq8 run 2 | 1950 / 50 | 16.69 | 14.45 | 86.6 % | 41.5 % | 169 / 399 / 799 | 0.431 | 0.851 | 1.10 | 3 / 0 / 0 / 0.34 |
| cab+wq8 run 3 | 1950 / 50 | 16.63 | 14.13 | 84.9 % | 41.3 % | 167 / 398 / 805 | 0.427 | 0.843 | 1.10 | 4 / 0 / 0 / 0.32 |

## Reading

- With the load poll at one second no row forms a hot worker: in all 24 runs no worker shows a one-second sample with
  waiting >= 10 or KV usage >= 0.95, waiting peaks at 3-5, and the busiest worker carries 27-36 % of the fleet's uncached
  prompt tokens (the 10 s-poll runs of the previous note had one worker at KV 1.00 with 15-38 waiting).
- `cache_aware` equals round robin on goodput and SLO share (14.50 vs 14.49 req/s, 87.0 % both) and beats it on the strict
  SLO (43.5 vs 39.2 %), prefix reuse (0.445 vs 0.391) and hit/oracle (0.880 vs 0.773). The worker-protection variants add
  0.1-0.2 req/s and the best tails (`ca+wq8`: TTFT p50 170, p90 358 ms). `least_load` is the weakest row (14.03 req/s,
  84.6 %, p50 202 ms; it does not see the prefix, reuse 0.391).
- `cache-aware-balanced` at its defaults (affinity credit capped at 16,384 tokens, saturation veto at 8 waiting or KV 0.8)
  costs about 3 % goodput and 0.02 reuse against the default affinity decision (14.07 vs 14.50 req/s, hit/oracle 0.840 vs
  0.880, balance 1.14 vs 1.06); its protection variants recover part of it (14.28 / 14.27). Run-to-run spread within a row is
  0.1-0.5 req/s, so the `cab` vs `ca` gap sits at the edge of what three runs resolve; the `cab+wq8` vs `ca+wq8` gap (-0.31)
  and `least_load`'s deficit hold in every run.
- hit/oracle is engine-truth cached/prompt divided by the trace oracle over the same rows (`trace_oracle.py`: longest
  already-seen `hash_ids` prefix, one unbounded shared cache, 0.506 over the 1,950 served rows); no row gets closer than
  0.89 because the oracle counts a repeat as cached wherever it lands, while four pools hold their own copies.
- Every row ran with `--worker-wedge-secs 600` (the liveness wedge veto false-fires on long prefills in this head) and no
  veto fired; the recovery lane's offline replay of both rules over these rows' traces found the longest silence with
  requests in flight at 0.45 s, so the table says nothing about the wedge rule either way. The re-run on the next head
  (85f9d28c: bounded veto, breaker reset, re-admission, relay snapshot) runs the rule at its default, the fleet on the
  default history window, adds the restricted-pool shape where the 2.6-5.3 s all-in-prefill silences were seen, and a
  router-restart row under the replay load.
