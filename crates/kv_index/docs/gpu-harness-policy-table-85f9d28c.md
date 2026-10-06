# Loop head 85f9d28c on the GB300 fleet: the policy table at the default wedge rule, the restricted pool, and a router restart under load

Companion to `gpu-harness-policy-table-673e0187.md` (same fleet, protocol and scripts). Head 85f9d28c carries the bounded
wedge veto, the breaker reset and re-admission, and the relay state snapshot (= `gpu-harness-servicer-snapshot-cddb1617.md`).
One build (`scripts/build-head7.sh`), then `scripts/rerun-campaign.sh`: phase A full pool, phase C router restart, phase B
restricted pool, `scripts/wedge-classify.py` over A and B. Every row with `--load-monitor-interval 1`, the wedge rule at its
default (3 s), the fleet on the servicer's default relay history window (10,000 batches), fresh labelled `--enable-igw`
gateway per run, per-second series with the vLLM servicer's load fields. Raw results `~/smg-perf/gpu/results/policy-table-85f9d28c*/`.

## Phase A: full pool, three runs per row (`policy-table-85f9d28c/README-table.md`)

| row | req/s | goodput req/s | within SLO | strict | TTFT p50 / p90 / p99 (ms) | engine reuse | hit/oracle | balance | hot-worker check |
|---|---|---|---|---|---|---|---|---|---|
| rr | 16.70 | 14.48 | 86.7 % | 40.7 % | 188 / 380 / 778 | 0.392 | 0.774 | 1.00 | ok (max waiting 3) |
| ca | 16.70 | 14.80 | 88.6 % | 45.2 % | 174 / 359 / 771 | 0.449 | 0.887 | 1.06 | ok (5) |
| ll | 16.61 | 14.26 | 85.9 % | 37.1 % | 196 / 380 / 772 | 0.392 | 0.775 | 1.12 | ok (3) |
| cab | 16.61 | 14.38 | 86.6 % | 41.1 % | 166 / 384 / 795 | 0.428 | 0.845 | 1.09 | ok (4) |
| ca+tu0.8 | 16.64 | 14.73 | 88.5 % | 44.7 % | 162 / 359 / 765 | 0.448 | 0.886 | 1.09 | ok (4) |
| ca+wq8 | 16.67 | 14.74 | 88.4 % | 44.7 % | 166 / 352 / 770 | 0.445 | 0.879 | 1.05 | ok (5) |
| cab+tu0.8 | 16.53 | 14.39 | 87.0 % | 42.0 % | 163 / 371 / 772 | 0.431 | 0.851 | 1.09 | ok (4) |
| cab+wq8 | 16.61 | 14.41 | 86.8 % | 42.1 % | 161 / 382 / 798 | 0.427 | 0.844 | 1.09 | ok (4) |

The 673e0187 picture within run-to-run spread, now with the wedge rule armed: no "vetoed by liveness" line and no stall
transition in the 24 runs. `cache_aware` leads round robin by 0.3 req/s and 1.9 points of SLO at reuse 0.449 vs 0.392;
`cache-aware-balanced` trails `cache_aware` by 0.4 req/s and 0.02 reuse in every variant; no hot worker. Relay path: the
rr and ll rows do not subscribe to KV events; every `cache_aware` gateway (the warm-up and the 18 runs of the six
cache-aware rows) received a state snapshot of the whole resident cache on all four relays (42,256-42,257 blocks per
676 k worker, 29,668 on the 474 k worker, 15-21 chunks), none a history serve, because the windows had rolled after the
warm-up.

## Phase C: router restart under the table's load (`policy-table-85f9d28c-router-restart-fix/README.md`, `scripts/router-restart-row2.sh`)

Recovery-lane specification: the `cache_aware` row at the replay's ~16.6 req/s, the fleet relaunched with
`SMG_KV_EVENT_HISTORY_BATCHES=300` so every relay window has rolled by the kill, the gateway SIGKILLed at bench+60 s and
restarted with the same flags by the script, which re-registers the four servicers; both gateway instances at debug level
appending to one log that the replay joins (`agree` per request); metrics every second. (A first attempt in the chain was
void: the launcher passed its own phase tag to `smg launch`, which refused to start; the corrected row ran 23:34-23:40.)

| from the SIGKILL | seconds |
|---|---|
| new process launched | 0.02 |
| `/health` 200 | 0.36 |
| four `POST /workers` accepted | 0.38 |
| first chat request 200 (first worker routable) | 0.64 |
| `/workers` shows four healthy | 0.65 |
| `smg_kv_event_resyncs_total{reason="snapshot"}` = 1 on every worker | 0.8 |
| `smg_kv_index_blocks` at the engines' resident count (42,251 / 42,252 / 42,254 / 29,663 against snapshot `blocks=` 42,242 / 42,253 / 42,231 / 29,650; pools 42,259 / 29,670) | 1.8 |

Servicer `collected_us` 830 / 752 / 862 / 540 (the batch-3 dense live-block store; 1.4-2.0 ms on cddb1617). GetLoads
`num_used_tokens` was 0 at that second: the requests in flight had died with the old gateway (88 non-400 failures, all
sent in the 10 s before the kill; none after it). Steady window (bench+10 s to the kill, 775 requests): TTFT p50 142 /
p99 734 ms, engine cached/prompt 0.452, `agree` 0.986. First 30 s after the first routable request (535 requests): TTFT
p50 153 / p99 746 ms, cached/prompt 0.468, `agree` 1.000 over 508 joined rows; p99 ratio 1.02 against the contract's
2.0 bound. Engine-truth hit fraction 1.00 in every 10 s bucket including the kill bucket; whole run goodput 14.67 req/s,
91.9 % within SLO over the served rows, reuse 0.443; no veto, no stall. The index is back at engine truth 1.8 s after
the kill, and the only cost of the restart is the in-flight requests of the old process.

## Phase B: restricted pool (`--num-gpu-blocks-override 12000`, 192 k tokens per worker, 128 sequences), one run per row (`policy-table-85f9d28c-kv12k/README-table.md`)

| row | req/s | goodput req/s | within SLO | strict | TTFT p50 / p90 / p99 (ms) | engine reuse | hit/oracle | balance | hot-worker check (max waiting / s waiting>=10 / s KV>=0.95 / top share) |
|---|---|---|---|---|---|---|---|---|---|
| rr | 16.66 | 14.19 | 85.2 % | 38.1 % | 205 / 439 / 995 | 0.368 | 0.727 | 1.00 | 11 / 1 / 4 / 0.25 |
| ca | 16.56 | 14.26 | 86.1 % | 38.9 % | 184 / 379 / 932 | 0.381 | 0.753 | 1.12 | 6 / 0 / 5 / 0.26 |
| ll | 16.64 | 14.11 | 84.8 % | 33.9 % | 198 / 385 / 792 | 0.369 | 0.730 | 1.03 | 5 / 0 / 3 / 0.27 |
| cab | 16.52 | 13.93 | 84.4 % | 36.2 % | 198 / 413 / 841 | 0.371 | 0.734 | 1.07 | 4 / 0 / 4 / 0.31 |
| ca+tu0.8 | 16.49 | 14.11 | 85.5 % | 40.7 % | 188 / 456 / 1136 | 0.380 | 0.751 | 1.21 | 11 / 1 / 10 / 0.34 (HOT by the KV rule) |
| ca+wq8 | 16.49 | 14.00 | 84.9 % | 42.7 % | 167 / 443 / 1310 | 0.378 | 0.747 | 1.25 | 3 / 0 / 9 / 0.35 |
| cab+tu0.8 | 16.71 | 14.23 | 85.2 % | 37.1 % | 185 / 416 / 784 | 0.375 | 0.742 | 1.11 | 4 / 0 / 1 / 0.28 |
| cab+wq8 | 16.68 | 14.21 | 85.2 % | 36.1 % | 203 / 401 / 860 | 0.373 | 0.738 | 1.03 | 2 / 0 / 0 / 0.26 |

(The README-table's title line says "full pool"; the directory and this table are the 12,000-block pool.) At 7 % of the
working set per worker and the one-second poll every policy keeps 13.9-14.3 req/s, where the 2d37b25a/d2179cd0 heads at
the ten-second poll had `cache_aware` at 5.0-8.6 against round robin's 12.8-14.2: the pools evict as fast as they fill
(engine reuse 0.37-0.38, hit/oracle 0.73-0.75 for every row) and the differences between rows are within one run's
noise, except that both token-usage-protection rows push one worker to KV >= 0.95 for 9-10 s (`ca+tu0.8` is the only HOT
verdict of the night) and have the worst p99 (1.1-1.3 s). No "vetoed by liveness" line in the eight runs either: over
the 32 runs of phases A and B the bounded wedge rule at its 3 s default never fired, so there is nothing to classify
(`policy-table-85f9d28c/README-wedge.md` has empty tables); the 2.6-5.3 s all-in-prefill silences of the e69487f8 runs did
not recur at the one-second poll, where no worker piles up.

Engine-side capture for the mock lane (`policy-table-85f9d28c-kv12k/engine-events/`, `scripts/phaseb-sidecar.sh`,
`kv_events_rollup.py`): each 8B worker's own KV-event stream (a second ZMQ subscriber on the engine's publisher) for the
whole phase, raw `kv-events-8b-w<i>.jsonl.gz` (77-101 MB each) in the captured-fixture line format, `*.per-second.csv`
(batches, stored events/blocks, removed events/blocks, cleared) and the gateway's `smg_kv_event_*` counters every 2 s
(`gateway-kv-metrics.txt`); `README.txt` has per-row totals. Over the eight rows each worker stored and removed 1.18-1.59 M
blocks (removals equal to stores within a few blocks: a full pool recycling), 3.5-5.1 k batches per row, peak removals
2,851-5,402 blocks in one second. vLLM's preemption counter is not observable on this launch (no `/metrics`, no
stats-logger line in the headless engine, no preemption field in the servicer's periodic line or in `GetLoads`); the
phase-B host logs were truncated by the phase-C relaunch before the sidecar copied them (they held only the servicer's
periodic lines). The per-second KV usage, waiting, running and queued token-work per worker are in each row's
`series-<policy>-run1.csv`.

## Fleet state and leftovers

`8b-w0..w3` left up on the 85f9d28c wheel at the full pool with the 300-batch relay window of phase C (relaunch with
`scripts/relaunch-8b.sh` for the default or `SMG_KV_EVENT_HISTORY_BATCHES=400000` for a session-long window), `drill-w0..w3`
on the same wheel, `http-8b` up; the labelled gateways are stopped. The layout-coverage study (gpt-oss-20b, a hybrid
Mamba model) is blocked on weights: copying another user's cache or downloading through the proxy was declined by the
permission layer and left to the user.
