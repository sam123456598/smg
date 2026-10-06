# Relay state snapshot on the GB300 fleet (leap/servicer cddb1617 = integrated 7085f728): the blind index is fixed

Hardware check requested by the servicer lane for the defect in `gpu-harness-e69487f8.md` section 4 (a fresh gateway on
warm engines learned nothing the engines held once the relay's history window had rolled). Code under test: `leap/servicer`
cddb1617 (pinned as `leap/servicer-check-cddb1617`, code paths identical to 7085f728 in batch 3; the batch-3 follow-up
c8b32b29 halves the collection lock hold, otherwise identical). Script `scripts/servicer-check.sh`, raw results
`~/smg-perf/gpu/results/servicer-check-cddb1617/` (README, warm-ups, the measured run with its debug gateway log).

Setup: `smg` and the binding wheel built from cddb1617 in their own worktree and target dir, proto / servicer / wheel
installed into both venvs (`KvEventBatch` fields end with `snapshot`), `8b-w0..w3` relaunched on the servicer **defaults**
(no `SMG_KV_EVENT_HISTORY_*` override: 10,000 batches / 256 MiB), eight warm-up replays (Mooncake rows 0-1999 at speedup 3
through fresh `cache_aware` gateways, `--load-monitor-interval 1 --worker-wedge-secs 600`, goodput 14.7-14.9 req/s,
89-91.5 % within SLO) so the windows roll, then one measured run through a fresh debug-level gateway of the same flags.
Three windows rolled (w0 after four replays, w2 and w3 after three); w1 is the affine worker, adds about 450 batches per
replay and was at 3,059 after eight, so it stayed on the history path.

| worker | relay answer to the measured gateway's cursor-0 subscribe | blocks (pool / 16) | chunks | `collected_us` (lock hold) | gateway |
|---|---|---|---|---|---|
| 8b-w0 (20061) | `the history no longer starts at the publisher's first batch; serving a state snapshot` through=24464 oldest=14465 holes=0 | 42,257 (42,259) | 21 | 1,941 | `served a state snapshot; replacing the worker's index state`, `snapshot applied; continuing with live events`, same second as the subscribe; `resyncs_total{reason="snapshot"}` = 1 |
| 8b-w1 (20062) | `serving from history cursor=0 batches=3059` (window not rolled) | - | - | - | history applied, no resync |
| 8b-w2 (20063) | snapshot, through=36465 oldest=26466 | 42,256 (42,259) | 21 | 1,959 | as w0, `snapshot` resync = 1 |
| 8b-w3 (20064) | snapshot, through=36023 oldest=26024 | 29,668 (29,670) | 15 | 1,412 | as w0, `snapshot` resync = 1 |

The snapshots carry the whole resident cache (the engines' pools were full after eight replays), the relay's lock hold is
about 2 ms per 42k-block collection, and the gateway logs the snapshot applied within the same second as the subscribe
(the off-lock ordering pass is well under a second here). No gaps, missed batches or other resyncs; applied batches over
the run 3,234 / 3,446 / 4,166 / 4,531.

Routing on the measured run: branch census 1,950 of 1,950 ok requests `event_hit` (the first 100 requests all hits, zero
`event_miss`; on e69487f8 the same situation gave 1,935 `event_miss`), engine truth 1,144 of them >= 50 % cached and the
other 806 partly cached, engine-truth cached/prompt 0.431; replay goodput 14.66 req/s, 89.4 % within SLO (strict 57.3 %),
prefix reuse 0.431, balance 1.1: the clean one-second-poll level (14.7 / 90.9 % / 0.45) instead of the blind index's
round-robin level. Verdict: fixed on this build. The integrated path is exercised again on 85f9d28c by the re-run's
router-restart row (relay window 300 batches, gateway SIGKILLed under load) and by the per-row relay-line counts of its
full-pool phase.
