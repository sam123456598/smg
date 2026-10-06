# KV event recovery: what the gateway does and what the wire protocol needs

Companion to `kv-router-leap.md` section 4.4. The gateway side (per-rank cursors, bounded gap
recovery, publisher-restart detection, the snapshot tail buffer) lives in
`model_gateway/src/worker/kv_event_recovery.rs` and is driven from
`model_gateway/src/worker/kv_event_monitor.rs`. This note records the wire semantics the gateway
relies on, the state machine, the fault drills, and the additions the servicers and
`crates/grpc_client/proto/common.proto` need so recovery converges to an exact index instead of a
best-effort one. It is written for the schema workstream to fold into its proto change; nothing in
the "proposed" section is implemented on the server yet.

## Wire semantics today

`SubscribeKvEventsRequest.start_sequence_number` is the **last sequence the client applied**, not
the first one it wants: the SGLang servicer asks its engine's replay socket for `cursor + 1`, the
mock engine streams buffered batches `> cursor`, and `0` means "no cursor, live only". Two
consequences shape the gateway's rules:

- a server never legitimately sends a sequence below the cursor as the first batch of a fresh
  connection, so one there is a restarted publisher (vLLM and SGLang count from 0 per process);
- a cursor of exactly 0 is indistinguishable from "no cursor". A rank whose only applied batch was
  sequence 0 resubscribes live, and a batch lost during that reconnect is settled as a gap.

| Server | `start_sequence_number != 0` | History gone | Snapshot | Ranks |
|---|---|---|---|---|
| Rust `engine_servicer` relay (vLLM, SGLang, TokenSpeed) | served from the relay's bounded history when the cursor is inside the window (the last `SMG_KV_EVENT_HISTORY_BATCHES` batches, 10,000 by default, within `SMG_KV_EVENT_HISTORY_BYTES`, 256 MiB), then live; `OUT_OF_RANGE` below the window, beyond the newest sequence, or before the relay's own start, and the gateway clears and resubscribes from zero; a cursor of 0 gets the whole window when it is complete from the publisher's first batch, else the **state snapshot** below (the window rolled, has a hole, or the relay started after the publisher), then live; live only before anything was relayed. ZMQ delivers nothing from before a subscription, so the relay asks the engine's replay for everything from 0 when it starts (retrying every second until the engine's replay answers, which covers an engine that published at registration and nothing since), once more when a subscription from zero finds it still holding nothing, and again before relaying a first live batch past sequence 1 (the publishers count from 0, the mock from 1) unless that batch carries the engine's startup clear; what the replay no longer reaches back to is counted as `unknown_before_start` and the window then starts late instead of passing as whole | `DATA_LOSS` on a publisher restart (its sequence going backwards on the relay's socket); the relay's own gaps are filled from the engine's replay socket, and what the replay cannot give is a hole in the window that a resume skips, so the gateway settles it once instead of looping | in band, from the relay's record of the engine's live blocks (`crates/engine_servicer/src/kv_state.rs`, one entry per live rank, tier and hash, folded from the stream it relays): batches marked `KvEventBatch.snapshot` (`KvSnapshotChunk {index, count, blocks}`), chunk 0 beginning with `AllBlocksCleared`, then every live block as `Stored` events with the original store's fields, one event per physical copy, parents before children, cut atomically at the relay's cursor under the lock that admits batches and stamped with consecutive sequence numbers ending at that cursor, so live events continue at cursor + 1 with no gap and no duplicate; every chunk carries `unknown_before`, the publisher sequences before the relay's record that even the replay no longer had (0 for a whole record) | relays rank 0 only |
| Python servicers (SGLang, vLLM) | replay when the engine offers one; `OUT_OF_RANGE` when it cannot honour a non-zero start | `DATA_LOSS` on a publisher restart or an unverifiable replay | none ("zero starts live rebuilding") | every DP rank, `dp_rank` set, a cursor per rank |
| `mock-worker --engine realistic` | replays its buffer `> cursor`, then live | never signalled (the publisher-restart fault hook keeps the stream up) | none | rank 0 |

Engine facts the rules lean on (`~/smg-perf/research/{vllm,sglang}-kv-events.md`): both
publishers number batches from 0 per process and restart at 0; SGLang's first batch after startup
carries `AllBlocksCleared`, vLLM's does not; SGLang's replay socket silently truncates large
replays at libzmq's default send high-water mark (1,000 frames) and has no snapshot; an idle engine
publishes nothing, so a quiet stream is not a dead one.

## The gateway's state machine

One `RankState` (cursor, pending replay, degraded flag, snapshot tail) per `(worker, dp_rank)`,
all of them over the worker's one `WorkerIndexState` (block map, physical copies per tier,
counters), which stays pooled across ranks as the monitor already does: the worker URL is the
routing target and its copies are interchangeable for a prefix hit. Cursors and copy counts never
mix, and nothing about blocks is tracked twice.

| Batch sequence `seq` against the rank's cursor `last` | Decision |
|---|---|
| no cursor yet | apply, cursor = `seq` |
| `last + 1` | apply |
| `<= last`, and the batch carries the engine's own `Cleared`, or it is the first batch on a fresh connection with `seq < last`, or `seq` is 0 or 1 under a larger cursor, or `seq + 1024 <= last` | **publisher restart**: clear the worker's index state, start the other ranks' cursors over, apply, cursor = `seq` |
| `<= last` otherwise | duplicate (replay overlap): skip |
| `> last + 1`, no replay pending | **gap**: remember `expected = last + 1`, reconnect with the cursor; the batch is not applied |
| `> last + 1`, replay pending | **unrecoverable gap**: the server skipped ahead anyway. `missed <= 1024`: keep the blocks, mark the rank degraded (an engine still holds most of them and never re-sends stores); more: clear the worker. Apply, cursor = `seq` |
| batch marked `KvSnapshotChunk` (in-band relay snapshot) | **snapshot**: chunk 0 clears the worker's index state and every cursor and counts the resync; every chunk is applied outside the rules above and sets its rank's cursor to its stamp, so the live batch after the last chunk is `last + 1`; a chunk with `unknown_before > 0` leaves the rank degraded (the relay's record started after the engine's and the replay did not reach back); a stream that ends before the last chunk forgets the cursors, and the next subscription asks from zero for a whole snapshot |
| snapshot in flight (out of band; no servicer sends one today) | hold in the rank's tail (bounded at 1,024 batches; overflow forces another snapshot) |

A restart on one rank clears the whole worker because the pooled copies cannot be attributed to a
rank; the other ranks' cursors start over so their next batch is taken as a first one instead of
clearing the index a second time. `OUT_OF_RANGE` / `DATA_LOSS` from the server do the same for
every rank and resubscribe live. Worker removal hands the pooled state to one `remove_worker`
pass. A reconnect sends rank 0's cursor (the servicers replay rank 0); other ranks dedup what
arrives. The reconnect backoff caps at 5 s: a worker that restarts is healthy again within seconds,
and until its stream is back the blocks it stores are invisible to routing, because the servicers
resume after the cursor and never resend them.

Every decision is a metric: `smg_kv_event_batches_total{disposition}` (applied, stale,
tail_overflow, snapshot), `smg_kv_event_gaps_total{outcome}` (replay_requested, unrecovered_kept,
unrecovered_cleared), `smg_kv_event_missed_batches_total`, `smg_kv_event_resyncs_total{reason}`
(out_of_range, data_loss, publisher_restart, gap_cleared, snapshot), `smg_kv_event_lag_seconds`
(publisher stamp to apply), `smg_kv_event_degraded_ranks`, `smg_kv_event_tail_depth`,
`smg_kv_event_subscriptions_total` (streams connected, a reconnect counts again) and
`smg_kv_index_blocks{worker}` (the blocks the index holds for the worker as it counts them, set
where applied batches are counted, after a reset and on removal: the soak reads the index's size
from the gateway instead of the mock's admin API).

A gateway restart, or a gateway started after the engines warmed up, now learns a worker's
resident blocks from the Rust relay whatever the age of the stream: the full history while the
window still starts at the publisher's first batch, the state snapshot once it has rolled (before
this, a fresh gateway behind a rolled window routed on `event_miss` for the whole run: 1,935 of
1,950 selections on the GB300 fleet while the engines served 56% of them from cache, see
`gpu-harness-e69487f8.md` section 4). What the gateway still cannot do without more server help:
learn which blocks changed during an unreplayable gap, or tell a restarted publisher that kept its
cache from one that lost it; both need an epoch.

## Liveness beside the health check

The contract asks for a dead or partitioned worker to be out of routing within 2 s; the health
state machine alone needs `failure_threshold * check_interval`, and the first drills measured 6 to
26 s. The gateway now learns it from the transport and from progress (`model_gateway/src/worker/
liveness.rs`):

- **Keepalive.** Every gRPC channel to a worker pings every second and gives the pong a second,
  so a worker that dies, freezes or falls behind a partition fails every stream on its connection
  within about 2 s. Load polls and KV subscriptions carry deadlines (3 s and 5 s), so one half-open
  connection can no longer hang the group's poll tick or the subscriber loop.
- **Unreachable.** A connection failure on the KV event stream or the load poll marks the worker;
  once nothing has been heard from it for the stall threshold (`--worker-stall-secs`, 2 s) a
  quarter-second sweep vetoes it. Any successful contact re-admits it: a poll answered, a health
  probe passed, an event batch, a response. A poll or probe that merely times out stays with the
  health state machine: a slow worker that still streams tokens must not flap.
- **Wedged.** Every response a worker streams counts as progress for it. Requests in flight, a
  reported waiting queue or a pile of in-flight requests (growing, or four deep), and no token for
  the wedge bound veto the worker while its health keeps passing; the next token clears it. A
  paused engine that still answers health and `GetLoads` is exactly this case. Two false
  positives shaped the rule: the policy lane saw all eight mock workers vetoed 3.5 s after
  registration at their first requests (the no-progress clock had run from registration), and the
  GPU lane saw it fire when a running batch was all in prefill and streamed nothing for 3 to 5 s.
  The clock therefore starts when the worker's load counter leaves zero (the first dispatch of a
  run of requests), never at registration or at the last token before an idle spell, and the
  threshold is a bound: `--worker-wedge-secs` (3 s) or, if longer, the time the engine may still
  need to prefill the prompts dispatched to it whose first token has not come back, over the
  prefill rate observed on that worker (first tokens summed over windows of at least a second;
  10k tokens/s until a rate has been seen), capped at 120 s or the configured threshold. The gRPC
  dispatch books each request's prompt length on its stream and releases it at the first response
  or when the stream is dropped; paths without a token count fall back to the configured
  threshold. A worker that still answers polls is thus never vetoed before the first token of its
  in-flight work is due, while a paused engine with chat-sized prompts in flight is vetoed at the
  threshold as before.

The veto is a routing flag next to the overload veto (`RoutingState::stalled`), visible as
`stalled` in `GET /workers`, `smg_worker_stalled{worker,reason}` and
`smg_worker_stall_transitions_total`. It never touches the health status.

Measured on the mock fleet with health checks 10 s apart, so only the veto can act (times from the
fault to the gateway reporting the worker out of routing, sampled every 100 ms; the 2026-10-05
evening column is the eight-worker fleet on cores 72-143 with the gateway on 64-71, shared with two
other lanes' runs, gateway from the integrated branch; hit/oracle is the engines' own
`cached_tokens` over their prompt tokens before and after the fault):

| Fault | Before | With the veto (first run) | 2026-10-05 evening | Detected by | Hit/oracle before -> after |
|---|---|---|---|---|---|
| worker killed | 3.8 to 5.3 s (health) | 2.3 s | 2.0 s | `stalled=unreachable` from the KV stream's reset | 91.9/94.3 -> 92.3/95.0 % |
| worker unreachable (listener closed, connections reset) | 3.1 to 7.0 s | 2.1 to 2.2 s | 2.1 s, back 0.4 s after the listener returns | same | see the run's drill.log |
| worker partitioned (blackhole) | 2.3 to 30 s | 2.0 to 2.5 s | 2.0 s, back 0.1 s after the link heals | keepalive failure on the KV stream | see the run's drill.log |
| worker restarted | 3.4 to 10.6 s | 2.6 s; stream resubscribed 0.2 s after it is back | 2.1 s; re-admitted 0.0 s after its port answers, stream resubscribed at 0.2 s, first request 0.3 s into fresh traffic, restart resync at 0.4 s | same; re-admitted on the first contact, breaker closed, warm-up slice | 92.4/94.8 -> 88.2/90.0 % (the window spans the outage and the cold cache) |
| engine paused, health answering | never | 3.2 s, routable 0.4 s after resume | 3.0 s, routable 0.0 s after resume | `stalled=wedged` | see the run's drill.log |

The 2 s threshold counts from the last contact; the keepalive adds up to 2 s before a partition
surfaces, the sweep up to 0.25 s, and the drill's own sampling 0.1 s.

**Re-admission.** Every worker now carries the registry's connect signal, not only ZMQ workers:
when a worker the health checker demoted answers a poll, a subscription or a request, the liveness
tracker signals the health manager, which promotes it on the spot and reschedules its probe. Each
contact also wakes the KV event subscriber out of its reconnect backoff (and out of a subscription
call hanging on a port that accepts but does not serve yet, which gets a 2 s deadline), so the
stream is back the moment the worker is heard from. A contact ends the subscriber's backoff no
sooner than 100 ms and abandons a hanging subscription call only half a second after it, so the
contacts of a healthy worker (every token it streams is one) never cut a live call short; every
stream connected counts in `smg_kv_event_subscriptions_total`. The outage's connection failures
also opened the circuit breaker, which held the returned worker out of routing for its 30 s
timeout and reopened on one error from the stale channel (the restart drill saw a first request
only 53.8 s after re-admission with the slice firing 63 times elsewhere): a worker whose
unreachable veto clears, or that health promotes back to Ready, now returns with a closed
breaker. Drill (2026-10-05 15:25, before the breaker fix): excluded in 2.2 s, re-admitted 0.8 s
after its port answered, stream resubscribed at 1.0 s, but the first request reached it only
53.8 s later; with the breaker reset (19:51): re-admitted 0.0 s after its port answered, stream
resubscribed at 0.2 s, first request 0.3 s into the fresh trickle, the publisher restart recognised
0.4 s after the port answered. The probe-gated path took 10.7 s.

**Warm-up slice.** A returned worker has an empty cache, so cache-aware routing had no reason to
send it anything; on the GB300 fleet a restarted worker served nothing for 59 s. Workers now
record when they became routable (promotion to Ready, or a liveness veto cleared). For
`--worker-warmup-secs` (60) after that, while the index holds fewer than `--worker-warmup-blocks`
(1,024) blocks for the worker, one cache miss in `1 / --worker-warmup-share` (a quarter) goes to
the least-loaded warming worker on the event-driven path, credited through the expected-wait
selector like any pick. A miss here is no overlap or a thin one, at or below the tree-mode
`cache_threshold` ratio: chat requests share their template's head with every holder, which is
affinity in name only. Hits are never diverted; the branch counts as `warmup_slice` in
`smg_cache_aware_policy_branch_total`. Growth is measured from the count first seen for the
current admission, not from zero: a restarted engine's stale blocks stay indexed until its first
batch reveals the restart, and an engine that gets no request sends no batch, so the stale count
would end the warm-up meant to break that circle; the baseline drops to zero when the index is
cleared underneath. Nothing is sliced when no worker is warming or every worker is (a young
fleet). Drill (the fleet older than the warm-up window when the fault lands, as a production fleet
is): with a trickle of unique prompts at 10 req/s after the restart, the returned worker served its
first request 0.3 s into the trickle, on the first slice decision; the run made 20 slice decisions
in all, and the returned worker applied 2,707 batches in the remaining 80 s.

**Gateway restart.** Measured in phases with the fleet re-registered by the drill: `/health` up
in 0.3 to 1.4 s, eight registrations accepted within 0.1 s of that, the first worker routable at
0.6 to 1.9 s, the whole fleet at 0.7 to 2.2 s (0.7 s on the evening run), whether the load generator is capped at 5 or 40
req/s per process or runs closed-loop. The 12 and 20 s restarts seen earlier were not reproducible once the fleet was
measured on uncontended cores; the control plane is not the bottleneck at this scale. Two
observations stand: nothing persists worker registrations across a restart, so an operator (or
service discovery) must re-register, and the KV event subscriptions start over with no cursor, so
the index knows a worker's blocks only as its servicer hands them over (the mock replays its
buffer; the Rust relay serves its history or its state snapshot; the Python bridge, which relays
per call and keeps nothing between calls, serves live events only).

## Fault drills

`~/smg-perf/chaos/` runs the contract's section 5 against a fleet of `mock-worker --engine
realistic` gRPC workers that publish KV events, with health checks on. **No root is needed**: a
crash is `SIGKILL`, a frozen worker is `SIGSTOP`/`SIGCONT`, a partition or an unreachable peer is a
user-space TCP proxy (`tcp-proxy.py`) between gateway and worker switched to `blackhole` or
`refuse`, CPU starvation is busy loops pinned to the worker's cores, overload is eight times the
harness's stream count, and the gateway restart is a kill plus re-registration. Four more drills
use the mock engine's admin fault hooks (`crates/mock_worker/README.md`:
`POST /admin/fault/{worker}/drop?batches=N`, `delay?ms=D`, `restart-publisher`, `pause`,
`resume`) and skip when the build lacks them. `RELAY=1` runs the same fleet as vLLM-wire ZMQ
EngineCore ranks with the Rust servicer (`crates/engine_servicer`, through the `smg` Python
binding) in front of each and the gateway subscribing to KV events through the servicer's relay.
Each drill prints `RESULT: PASS|FAIL|SKIP` with the measured numbers and appends to
`~/smg-perf/results/chaos.tsv`; drill logs carry wall-clock stamps that line up with the gateway's
and the relays' logs, and every metric sample a drill polls is traced to `.metric_trace` in its
result directory. Timings are read from the gateway's counters (a resubscription from
`smg_kv_event_subscriptions_total`), never from its log, which goes through a lossy non-blocking
channel. The relay rules are written for a relay with history: a drop passes when the relay's
`publisher_gaps` is at least 1 with `gap_batches_lost` 0, the gateway's unrecovered count is 0 and
the worker's applied batches keep growing after the fault (the gateway sees no gap at all); a
publisher restart passes when a resync is counted after the fault against a baseline taken before
it is armed. The direct-path rules are unchanged.

Hook drills, direct and through the relay (events lost, time to detection, detecting counter;
the relay column is the 2026-10-05 evening run with the relay's bounded history, the earlier
history-less numbers in parentheses):

| Drill | Direct gRPC mock | Through the Rust relay |
|---|---|---|
| 20 batches dropped | gap seen in 0.6 to 0.8 s (`gaps_total{replay_requested}`), recovered by replay (20 missed, replayed), 1,299 batches applied after the fault, hit/oracle 88 to 92 % | the relay sees the gap 0.4 s after it is armed (`publisher_gaps` 1) and refills it from the engine's replay socket (`gap_batches_recovered` 21, `gap_batches_lost` 0); the gateway sees no gap (`replay_requested` 0, unrecovered 0, missed 0) and applied 1,189 batches after the fault; hit/oracle 91.3/93.6 -> 92.6/95.0 % (without history: gap seen in 0.4 s, 41 to 87 batches settled as lost, rank degraded) |
| publisher restart (cache kept) | `resyncs_total{publisher_restart}` in 0.2 to 0.3 s, batches keep applying | the relay reads the restart on the next batch (`publisher_restarts` 0 -> 1) and ends the stream with `DATA_LOSS`; `resyncs_total{data_loss}` counted 0.0 s after the fault was armed, 31 batches applied after it, nothing stale; hit/oracle 91.2/93.6 -> 92.5/94.8 % (without history: counted in 0.0 s in one run, missed in two) |
| 1.5 s publish delay | mean apply lag 1.41 to 1.51 s over 269 to 429 batches | mean apply lag 1.48 s over 525 batches; hit/oracle 90.9/93.3 -> 92.4/94.9 % (without history: 1.44 to 1.51 s) |
| engine paused 8 s | `stalled=wedged` 3.0 to 3.2 s in, routable 0.0 to 0.4 s after resume | (direct only) |

Relay state snapshot drills (2026-10-05 evening, gateway and relay from 85f9d28c, eight ZMQ mock
ranks behind the Rust servicer with `SMG_KV_EVENT_HISTORY_BATCHES=100` so the window has rolled,
engine truth from `GET /admin/cache/{worker}`; the index is compared to the engine within 1 % or
32 blocks while the load runs, because the two sides are read tens of milliseconds apart while the
engine stores, and exactly once the load is over):

| Drill | Result |
|---|---|
| gateway restart under load (`drill-gateway-restart.sh`, `RELAY=1`) | serving again 0.6 s after the kill (`/health` 0.4 s, registrations 0.5 s); every worker resynced from a relay snapshot (`resyncs_total{snapshot}` 1 each, 5 to 6 chunks for the busy workers); `smg_kv_index_blocks` within 1 % of the engines' resident counts 1.9 s after the registrations (seven of eight exactly equal under load) and exactly equal for all eight once the load ended (4,857 to 5,079 blocks per worker); cached/oracle 0.974 over the next 10 s, the steady-state value; p99 0.62 to 0.61 s |
| gap beyond the window (`drill-gap-beyond-window.sh`) | a second gateway keeps the fleet busy; the first gateway's link to the busiest worker's relay is blackholed for 15 s (unreachable veto 2.0 s in, back 0.0 s after the heal); on reconnect its cursor lay below the window: `OUT_OF_RANGE`, resubscription from zero, snapshot resync 17.3 s after the cut (the 15 s of blackhole plus the reconnect), index equal to the engine's resident set at once (1,468 = 1,468 under load, 2,266 = 2,266 after it); nothing settled as unrecovered, no degraded rank, no relay gap |

Three things the drills showed. The snapshot is the relay's knowledge: when the engine's own replay
cannot refill a gap (150 batches dropped on the wire against a 40-step replay buffer) the relay
keeps a hole and its snapshot lacks what the hole held (3,982 blocks against the engine's 4,230);
no subscriber can do better, a real engine's 10,000-step replay makes the case an artefact of the
drill, and the index equals the engine wherever the relay saw every batch. The relay's ZMQ
subscription joins after the publisher binds, and a PUB drops what it sends before a SUB has
joined: in a fleet loaded the instant it registered, two near-idle workers' only batches were lost
(the relays relayed nothing), their relays had no state, and the gateway came back with 0 against
13 and 26 resident blocks; two seconds of settling before the load avoids it in the drill, and the
relay should seed itself from the engine's replay socket at start, where those batches are. With
one gateway a cut idles the engine, so the window never rolls past the cursor and the resume is
served from history (exact); the window rolls only while the engine publishes for someone else,
which is why the drill runs a second gateway, the shape of a multi-router deployment.

## Publication drill table

One pass of the contract's section 5 on the fleet the publication cites, 2026-10-05 evening, gateway and
mock engines built from the pushed head (binaries named below by sha256), eight `mock-worker --engine
realistic` workers on cores 72-143 with the gateway and the load generators on 64-71 (both shared
with two other lanes' runs that evening), health checks 10 s apart so only the liveness vetoes act,
load 8 x oha at 8 streams each (64 streams; 512 for the overload row), the fleet settled 2 s after
registration before the load (the relays' ZMQ subscriptions join first). The relay topology runs the
same mocks as vLLM-wire ZMQ ranks behind the Rust servicer (`crates/engine_servicer` through the
`smg` binding, wheel named below) with a 100-batch history window so a resubscription from zero
receives the relay's state snapshot. Sources: routing state (`is_healthy`, `stalled`) from
`GET /workers` sampled every 100 ms; every event-side timing from the Prometheus counters sampled
every 200 ms (`smg_kv_event_subscriptions_total`, `smg_kv_event_resyncs_total{reason}`,
`smg_kv_event_gaps_total{outcome}`, `smg_kv_event_lag_seconds`, `smg_kv_index_blocks`,
`smg_worker_stalled{reason}`); engine truth (admitted requests, cached and oracle tokens, resident
block keys) from the mock's admin API; nothing from the gateway's log. Budgets are the pass rules as
the drills implement them (`~/smg-perf/chaos/`), with the contract's 2 s exclusion given 0.5 s for
the drill's own sampling.

Two caveats. The drills compare block counts and the engines' own hit ratio, not block hashes: the
mock's dump is its engine keys while the index keeps content hashes, and a gateway dump endpoint is
proposed, not started (section 6 below). The fleet's steady cached/oracle ratio is 0.974 in every
run; the recovery checks return to that value, and the gap to 0.98 is the workload's (eight fixed
prompts whose first visit on each worker is a miss the oracle counts), not a recovery matter.

Binaries (built from the pushed head c719dea6; the loop head moved to ce53d732 before this table was
written with changes to `kv_index`, the mock and the documents only, so the gateway under test is
the one these commits describe): `smg` sha256
9f20d1001291b935ece05644d296faeb3c7febd5d85692479740e02367e60704, `mock-worker`
4e6935b78cd27b91175857b21d7d2d393cdacbded673bdbe0fd5256a96fa28a2, servicer wheel
`smg-1.11.0-cp38-abi3-linux_aarch64.whl` 2469e4864bce401d288edb5aff8d701cdc3cf0240c08b9d446ce3152ef31a41a
(its `smg_rs.abi3.so` 3f1e11c94c34384561e9b01123af42b94f74da9cae60b5951a3f8be463b61f86). Results and
every log under `~/smg-perf/results/chaos-*-20261005-21{29..46}*/`, verdicts in `chaos.tsv` between the
`PUBLICATION-DIRECT-START` and `PUBLICATION-END` markers.

| Drill | Topology | Pass rule (budget) | Measured | Detected by |
|---|---|---|---|---|
| engine killed (SIGKILL) | direct | out of routing <= 2.5 s (the contract's 2 s plus 0.5 s for the drill's sampling); no hung stream; slowest request < 30 s | PASS: out of routing in 2.1 s; 0 errored requests around the kill; slowest 0.70 s; hit/oracle 91.9/94.3 -> 92.3/95.0 % in the afternoon pass | `smg_worker_stalled{reason="unreachable"}` from the KV stream's reset |
| worker partitioned (blackhole) | direct | out of routing <= 2.5 s; back after the heal; slowest < 60 s | PASS: out in 2.0 s, back 0.1 s after the heal; slowest 2.65 s | same, by keepalive failure |
| worker unreachable (listener closed, connections reset) | direct | out of routing <= 2.5 s; back after the listener returns; slowest < 30 s | PASS: out in 2.2 s, back 0.3 s; slowest 0.77 s | same |
| engine restarted (same port, empty cache; the fleet older than the warm-up window) | direct | out of routing <= 2.5 s; re-admitted <= 2.5 s after the port answers; stream resubscribed <= 2.5 s; first request <= 10 s into a 10 req/s trickle of unique prompts; the publisher restart counted as a resync; batches apply after | PASS: out in 2.0 s; re-admitted 0.8 s after the port answered, stream resubscribed at 1.0 s; first request 0.3 s into the trickle (warm-up slice); restart resync 1.1 s after the port answered; 1,422 batches applied after | `smg_kv_event_subscriptions_total`, `smg_kv_event_resyncs_total{reason="publisher_restart"}`, the engine's admitted-request count |
| engine paused 8 s, health and load polls answering | direct | vetoed as wedged within the pause; routable after resume; gateway RSS growth < 512 MB | PASS: `wedged` 3.1 s into the pause; routable 0.0 s after resume; slowest 5.86 s; RSS +49 MB | `smg_worker_stalled{reason="wedged"}` (prefill-aware bound at the 3 s floor for chat-sized prompts) |
| gateway restarted under load (SIGKILL, fleet re-registered) | direct | serving again <= 10 s; p99 over the run spanning the restart <= 2x the steady p99 + 0.5 s; no hung stream | PASS: serving again 0.8 s (`/health` 0.4 s, registrations 0.5 s, first worker 0.6 s); p99 0.58 -> 0.64 s (load capped at 40 req/s per generator) | `/health`, `/workers` |
| 20 event batches dropped on the wire | direct | gap detected (`replay_requested` >= 1); batches apply after | PASS: gap seen 0.6 s after the drop, replayed by the engine (20 missed, recovered); 738 batches applied after | `smg_kv_event_gaps_total{outcome="replay_requested"}`, `smg_kv_event_missed_batches_total` |
| publisher delayed 1.5 s for 10 s | direct | mean apply lag over the window >= 0.75 s; batches apply | PASS: mean lag 1.49 s over 267 batches | `smg_kv_event_lag_seconds` |
| publisher restarted (cache kept, sequence back to 1) | direct | a resync counted after the fault (baseline before arming it); batches apply after; **and** the worker is routed to again within 10 s and its applied counter keeps climbing (added 2026-10-06 after the soaks) | FAIL as of fe58b2a2, request success only: `publisher_restart` resync 0.2 s after the restart and 14 batches applied after it, but the soaks (s5, s6) showed the emptied worker never routed to again (its index empty, every prompt holds an overlap elsewhere, and the warm-up slice keys on admission) and its applied counter frozen while the mock kept publishing; both under repair in the recovery lane | `smg_kv_event_resyncs_total{reason="publisher_restart"}`, engine admission count, `smg_kv_event_batches_total{disposition="applied"}` |
| engine frozen 8 s (SIGSTOP, then SIGCONT) | direct | no hung stream; back after SIGCONT; RSS growth < 512 MB | PASS: back 0.0 s after SIGCONT; slowest 2.05 s; RSS +46 MB over the stall | `/workers`, `smg_kv_event_lag_seconds` |
| engine CPU-starved 15 s (eight busy loops on its two cores) | direct | no hung stream; slowest < 60 s; healthy after the hogs; RSS growth < 512 MB | PASS: healthy 0.0 s after the hogs stopped; slowest 0.70 s; RSS +58 MB | `/workers` |
| overload, 512 streams (8x the harness's 64) | direct | no hung stream; slowest < 60 s; RSS growth < 1,024 MB | PASS: p99 1.39 s, slowest 1.45 s, RSS +96 MB | load generator, RSS |
| gateway restarted under load (history window 100, rolled) | relay | as direct, plus: `smg_kv_index_blocks` within 1 % of each engine's resident count <= 2.5 s after the registrations and equal once the load is over; cached/oracle >= 0.98 or back at the steady ratio over the next 10 s (a `snapshot` resync per worker whose window rolled, reported) | FAIL on the c719dea6 relay: serving again 0.6 s; the five busy workers exact 0.9 s after the registrations (4,171 to 4,821 blocks) and exact once the load was over (6,654 to 7,849), a snapshot resync each; cached/oracle 0.974 (steady 0.973); but three near-idle workers came back with 0 against 13, 13 and 14 resident blocks: their relays' ZMQ subscriptions joined after the batches the mock published at registration, so two relays had no state and one a single block (served `through=4 blocks=1`). Fixed in the servicer lane's ec2121b7 (the relay replays from zero when its first batch is past sequence 1): re-run below | `smg_kv_event_resyncs_total{reason="snapshot"}`, `smg_kv_index_blocks`, `GET /admin/cache` |
| gap beyond the relay's window (100 batches; the first gateway's link to the busiest relay cut 15 s while a second gateway keeps the engine publishing) | relay | a `snapshot` resync after the cut; nothing settled as unrecovered; no degraded rank; index within 1 % <= 10 s and equal once the load is over | PASS: out of routing 2.0 s into the cut, back 0.0 s after the heal; the cursor lay below the window: `OUT_OF_RANGE`, resubscription from zero, snapshot resync 17.3 s after the cut (15 s of blackhole plus the reconnect), one chunk; index within 1 % 0.4 s later and equal once the load was over; no unrecovered gap, no degraded rank | `smg_kv_event_resyncs_total{reason="out_of_range"}` then `{reason="snapshot"}`, `smg_kv_event_gaps_total`, `smg_kv_index_blocks` |
| 20 event batches dropped on the wire | relay | relay `publisher_gaps` >= 1 with `gap_batches_lost` 0; gateway unrecovered 0; applied batches keep growing | PASS: the relay saw the gap 0.6 s after it was armed and refilled it from the engine's replay (recovered 21, lost 0); the gateway saw no gap (`replay_requested` 0, unrecovered 0, missed 0); 865 batches applied after | relay counters, `smg_kv_event_gaps_total` |
| publisher delayed 1.5 s for 10 s | relay | mean apply lag over the window >= 0.75 s; batches apply | PASS: mean lag 1.48 s over 420 batches | `smg_kv_event_lag_seconds` |
| publisher restarted (cache kept) | relay | a resync counted after the fault; batches apply after; the worker routed to again within 10 s | FAIL as of fe58b2a2 for the same reason, request success only: the relay reads the restart on the next batch and ends the stream with `DATA_LOSS`, `data_loss` resync 0.2 s after the restart, 12 batches applied after it, none stale; routing back to the emptied worker is the open P1 | `smg_kv_event_resyncs_total{reason="data_loss"}` |

Re-run of the relay gateway-restart row on the servicer lane's ec2121b7 (the relay replays from
zero when its first batch is past sequence 1): still FAIL for the other half of the race, four
near-idle workers whose relays never saw a batch came back at 0 (the two busy ones exact). Closed on
the batch-8 head fe58b2a2 (the relay takes the publisher's replay at start, retrying until the
replay socket answers, and again on a from-zero subscription while it holds nothing; a926881d),
binaries built from fe58b2a2: `smg` sha256
fca62029018c3eb9697aeb4999702a4972fced6001b60f2f5191b561f233bc40, `mock-worker`
a7196b61a6fda02b180889e74a355e20c2a1ed4114fadd0d1ab004f12c072a03, wheel
6ad83f3f2a1a10bb38fc8e54b93b67f6b8bf06c18169283745cbc2373f895067 (`smg_rs.abi3.so`
2d2b1aacd3d7d921d0091473372e933c059d2309ea6e9c64252f967516670780), no settle before the load
(02:41, `~/smg-perf/results/chaos-gateway-restart-20261006-024138`): PASS. Serving again 0.4 s
(`/health` 0.2 s, registrations 0.3 s); five relays primed from the publisher's replay at start
(56, 85, 65, 65, 65 batches), every worker's index within 1 % of its engine's resident set 0.8 s
after the registrations (all eight exactly equal already: 6,405, 9,734, 1,700, 1,524, 1,603 and three
idle workers at 0 = 0) and equal once the load was over (10,738 to 14,965 on the busy five); five
snapshot resyncs; cached/oracle 0.974 over the next 10 s (steady 0.973); p99 0.81 -> 0.80 s. The
direct topology never had the gap: the mock replays its buffer to a subscriber from zero.

## Comparison with Dynamo (`lib/llm/src/kv_router/indexer/recovery/`)

| Property | Dynamo | SMG after this branch |
|---|---|---|
| Cursor | per (worker, dp_rank), `Initial`/`Live` | same |
| Gap | one recovery request; the worker answers `Events` or a full `TreeDump`; live tail buffered <= 1,024 during recovery | one replay request; settled if the server skips ahead (kept or cleared by size); tail buffer ready, snapshot pending server support |
| Publisher restart | new incarnation from discovery triggers a rank reset with a barrier | detected from the stream (fresh-connection rule, clear below cursor, counter at its start, far-below window) and from the servicers' `DATA_LOSS`; epoch proposed below |
| Resync | transactional per-rank replacement from a tree dump | clear + live rebuild today; atomic replacement once snapshots exist |
| Worker removal | broadcast to all lanes, full-tree sweep | O(worker's blocks) via the reverse map |
| Lag metric | none found | `smg_kv_event_lag_seconds` |

## Proposed additions

### 1. Publisher identity on every batch

```proto
message KvEventBatch {
  uint64 sequence_number = 1;
  double timestamp = 2;
  repeated KvCacheEvent events = 3;
  optional int32 dp_rank = 4;
  // New: changes whenever the publisher (engine process) restarts. Sequence numbers are only
  // comparable within one epoch. Servicers derive it from the engine process start time or a
  // random 64-bit value chosen at publisher creation; the mock engine's `generation` is one.
  optional uint64 publisher_epoch = 5;
  // New: set on the first batch of an epoch whose cache survived the restart (a publisher
  // restart without an engine restart), so the subscriber keeps its blocks and only renumbers.
  optional bool cache_retained = 6;
}
```

With an epoch the gateway no longer infers restarts from the stream, and an epoch change is a
precise "this rank's cache is empty" signal unless `cache_retained` says otherwise.

### 2. Resume with intent

```proto
message SubscribeKvEventsRequest {
  // Last sequence applied (unchanged semantics); meaningful only with has_cursor.
  uint64 start_sequence_number = 1;
  // New: distinguishes a cursor of 0 from "no cursor".
  bool has_cursor = 2;
  // New: the epoch the cursor belongs to; the server replays only if it matches.
  optional uint64 publisher_epoch = 3;
  // New: when the server cannot replay after start_sequence_number, send a snapshot of the
  // rank's current blocks first (see 3) instead of failing with OUT_OF_RANGE / DATA_LOSS.
  bool snapshot_if_unreplayable = 4;
  // New: which data-parallel rank to subscribe to (today: rank 0 only, or all ranks on one
  // stream). A separate stream per rank keeps each publisher's sequence space contiguous and
  // lets a cursor name one rank.
  optional int32 dp_rank = 5;
}
```

### 3. Snapshots on the stream (implemented 2026-10-05, Rust relay and gateway)

```proto
message KvEventBatch {
  ...
  KvSnapshotChunk snapshot = 7;   // set on every chunk of a relay state snapshot
}
message KvSnapshotChunk {
  uint32 index = 1;   // 0-based chunk position
  uint32 count = 2;   // chunks in the snapshot
  uint64 blocks = 3;  // live blocks in the whole snapshot, physical copies counted
}
```

The relay builds the snapshot from its own record of the stream it relays (one entry per live
rank, tier and engine hash, with the parent, tokens, LoRA id, cache level, extra keys and the
store's tier, medium, group, locality, ownership, session and namespace; copies per block capped
at 8 like the gateway's counter), not from the engine, so it costs the engine nothing and needs
no engine API; it is exact for everything the relay saw, and a hole in the relay's window (a gap
the engine's replay could not fill) leaves it as inexact as the live stream was. The cut is
atomic: the entries are collected (an `Arc` clone each) under the lock that admits batches,
together with the relay's cursor; ordering, run merging and encoding happen outside the lock as
the subscriber polls, so a slow snapshot reader never holds the publisher task. Each rank's
entries sit in a dense slot vector in store order under a hash index, so the collection is a
sequential scan: for a 676k-block state (the GB300 8B workers' pool) it holds the lock for 11-13 ms
in a release build on a Grace host (28 ms when it iterated a hash map; 160 ms in a debug build),
and a second live subscriber's worst batch latency during the snapshot is exactly that figure
(0.02 ms median otherwise); the ordering pass then takes about 0.5 s on the blocking pool and the
331 chunks encode in 50 ms as they are polled (`a_676k_block_snapshot_is_cut_atomically_...` and
`a_snapshot_of_a_large_state_is_one_brief_pass` print the figures).

Differences from the proposal above: the marker is a message with the chunk's position and the
block total instead of two flags; chunk 0 carries the `AllBlocksCleared` itself; the chunks are
stamped `cursor - count + 1 ..= cursor` (consecutive, so the gateway's existing per-rank cursor
takes them without a special case and a gateway that predates the marker still applies them in
order); a snapshot is served only to a subscription from zero, a stale cursor below the window
stays `OUT_OF_RANGE` and gets the snapshot on the resubscription from zero (one round trip more,
no capability negotiation, no loop for an older gateway); the gateway's tail buffer is not
involved because the stream is ordered. Nothing is returned for an impossible snapshot: the
relay always has a record (empty at worst, which is a snapshot of one clear).

### 4. Capabilities

Rather than probing with a request and interpreting an error, the first message of a stream (or a
tiny `KvEventStreamInfo`) states `replay_supported`, `snapshot_supported`, `retained_batches`
(replay buffer size) and `block_size`. The gateway uses them to pick a recovery path without a
failed round trip and exposes them as metrics.

### 6. Pushed load updates (design note, 2026-10-05; option A built the same night, see the end)

**Why.** The load-aware policies (`least_load`, `cache-aware-balanced`) score on five numbers the
gateway polls per worker with `GetLoads` every `load_monitor_interval` (10 s by default): queued
requests, queued uncached token-work, running requests, token usage, generation rate. On the GB300
fleet the 10 s picture produced the hot worker (one fallback target at KV 1.00 with a 15-38-deep
queue) and a 1 s poll removed it (`gpu-harness-e69487f8.md` section 6), but a 1 s poll is 10,000
RPCs per second per gateway at fleet scale and is ruled out. The policy lane's v2 already prices
in-flight work locally (token-work dispatched since the last report, reset when a report arrives)
and treats the engine's report as a slow correction; what it needs is that correction arriving when
the engine's state changes, not on a timer.

**Option A: piggyback on the KV-event stream.** Every batch the relay streams already crosses the
wire once per scheduler step of the engine, which is exactly when queue, running set, KV usage and
rate change. Add one message to `KvEventBatch` (field 8; 5-7 are taken or reserved):

```proto
message EngineLoad {
  uint32 running_requests = 1;         uint32 waiting_requests = 2;
  uint32 waiting_uncached_tokens = 3;  uint32 token_usage_permille = 4;
  uint32 gen_tokens_per_second = 5;    uint32 age_ms = 6;   // of the sample behind it
  bool heartbeat = 7;  // the batch carries no events and no new sequence: read the load, admit nothing
}
message KvEventBatch { ...; EngineLoad load = 8; }
```

The servicer (not the relay, which stays engine-neutral) attaches the record at send time from the
bookkeeping `GetLoads` answers from, per rank (`dp_rank` is on the batch already). Cost: six
varints and a bool, about 20-30 bytes per batch (at most 40), against batches of 7-27 KB on the
recorded engines (SGLang median 6.8 KB, vLLM median 26.7 KB; a 64-block store encodes to 3.4 KB):
under one percent. Rate: whatever the event rate is, 15-20 batches per second per worker on the 8B
fleet at 3x Mooncake, so the gateway sees a record every 50-100 ms per loaded worker, and none for
an idle one. Idle engines therefore need the heartbeat: a batch with no events, the last relayed
sequence as its stamp and `heartbeat = true`, sent when no batch went out for `heartbeat_interval`
(1 s, backing off to 5 s after two idle heartbeats; an idle engine's load does not move). At fleet
scale that is at most one 30-byte one-way message per second per idle worker on a stream that
already exists, no RPC, no server work beyond a timer; the gateway reads the load, skips admission
(the stamp is a duplicate by design) and treats a worker with no record for twice the interval as
having no fresh snapshot, which the policies already score from live in-flight alone.

**Option B: a `SubscribeLoads` server stream with on-change semantics.** One more long-lived stream
per worker (the connection is shared; HTTP/2 multiplexes it with the event stream), the servicer
pushing a record when a field moved past a threshold (a running or waiting request, 10 % or 4,096
tokens of queued token-work, 0.02 of token usage) and never more often than `min_interval`, plus a
heartbeat every `max_interval`. Decoupled from events, so it also serves engines that publish none
and keeps load updates flowing when the relay is replaying or snapshotting. Cost: bounded only by
`min_interval` in the worst case (10,000 workers at 100 ms is 100,000 messages per second per
gateway), so at fleet scale `min_interval` must be 500 ms or more, which is back to a slow picture
exactly in the bursts the thresholds are for; the steady-state rate under the thresholds is far
lower, but the bound is what has to be provisioned.

**Option C: both.** A for the hot path, B for the idle heartbeat and for event-less engines; two
code paths to reconcile on the gateway for one struct.

**Gateway reconciliation (either option).** A pushed record replaces the polled
`SchedulerLoadSnapshot` for its rank (same struct, same `smg_engine_*` gauges, same per-worker
aggregation across ranks) and resets the in-flight tally the way a poll does, so v2's shape is
unchanged and the correction merely arrives sooner: with records every 100 ms the in-flight term
covers 100 ms of dispatches instead of 10 s. One hazard the poll path shares and this makes
visible: a request dispatched just before the record was sampled is in neither the record nor the
tally once the tally resets. Keep dispatches stamped and reset only those older than the record's
sample (`age_ms` plus the gateway-side one-way latency, 20 ms covers it), so a request is counted
exactly once at every instant. A record older than the heartbeat bound demotes the worker to the
"no fresh snapshot" scoring, never to zero load.

**Built (option A).** `KvEventBatch.load` (field 8) carries `EngineLoad {running_requests,
waiting_requests, optional waiting_uncached_tokens (absent when the servicer cannot tell),
token_usage (the poll's double), gen_throughput, max_running_requests, age_ms, sample, load_only}`.
The Rust relay attaches it to every batch a subscriber receives (history, live and snapshot chunks)
from a `LoadSource` each servicer installs over its own `GetLoads` figures (the vLLM servicer's
queued token-work estimate included; SGLang and TokenSpeed report none yet), and while the
publisher is quiet a subscriber's stream sends `load_only` batches (no events, the last sequence
repeated): on a record change checked every 100 ms, and as a heartbeat after 1 s of silence,
backing off to 5 s after two unchanged heartbeats; the mock worker's direct gRPC stream does the
same from its `GetLoads` snapshot. The gateway's KV-event monitor hands every record to the worker
monitor as a poll of that worker received now (`apply_pushed_load`): the rank's entry in the
worker's report is replaced, the overload verdict, the wedged rule, the load-aware policies'
`update_loads` and the `smg_engine_*` gauges run at once, and the shared load snapshot is
republished in one rebuild per 100 ms window for every worker that pushed (the rebuild is O(fleet),
the records arrive per scheduler step). Every `WorkerLoadResponse`, polled or pushed, now carries
`sampled_at` (receipt time for a poll; receipt less `age_ms` and a 20 ms one-way margin for a
record), which the policy lane's time-aware in-flight tally resets against. A `load_only` batch
never enters admission (counted as `smg_kv_event_batches_total{disposition="load_only"}`); the
poll stays as it was, the floor for workers whose servicer predates the field.

**Recommendation, as decided.** A first: one optional message, the servicer already has the
numbers and the per-worker stream, the gateway already owns one task per worker on that stream
(`kv_event_monitor`) that can hand the record to the load state; B only if event-less gRPC engines
need load pushing. Open choices for the policy lane: the field set (the five above plus
`max_total_tokens` once, or `token_usage_permille` as proposed), the heartbeat interval and
backoff, whether a record on a snapshot chunk is wanted (it is the servicer's current load, which is
fine, but the chunk is not a scheduler step), and whether `GetLoads` stays as the probe for the
first report and for gauges. The poll stays as the fallback for workers whose servicer predates the
field.

### 5. Servicer-side changes that need no proto change

- Done on the Rust relay: a non-zero cursor it cannot honour is `OUT_OF_RANGE`, and one
  subscription per engine, taken when the servicer starts serving rather than at the first
  `SubscribeKvEvents` (`SMG_KV_EVENT_RELAY_START=lazy` restores the latter for a memory-constrained
  host; the record costs about 230 bytes per live block, 10 MB for an 8B worker's 42k-block pool,
  next to the history's 256 MiB cap), keeps a bounded history (the engines' `buffer_steps`,
  10,000, within a byte budget) that serves resumes before live events, and the live-block
  record that serves the state snapshot, so a servicer that outlives a gateway outage or restarts
  beside a warm engine knows the engine from its first batch; the vLLM, SGLang and TokenSpeed
  launchers all pass the publisher's `replay_endpoint`, so a late join is filled from the engine's
  replay on each of them, not only marked. Its counters are logged with every gap, restart and refusal (the servicer has no
  metrics endpoint): `relayed`, `undecodable_batches`, `served_from_history`, `served_snapshots`,
  `out_of_range`, `publisher_gaps`, `gap_batches_recovered`, `gap_batches_lost`,
  `publisher_restarts`, `subscribers_lagged`, `unknown_before_start`; a served snapshot logs one
  info line with the cut, the block and entry counts, the microseconds the lock was held and
  `unknown_before`; a late join logs `joined a publisher already counting` with what the replay
  recovered (info when it reached back to 0, warn with `unknown_before` otherwise). Cost of the record, measured
  by `crates/engine_servicer/benches/kv_relay_apply.rs` (release build, Grace host, cores shared
  with other builds, 8 ranks x 676k live blocks, batches of a 64-block chain of 1,024 tokens):
  resident set 906 MiB for the history (10,000 batches) and the normalizer's own per-hash record
  (176 B per live block) and 1,328 MiB more for the live-block record (257 B per live block);
  the publisher task's per-batch path (msgpack decode, normalize, history push) 70 us per batch
  storing one chain and evicting one (918k blocks/s on one core), 181 us with the record
  (354k blocks/s); a real relay behind a ZMQ publisher absorbed 36k batches/s (2.3M blocks/s)
  of 64-block stores with no gap. Read the counters next to the
  gateway's `smg_kv_event_gaps_total{outcome}` and `smg_kv_event_resyncs_total{reason}`: a drop
  drill should show `publisher_gaps` and `gap_batches_recovered` on the relay and
  `replay_requested` with no `unrecovered` outcome on the gateway; a restart drill
  `publisher_restarts` on the relay and `data_loss` or `publisher_restart` on the gateway; a fresh
  gateway on warm engines `served_from_history` or `served_snapshots` on every relay and, for the
  latter, one `snapshot` resync per worker on the gateway and no `event_miss` flood.
- The SGLang servicer should request replays in chunks below libzmq's send high-water mark so a
  long replay is not truncated into a second gap.

### 6. Router-to-router bootstrap (proposed, not started)

A gateway endpoint (`GET /kv-index/dump`, per worker) that serves the index's blocks the way the
relay's snapshot encodes them, with the cursor each rank's state stands at, so a second replica
bootstraps from a peer in one transfer instead of asking every relay for a snapshot, then
subscribes to the relays with the peer's cursors. The dump is exact to the peer's cursors and
reuses the snapshot's chunking and watermark rules; the relay snapshot stays the source of truth
when no peer is up.
