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
| Rust `engine_servicer` relay (vLLM, SGLang, TokenSpeed) | served from the relay's bounded history when the cursor is inside the window (the last `SMG_KV_EVENT_HISTORY_BATCHES` batches, 10,000 by default, within `SMG_KV_EVENT_HISTORY_BYTES`, 256 MiB), then live; `OUT_OF_RANGE` below the window, beyond the newest sequence, or before the relay's own start; a cursor of 0 gets the whole window when it is complete from the publisher's first batch, else live only | `DATA_LOSS` on a publisher restart (its sequence going backwards on the relay's socket); the relay's own gaps are filled from the engine's replay socket, and what the replay cannot give is a hole in the window that a resume skips, so the gateway settles it once instead of looping | none | relays rank 0 only |
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
| snapshot in flight | hold in the rank's tail (bounded at 1,024 batches; overflow forces another snapshot) |

A restart on one rank clears the whole worker because the pooled copies cannot be attributed to a
rank; the other ranks' cursors start over so their next batch is taken as a first one instead of
clearing the index a second time. `OUT_OF_RANGE` / `DATA_LOSS` from the server do the same for
every rank and resubscribe live. Worker removal hands the pooled state to one `remove_worker`
pass. A reconnect sends rank 0's cursor (the servicers replay rank 0); other ranks dedup what
arrives. The reconnect backoff caps at 5 s: a worker that restarts is healthy again within seconds,
and until its stream is back the blocks it stores are invisible to routing, because the servicers
resume after the cursor and never resend them.

Every decision is a metric: `smg_kv_event_batches_total{disposition}` (applied, stale,
tail_overflow), `smg_kv_event_gaps_total{outcome}` (replay_requested, unrecovered_kept,
unrecovered_cleared), `smg_kv_event_missed_batches_total`, `smg_kv_event_resyncs_total{reason}`
(out_of_range, data_loss, publisher_restart, gap_cleared), `smg_kv_event_lag_seconds` (publisher
stamp to apply), `smg_kv_event_degraded_ranks`, `smg_kv_event_tail_depth`.

What the gateway still cannot do without server help: learn which blocks changed during an
unreplayable gap, replace a worker's state atomically, tell a restarted publisher that kept its
cache from one that lost it, or know a worker's resident blocks after a gateway restart. All of
these need an epoch or a snapshot.

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
  the wedge threshold (`--worker-wedge-secs`, 3 s) veto the worker while its health keeps passing;
  the next token clears it. A paused engine that still answers health and `GetLoads` is exactly
  this case.

The veto is a routing flag next to the overload veto (`RoutingState::stalled`), visible as
`stalled` in `GET /workers`, `smg_worker_stalled{worker,reason}` and
`smg_worker_stall_transitions_total`. It never touches the health status.

Measured on the mock fleet with health checks 10 s apart, so only the veto can act (times from the
fault to the gateway reporting the worker out of routing, sampled every 100 ms):

| Fault | Before | With the veto | Detected by |
|---|---|---|---|
| worker killed | 3.8 to 5.3 s (health) | 2.3 s | `stalled=unreachable` from the KV stream's reset |
| worker unreachable (listener closed, connections reset) | 3.1 to 7.0 s | 2.1 to 2.2 s | same |
| worker partitioned (blackhole) | 2.3 to 30 s | 2.0 to 2.5 s | keepalive failure on the KV stream |
| worker restarted | 3.4 to 10.6 s | 2.6 s; stream resubscribed 0.2 s after it is back | same; health re-admits it at its next probe |
| engine paused, health answering | never | 3.2 s, routable 0.4 s after resume | `stalled=wedged` |

The 2 s threshold counts from the last contact; the keepalive adds up to 2 s before a partition
surfaces, the sweep up to 0.25 s, and the drill's own sampling 0.1 s.

**Gateway restart.** Measured in phases with the fleet re-registered by the drill: `/health` up
in 0.3 to 1.4 s, eight registrations accepted within 0.1 s of that, the first worker routable at
0.6 to 1.9 s, the whole fleet at 0.7 to 2.2 s, whether the load generator is capped at 5 or 40
req/s per process or runs closed-loop. The 12 and 20 s restarts seen earlier were not reproducible once the fleet was
measured on uncontended cores; the control plane is not the bottleneck at this scale. Two
observations stand: nothing persists worker registrations across a restart, so an operator (or
service discovery) must re-register, and the KV event subscriptions start over with no cursor, so
the index knows a worker's blocks only as the engine re-reports them (the mock replays its buffer;
real engines do not).

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
`~/smg-perf/results/chaos.tsv`.

Hook drills, direct and through the relay (events lost, time to detection, detecting counter):

| Drill | Direct gRPC mock | Through the Rust relay |
|---|---|---|
| 20 batches dropped | gap seen in 0.8 s (`gaps_total{replay_requested}`), recovered by replay, hit rate 88% to 91% | gap seen in 0.4 s, not recovered: the relay keeps no history, 41 to 87 batches settled as lost, rank degraded |
| publisher restart (cache kept) | `resyncs_total{publisher_restart}` in 0.3 s, batches keep applying | the relay ends the stream with `DATA_LOSS` and the gateway rebuilds; counted in 0.0 s in one run, missed in two (the restart only shows when the worker publishes again) |
| 1.5 s publish delay | mean apply lag 1.41 s over 269 batches | mean apply lag 1.44 to 1.51 s |
| engine paused 8 s | `stalled=wedged` 3.2 s in | (direct only) |

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

### 3. Snapshots on the stream

```proto
message KvEventBatch {
  ...
  // New: this batch is part of a snapshot of the rank's resident blocks, not a live event.
  // The first snapshot batch is preceded by a Cleared event; snapshot batches carry Stored
  // events in parent order; the batch with snapshot_complete = true carries the sequence
  // number of the last live event the snapshot includes (its watermark). Live events resume
  // after it with sequence_number = watermark + 1.
  optional bool snapshot = 7;
  optional bool snapshot_complete = 8;
}
```

A snapshot is produced from the engine's own view (SGLang's radix cache, vLLM's block pool via
the KV-event replay buffer plus the connector's resident set) so it is a consistent cut at the
watermark. If the server cannot build one, it answers `FAILED_PRECONDITION` with a message and the
gateway falls back to today's live rebuild, marked degraded.

Gateway behaviour with this in place (already coded, behind `RankState::begin_snapshot` /
`finish_snapshot`): on a gap the server cannot replay, request a snapshot; hold live batches in the
bounded tail while it streams (a tail overflow restarts the snapshot); replace the worker's blocks
atomically when the snapshot completes (needs `PositionalIndexer::replace_worker`, a swap of the
block map and index entries under one guard, to be added in the indexer workstream); then apply
the tail from the watermark.

### 4. Capabilities

Rather than probing with a request and interpreting an error, the first message of a stream (or a
tiny `KvEventStreamInfo`) states `replay_supported`, `snapshot_supported`, `retained_batches`
(replay buffer size) and `block_size`. The gateway uses them to pick a recovery path without a
failed round trip and exposes them as metrics.

### 5. Servicer-side changes that need no proto change

- Done on the Rust relay: a non-zero cursor it cannot honour is `OUT_OF_RANGE`, and one
  subscription per engine keeps a bounded history (the engines' `buffer_steps`, 10,000, within a
  byte budget) that serves resumes before live events. Its counters are logged with every gap,
  restart and refusal (the servicer has no metrics endpoint): `relayed`, `undecodable_batches`,
  `served_from_history`, `out_of_range`, `publisher_gaps`, `gap_batches_recovered`,
  `gap_batches_lost`, `publisher_restarts`, `subscribers_lagged`. Read them next to the gateway's
  `smg_kv_event_gaps_total{outcome}` and `smg_kv_event_resyncs_total{reason}`: a drop drill should
  show `publisher_gaps` and `gap_batches_recovered` on the relay and `replay_requested` with no
  `unrecovered` outcome on the gateway; a restart drill `publisher_restarts` on the relay and
  `data_loss` or `publisher_restart` on the gateway.
- The SGLang servicer should request replays in chunks below libzmq's send high-water mark so a
  long replay is not truncated into a second gap.
