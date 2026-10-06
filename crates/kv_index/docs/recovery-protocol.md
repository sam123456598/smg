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
| Rust `engine_servicer` relay (vLLM, SGLang, TokenSpeed) | served from the relay's bounded history when the cursor is inside the window (the last `SMG_KV_EVENT_HISTORY_BATCHES` batches, 10,000 by default, within `SMG_KV_EVENT_HISTORY_BYTES`, 256 MiB), then live; `OUT_OF_RANGE` below the window, beyond the newest sequence, or before the relay's own start, and the gateway clears and resubscribes from zero; a cursor of 0 gets the whole window when it is complete from the publisher's first batch, else the **state snapshot** below (the window rolled, has a hole, or the relay started after the publisher), then live; live only before anything was relayed | `DATA_LOSS` on a publisher restart (its sequence going backwards on the relay's socket); the relay's own gaps are filled from the engine's replay socket, and what the replay cannot give is a hole in the window that a resume skips, so the gateway settles it once instead of looping | in band, from the relay's record of the engine's live blocks (`crates/engine_servicer/src/kv_state.rs`, one entry per live rank, tier and hash, folded from the stream it relays): batches marked `KvEventBatch.snapshot` (`KvSnapshotChunk {index, count, blocks}`), chunk 0 beginning with `AllBlocksCleared`, then every live block as `Stored` events with the original store's fields, one event per physical copy, parents before children, cut atomically at the relay's cursor under the lock that admits batches and stamped with consecutive sequence numbers ending at that cursor, so live events continue at cursor + 1 with no gap and no duplicate | relays rank 0 only |
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
| batch marked `KvSnapshotChunk` (in-band relay snapshot) | **snapshot**: chunk 0 clears the worker's index state and every cursor and counts the resync; every chunk is applied outside the rules above and sets its rank's cursor to its stamp, so the live batch after the last chunk is `last + 1`; a stream that ends before the last chunk forgets the cursors, and the next subscription asks from zero for a whole snapshot |
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
(publisher stamp to apply), `smg_kv_event_degraded_ranks`, `smg_kv_event_tail_depth`.

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

### 5. Servicer-side changes that need no proto change

- Done on the Rust relay: a non-zero cursor it cannot honour is `OUT_OF_RANGE`, and one
  subscription per engine, taken when the servicer starts serving rather than at the first
  `SubscribeKvEvents` (`SMG_KV_EVENT_RELAY_START=lazy` restores the latter for a memory-constrained
  host; the record costs about 230 bytes per live block, 10 MB for an 8B worker's 42k-block pool,
  next to the history's 256 MiB cap), keeps a bounded history (the engines' `buffer_steps`,
  10,000, within a byte budget) that serves resumes before live events, and the live-block
  record that serves the state snapshot, so a servicer that outlives a gateway outage or restarts
  beside a warm engine knows the engine from its first batch. Its counters are logged with every gap, restart and refusal (the servicer has no
  metrics endpoint): `relayed`, `undecodable_batches`, `served_from_history`, `served_snapshots`,
  `out_of_range`, `publisher_gaps`, `gap_batches_recovered`, `gap_batches_lost`,
  `publisher_restarts`, `subscribers_lagged`; a served snapshot logs one info line with the cut,
  the block and entry counts and the microseconds the lock was held. Cost of the record, measured
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
