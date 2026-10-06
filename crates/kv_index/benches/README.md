# kv_index benchmarks

| File | What it is |
| --- | --- |
| `throughput_bench.rs` | Criterion micro-benchmarks of the indexers (see its module doc). |
| `mooncake_replay.rs` | Open-loop replay of a Mooncake indexer corpus by Dynamo's method, against this crate's indexers (`cargo bench -p kv-index --bench mooncake_replay -- --help`). |
| `../docs/protocol/bisect_sustained.py` | Threshold search for sustained throughput: brackets the highest offered rate at which trials keep up (3 fresh-process trials per point, geometric bisection to within 10%), for either harness. |
| `../docs/protocol/publish_protocol.py` | Guardrail 5 runner: N fresh-process trials with an interleaved same-binary control pair, the lock held per trial, a foreign-load check on the measurement cores before and after each trial, medians with bootstrap 95% confidence intervals, markdown output. |
| `../docs/protocol/hostload.py` | The foreign-load sampler the two scripts share (per-process CPU on a core set over a short interval). |

## Why a corpus and a replay

Dynamo's indexer benchmark (`lib/bench/kv_router/INDEXER_BENCH.md` in ai-dynamo/dynamo) defines
the measurement this crate compares itself against: the Mooncake trace replayed open loop, with
deadlines scaled into a window, 128 query lanes, events sharded by worker, and throughput counted
in block operations (requested, stored and removed block hashes) over the time from the start of
issue to the last completion, drain included. Reproducing that definition here has two steps:

1. Dynamo's binary prepares the schedule exactly as it would run it (trace parsing, duplication,
   deadline sort, dense ids) and, with the patch above, writes it to a file instead of running.
2. `mooncake_replay` loads that file and runs the same open-loop protocol against a backend behind
   a small trait, with no dependency on Dynamo's crates at measurement time.

The same file replays byte-for-byte against both harnesses, so the two can be checked against each
other (the parity table below) and the loop can measure new indexers here while Dynamo's binary
remains the arbiter for published comparisons.

## Corpus format: `SMGMCK01`, version 1

All integers are little-endian. One file holds one prepared schedule for one window; deadlines
are already scaled to that window. The replay rescales linearly when asked for another window
(`--benchmark-duration-ms`), which is what Dynamo does per trial.

| Section | Layout |
| --- | --- |
| Magic | 8 bytes `SMGMCK01` |
| Header | u32 version (1), u32 block_size, u64 reference_window_ns, u64 trace_duplication_factor, u64 trace_length_factor, u64 inference_worker_duplication_factor, u64 logical_workers (max worker id + 1) |
| Totals | 7 × u64: requests, stored_events, removed_events, cleared_events, request_blocks, stored_blocks, removed_blocks |
| Trace path | u64 length, UTF-8 bytes (informational) |
| Query hashes | u64 count, count × u64 local block hash |
| Stored blocks | u64 count, count × (u64 block_hash, u64 tokens_hash) |
| Removed hashes | u64 count, count × u64 block_hash |
| Operations | u64 count, then per operation: u32 id, u64 deadline_ns, u64 worker_id, u8 kind, kind-specific fields |

Kind-specific fields:

| kind | Fields |
| --- | --- |
| 0 query | u64 start into the query hash slab, u32 length |
| 1 stored | u32 dp_rank, u64 event_id, u8 has_parent, u64 parent (0 when absent), u8 has_start_position, u32 start_position (0 when absent), u64 start into the stored-block slab, u32 length |
| 2 removed | u32 dp_rank, u64 event_id, u64 start into the removed-hash slab, u32 length |
| 3 cleared | u32 dp_rank, u64 event_id |

Operations appear in Dynamo's issue order (deadline, queries before events at equal deadlines,
worker id, trace order) and ids are their dense positions; the loader verifies both, and the
totals. The export is deterministic: two exports of the same arguments hash identically.

## What the replay does

The protocol follows `mooncake_open_loop.rs` in Dynamo step by step:

- Query lane = `worker_id % query_lanes`. Event lane = round robin over `(worker_id, dp_rank)` in
  order of first appearance (`ThreadPoolIndexer`'s assignment). Event issuers own contiguous
  worker ranges (`contiguous_worker_issuer`); query lanes are sharded over the query issuers in
  contiguous ranges (one issuer by default, as in Dynamo).
- Before the trial: `malloc_trim`, a quiescence sleep (`--pre-run-quiescence-ms`, 5000), a pass
  over the corpus pages, thread pinning (`--issuer-cpus`, `--query-issuer-cpu`, `--backend-cpus`).
- Issue: start is 20 ms after the barrier; each issuer sleeps to the absolute monotonic deadline
  (`clock_nanosleep`, `TIMER_ABSTIME`) and spins the last `--issuer-spin-us`; the queries of a
  deadline are published before its events; `accepted_ns` is stamped after the handoff.
- Lanes record started/finished for lookups and finished for events. Completion edges give the
  maximum queue depth and outstanding updates; FIFO is checked per worker.
- Validity, as Dynamo: the generator is valid when nothing failed and the issue span is at most
  1.01 × window; `kept_up` additionally needs the last completion within 1.10 × window.
- Rates: `achieved_block_ops_per_sec = total_block_ops / (last_completion − start)`, drain
  included; `offered` divides by the window; `actual_issue` by the issue span. Percentiles are
  nearest-rank (p50, p99, p99.9, max). The result JSON uses Dynamo's field names, plus `harness`,
  `corpus`, `mirror_dynamo_costs`, `rejected_events` and a `provenance` object (argv, binary and
  corpus blake3, trace parameters).

Differences from Dynamo's runner, all on the harness side:

- Lanes are OS threads (query lanes park and are unparked by the issuer; event lanes block on
  `std::sync::mpsc`) instead of tokio tasks on a `Notify` and `flume` channels. Both are
  unbounded with one consumer per lane.
- `--issuer-threads` sets the number of event issuers (Dynamo derives it from `--issuer-cpus`),
  and `--query-issuer-threads` the number of query issuers. Dynamo publishes every lookup from one
  thread and its event issuers wait for that thread at each deadline; on this host that single
  thread caps the generator near 1.5B block ops/s whatever the number of event issuers (table
  below). With several query issuers the per-deadline flag becomes a pending count that each
  issuer retires for its share, so events still wait for every query of their deadline.
- `--mirror-dynamo-costs` (default on) charges the lanes what Dynamo's harness charges every
  backend: each event arrives as an owned payload in Dynamo's block layout (40 bytes per block,
  allocated before the trial) that the lane converts into this crate's 16-byte blocks and frees
  after the apply, and each lookup copies its hashes into this crate's hash type, as the adapter
  does inside Dynamo's binary. With it off, lanes read the corpus slabs and copy nothing; that is
  the cheapest way to drive a backend here, not a number comparable with Dynamo's.
- Backends: `positional` (this crate's `PositionalIndexer`), `reference` (the single-threaded
  exactness reference; small corpora only) and `null` (no indexer: the harness's own ceiling on a
  layout). A new index plugs in by implementing `ReplayBackend` (four slice-based methods).

## Commands

Export the standard corpus from a Dynamo checkout with the out-of-tree corpus-export patch applied
(the patch and the adapter that lets Dynamo's binary drive this crate's indexers are kept with the
measurement scripts, outside this repository; any backend subcommand works):

```
mooncake_bench lib/kv-router/traces/mooncake_trace.jsonl \
  --num-unique-inference-workers 128 --trace-duplication-factor 20 --trace-length-factor 4 \
  --query-lanes 128 --benchmark-duration-ms 3000 --export-corpus mooncake-w128-dup20-len4-3000ms.smgmck \
  positional
```

Replay it on Dynamo's competitor layout (issuers on 0-3, query issuer on 4, lanes on 5-63):

```
cargo bench -p kv-index --bench mooncake_replay --no-run
numactl --interleave=all target/release/deps/mooncake_replay-<hash> mooncake-w128-dup20-len4-3000ms.smgmck \
  --backend positional --query-lanes 128 --event-lanes 64 --issuer-threads 4 \
  --issuer-cpus 0-3 --query-issuer-cpu 4 --backend-cpus 5-63 --result-json-output result.json
```

Harness ceiling for a layout (no indexer): 16 event issuers, 4 query issuers, 50 ms window from
the same corpus:

```
numactl --interleave=all target/release/deps/mooncake_replay-<hash> mooncake-w128-dup20-len4-3000ms.smgmck \
  --backend null --benchmark-duration-ms 50 --issuer-threads 16 --issuer-cpus 0-15 \
  --query-issuer-threads 4 --query-issuer-cpus 16-19 --backend-cpus 24-63 --result-json-output ceiling.json
```

`--offered-block-ops-per-sec <rate>` sets the window from the corpus's block-op total instead of
`--benchmark-duration-ms`, which is what a threshold search moves. One process per trial, as
Dynamo's method requires.

## Sustained throughput: the threshold search

The contract defines sustained throughput as the highest offered rate at which a trial keeps up
(generator valid, achieved at least 99% of offered). A window-driven replay only says "keeps up
at this window", so the threshold has to be bracketed:

```
python3 docs/protocol/bisect_sustained.py --lock /tmp/measure.lock --out out/bisect \
  --lo 107e6 --hi 427e6 --trials 3 --tolerance 0.10 \
  --command "numactl --interleave=all <mooncake_replay> <corpus> --backend positional ... \
             --offered-block-ops-per-sec {rate} --result-json-output {json}"
```

`--lo` must keep up and `--hi` must fail (`--verify-ends` checks both first); each point runs
three fresh processes and passes only if all three keep up; the search moves the geometric
midpoint until the bracket is within the tolerance and writes `bracket.json` and `bracket.md`.
For Dynamo's binary use `{window_ms}` in the template with `--total-block-ops` (the corpus
total, 320,105,993 for the standard corpus), and the script derives the window per point.

First brackets on the development host (competitor layout, 3 trials per point, every trial of a
point must keep up; host daemons recorded as foreign load on every trial):

| Indexer, harness | Keeps up at | Fails at | Trials at the failing rate (achieved / offered) | Lookup p50 / p99 (us) at the kept-up rate |
| --- | --- | --- | --- | --- |
| SMG `PositionalIndexer`, SMG harness | 107.0M (3000 ms window, parity trials) | 116.7M | 99.6%, 99.2%, 94.8% | 14-17 / 58-138 |
| Dynamo CRTC, Dynamo harness | 682.2M | 727.2M | 99.3%, 97.5%, 99.7% | 3.5-3.6 / 14 |

Both searches ended on a point where one trial of three fell short while the other two kept up
(the SMG points at 127.2M and 116.7M: 96.5% and 94.8%; CRTC at 727.2M: 97.5%; at 826.4M one CRTC
trial achieved 82.3%). With the strict rule the brackets are conservative; the per-trial ratios
are in `bracket.json`, and a looser rule (two of three) would move both upper ends by one point.

## Publication protocol (guardrail 5)

```
python3 docs/protocol/publish_protocol.py --name "<system, harness>" --trials 20 \
  --lock /tmp/measure.lock --cores 0-63 --allow '<background daemon regex>' --out out/protocol/<tag> \
  --command "<one trial, with {json} for the result path>"
```

Each subject trial is followed by a control trial of the same command (or `--control-command`),
so the pair shows the noise floor an A/A comparison would show before any difference under 5% is
called. The lock is held per trial or, with `--lock-scope run`, for the whole run (one queue wait,
all trials of a series under the same conditions). The measurement cores are sampled for one
second before and after every trial: every process above 5% of a core is recorded with its peak,
and a trial is discarded (kept and listed with the offender) when a process above 50% of a core
is neither the trial nor allow-listed; discarded trials are replaced until each series has the
requested number of usable ones or twice that many were attempted. The summary gives medians with
percentile-bootstrap 95% intervals (10,000 resamples) of achieved block ops/s and lookup p50/p99
per series, the subject-minus-control difference with its own interval, the discarded trials and
why, and the background processes seen. The sampled rows are stored raw, so re-running on the
same output directory resumes an interrupted run and re-summarises finished trials under other
thresholds.

### Query issuers

Same corpus, null backend, 16 event issuers on CPUs 0-15, Q query issuers on 16..16+Q-1, lanes on
24-63; cells give the generator verdict and the rate actually issued:

| Query issuers | 150 ms (2.13B offered) | 75 ms (4.27B offered) | 50 ms (6.40B offered) | 35 ms (9.15B offered) |
| --- | --- | --- | --- | --- |
| 1 | invalid, 1.55B issued | - | invalid, 1.44B issued | - |
| 2 | valid, 2.13B issued | - | invalid, 2.62B issued | - |
| 4 | valid, 2.13B issued | invalid, 3.55B issued | invalid, 3.11B issued | - |
| 8 | - | - | invalid, 4.98B issued | invalid, 4.45B issued |

Event issuers at four query issuers, same layout:

| Event issuers | 150 ms (2.13B offered) | 50 ms (6.40B offered) |
| --- | --- | --- |
| 4 | invalid, 1.66B issued | invalid, 1.73B issued |
| 8 | valid, 2.13B issued | invalid, 3.11B issued |
| 16 | valid, 2.13B issued | invalid, 3.11B issued |

Sharding the query issuer moves the idle-lane ceiling from below 1.55B (one issuer, whatever the
number of event issuers) to 2.13B on schedule with two or four, and the generator issues 3.5-5B
block ops/s at shorter windows without holding the schedule; four event issuers cap near 1.7B,
eight are enough for 2.13B. These are worst-case numbers (every publish wakes a parked lane); with
a busy backend the same layouts issue far more on schedule, as the overloaded `PositionalIndexer`
rows above show.

## Measurement method for published numbers

This is the method guardrail 5 asks for, as the runner implements it; a publication can cite
this section and the `summary.json` files it produces.

- **Workload.** The standard corpus: Mooncake trace, 128 workers, duplication 20, length
  factor 4, 128-token blocks; 2,446,195 operations, 320,105,993 block ops. Dynamo's binary
  prepares the same schedule from the trace per trial; the SMG harness replays the exported file.
  The offered rate is the corpus's block ops over the window.
- **Layouts.** Competitor layout (Dynamo's documented one): event issuers on CPUs 0-3, query
  issuer on 4, 64 event lanes and 128 query lanes floating over 5-63. Scaled layout, for
  indexers beyond that generator's reach: 8 event issuers on 0-7, 4 query issuers on 8-11 (one,
  on 8, in Dynamo's harness), lanes on 12-63. Every system in one scoreboard entry runs on one
  layout, so backend cores are equal; the issuer side is scaled until the indexer, not the
  generator, is the limit. `numactl --interleave=all`, one process per trial, mimalloc in both
  harnesses, 5 s quiescence before each trial.
- **Points.** Sustained: the kept-up end of the threshold search (3 fresh processes per point,
  all must achieve at least 99% of offered with a valid generator, geometric bisection until the
  bracket is within 10%). Overload (capacity): at least twice the sustained rate for the SMG
  backends; the 300 ms window for the competitor, which its generator can still issue on schedule.
  Every point is reported with lookup p50 and p99 at that load.
- **Trials and controls.** 20 usable trials per series as fresh processes, each followed by a
  control trial of the same binary and configuration, interleaved, so the control series is an
  A/A measurement of the noise floor taken under the same conditions. No difference under 5% is
  called without the control pair showing a floor below it.
- **Statistics.** Medians; 95% confidence intervals by the percentile bootstrap with 10,000
  resamples of the trial medians; the subject-minus-control difference of medians carries its own
  bootstrap interval. Overload capacity is compared only within one harness.
- **Host conditions.** The measurement cores are sampled for one second before and after every
  trial. Every process above 5% of a core is recorded with its peak; a trial is discarded, kept and
  listed with the offending process, when a process above 50% of a core is neither the trial nor
  on the allow list of this host's permanent daemons (monitoring, proxies, session tooling, kernel
  threads). Discarded trials are replaced until the series has its 20 usable trials or twice that
  many were attempted. Series hold the measurement lock once, for at most 45 minutes per hold,
  announcing themselves in the lock's owner note.
- **Provenance.** Each result JSON carries the command line, binary and corpus hashes, the
  layout and the trace parameters; the summaries carry the discard list and the background
  processes seen, so a published row states its conditions.

## First protocol run (competitor layout)

Both systems through the runner above on the development host, competitor layout (issuers on
0-3, query issuer on 4, 64 event lanes and 128 query lanes on 5-63), 20 usable trials per series
with the interleaved same-binary control, one lock hold per series, lane cores sampled around
every trial (record 5%, discard 50%). The sustained points are the kept-up ends of the brackets
above; the 750 ms window offers 427M.

| System, harness | Load | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) | Lookup p50 [CI] (us) | Lookup p99 [CI] (us) | Subject minus control: achieved, p50, p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SMG PositionalIndexer, SMG harness | sustained bracket (107M offered) | 20 / 2 | 20 of 20 | 106.5 [106.2, 106.5] | 16.6 [16.5, 16.7] | 111 [105, 119] | -0.0 [-0.3, +0.4], -0.1 [-0.2, +0.1], +0.2 [-10.2, +8.8] |
| control (same binary) (SMG PositionalIndexer) | same | 21 / 1 | 17 of 21 | 106.5 [106.1, 106.5] | 16.7 [16.5, 16.8] | 111 [108, 116] | |
| Dynamo CRTC, Dynamo harness | sustained bracket (682M offered) | 21 / 5 | 16 of 21 | 679.8 [676.5, 679.9] | 3.3 [2.9, 3.4] | 13 [12, 14] | +1.6 [-2.5, +11.6], +0.0 [-0.4, +0.4], +0.1 [-1.4, +1.2] |
| control (same binary) (Dynamo CRTC) | same | 20 / 6 | 11 of 20 | 678.1 [668.2, 679.9] | 3.3 [3.0, 3.5] | 13 [12, 14] | |
| Dynamo CRTC, Dynamo harness | 750 ms window (427M offered) | 20 / 0 | 20 of 20 | 426.0 [426.0, 426.1] | 3.4 [3.3, 3.4] | 14 [13, 14] | +0.0 [-0.0, +0.1], +0.0 [-0.1, +0.1], +0.2 [-0.1, +0.4] |
| control (same binary) (Dynamo CRTC) | same | 20 / 0 | 19 of 20 | 426.0 [426.0, 426.0] | 3.4 [3.3, 3.4] | 13 [13, 14] | |

Overload (capacity) points on the same layout, added when their series completed:

| System, harness | Load | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) | Lookup p50 [CI] (us) | Lookup p99 [CI] (us) |
| --- | --- | --- | --- | --- | --- | --- |
| SMG PositionalIndexer, SMG harness | 750 ms window (427M offered, overloaded) | 22 / 4 | 0 of 22 | 138.5 [136.2, 140.4] | 14.8 [14.7, 14.9] | 225 [223, 226] |
| control (same binary) | same | 20 / 6 | 0 of 20 | 140.1 [138.7, 170.1] | 14.6 [13.4, 14.8] | 224 [221, 238] |
| Dynamo CRTC, Dynamo harness | 300 ms window (1.07B offered, overloaded) | 20 / 11 | 0 of 20 | 857.4 [807.4, 868.8] | 2.9 [2.9, 2.9] | 12 [12, 13] |
| SMG PositionalIndexer, SMG harness, cores 26 and 35 excluded from the lane mask | 750 ms window | 20 / 12 | 0 of 20 | 133.9 [131.9, 137.7] | 14.5 [13.4, 14.6] | 240 [234, 243] |

The control pairs put the noise floor at or below one unit in the last digit for throughput and
lookup p50 at sustained load; lookup p99 for the SMG indexer has a wider floor (about ±10 us at
20 trials), and overload capacity has the widest (the SMG 750 ms subject and control differ by
1.7M with an interval of [-31.7, +1.2]). Discarded trials were replaced; the reasons were
other users' jobs crossing 50% of a core (git operations, a source-control filesystem, a backup
agent, a load generator). Excluding cores 26 and 35 from the lane mask did not shrink the spread
(achieved-rate interval width 5.8M against 4.2M with the full mask; p99 widths 9 against 4 us),
so the full mask stays. At 750 ms the competitor still keeps up (99.8% of 427M); its capacity is
the 300 ms row.

### The competitor at its branch head (re-measured when its branch moves)

ai-dynamo/dynamo `rupei/crtc-writer-lookup` moved from `50bdb355f8` (every row above) to
`2b20fc1d35` on 2026-10-05: in the bench binary, an idle event lane now frees graveyard garbage
in `GRAVEYARD_NODES_PER_TASK` chunks while its channel stays empty instead of draining it all at
once (`drain_graveyard_while`), and crossbeam-epoch moves to 0.9.21; the other changes (a jemalloc
preload and huge-page opt-outs for the Python extension's heap, docs) are outside the binary. Rebuilt
with the same out-of-tree patches and features, competitor layout, 3 trials per point (21:10-21:45,
host load 27-50 with other users' jobs on the measurement cores): under local memory the kept-up
point is unchanged (859.7M passes 99.5-99.6%, 923.9M fails 97.2-99.5%); under `--interleave=all`
the first pass failed the published 682.2M point (99.5%, 98.5%, 98.9%), a bracket then gave 590.8M
kept up / 634.9M failing (one trial at 92.4% under foreign load) and a 20-trial series at 590.8M
read 587.9M [585.2, 588.5], 14 of 19 kept up with 10 of 29 attempts discarded, p50 3.5 / p99 14.0
us; but an interleaved same-session control of the two binaries afterwards kept up 3 of 3 for both
at 682.2M (old 99.2-99.6%, new 99.2-99.3%) and at 590.8M (99.1-99.7% both) with identical lookup
latencies (p50 3.2-3.6, p99 13-15 us); the first-pass failure and the lower bracket were the
host's foreign load during those minutes (results: `competitor-head/`, `bisect-dynamo-head2b20`,
`protocol/dynamo-sustained-head2b20`).

Quiet-host rows for the new head (02:15-02:56 on 2026-10-06, host load 16-22, the editor server
still present and costing discards), competitor layout, brackets with verified ends and 20-trial
series with interleaved controls (`bisect-dynamo-head2b20q[-localmem]`,
`protocol/dynamo-sustained-head2b20q[-localmem]`):

| Memory | Keeps up at | Fails at (trials) | Series at the kept-up point: achieved median [95% CI] (M) | Per lane core (M) | Kept up | Lookup p50 / p99 (us) | Control |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `--interleave=all` (published method) | 744.5M (430 ms; 692.8M 3 of 3 clean) | 800.0M (98.9%, 99.2%, 99.3%) | 739.1 [736.8, 740.8] | 12.5 | 13 of 20 (6 discarded) | 3.3 [3.1, 3.4] / 13.8 [13.0, 14.0] | 739.1 [735.6, 741.1], 14 of 21 |
| local (`cpunodebind=0 membind=0`) | 924.0M (346 ms) | 992.9M (97.4%, 96.6%, 98.0%) | 918.1 [916.1, 919.4] | 15.5 | 16 of 22 (3 discarded) | 2.7 [2.7, 2.7] / 9.9 [9.9, 10.0] | 917.9 [914.5, 918.9], 12 of 20 |

These are the competitor's top-of-stack rows: 744.5M interleaved against the 682.2M published for
`50bdb355f8` (+9%), 924.0M local against 859.7M (+7%), lookups unchanged. The gain is the host, not
the head: the measured `50bdb355f8` binary and the new one, interleaved at these points half an
hour later (`competitor-head/newpoints-*`), read 99.0/98.6/98.3% against 97.9/96.5/98.0% at 744.5M
and 99.5/98.2/99.4% against 99.4/99.4/98.8% at 924.0M, indistinguishable within trial noise and
both below the points' own series medians, so the two heads are one system at this protocol's
resolution and the row to cite for either is the 20-trial series above (kept up 13 of 20 and 16
of 22, the shortfalls being the editor server).

The chain index (formerly the run index) re-measured in the same conditions (06:52-07:28 on 2026-10-06, competitor layout,
the same harness build as the local-memory rows, kv-index `9f9c7c04`; the editor server closed,
the soaks running from 07:00; `bisect-dynrun-quiet[-localmem]`, `protocol/dynrun-sustained-quiet[-localmem]`):

| Memory | Keeps up at | Fails at (trials) | Series at the kept-up point: achieved median [95% CI] (M) | Per lane core (M) | Kept up | Lookup p50 / p99 (us) | Control |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `--interleave=all` | 1,067.0M (300 ms) | 1,163.6M (98.7%, 99.4%, 93.4%); 1,509M and 2,134M generator-invalid | 1,063.6 [1,063.3, 1,063.9] | 18.0 | 19 of 20 (0 discarded) | 1.3 [1.2, 1.3] / 3.7 [3.4, 4.0] | 1,063.4 [1,062.8, 1,063.6], 19 of 20 |
| local | 1,383.8M (231 ms) | 1,509.0M (99.5%, 98.6%, 98.8%) | 1,379.3 [1,377.1, 1,380.0] | 23.4 | 16 of 20 (5 discarded) | 1.3 [1.3, 1.3] / 3.6 [3.6, 3.6] | 1,379.0 [1,377.2, 1,380.0], 18 of 21 |

Equal cores, one binary build, 20 trials on both sides, the same night: under Dynamo's published
method the chain index sustains 1,063.6M against the competitor's 739.1M (1.44x; 18.0 against
12.5M per lane core) at lookup p99 3.7 against 13.8 us; under local memory 1,379.3M against
918.1M (1.50x; 23.4 against 15.6M per lane core) at p99 3.6 against 9.9 us. Both of the run
index's points are the same as measured earlier under load (1,061.7M at 300 ms, 1,383.8M), so its
numbers did not move with the host while the competitor's did. The interleaved bracket ends on a
point the generator cannot issue (150 ms and 212 ms windows invalid in Dynamo's harness), so
1,067M is "keeps up at the highest offered rate this harness issues on schedule with one query
issuer", not the index's ceiling.

## Scaled layout: equal backend cores and same-binary rows

The competitor layout gives the two harnesses different lane sets and leaves the generator as the
limit past about 1.5B block ops/s (one query issuer, see Issuer scaling). The scaled layout fixes
both: 8 event issuers on CPUs 0-7, 4 query issuers on 8-11 in the SMG harness (one, on 8, in
Dynamo's), and 64 event lanes plus 128 query lanes on 12-63 for every system, so every row below
ran on 52 lane cores and every total divides by the same number. The rows a publication cites are
the same-binary ones: Dynamo's `mooncake_bench` running its CRTC and, through the adapter kept
with the measurement scripts outside this repository, this crate's `PositionalIndexer` and
`ChainIndex`, one binary, one generator, one lane scheduler. The SMG harness rows are the
cross-check: the two harnesses agree to 0.2% at 107M (Parity) and the SMG harness reads lower at
high rates, since it mirrors Dynamo's per-event costs but not its lane scheduling.

Binaries, one build each, every trial of the entry on these: Dynamo `mooncake_bench` from
ai-dynamo/dynamo `50bdb355f8` with features `mooncake,router-bench` (mimalloc), plus the
out-of-tree wiring and lane-CPU patches; the same-binary build adds this crate as a path
dependency at `408b3254` (indexers as in `perf/kv-router-leap` `b4943d69`, the chain index with the
lane pool); the SMG replayer `mooncake_replay` built from `93876aa0` (the same indexers). Every
result JSON carries its command line and the hashes of its binary and of the trace or corpus, and
every series directory carries a provenance file naming the build it ran on.

### Brackets

Threshold searches as above (3 fresh processes per point, all three must achieve 99% of offered
with a valid generator, geometric bisection to within 10%; a trial that fell short while a foreign
process sat above half a core on the measurement cores is replaced, not counted). Per lane core
is the kept-up rate over the 52 lane cores.

| Indexer, harness | Keeps up at | Per lane core (M) | Fails at | Ratio | Trials at the failing rate (achieved / offered) | Lookup p50 / p99 (us) at the kept-up rate | Points, replaced trials |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Dynamo CRTC, Dynamo harness | 480.8M | 9.25 | 511.2M | 1.063 | 99.7%, 99.7%, 89.4% | 3.5-3.6 / 14 | 6, 0 |
| SMG `PositionalIndexer`, Dynamo harness | 132.4M | 2.55 | 143.1M | 1.080 | 93.9%, 94.3%, 95.2% | 30.6-31.7 / 449-516 | 7, 0 |
| SMG `ChainIndex`, Dynamo harness | 656.4M | 12.62 | 718.7M | 1.095 | 99.7%, 99.8%, 98.9% | 1.3-1.4 / 4 | 6, 3 |
| SMG `PositionalIndexer`, SMG harness | 122.6M | 2.36 | 132.4M | 1.080 | 99.4%, 96.2%, 96.3% | 28.1-34.4 / 347-487 | 7, 0 |
| SMG `ChainIndex`, SMG harness | 571.7M | 10.99 | 611.3M | 1.069 | 99.8%, 99.8%, 96.9% | 1.1 / 6 | 7, 0 |
| no indexer (harness ceiling), SMG harness | 1669.9M | 32.11 | 1766.1M | 1.058 | 98.5%, 98.9%, 99.8% | 0.4-0.5 / 3-4 | 7, 0 |

The strict rule makes every bracket conservative: the competitor's failing point at 511.2M and
the chain index's at 718.7M each lost one trial of three (89.4% with nothing above 5% of a core in
the samples around it; 98.9%), and the competitor achieved 98.9-99.3% at 653.3M and 69-74% of
1.07B (736-790M) when overloaded, while the chain index achieved 99.5-99.6% in two trials of three
at 1,033M. In one binary and on the same 52 cores the chain index keeps up with 1.37x the
competitor's sustained rate (12.6 against 9.3M block ops/s per lane core) with lookups at p50
1.3 us and p99 4 us against 3.5 us and 14 us; the positional design keeps up at 0.28x of it. The
SMG harness brackets read 7-13% lower than Dynamo's for the same indexers (positional 122.6
against 132.4M, chain index 571.7 against 656.4M); the null backend's 1.67B is the harness's own
ceiling on this layout (the generator, with the lanes doing nothing).

### 20-trial series

Same method as the first protocol run: 20 usable trials per series as fresh processes, each
followed by a control trial of the same binary and configuration, one lock hold per series, the
measurement cores sampled around every trial (record 5%, discard 50%), medians with bootstrap 95%
intervals. Sustained points are the kept-up ends of the brackets above (the window that offers
them in Dynamo's harness is given); overload is the 300 ms window for the competitor and the
positional index in Dynamo's harness, 200 ms for the chain index there, and twice the sustained rate
in the SMG harness. Per lane core divides the achieved median by the 52 lane cores.

Same-binary rows, Dynamo's harness (one binary, one generator, one lane scheduler):

| System, harness | Load | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) | Per lane core (M) | Lookup p50 [CI] (us) | Lookup p99 [CI] (us) | Subject minus control: achieved, p50, p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Dynamo CRTC, Dynamo harness | sustained bracket (666 ms window, 481M offered) | 20 / 9 | 17 of 20 | 479.3 [478.9, 479.5] | 9.22 (52 cores) | 3.5 [3.5, 3.5] | 14 [14, 14] | +0.1 [-0.3, +0.5], +0.0 [-0.1, +0.0], -0.3 [-0.5, -0.0] |
| control (same binary) (Dynamo CRTC) | same | 25 / 4 | 21 of 25 | 479.2 [479.0, 479.3] | | 3.5 [3.5, 3.5] | 14 [14, 14] | |
| Dynamo CRTC, Dynamo harness | overload (300 ms window, 1.07B offered) | 20 / 2 | 0 of 20 | 764.5 [734.8, 811.3] | 14.70 (52 cores) | 2.7 [2.7, 2.8] | 12 [12, 12] | -5.4 [-56.1, +41.4], +0.0 [-0.0, +0.1], +0.3 [-0.1, +0.6] |
| control (same binary) (Dynamo CRTC) | same | 20 / 2 | 0 of 20 | 769.9 [756.4, 808.1] | | 2.7 [2.6, 2.7] | 12 [11, 12] | |
| SMG PositionalIndexer, Dynamo harness | sustained bracket (2417 ms window, 132M offered) | 20 / 2 | 16 of 20 | 131.6 [131.4, 131.8] | 2.53 (52 cores) | 27.3 [25.5, 32.2] | 413 [343, 478] | +0.0 [-0.3, +0.3], -3.5 [-5.4, +4.0], -44.2 [-117.9, +49.7] |
| control (same binary) (SMG PositionalIndexer) | same | 21 / 1 | 15 of 21 | 131.6 [131.4, 131.7] | | 30.8 [27.2, 31.6] | 457 [393, 482] | |
| SMG PositionalIndexer, Dynamo harness | overload (300 ms window, 1.07B offered) | 20 / 0 | 0 of 20 | 144.7 [143.9, 145.1] | 2.78 (52 cores) | 23.4 [23.1, 23.6] | 565 [558, 571] | +0.4 [-0.8, +1.3], +0.1 [-0.4, +0.3], +3.0 [-6.1, +15.3] |
| control (same binary) (SMG PositionalIndexer) | same | 20 / 0 | 0 of 20 | 144.3 [143.4, 145.3] | | 23.3 [23.3, 23.6] | 562 [550, 567] | |
| SMG ChainIndex, Dynamo harness | sustained bracket (488 ms window, 656M offered) | 20 / 4 | 17 of 20 | 654.6 [654.4, 654.6] | 12.59 (52 cores) | 1.3 [1.2, 1.3] | 4 [4, 4] | +0.1 [-0.2, +0.3], -0.0 [-0.1, +0.1], -0.0 [-0.2, +0.1] |
| control (same binary) (SMG ChainIndex) | same | 22 / 2 | 19 of 22 | 654.5 [654.3, 654.6] | | 1.3 [1.3, 1.3] | 4 [4, 4] | |
| SMG ChainIndex, Dynamo harness | overload (200 ms window, 1.60B offered) | 20 / 2 | 0 of 20 | 1171.0 [1140.1, 1227.9] | 22.52 (52 cores) | 1.2 [1.2, 1.2] | 4 [3, 4] | -7.1 [-66.8, +61.3], +0.0 [-0.0, +0.0], +0.0 [-0.2, +0.3] |
| control (same binary) (SMG ChainIndex) | same | 20 / 2 | 0 of 20 | 1178.1 [1155.8, 1220.5] | | 1.2 [1.2, 1.2] | 3 [3, 4] | |

SMG harness rows (the cross-check):

| System, harness | Load | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) | Per lane core (M) | Lookup p50 [CI] (us) | Lookup p99 [CI] (us) | Subject minus control: achieved, p50, p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SMG PositionalIndexer, SMG harness | sustained bracket (2611 ms window, 123M offered) | 21 / 2 | 17 of 21 | 122.0 [121.8, 122.0] | 2.35 (52 cores) | 34.2 [32.9, 34.4] | 497 [478, 505] | +0.0 [-0.1, +0.8], +0.2 [-1.4, +0.7], +7.6 [-86.1, +19.6] |
| control (same binary) (SMG PositionalIndexer) | same | 20 / 3 | 14 of 20 | 121.9 [121.1, 122.0] | | 34.0 [33.7, 35.0] | 490 [481, 571] | |
| SMG PositionalIndexer, SMG harness | overload (1305 ms window, 245M offered) | 21 / 1 | 0 of 21 | 132.1 [130.9, 134.1] | 2.54 (52 cores) | 32.0 [31.2, 32.4] | 605 [601, 615] | -0.5 [-2.2, +2.4], +0.1 [-0.8, +1.5], +7.7 [-2.4, +18.9] |
| control (same binary) (SMG PositionalIndexer) | same | 20 / 2 | 0 of 20 | 132.6 [131.2, 133.1] | | 31.9 [30.5, 32.1] | 597 [592, 605] | |
| SMG ChainIndex, SMG harness | sustained bracket (559 ms window, 572M offered) | 21 / 0 | 18 of 21 | 570.3 [570.0, 570.6] | 10.97 (52 cores) | 1.1 [1.1, 1.1] | 6 [6, 6] | -0.1 [-0.5, +0.4], -0.0 [-0.0, +0.0], +0.0 [-0.0, +0.1] |
| control (same binary) (SMG ChainIndex) | same | 20 / 1 | 20 of 20 | 570.3 [570.1, 570.5] | | 1.1 [1.1, 1.1] | 6 [6, 6] | |
| SMG ChainIndex, SMG harness | sustained, re-bracketed (470 ms window, 682M offered; the 571.7M point was decided by one trial at 96.9% of 611.3M, the new bracket keeps up at 681.6M and fails at 719.8M on one generator-invalid trial, 760M fails) | 20 / 6 | 19 of 20 | 679.6 [679.3, 679.8] | 13.07 (52 cores) | 1.1 [1.1, 1.1] | 6 [6, 6] | +0.0 [-0.4, +0.5], +0.0 [-0.0, +0.0], -0.0 [-0.2, +0.2] |
| control (same binary) (SMG ChainIndex) | same | 21 / 5 | 20 of 21 | 679.6 [679.1, 679.7] | | 1.1 [1.1, 1.1] | 6 [6, 6] | |
| SMG ChainIndex, SMG harness | overload (279 ms window, 1.14B offered) | 21 / 0 | 2 of 21 | 1069.9 [980.0, 1114.6] | 20.58 (52 cores) | 1.0 [1.0, 1.1] | 8 [7, 8] | +41.7 [-71.4, +127.2], +0.0 [-0.1, +0.1], -1.0 [-2.0, +0.1] |
| control (same binary) (SMG ChainIndex) | same | 20 / 1 | 1 of 20 | 1028.3 [958.0, 1076.7] | | 1.0 [1.0, 1.1] | 9 [8, 9] | |
| no indexer (harness ceiling), SMG harness | sustained bracket (191 ms window, 1.67B offered) | 20 / 2 | 18 of 20 | 1667.9 [1667.5, 1668.1] | 32.07 (52 cores) | 0.5 [0.5, 0.5] | 4 [4, 4] | -0.1 [-0.5, +2.6], +0.0 [+0.0, +0.0], +0.0 [-0.0, +0.0] |
| control (same binary) (no indexer (harness ceiling)) | same | 21 / 1 | 19 of 21 | 1667.9 [1665.4, 1668.1] | | 0.5 [0.5, 0.5] | 4 [4, 4] | |

The null backend's overload series (3.34B offered, twice its sustained rate) has no usable row:
the generator was invalid in all 40 subject trials, which places the harness's own ceiling on this
layout between 1.67B (kept up) and 3.34B, as the query-issuer grid above predicts (2.13B on
schedule with four query issuers).

Reading the rows. In one binary and on the same 52 cores the chain index sustains 654.6M against
the competitor's 479.3M (1.37x; 12.6 against 9.2M block ops/s per lane core) with lookups at p50
1.3 us and p99 4 us against 3.5 us and 14 us; overloaded it achieves 1,171M at the 200 ms window
(22.5M per core, p99 still 4 us) where the competitor achieves 764.5M at 300 ms (14.7M per core,
p99 12 us). The positional design sustains 131.6M (2.5M per core) at p99 413 us and is the
reference for what the chain index replaced. The control pairs put the noise floor at or below one
unit in the last digit for sustained throughput and lookup p50 and within 0.3 us for the run
index's p99; the overload (capacity) rows carry the widest intervals (the competitor's 300 ms
subject and control differ by 5.4M inside [-56, +41]; the chain index's 200 ms pair by 7.1M inside
[-67, +61]), so capacities are compared within one harness and layout only; the competitor's
capacity on this layout (764.5M on 52 cores) is below its competitor-layout figure (857.4M on 59
cores). The SMG harness reads 7% lower than Dynamo's for the same indexers at their sustained
points (positional 122.0 against 131.6M, chain index 570.3 against 654.6M) with the same latencies
within 2 us at p99, and its run-index overload row shows the bracket is conservative: at 1.14B
offered the index still achieved 1,069.9M [980, 1,115] (94% of offered, 2 of 21 trials kept up),
so its ceiling in this harness lies near 1.1B; the re-bracketed point (681.6M kept up, 719.8M
lost to one generator-invalid trial, 760M failing at 93.2% in one trial) and its 20-trial series
are the row above the overload row (679.6M, 19 of 20, p99 6 us).

Discarded trials were replaced and are listed per series in `summary.md`: across the twelve
scaled series 0-13 of 40-58 attempts each were discarded for other users' jobs above half a core
on the measurement cores (a configuration agent, a package proxy, kubectl, dnf, a sync daemon, git,
a publish daemon) and one for an invalid generator; throughout, other workstreams measured on
cores 67-71 and 77-135 and built and served engines on 72-143, outside the sampled cores (host
load average 35-110), which the sustained rows are robust to and the capacity rows may not be.
The first scaled series of the day (set aside, kept on disk under `-badmask`, not cited) ran with
the lanes on cores 5-63 under issuers on 0-11, seven cores shared, because the wrapper applied its
default lane mask before the layout selection; the replayer now refuses issuer CPUs inside the lane
set and prints the layout at the top of every log.

### Two-socket lane sets: the generator is the limit in Dynamo's harness

A same-binary comparison on all lane cores of both sockets (lanes 5-63 and 72-143, 131 cores, 128
event lanes and 128 query lanes, event issuers on 0-3 and 64-71, query issuer on 4, `numactl
--interleave=all`) was prepared and smoke-tested with the out-of-tree adapter built against the
chain index at `9f9c7c04` (binary and commits in the measurement scripts' provenance file). At the
3000 ms window (107M) it keeps up; at 500 ms (640M offered) the generator is invalid: the issue
span is 743 ms, read and update issue lag p99 240 ms, issuers busy the whole time, 430M achieved.
One trial each at 500 ms localises it: lanes on socket 0 only (5-63) with the same issuers keep up
(639M, read lag p99 0.85 ms); lanes on socket 1 only (72-143) keep up with read lag p50/p99 9.8/33
ms; both sockets with 32 query lanes still fail (span 519 ms); 64 event lanes on both sockets are
worse (span 815 ms). Dynamo's harness has one query issuer and its event issuers wait for it at
every deadline, so each publish to a lane parked on the other socket costs a cross-socket wake:
its generator ceiling on a two-socket lane set is 430-550M block ops/s, below the single-socket
52-lane sustained points above (chain index 656M, competitor 481M). Brackets on that layout would be
generator rows for both systems, so none were run; the harness was not changed. The SMG harness
with five query issuers issues on schedule on the same 131 lanes (read lag p99 78 us) but the run
index achieved only 528M of 640M there, a lane-side cliff that the rows below take apart.

### Two-socket lane sets in the SMG harness: where the lane cost goes

One variable per row, chain index at `9f9c7c04` through the replayer at `0d8f3ba9` (this branch's
`70702f80` on that tree: `--issuer-by-lane`), three fresh processes at the 500 ms window (640M
offered) and one at 300 ms (1,067M), the host otherwise idle (19:05-19:25). Lane CPU per event is
the median lane's event-lane CPU over its events, read per socket from the pinning (lane `i` on
backend CPU `i mod n`); per lane core divides the achieved median by the lane cores.

| Row | Configuration (chain index unless said, 500 ms window = 640M offered, 3 trials) | Achieved (M), kept up | Per lane core (M) | Lane CPU per event, socket 0 / 1 (us) | Lookup p50 / p99 (us) | 300 ms (1,067M offered): achieved (M) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | socket 0 only: 60 lane cores (12-71), issuers 0-7 and 8-11, 64 lanes pinned, interleave | 637 (637-638), 3 of 3 | 10.62 (60) | 5.7 / - | 1.1 / 6 | 749 |
| 2 | socket 1 only: 60 lane cores (72-131), issuers 136-143 and 132-135, 64 lanes pinned, interleave | 602 (537-616), 2 of 3, generator invalid in 1 | 10.03 (60) | - / 5.7 | 1.1 / 6 | 899 |
| 3 | socket 0 only, `--cpunodebind=0 --membind=0` (all memory local) | 639 (638-639), 3 of 3 | 10.65 (60) | 4.2 / - | 0.9 / 4 | 1061 |
| 4 | socket 1 only, `--cpunodebind=1 --membind=1` | 639 (638-639), 3 of 3 | 10.65 (60) | - / 4.1 | 0.8 / 4 | 1047 |
| 5 | socket 1 only, no numactl (first touch) | 639 (597-639), 3 of 3 | 10.64 (60) | - / 5.1 | 1.1 / 7 | 924 |
| 6 | socket 0 only, 64 lane cores, 128 lanes pinned two per core (lane-count control) | 638 (638-639), 3 of 3 | 9.96 (64) | 6.1 / - | 1.1 / 7 | 911 |
| 7 | both sockets: 131 lane cores (5-63, 72-143), issuers on socket 0, 128 lanes floating, interleave | 519 (506-529), 0 of 3 | 3.96 (131) | 18.8 (floating, all lanes) | 1.3 / 10 | 520 |
| 8 | same, 128 lanes pinned (59 on socket 0, 69 on socket 1) | 542 (531-570), 0 of 3 | 4.13 (131) | 11.7 / 14.9 | 1.3 / 9 | 504 |
| 9 | both sockets, floating, the scaled-scoreboard replayer (`b4943d69` index) | 536 (476-548), 0 of 3 | 4.09 (131) | 18.9 (floating, all lanes) | 1.4 / 10 | 561 |
| 10 | 64 lane cores per socket (8-71, 72-135), 4+4 event and 4+4 query issuers, lanes pinned, each socket's issuers feed its own lanes (`--issuer-by-lane`), interleave | 524 (502-525), 0 of 3 | 4.09 (128) | 13.4 / 19.6 | 1.2 / 10 | 532 |
| 11 | same, issuers by worker id (half the feeds cross the interconnect) | 529 (497-546), 0 of 3 | 4.13 (128) | 11.1 / 13.7 | 1.3 / 9 | 579 |
| 12 | split, own-socket feeding, no numactl (first touch) | 508 (497-516), 0 of 3 | 3.97 (128) | 17.9 / 10.5 | 1.2 / 9 | 540 |
| 13 | split, own-socket feeding, `--membind=0` | 506 (504-551), 0 of 3 | 3.95 (128) | 20.4 / 13.6 | 1.5 / 11 | 477 |
| 14 | split, own-socket feeding, `--membind=1` | 499 (421-500), 0 of 3 | 3.90 (128) | 13.6 / 19.9 | 1.5 / 11 | 560 |
| 15 | split cores but 64 lanes, which pinning puts all on socket 0 while the socket-1 issuers feed half of them across the interconnect (feeding control) | 639 (639-639), 3 of 3 | 4.99 (128) | 6.2 / - | 1.2 / 8 | 1060 |
| 16 | both sockets, pinned, `--mirror-dynamo-costs false` (no payload allocation or free in the lanes) | 578 (556-589), 1 of 3 | 4.41 (131) | 10.9 / 17.5 | 0.9 / 4 | 561 |
| 17 | socket 0 only, `--mirror-dynamo-costs false` | 637 (621-639), 3 of 3 | 10.62 (60) | 4.6 / - | 0.6 / 3 | 658 |
| 18 | both sockets, pinned, null backend (harness floor) | 639 (636-640), 3 of 3 | 4.88 (131) | 4.7 / 4.7 | 0.4 / 6 | 897 (generator invalid) |
| 19 | socket 0 only, null backend | 639 (610-640), 3 of 3 | 10.65 (60) | 1.4 / - | 0.4 / 4 | 1062 |

Reading, for the chain index's owners:

- **Not wake latency or feeding.** Lanes fed across the interconnect keep up as well as lanes fed
  locally: row 15 (socket-1 issuers feeding socket-0 lanes) keeps up at 640M and achieves 1,060M at
  300 ms, and own-socket feeding (row 10) is no better than feeding by worker id (row 11). The
  issue lag stays in the microseconds to low milliseconds wherever the lanes are pinned.
- **Not memory placement.** With lanes on both sockets the per-event lane cost is 11-20 us whether
  the memory is interleaved (rows 8, 10), first-touched (row 12) or bound to either socket (rows 13,
  14); binding only moves which socket's lanes are slower, and the slower lanes are the ones on the
  memory's home socket. On one socket, local memory does matter: it cuts the per-event cost from 5.7
  to 4.2 us and lifts the 300 ms point from 749M to 1,061M (rows 1 and 3, 2 and 4), which is the
  cost of `--interleave=all` to a single-socket lane set.
- **Not the lane count, the harness's payload path, or the pool.** 128 lanes on one socket cost
  6.1 us per event and keep up (row 6); with the payload allocation and free removed the two-socket
  cost is still 10.9 / 17.5 us against 4.6 us on one socket (rows 16, 17); the null backend's lanes
  pay 4.7 us on two sockets against 1.4 us on one (rows 18, 19, the harness's own share: the channel
  receive and the payload free of memory the issuers allocated) and still keep up; the SMG harness
  has no lane pool (lanes own their workers by first appearance), so stealing is not involved.
- **What is left is the index's shared writable state.** Lanes applying events on both sockets pay
  two to three times the per-event CPU of lanes on one socket, with the same events per lane, the
  same binary (the `b4943d69` index behaves the same, row 9) and lookups unaffected; the extra time
  is cache lines of the chain index written from both sockets bouncing across the interconnect. The
  duplicated corpus makes the sharing true sharing: twenty workers carry the same content, so the
  runs they converge on (coverage words, run metadata, arena and lane-map lines) are written by
  lanes on both sockets. The fix is placement or partition, not parking: route the workers that
  share content to lanes on one socket, or keep per-socket copies of the mutable run state and
  arenas so that no line is written from both sockets; the single-socket 1,061M at 300 ms with
  local memory is the figure a two-socket layout has to beat per socket.

### Local memory on one socket: what `--interleave=all` costs each system

Dynamo's harness, competitor layout (lanes 5-63, event issuers 0-3, query issuer 4, 64 event and
128 query lanes), chain index at `9f9c7c04` and the competitor in the same binary build, 300 ms and
200 ms windows, `numactl --cpunodebind=0 --membind=0` against `--interleave=all` (the published
method), three fresh processes each, all interleaved inside one lock hold (19:30-19:35, host
quiet). Per lane core divides the median by the 59 lane cores. Kept up is the protocol's rule
(generator valid and achieved at least 99% of offered); the harness's own `kept_up` flag, which
lets replay plus drain run 10% past the window, is shown beside it because it says yes to two
rows the rule says no to.

| Window (offered) | Memory | System | Achieved per trial (M) | Median (M) | Per lane core (M) | Achieved / offered | Kept up (99% rule) | Harness flag | Lookup p50 / p99 (us) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 300 ms (1,067M) | local | SMG `ChainIndex` | 1,063.0, 1,064.2, 1,064.1 | 1,064.1 | 18.0 | 99.6-99.7% | 3 of 3 | 3 of 3 | 1.2 / 3 |
| 300 ms (1,067M) | local | Dynamo CRTC | 991.1, 1,031.2, 1,026.8 | 1,026.8 | 17.4 | 92.9-96.6% | 0 of 3 | 3 of 3 | 2.6 / 9 |
| 300 ms (1,067M) | interleaved | SMG `ChainIndex` | 1,061.7, 1,059.1, 1,062.6 | 1,061.7 | 18.0 | 99.3-99.6% | 3 of 3 | 3 of 3 | 1.3 / 4 |
| 300 ms (1,067M) | interleaved | Dynamo CRTC | 802.5, 860.6, 847.7 | 847.7 | 14.4 | 75.2-80.7% | 0 of 3 | 0 of 3 | 2.9 / 13 |
| 200 ms (1,601M) | local | SMG `ChainIndex` | 1,573.9, 1,552.5, 1,555.7 | 1,555.7 | 26.4 | 97.0-98.3% | 0 of 3 | 3 of 3 | 1.2 / 3 |
| 200 ms (1,601M) | local | Dynamo CRTC | 1,129.5, 1,106.4, 1,149.4 | 1,129.5 | 19.1 | 69.1-71.8% | 0 of 3 | 0 of 3 | 2.1 / 8 |
| 200 ms (1,601M) | interleaved | SMG `ChainIndex` | 1,279.6, 1,301.6, 1,177.6 | 1,279.6 | 21.7 | 73.6-81.3% | 0 of 3 (generator invalid in 2) | 0 of 3 | 1.2 / 4 |
| 200 ms (1,601M) | interleaved | Dynamo CRTC | 933.2, 898.4, 972.1 | 933.2 | 15.8 | 56.1-60.7% | 0 of 3 (generator invalid in 2) | 0 of 3 | 2.3 / 10 |

Local memory is worth 21% to the competitor at 300 ms (848M to 1,027M, lookup p99 13 to 9 us,
still short of the 99% bar at 93-97%) and nothing to the chain index there, which keeps up either
way at p99 3-4 us (its 1,062-1,064M at 300 ms is the first kept-up point above 1.06B in Dynamo's
harness, under both memory settings); at 200 ms the chain index reaches 97-98% of 1.6B with local
memory (1,556M, 26.4M per lane core, drain 2-6 ms) against 74-81% interleaved, the competitor
69-72% against 56-61%, and under interleaving the generator itself is invalid in two trials of
three for both systems at that window. The kept-up points under local memory are bracketed in the
rows that follow. The published rows keep `--interleave=all` because it is Dynamo's method; a
process bound to its socket is the deployment-realistic setting on one socket; both are reported
and neither system was changed for either.

#### Kept-up points and 20-trial series under local memory

Same harness, layout and binaries, `numactl --cpunodebind=0 --membind=0` throughout (19:50-21:00,
other users' jobs on the host costing discards: an editor server at up to 28 cores, a
configuration agent, a package proxy). Brackets as in the threshold search above (3 fresh
processes per point, all three at 99% with a valid generator, geometric bisection to within 10%,
verified ends, generator-invalid points marked); series as in the publication protocol (20 usable
trials with interleaved same-binary controls, one hold per series, discards at half a core).

| Indexer, harness | Keeps up at | Per lane core (M) | Fails at | Ratio | Trials at the failing rate (achieved / offered) | Lookup p50 / p99 (us) at the kept-up rate | Points |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Dynamo CRTC, Dynamo harness | 859.7M (372 ms) | 14.6 | 923.9M | 1.075 | 98.9%, 98.4%, 98.6% (1,067M: 93.6-95.4%) | 2.7 / 10 | 4 |
| SMG `ChainIndex`, Dynamo harness | 1,383.8M (231 ms) | 23.5 | 1,509.0M | 1.091 | 97.9%, 99.3%, 99.1% (2,134M: generator invalid in all three) | 1.2-1.3 / 3-4 | 5 |

| System, harness | Load | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) | Per lane core (M) | Lookup p50 [CI] (us) | Lookup p99 [CI] (us) | Subject minus control: achieved, p50, p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SMG `ChainIndex`, Dynamo harness | sustained bracket (231 ms window, 1,384M offered) | 22 / 3 | 21 of 22 | 1,379.4 [1,378.2, 1,379.8] | 23.4 | 1.3 [1.3, 1.3] | 3 [3, 4] | -0.2 [-1.6, +1.5], +0.0 [+0.0, +0.1], +0.0 [-0.0, +0.1] |
| control (same binary) (SMG `ChainIndex`) | same | 20 / 5 | 20 of 20 | 1,379.6 [1,378.1, 1,380.4] | | 1.2 [1.2, 1.3] | 3 [3, 4] | |
| Dynamo CRTC, Dynamo harness | sustained bracket (372 ms window, 860M offered) | 20 / 4 | 19 of 20 | 856.7 [856.2, 856.8] | 14.5 | 2.7 [2.7, 2.7] | 10 [10, 10] | +0.3 [-0.4, +1.0], +0.0 [-0.0, +0.0], +0.0 [-0.1, +0.1] |
| control (same binary) (Dynamo CRTC) | same | 21 / 3 | 19 of 21 | 856.3 [855.8, 856.7] | | 2.7 [2.7, 2.7] | 10 [10, 10] | |
| SMG `ChainIndex`, Dynamo harness | 200 ms window (1.60B offered) | 21 / 0 | 1 of 21 | 1,564.3 [1,536.4, 1,568.1] | 26.5 | 1.2 [1.2, 1.2] | 3 [3, 4] | +15.4 [-16.1, +37.3], -0.0 [-0.0, +0.0], -0.0 [-0.1, +0.1] |
| control (same binary) (SMG `ChainIndex`) | same | 20 / 1 | 0 of 20 | 1,548.9 [1,527.1, 1,566.1] | | 1.2 [1.2, 1.3] | 4 [3, 4] | |
| Dynamo CRTC, Dynamo harness | 200 ms window (1.60B offered) | 20 / 7 | 0 of 20 | 1,120.2 [1,109.0, 1,126.0] | 19.0 | 2.1 [2.1, 2.1] | 8 [8, 8] | -5.0 [-20.2, +10.6], -0.0 [-0.0, +0.0], -0.1 [-0.2, +0.1] |
| control (same binary) (Dynamo CRTC) | same | 21 / 6 | 0 of 21 | 1,125.2 [1,112.6, 1,134.1] | | 2.1 [2.1, 2.1] | 8 [8, 8] | |

Under local memory on one socket, in one binary and on the same 59 lane cores, the chain index's
kept-up point is 1,383.8M against the competitor's 859.7M (1.61x; 23.5 against 14.6M block ops/s
per lane core) at lookup p99 3-4 us against 10 us, and at the 200 ms window the chain index achieves
1,564M (26.5M per core, p99 3 us, 97.7% of offered) against the competitor's 1,120M (19.0M per
core, p99 8 us, 70%). Against the published interleaved rows on this layout, local memory moves the
competitor's kept-up point from 682M to 860M (+26%) and the chain index's from 1,062M to 1,384M
(at least +30%); the ratio between the two systems is 1.56x interleaved and 1.61x local. The
control pairs stay within one unit in the last digit for the sustained rows and within the
intervals for the 200 ms rows. Dynamo's published method is `--interleave=all`; on one socket a
process bound to its socket is the deployment-realistic setting; both are reported and neither
system was changed for either.

### T7, first window (loaded host): the sharded chain index on two sockets

Replayer from `ce53d732` (`--shards`, `--lane-memory`), layout agreed with the index's owners: lane
cores 8-71 and 72-135 (64 + 64), 128 event lanes pinned one per core so `--shards 2` follows the
NUMA node, 128 query lanes, event issuers 0-3 and 136-139, query issuers 4-7 and 140-143,
`--issuer-by-lane`. The 00:10-02:15 window on 2026-10-06 ran with other users' jobs on the
measurement cores throughout (an editor server floating at 1.6-11 cores, a backup's tar and zstd,
git, chef), so every row below is labelled loaded host and the clean repeat is scheduled; the
protocol's half-core rule replaced shortfalls under foreign load (up to five per point in the
rescued brackets) and every trial's foreign processes and their cores are in the records.

- (e) one socket, lanes 8-71, `--shards 1 --lane-memory local`, no numactl: bracket keeps up at
  951.4M (14.9M per lane core; fails at 1,037.6M on one trial at 97.6%); 20-trial series at 951.4M:
  941.9M [881.4, 945.5], 10 of 20 kept up (the other ten fell short under foreign load below the
  discard threshold), p50 1.0 / p99 7.3 us, lane CPU 5.0-6.2 us per event; against 1,061M kept up
  at 300 ms with `cpunodebind=0 membind=0` on 59 cores, lane-local memory without the process
  binding reads lower, which the clean window has to separate from the load.
- (d) two sockets, `--shards 1 --lane-memory local`, interleave: bracket keeps up at 412.0M, fails
  at 446.0M; lane CPU 9.0-13.3 us per event (the two-socket cliff of the unsharded index, as in the
  diagnosis above).
- (c) two sockets, `--shards 1`, interleave: no point verified (300M: two trials at 99.7% and four
  replaced).
- (a) two sockets, `--shards 2 --lane-memory local`, interleave: no kept-up point verified. At
  1,067M and 800M the first pass lost two trials of three to foreign load; the rescued bracket at
  700M ran eight trials, all under foreign load, achieving 572-684M (best three 96.7-97.7%, p50
  1.8-1.9 / p99 10-11 us); at 500M two of three counted trials kept up (99.5%, 99.3%) and one did
  not (96.5%). Lane CPU per event 6.4-8.3 us on socket 0 and 6.3-9.1 us on socket 1: the lowest of
  any two-socket row and 1.3x the single socket's, where the unsharded index on two sockets pays
  10-18 us. Duplicated content: 1,566,377 distinct blocks summed over the two shards against
  1,532,076 in one shard, 1.022, so 2.2% of resident blocks are held on both shards (pair sharing of
  stored events across the socket boundary is 50%, measured on events, not resident blocks); arena
  55.0 MB against 52.9 MB.
- A/B at 500M in the last minutes, three trials each interleaved: (a) 482, 439, 461M (87.7-96.5%,
  drains 23-89 ms), (c) 329, 488, 497M (one trial disturbed to 65.7%), (d) 498, 495, 497M (kept up
  3 of 3). In (a) the lanes finish the window on time (median 640 ms of 640) except one lane per
  trial 25-55 ms late (a different lane each time, 15-17% busy like the others), which sets the
  drain and costs 3-12% of achieved; its issue lag p99 is 15-35 ms against 4-25 ms unsharded. Whether
  that tail is foreign preemption of mostly idle lanes or the shard path (lookups walk both shards;
  p50 1.8 against 1.2-1.5 us) is for the clean window.

### T7, second window (host daemons present): issuer-built payloads

The 05:00-07:00 window on 2026-10-06 had the editor server closed and no builds, but host daemons
and other users' jobs sat above half a core in most samples (dotsync2 at about one core, the bpf
usage tracer, polkitd, mcdaemon, chef-client, a cf_manager service, certreq); the half-core rule
replaced every trial of the first pass, so from 05:07 those names were recorded as background for
the window and the rule kept for everything else, and every row is labelled as such. Layout as in
the first window with two cores per socket left free (lanes 10-71 and 72-133, 124 pinned lanes,
issuers 0-3,136-139 and 4-7,140-143). The index's owners had found between the windows that the
two-socket doubling of lane CPU was the harness's mirrored payload path, not the index: under
`--mirror-dynamo-costs true` every event's owned payload is built before the trial by the main
thread in one glibc arena and freed by the lane, so 124 lanes on two sockets contend on that
arena; the replayer gained `--payload-home main|issuer` (issuer: each pinned event issuer builds
its own dispatch's payloads on its socket, lanes free into that issuer's arena). Dynamo's harness
builds its owned events on the main thread with the system allocator (`prepare_open_loop_trial`),
so `main` mirrors their method and `issuer` removes a harness cost both systems would pay; rows
with `issuer` carry that caveat. Lane binary: leap/indexer-run `d89d2702` (the pushed head's
kv_index plus `22a87cb5`, `16304c24`, `d89d2702`); row (a) on the `ce53d732` binary of the first
window.

| Row | Keeps up at | Per lane core (M) | Fails at (trials) | Series at the kept-up point (M), kept up | Lookup p50 / p99 (us) | Lane CPU per event block, socket 0 / 1 (ns) |
| --- | --- | --- | --- | --- | --- | --- |
| (e3) one socket, lanes 10-71, `--shards 1 --lane-memory local --payload-home issuer`, no numactl | 1,234.0M | 19.9 (62) | 1,345.8M (99.6%, 99.6%, 70.8%) | 1,229.8 [1,224.6, 1,230.0], 15 of 20 | 0.9 / 6 | 47 |
| (a3) two sockets, `--shards 2 --lane-memory local --payload-home issuer`, interleave | 745.5M | 6.0 (124) | 813.7M (92.1%, 99.2%, 95.0%) | 720.4 [697.1, 734.8], 7 of 25 | 1.8 / 10 | 55 / 56 |
| (a) two sockets, `--shards 2 --lane-memory local`, payloads from the main thread, `ce53d732` | 700.0M | 5.6 (124) | 750.5M (99.0%, 94.5%, 99.6%) | 681.1 [660.7, 696.6], 10 of 23 | 1.8 / 11 | 74 / 94 |
| (c3) two sockets, `--shards 1 --payload-home issuer`, interleave | no point verified at 400M or 300M | | | | | |
| (d) two sockets, `--shards 1 --lane-memory local`, interleave | 300.0M | 2.4 (124) | 324.8M (97.7%, 99.5%, 96.5%) | 297.1 [295.0, 298.7], 11 of 21 | 1.2 / 8 | 79 / 79 |

Reading. With issuer-built payloads one socket sustains 1,234M on 62 lanes (47 ns of lane CPU per
event block) where the mirrored-main single socket read 951M in the first window; the two-socket
sharded index then pays the single-socket cost per block on both sockets (55 / 56 ns against 74 /
94 with main-thread payloads) but sustains 745.5M, 0.6x one socket: its trials are bimodal, 11 of
31 subject trials keep up at 99.1-99.8% with drains of 0-4 ms and the rest fall to 77-98% with
drains of 9-127 ms at the same lane CPU, i.e. single lanes stalled off-CPU, and the index's owners
read a socket-wide shift of socket 1's lane cost that varies by trial (66-84 ns in 26 of 30 trials,
54 in the others) rather than particular lanes sharing cores with daemons. The duplicated-content
share of the two shards is 2.3% (1,566,806 summed distinct blocks over 1,532,076), arena 53.0 MB
against 47.7 MB. Which cores the host's daemons favour, from the window's 300+ samples
(`foreign-cores.py`): the bpf usage tracer is pinned to core 103, falcon_proxy to 129, strobelight
to 45, smc_proxy to 52; the floating ones (the editor's native server, polkitd, dotsync2, below,
fetch_krl) concentrate on socket 1's lane cores 121-128, 138-139 and 87. The sampler now records
every thread's core per flagged process and each socket's mean frequency before and after every
trial (`t7-table.py --trials <row>` joins them per trial), and the next two-socket row is the
index's owners' stealing lanes, since freeing cores does not remove a socket-wide shift.

## Plugging in a new index

`ReplayBackend` is four slice-based methods plus a per-lane state type. The run-compressed index
(`ChainIndex`: `intern_worker`, `apply_stored(worker, &[StoredBlock], parent, &mut ChainBlockMap)`,
`apply_removed`, `apply_cleared`, `find_matches(&[ContentHash], early_exit)`) maps onto it exactly
as `Positional` does, with `ChainBlockMap` as the per-worker map held in the lane; add a
`BackendKind` variant and a `run()` arm in `main`.

## Parity

The same corpus (128 workers, duplication 20, length factor 4; 2,446,195 operations, 320,105,993
block ops) was replayed against this crate's `PositionalIndexer` through both harnesses on one
144-CPU Neoverse-V2 host, Dynamo's competitor layout (event issuers on CPUs 0-3, query issuer on
4, 64 event lanes and 128 query lanes on 5-63, `numactl --interleave=all`, one process per trial,
3000 ms and 750 ms windows). Dynamo's binary ran the indexer through the out-of-tree adapter.

| Window | Harness | Achieved per trial (M block ops/s) | Lookup p50 (us) | Lookup p99 (us) | Scheduled to finished p99 (us) | Kept up |
| --- | --- | --- | --- | --- | --- | --- |
| 3000 ms | Dynamo `mooncake_bench`, SMG adapter | 106.1, 106.2, 106.2 (mean 106.2) | 17.3-18.2 | 113-138 | 3278-6972 | yes |
| 3000 ms | SMG `mooncake_replay` | 106.1, 106.2, 106.4 (mean 106.2) | 14.0-16.9 | 58-114 | 517-2350 | yes |
| 750 ms | Dynamo `mooncake_bench`, SMG adapter | 131.6, 132.4, 134.7, 136.9, 137.7, 140.9, 142.7, 144.6 (mean 137.7) | 15.5-21.1 | 378-487 | 1234-119188 | no |
| 750 ms | SMG `mooncake_replay` | 106.0, 135.4, 135.9, 141.1, 141.3, 143.5, 147.7, 148.2, 169.6, 172.2, 184.5 (mean 147.8) | 13.2-18.3 | 199-302 | 498-10019 | no |

Diagnostics at 750 ms, SMG harness: 64 lanes pinned round robin: 121.4; 59 lanes pinned one per core: 115.8, 113.1 (M block ops/s).

At the sustained window the two harnesses agree on throughput to 0.2%, with lookup latency a
little lower here (parked OS-thread lanes against tokio tasks). At the overloaded window, which
measures capacity, the SMG harness spreads wider: most trials sit in or just above Dynamo's
band, three ran 20-35% faster and one slower. The per-lane diagnostics explain the spread: an
event lane spent 36-41 us of CPU per event in a trial where it kept a core to itself and 50-74 us
across floating trials, and with 64 lanes on 59 cores the drain is set by whichever lanes end up
sharing cores and by what else the host is doing. Pinning does not help on this layout: 64 pinned
lanes double up five cores, and 59 lanes leave 128 workers unevenly spread (three workers on some
lanes, two on others). Dynamo's harness, whose query lanes are tokio workers on the same cores,
sits consistently in the slow mode. Capacity under overload is therefore a property of the lane
layout and the host as much as of the indexer, in both harnesses: sustained numbers are
comparable across harnesses, capacity numbers should be compared within one harness and over
many trials, and published comparisons keep using Dynamo's binary.

## Issuer scaling and the measurable ceiling

The generator has to issue the whole corpus within 1.01 x the window for a trial to count, so the
harness has a ceiling of its own. Measured with the `null` backend (lanes do no work) by
rescaling the 3000 ms corpus to shorter windows:

One query issuer on CPU 16, event issuers on CPUs 0..N-1, lanes on 17-63, null backend; cells give the generator verdict, the rate actually issued and the issue-lag p99 for reads (r) and updates (u):

| Event issuers | 300 ms (1.07B offered) | 150 ms (2.13B offered) | 100 ms (3.20B offered) | 75 ms (4.27B offered) | 50 ms (6.40B offered) |
| --- | --- | --- | --- | --- | --- |
| 1 | INVALID 0.32B issued, lag p99 r 87 / u 679963 us | INVALID 0.29B issued, lag p99 r 45467 / u 928753 us | INVALID 0.30B issued, lag p99 r 108645 / u 964914 us | INVALID 0.31B issued, lag p99 r 125718 / u 934179 us | INVALID 0.29B issued, lag p99 r 149054 / u 1028555 us |
| 2 | INVALID 0.77B issued, lag p99 r 85 / u 113491 us | INVALID 0.74B issued, lag p99 r 44837 / u 280203 us | INVALID 0.70B issued, lag p99 r 110769 / u 354107 us | INVALID 0.73B issued, lag p99 r 116020 / u 357057 us | INVALID 0.67B issued, lag p99 r 159181 / u 420190 us |
| 4 | valid 1.07B issued, lag p99 r 88 / u 141 us | INVALID 1.57B issued, lag p99 r 52848 / u 52953 us | INVALID 1.58B issued, lag p99 r 101456 / u 101607 us | INVALID 1.48B issued, lag p99 r 139067 / u 139189 us | INVALID 1.55B issued, lag p99 r 155040 / u 155227 us |
| 8 | valid 1.07B issued, lag p99 r 99 / u 94 us | INVALID 1.54B issued, lag p99 r 56819 / u 56868 us | INVALID 1.61B issued, lag p99 r 98170 / u 98234 us | INVALID 1.47B issued, lag p99 r 140528 / u 140618 us | INVALID 1.55B issued, lag p99 r 154695 / u 154785 us |
| 16 | valid 1.07B issued, lag p99 r 168 / u 152 us | INVALID 1.55B issued, lag p99 r 56348 / u 56333 us | INVALID 1.58B issued, lag p99 r 101028 / u 101059 us | INVALID 1.47B issued, lag p99 r 140846 / u 140902 us | INVALID 1.50B issued, lag p99 r 161154 / u 161223 us |

Ceiling (highest offered rate with a valid generator):
- event issuers 1: no valid window in the grid
- event issuers 2: no valid window in the grid
- event issuers 4: 1.07B block ops/s (300 ms window)
- event issuers 8: 1.07B block ops/s (300 ms window)
- event issuers 16: 1.07B block ops/s (300 ms window)

Same layouts with the overloaded `PositionalIndexer` (lanes busy, so publishes rarely wake a parked lane):

- t16 q1 150 ms: valid True, achieved 115.0M, lookup p50 9.8 us p99 396 us, drain 2634 ms
- t16 q1 50 ms: valid True, achieved 118.0M, lookup p50 7.8 us p99 323 us, drain 2661 ms

Two facts matter for measuring indexers far beyond today's numbers. First, event issuers scale
(about 0.3-0.4B block ops/s per thread here), but with one query issuer the generator stalls near
1.5B block ops/s whatever their number, because event issuers wait at every deadline for the
query issuer, which pays a wake-up per published query when the lanes are idle; the query issuer
is now sharded (`--query-issuer-threads`). Second, the null backend is the worst case for that
cost: with a busy backend the lanes rarely park, and 16 event issuers with a single query issuer
issued the whole corpus on schedule even at the 50 ms window (6.4B block ops/s offered) against
the overloaded `PositionalIndexer`.
