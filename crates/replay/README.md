# replay

Replays a Mooncake-format trace (`{timestamp, input_length, output_length,
hash_ids}` per line, one `hash_id` per 512-token block) through the gateway and
scores routing quality end to end.

- Every `hash_id` becomes a deterministic text block (`--words-per-block`, 480
  words, about one token each), so rows that share ids share prompt prefixes
  after the gateway tokenizes them.
- Requests are sent open-loop at `timestamp / --speedup` as streaming chat
  completions with `stream_options.include_usage`, recording TTFT, inter-token
  latencies, end-to-end latency, the serving worker (`system_fingerprint`, which
  the gateway sets from the worker's `weight_version` label), and the
  engine-reported `cached_tokens`.
- With `--admin <mock admin url>` each request is joined with the mock fleet's
  record of it (`GET /admin/requests`), adding the arrival-time oracle (the most
  cached tokens any worker held when it arrived) and the queue wait.

Output: `summary.json` (mean/p50/p90/p99 TTFT, per-request mean ITL (TPOT)
distribution, e2e latency, goodput at the SLO `--slo-ttft-ms` / `--slo-itl-ms`
(default 500 ms TTFT and 50 ms per-request mean ITL; a strict variant uses the
per-request p99), prefix reuse = cached / prompt tokens, oracle prefix reuse,
hit-over-oracle, per-worker request and uncached-token counts, balance) and
`requests.csv` with one row per request.

```bash
replay --trace mooncake_trace.jsonl --gateway http://127.0.0.1:31000 \
  --model mock-model --speedup 4 --limit 5000 --admin http://127.0.0.1:31002 --out out/
```

With `--gateway-log <file>` (the gateway run at `--log-level debug`) the
gateway's routing decisions are joined to the requests by request id (the
`x-request-id` header, which is the response id without its uuid tail) and
`t4.md` prints the T4 table (the hardware harness's columns): `phase, idx, worker, branch,
prompt_tokens, engine cached_tokens, implied overlap, agree`. The implied
overlap is the gateway's stated credit when its log carries one
(`overlap_tokens=`, `overlap_blocks=` or the tree path's `matched_ratio=`);
otherwise `agree` compares the branch's claim of an overlap (`event_hit`,
`event_spill`) with whether the engine served cached tokens. With `--admin`
the summary also carries `engine_truth_per_worker`, each engine's own account
of the prompt, cached and oracle tokens it served.

The mock fleet it is meant for is `mock-worker --engine realistic --admin-port`;
see `crates/mock_worker/README.md` for the engine's scheduler, KV pool and
timing model (and the caveat that its timing polynomials were validated against
hardware only with prefix caching off).

## Soak (guardrail 3)

A soak keeps one gateway and one mock fleet up for 24 hours and replays the
trace window by window with this replayer (`--skip` advancing by `--limit`
each window, `--admin` for the oracle join, a label per window), while:

- a fault scheduler drives the mock's admin fault hooks on a cycle, over the
  workers in turn: drop 20 batches, a 1000 ms publishing delay for 60 s, a
  publisher restart, pause 30 s then resume, and a worker restart (cache
  reset plus publisher restart, what the index sees when an engine restarts);
- a sampler appends one row per minute: gateway RSS and its allocator gauges,
  its cache-aware branch counters, match-ratio mean, engine cache-hit gauge
  and KV-subscription failures from `/metrics`, and from the mock's admin API
  the last minute's requests, hit/oracle, prefix reuse, per-worker balance,
  preemptions and KV batches.

The report reads the guardrail on the allocator's live bytes at idle (hour N
against hour 1, within 5%), with RSS as the retention indicator, and prints
hit/oracle, reuse and balance in the minutes before and after each fault, the
counters, and the per-window table (rows served, TTFT, goodput, hit/oracle,
reuse, balance, active workers). The scripts live outside the crate.
