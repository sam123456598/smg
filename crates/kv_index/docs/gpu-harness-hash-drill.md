# Hash-check drill (guardrail 7: a worker hashing differently is detected, routing keeps working)

2026-10-05, the GB300 host. Four Qwen3-0.6B workers from the host venv through the loop-head Rust servicer
(`smg` wheel built from f4dc134b, `scripts/hash-drill.sh`, `SMG_KV_EVENT_HASH_CHECK=vllm-sha256-cbor` on all
four), gateway = the loop-head `smg` binary, policy cache_aware, `--log-level warn`, cores 64-71.
Workers 0-2 run with vLLM's default `NONE_HASH` seed (`PYTHONHASHSEED` unset); worker 3 runs with
`PYTHONHASHSEED=12345`, which vLLM uses verbatim as the `NONE_HASH` seed, so every chain it publishes
differs from the relay's default-seed rehash while its token ids and block layout are identical.

Workload: `bench_prefix.py` 256 prompts, 16 prefixes, 1024+128 tokens, 8 req/s through the gateway:
256/256 ok, 240 prefix hits, cached mean 960 tokens, TTFT p50 17.7 ms, p99 39 ms (routing unaffected).

Relay counters logged by each servicer when the gateway restart closed its `SubscribeKvEvents` stream
(`results/hash-drill-counts.txt`):

| worker | seed | forwarded_stored | hash_checked | hash_mismatch | hash_unverifiable |
|---|---|---|---|---|---|
| drill-w0 | default | 260 | 962 | 0 | 0 |
| drill-w1 | default | 261 | 962 | 0 | 0 |
| drill-w2 | default | 266 | 962 | 0 | 0 |
| drill-w3 | `PYTHONHASHSEED=12345` | 266 | 964 | **964** | 0 |

The counter rises only on the mismatching worker, for every checked block, and nothing is dropped
(`dropped: {}`), so routing to that worker keeps working on the engine's own hashes. Note for operators:
the harness's earlier containers set `PYTHONHASHSEED=0`, which would make every worker mismatch under this
check; the launchers now leave it unset unless asked.
