# `GetLoads` from a live vLLM 0.31.0 worker behind the Rust servicer (loop head e69487f8)

Recorded on devgpu044 (GB300) on 2026-10-05 against `8b-w0` of the harness fleet: `vllm serve Qwen/Qwen3-8B --grpc
--servicer-impl rust --max-model-len 40960 --max-num-seqs 128 --gpu-memory-utilization 0.4` (676,144-token KV pool),
with `scripts/getloads_dump.py` polling `GetLoads` every 0.5 s. One JSON object per line: `epoch`, `worker`, `version`
(the engine's), `loads` (the `SchedulerLoad` message with defaults printed, so every field is present) and `gateway`
(the gateway's `smg_engine_*` gauges for the same worker at that moment; the gateway polls loads every
`--load-monitor-interval` seconds, 10 by default, which is the lag visible between the two columns).

| file | load | what it shows |
|---|---|---|
| `qwen3-8b-vllm0.31-getloads-bench16.jsonl` | `bench_prefix.py` through a `cache_aware` gateway: 16 prefixes x 20 prompts, 1,024 + 128 tokens, 64 out, 16 req/s (`bench16.json`) | `gen_throughput` up to 1,426 tok/s, `cache_hit_rate` up to 0.889, `num_used_tokens` up to 12,640, `utilization` = `token_usage`; `num_waiting_reqs` and `num_waiting_uncached_tokens` stay 0 because vLLM admits this load straight into the running batch |
| `qwen3-8b-vllm0.31-getloads-burst96x8192.jsonl` | 96 fresh 8,192-token prompts at once over gRPC (`getloads_under_load.py`), 64 output tokens each | the queue forms: `num_waiting_reqs` 89 -> 0 over 9 s, `num_waiting_uncached_tokens` up to 472,415 (73 waiting x 8,192 x (1 - hit rate 0.21)), `token_usage` up to 0.997, `gen_throughput` 4 -> 385 tok/s during the prefill wave and 1,235-1,675 in decode, `cache_hit_rate` decaying 0.89 -> 0 as the fresh prompts get their first outputs |

The servicer derives the three new fields from its own bookkeeping (`engine_servicer::load_tracker`): queued token-work
from the forwarded requests the engine has not started, discounted by the recent hit rate; throughput from the tokens it
streamed in the last two seconds; the hit rate from the last 64 first outputs. `max_total_num_tokens`, `num_used_tokens`,
`token_usage` and the counts are the engine's own scheduler stats.
