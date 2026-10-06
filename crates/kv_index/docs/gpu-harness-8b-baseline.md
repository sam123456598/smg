# Qwen3-8B baseline (2026-10-05, the GB300 host)

Fleet: 4 x `vllm serve Qwen/Qwen3-8B --grpc --servicer-impl rust` run from the host venv (`scripts/run-vllm-grpc-host.sh`, vLLM 0.31.0, one per GB300, `--gpu-memory-utilization 0.4` (0.3 on GPU 3), `--max-model-len 8192`), gateway `smg 1.11.0` (branch leap/gpu-harness) at `--log-level warn` on cores 64-71, clients on 64-71 (`scripts/bench_prefix.py`). Workload: prefix repetition, 16 prefixes x 32 prompts, 1024 prefix + 128 suffix tokens, 64 output tokens with ignore_eos, Poisson 16 req/s, concurrency 64, 512 requests (128 at 4 req/s).

| run | ok | req/s | cached tokens mean | TTFT p50 / p90 / p99 (ms) | TPOT p50 / p90 / p99 (ms) | E2E p50 / p90 / p99 (ms) |
|---|---|---|---|---|---|---|
| gateway cache_aware, 4 x 8B workers, 16 req/s (run 1, cold engines) | 512/512 | 16.88 | 992 | 78.64 / 213.47 / 552.55 | 5.133 / 9.216 / 20.331 | 418.3 / 803.7 / 1626.4 |
| gateway cache_aware (run 2, warm) | 512/512 | 15.1 | 1040.4 | 57.75 / 90.35 / 369.22 | 4.539 / 5.687 / 10.374 | 330.1 / 439.6 / 809.1 |
| gateway round_robin (run 1, cold) | 512/512 | 14.99 | 956.2 | 106.9 / 18621.31 / 21076.62 | 8.17 / 16.714 / 20.157 | 702.3 / 19126.9 / 21414.9 |
| gateway round_robin (run 2, warm) | 512/512 | 12.61 | 896 | 72.87 / 4982.87 / 9755.12 | 4.66 / 8.338 / 95.989 | 375.8 / 5928.0 / 10391.5 |
| one plain 8B engine, 0.3 of GPU 3 for KV, 4 req/s (= one worker's share) | 128/128 | 3.53 | 896 | 111.64 / 278.31 / 689.43 | 4.573 / 15.91 / 83.782 | 401.9 / 640.8 / 959.5 |
| one plain 8B engine, 0.3 KV, 16 req/s (the whole fleet load) | 512/512 | 13.92 | 1052.2 | 1511.56 / 3678.63 / 4156.33 | 87.354 / 297.38 / 519.641 | 1946.6 / 4143.4 / 4489.2 |
| one plain 8B engine, 0.1 KV (KV-starved, invalid), 4 req/s run 2 | 128/128 | 3.52 | 896 | 294.24 / 6206.4 / 8179.48 | 6.05 / 8.981 / 93.758 | 657.6 / 7548.0 / 8298.9 |
| one plain 8B engine, 0.1 KV (invalid), 16 req/s run 2 | 512/512 | 9.57 | 1037.0 | 370.49 / 14836.2 / 14942.56 | 8.306 / 82.877 / 1012.868 | 994.0 / 15201.0 / 15221.1 |

Reading: at 16 req/s the cache-aware gateway keeps every request on the worker that already holds its prefix (cached mean 1040 of 1152 tokens) and the fleet absorbs the load (TTFT p50 58 ms, p99 369 ms in the warm run); round robin spreads each prefix over all four workers, pays the uncached 1152-token prefill four times as often on an 8B model and falls behind the arrival rate (12.6-15 req/s delivered, p90 TTFT 5-19 s). The direct single-engine rows show the engine's own behaviour at one worker's share of the load (4 req/s: TTFT p50 112 ms) and at the full fleet load (saturated). The first reference instance ran with only 0.1 of the GPU for KV (about 80 k tokens), which 64 in-flight requests of 1216 tokens exceed, so those two rows are kept only as a record of a misconfigured reference. Cold-engine first runs include FlashInfer kernel autotuning on first use of each shape; run 2 is the number to quote.
