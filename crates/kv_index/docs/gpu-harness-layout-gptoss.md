# Layout coverage of the KV-event path, part 1: gpt-oss-20b (MoE, sliding-window layers) on vLLM 0.31

The index sees only block-hash chain events, so the question for other attention layouts is what the engine publishes.
This note records the first corpus: `openai/gpt-oss-20b` (24 layers: 12 `sliding_attention` with window 128, 12
`full_attention`; MXFP4 MoE) served by vLLM 0.31.0 through the 85f9d28c Rust servicer on one GB300, from a local
read-only copy of the weights, with `--kv-cache-memory-bytes 4 GiB` (87,333 tokens) so the cache evicts constantly, 32
sequences, `--block-size 16`, `sha256_cbor` hashes, KV events on. The capture, the decoded per-block events, the census
and the two offline drivers (exactness oracle, hash reproduction) are kept with the harness results outside the tree,
next to a README that says how to re-run them; nothing from this corpus is checked in. The hybrid Mamba/attention corpus
is not taken: its weights are not on the host and fetching them is the user's call.

## Table

| model | engine | prefix caching | KV-cache groups and block sizes | holes seen | hashes reproduced | SSM state published |
|---|---|---|---|---|---|---|
| gpt-oss-20b (12 sliding_attention / window 128, 12 full_attention, MXFP4 MoE) | vLLM 0.31.0, FlashInfer bf16 prefill, trtllm-gen decode | on (`sha256_cbor`, block 16, 87,333-token pool) | two groups, `[16, 16]`: group 0 `sliding_window` (128), group 1 `full_attention`; every event carries `group_idx` and `kv_cache_spec_kind`, group 0 also `kv_cache_spec_sliding_window` | yes: 1,744 removals of blocks with stored descendants; the window group stores only the blocks inside the window (6,358 against 148,707) and removes them as it moves; vLLM also keeps up to two physical copies per hash and removes them one at a time (6,428 single-copy removals) | full-attention group 148,707 / 148,707 with the servicer's `vllm_block`; window group: the same hashes, once its `block_hashes` are read against the tail of `token_ids` (below) | n/a (no SSM layers) |
| hybrid Mamba (granite-4.0-h-tiny or Nemotron-H-8B) | vLLM 0.31 (GraniteMoeHybrid, NemotronH registered), SGLang 0.5.21 (GraniteMoeHybrid overrides present) | not run | - | - | - | not run: weights not on the host |

## Served and captured

Served after a ten-minute first start (13.8 GB of weights read in place, torch.compile 17.6 s, FlashInfer kernels
fetched, CUDA graphs), served name `openai/gpt-oss-20b`; vLLM's hybrid KV-cache manager logged `kv cache group sizes
[16, 16]`. Capture: 1,132 batches (sequence 0-1131, no gaps), 83.9 MB raw / 9.5 MB gzipped, from three Mooncake
replays of 300 rows each (265 served per phase; the rest exceed the 16,384-token context): `BlockStored` 4,400 events
(2,200 per group) carrying 155,065 blocks (group 1: 148,707; group 0: 6,358); `BlockRemoved` 145,840 events of one hash
each (group 1: 139,962; group 0: 5,878); `AllBlocksCleared` 0; `extra_keys` null throughout; hashes u64, identical for
the same token block in both groups (all 4,213 distinct window-group hashes appear among the 91,262 full-attention ones);
only 3 position-0 stores in the whole run, because the 64-token harmony preamble (system prompt with knowledge cutoff,
current date, reasoning level and channels, then the developer turn) is shared by every request and stays hot.

## What this layout does to the index

1. **Physical copies.** vLLM keeps more than one physical block per hash (a second copy on a re-hit, and the window
   group's copy) and publishes `BlockRemoved` when one copy is evicted while the hash stays cached. An index with
   single-copy semantics reads the first removal as the eviction and rejects every later child store (parent not held):
   fed the raw per-block events, both the reference indexer and the production chain index reject 144,443 of 155,065
   stores and end empty, agreeing with each other exactly (0 disagreeing outcomes, 19,382 lookups, 0 mismatches). With
   copy counting (a store when the first copy appears, a removal when the last goes, which is how the gateway's monitor
   counts copies per tier) all 148,157 stores are accepted and 296,314 lookups (every chain head) score identically in
   both indexers.
2. **Production misses 24-32 blocks.** On the copy-counted two-group feed the production index ends with 8,261 blocks
   against the reference's 8,293 (32 missing, at positions 4 and 5, 0 phantom); on the window-group-only feed 424 against
   448 (24 missing); on the full-attention-only feed 8,745 against 8,745. The sampled lookups did not change score. This
   is the run-index lane's item, with the feed files kept beside the capture.
3. **Window-group events are tail-aligned.** A window-group `BlockStored` carries the token ids of the whole span the
   request computed but lists only the hashes of the blocks inside the window: in 2,179 of 2,200 events the listed hashes
   belong to the last `len(block_hashes) x 16` tokens of `token_ids`, not the first. Read head-aligned, 5,629 of the
   window group's 5,641 verifiable hashes mismatch the servicer's reproduction; read tail-aligned they are the
   full-attention group's hashes. The full-attention group stores each shared hash first (4,204 in the same batch listed
   earlier, 2,154 in an earlier batch), so a consumer that slices the window group from the head would file a second
   copy of the hash under the wrong content from the first block after the shared preamble onward. The servicer's
   normalizer has dropped the window group whole on ranks with a main-attention group since d868d876 (in the 85f9d28c
   wheel), stores and removals alike, so no window event reached the live index; the tail rule matters only for a
   pure sliding-window model, and the servicer lane's group-aware normalizer now takes the last k blocks there and
   drops the k=0 placeholder events (1,399 of the 2,200 window-group events carry no hashes).
   **Live result, unexplained.** In both `cache_aware` phases every `event_hit` had `overlap_blocks=4` (the preamble)
   and the rest were `event_miss` (phase 3: 42 hits, 264 misses), while the engine served 158 of those 265 requests with
   at least half the prompt cached (engine-truth reuse 0.50). An earlier draft blamed the window group's token layout;
   that is withdrawn. What the data supports: the live picture is the signature of single-copy semantics on this
   stream. Fed the raw full-attention events with one copy per hash, both indexers cut every chain right after the
   preamble (the fork block at position 4 is computed concurrently by the first requests, gets two physical copies,
   and the eviction of one copy reads as its removal; 93 % of stores then fail on a missing parent), which is exactly
   a four-block overlap for every later request; fed copy-counted events they hold the chains. Where the second copy
   is collapsed on the live path (the relay's live-block store, the normalizer, or the monitor's copy accounting) is
   what the re-run on the group-aware wheel, with its drop reasons and hash counters in the relay-closed line, has to
   say; the oracle's position-4 and -5 discrepancy (item 2) sits at the same fork and is the run-index lane's item.
4. **Relay paths on this stream.** Late join: the relay started with the first `cache_aware` subscription after 385
   batches, applied 361 live batches and never learned what was stored before (relay-closed line `relayed: 747,
   served_snapshots: 1, publisher_gaps: 0, live_blocks=8201` against 8,745 hashes live in the engine). That is the
   behaviour before the boot-time relay start and the start-time replay priming, which the pushed head carries since
   a926881d; 85f9d28c predates them, so a re-run on the current head should show the relay primed from the publisher's
   replay before the first subscription. Snapshot: the next fresh gateway, after the 300-batch window had rolled, got
   `through=745 blocks=8344 chunks=5`, one `snapshot` resync, index gauge 8,348. Hash check: the servicer accepted
   `SMG_KV_EVENT_HASH_CHECK=vllm-sha256-cbor` (no "check off" warning) but 85f9d28c surfaces no `hash_checked` /
   `hash_mismatch` counts in logs or metrics; the offline reproduction above stands in until the servicer lane's
   follow-up.

## Re-run on 823792b5: the cap was the servicer's, and it is gone

Same session on the pushed head 823792b5 (servicer wheel with the KV-cache group policy, the normalizer and hash-check
counters, the pushed load records and the start-time replay priming; gateway and replay client from the same revision),
same engine launch, same three phases, plus the replay joining each debug gateway's decisions (`agree` per request) and a
fourth phase that sends one fixed 3,656-token prompt twice through the phase-3 gateway.

| phase (823792b5 wheel + gateway) | served | router branches | router credit / prompt | engine reuse | `agree` | `event_hit` overlap blocks min / median / max |
|---|---|---|---|---|---|---|
| 2: first `cache_aware` gateway, rows 300-599 | 260 | `event_hit` 260, `event_miss` 0 | 0.518 | 0.518 | 260 / 260 | 4 / 34 / 364 |
| 3: fresh gateway after the window rolled (snapshot), rows 0-299 again | 265 | `event_hit` 265, `event_miss` 0 | 0.503 | 0.503 | 264 / 265 | 4 / 154 / 365 |
| 4: trace prompt, send 1 | 1 | `event_hit` 4 of 228 blocks (the preamble, which the engine had meanwhile evicted: `cached_tokens` 0) | | | | |
| 4: trace prompt, send 2 | 1 | `event_hit` 228 of 228 blocks; engine `cached_tokens` 3,648 of 3,656 (every full block) | | | | |

The router's credit now equals engine truth on every request but one, and the overlap distribution is the engine's
(34 blocks for the 544-token shared prefixes, 364-365 for the 5,824-token ones). Relay closing line: relayed 1,135,
served_snapshots 2, publisher_gaps 0, `forwarded_stored` 2,199, `forwarded_removed` 140,173, `duplicate_stores` 1,
`dropped: {NonMainAttentionGroup: 8,077}` (the window group's 2,200 stores and 5,877 removals), `window_only_stores` 0,
`tail_aligned_stores` 0, `hash_checked` 10,558, `hash_mismatch` 0, `hash_unverifiable` 138,377, live_blocks 8,762; the
relay asked the publisher's replay at start (it held nothing yet) and the late-join loss did not recur. The
`hash_unverifiable` count is exactly the single-copy cascade of item 1: the checker's parent memory forgets a hash at its
first removal, so 93 % of the main group's stores cannot be verified although the offline reproduction verifies all of
them; a copy-counted parent memory would check them all.

Trace, both sides: the gateway's tokenization of the HF-rendered prompt string and the HF tokenizer agree token for
token (3,654); the engine's stored prompt for the chat request has two more tokens before the user text (the gateway's
own chat rendering, 3,656) and is identical to the gateway's ids from the user text on (3,550 of 3,550 compared), so the
lookup and the stored blocks hash the same ids, which is what the 228-of-228 overlap shows.

Attribution (A/B): the 85f9d28c servicer wheel under the same 823792b5 gateway brings the cap back in full: phase 2
`event_hit` 61 (all at 4 blocks) / `event_miss` 199, router credit 0.003 of the prompt against engine reuse 0.518; phase 3
30 / 235, credit 0.001 against 0.503; `agree` 0 of 525; the trace prompt's second send alone matched 228 blocks (a fresh
chain, no eviction involved). The four-block cap was therefore on the servicer side of 85f9d28c (what the relay forwarded
or how the normalizer chained stores after a copy's removal), not in the gateway's index, and 823792b5 removes it. The
servicer lane names the mechanism from its side; this run closes the question for the harness.

## Next

When the group-aware normalizer ships, the first run on the fleet is this same gpt-oss replay with the new wheel,
reading the relay's drop reasons and hash counters live and measuring `event_hit` overlap against engine reuse, with
one request's lookup traced to the block where its overlap stops; the hybrid Mamba corpus follows once its weights are
on the host.
