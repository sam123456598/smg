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
   earlier, 2,154 in an earlier batch), so a consumer that slices the window group from the head files a second copy of
   the hash under the wrong content from the first block after the shared preamble onward. That is the shape of the
   live result: in both `cache_aware` phases every `event_hit` had `overlap_blocks=4` (the preamble) and the rest were
   `event_miss` (phase 3: 42 hits, 264 misses), while the engine served 158 of those 265 requests with at least half the
   prompt cached (engine-truth reuse 0.50). The servicer's normalizer and the gateway's content hashing both need the
   tail rule for `kv_cache_spec_kind = sliding_window`; the servicer lane is folding a tail-aligned window-group store
   into its vLLM scenario (dropped whole through the group gate rather than sliced from the head).
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

## Next

When the group-aware normalizer ships, the first run on the fleet is this same gpt-oss replay with the new wheel,
measuring `event_hit` overlap against engine reuse; the hybrid Mamba corpus follows once its weights are on the host.
