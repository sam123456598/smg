//! The split counters in the stats name the cause of every split: a chain diverging inside a
//! run, a removal leaving a hole, a store entering a run under a parent the worker did not hold
//! up to. The churn harness reads them to attribute fragmentation; this keeps them honest.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use kv_index::{
    compute_content_hash, request_prefix_hashes, ChainBlockMap, ContentHash, ShardedChainIndex,
    StoredBlock,
};

fn chain(stream: u64, shared: &[ContentHash], len: usize) -> Vec<StoredBlock> {
    let mut contents: Vec<ContentHash> = shared.to_vec();
    for position in contents.len()..len {
        contents.push(compute_content_hash(&[stream as u32, position as u32]));
    }
    contents
        .iter()
        .zip(request_prefix_hashes(&contents))
        .map(|(&content_hash, seq_hash)| StoredBlock {
            seq_hash,
            content_hash,
        })
        .collect()
}

#[test]
fn splits_are_counted_by_cause() {
    let index = ShardedChainIndex::new(2, 8);
    // Both on shard 0: a split needs the chains in one index (sharding is by worker).
    let a = index.intern_worker_in(0, "a").unwrap();
    let b = index.intern_worker_in(0, "b").unwrap();
    let (mut ma, mut mb) = (ChainBlockMap::default(), ChainBlockMap::default());
    let chain_a = chain(1, &[], 100);
    index.apply_stored(a, &chain_a, None, &mut ma).unwrap();
    let before = index.stats();
    assert_eq!(
        (
            before.splits_by_branch,
            before.splits_by_hole,
            before.splits_by_mid_run_store
        ),
        (0, 0, 0)
    );

    // A chain sharing the first 50 blocks diverges inside a's run: one branch split.
    let shared: Vec<ContentHash> = chain_a[..50]
        .iter()
        .map(|block| block.content_hash)
        .collect();
    let chain_b = chain(2, &shared, 100);
    index.apply_stored(b, &chain_b, None, &mut mb).unwrap();
    let stats = index.stats();
    assert_eq!(stats.splits_by_branch, 1, "a divergence inside a run");
    assert_eq!(stats.splits_by_hole, 0);
    assert_eq!(stats.runs_live, 3);

    // a drops blocks 20..30 and keeps the rest: a hole, so the tail becomes its own run.
    let hole: Vec<_> = chain_a[20..30].iter().map(|block| block.seq_hash).collect();
    index.apply_removed(a, &hole, &mut ma);
    let stats = index.stats();
    assert_eq!(stats.splits_by_hole, 1, "a removal that leaves a hole");
    assert_eq!(stats.splits_by_branch, 1);

    // Deaths: when every holder of a run is gone, the run is unlinked and counted.
    let died_before = index.stats().runs_died;
    let tail: Vec<_> = chain_b[50..].iter().map(|block| block.seq_hash).collect();
    index.apply_removed(b, &tail, &mut mb);
    assert!(
        index.stats().runs_died > died_before,
        "b's private tail run died with its only holder"
    );
}
