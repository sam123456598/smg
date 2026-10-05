//! Per-request and per-worker inputs a selection policy sees.
//!
//! The host (cache-aware routing) gathers these once per request from the indexer or tree, the
//! worker's routing state and the latest backend load snapshot, so a policy never touches a lock
//! or a tree itself. Everything a policy may want is here, and nothing a policy must not do (no
//! handles to workers, no mutable shared state).

/// The request being routed.
#[derive(Debug, Clone, Copy)]
pub struct RequestInputs<'a> {
    /// Prompt length in tokens (the routing key's length for text routing).
    pub prompt_tokens: usize,
    /// Cache block size in tokens, at least 1.
    pub block_size: usize,
    /// Prompt length in blocks, at least 1 (an empty prompt still costs one block of accounting).
    pub request_blocks: usize,
    /// Mean in-flight request count over the healthy fleet.
    pub avg_load: f64,
    /// Chain hash of the prompt's blocks, position `i` covering blocks `0..=i`, when the host
    /// computed them (event-driven and token routing). Policies that key on prefixes need them;
    /// without them they fall back to load-only behaviour.
    pub prefix_hashes: Option<&'a [u64]>,
}

/// One eligible worker as the policy sees it. Unknown signals are `None`, never zero: a policy
/// decides for itself how to treat a worker it has no load data for.
#[derive(Debug, Clone)]
pub struct CandidateInputs<'a> {
    /// Position in the host's worker slice; policies return picks by position in the candidate
    /// slice, the host maps them back.
    pub idx: usize,
    /// Worker URL: the stable identity for deterministic tie-breaking and hashing.
    pub url: &'a str,
    /// Prefix blocks this worker holds on the device tier (GPU), undecayed.
    pub device_blocks: f64,
    /// Prefix blocks held on a host-memory tier, when the index tracks tiers.
    pub host_blocks: f64,
    /// Prefix blocks held on a disk or shared tier, when the index tracks tiers.
    pub disk_blocks: f64,
    /// The host's decayed affinity score (device overlap after the waiting-prefill decay); what
    /// the pre-policy cache-aware decision ranked on.
    pub effective_score: f64,
    /// Requests the router currently has in flight on this worker.
    pub active_requests: usize,
    /// Tokens the backend reports as waiting to be prefilled (uncached), plus anything the router
    /// has booked optimistically since the last report.
    pub active_prefill_tokens: Option<u64>,
    /// KV blocks the requests in flight on this worker are estimated to hold (the host takes
    /// each to hold this request's blocks, plus any credited output blocks), when known.
    pub decode_blocks: Option<f64>,
    /// KV cache utilisation in `[0, 1]`, when reported.
    pub kv_usage: Option<f64>,
    /// Requests waiting in the backend's queue, when reported.
    pub queue_depth: Option<u64>,
    /// Requests the backend reports as running, when reported.
    pub running_requests: Option<u64>,
    /// Multiplier applied to a cost by taint-aware pickers; `1.0` when untainted.
    pub taint: f64,
    /// The host's expected wait on this worker, in seconds: queued token-work plus what this
    /// router dispatched since the worker's last report, over the worker's drain rate, plus the
    /// KV-pressure barrier. The number the host's own expected-wait selector ranks on; gathered
    /// only for policies that ask (`Needs::expected_wait`).
    pub expected_wait_secs: Option<f64>,
    /// Tokens per second that wait drains at (the worker's live generation rate, else the host's
    /// default), so a policy can price token-work it saves in the same unit. Gathered with the
    /// wait.
    pub drain_tokens_per_sec: Option<f64>,
    /// Requests this router dispatched to the worker since its last load report; `queue_depth`
    /// and `running_requests` do not include them yet.
    pub dispatched_since_report: u64,
}

impl CandidateInputs<'_> {
    /// Prompt tokens this worker would still have to prefill after its device-resident prefix.
    pub fn uncached_prompt_tokens(&self, request: &RequestInputs<'_>) -> usize {
        let cached = (self.device_blocks.max(0.0) * request.block_size as f64) as usize;
        request.prompt_tokens.saturating_sub(cached)
    }

    /// Waiting prefill tokens, treating an absent report as an empty queue.
    pub fn active_prefill_or_zero(&self) -> u64 {
        self.active_prefill_tokens.unwrap_or(0)
    }
}
