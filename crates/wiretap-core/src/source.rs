use crate::proto::Checkpoint;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// One checkpoint pulled off a [`Source`]. Wraps the proto type so the trait
/// stays stable if we add metadata (lag, partial flags, etc.) later.
#[derive(Debug, Clone)]
pub struct CheckpointBatch {
    pub checkpoint: Checkpoint,
}

impl CheckpointBatch {
    /// Sequence number; on the real Sui proto the field is optional, but the
    /// server always sets it for streamed checkpoints. We default to 0 on the
    /// (impossible) None case rather than panicking.
    pub fn seq(&self) -> u64 {
        self.checkpoint.sequence_number.unwrap_or(0)
    }
}

/// Abstraction over "something that yields ordered checkpoints". Lets us swap
/// real gRPC for a mock in tests, and lets the pipeline drive backfill via the
/// same trait that drives streaming.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Open a streaming subscription. The Sui v2 SubscribeCheckpoints RPC
    /// always begins at the *server's* current latest checkpoint — there is
    /// no client-side start argument. To catch up from an earlier point, the
    /// pipeline calls [`Source::fetch_range`] for the backfill window before
    /// (or alongside) this stream.
    async fn subscribe(
        &self,
    ) -> crate::Result<BoxStream<'static, crate::Result<CheckpointBatch>>>;

    /// Fetch checkpoints [from, to] inclusive. Used for the initial catch-up
    /// before subscribe, for the gap fill after a reconnect, and for the
    /// `wiretap backfill` CLI command.
    async fn fetch_range(&self, from: u64, to: u64) -> crate::Result<Vec<CheckpointBatch>>;

    /// Current chain tip sequence number — used to compute lag and pick a
    /// starting point when the user asks for "latest".
    async fn latest_checkpoint(&self) -> crate::Result<u64>;
}
