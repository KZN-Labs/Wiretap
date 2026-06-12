use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single filtered, decoded event handed to user code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub checkpoint: u64,
    pub checkpoint_timestamp_ms: u64,
    pub tx_digest: String,
    pub event_seq: u64,
    pub event_type: String,
    pub package_id: String,
    pub module: String,
    pub sender: String,
    /// Decoded JSON payload (may be `Null` if the fullnode only supplied BCS).
    pub json: Value,
    /// Raw BCS bytes of the event payload, base64-encoded for JSON-friendly transport.
    pub bcs_b64: String,
}

/// User-implemented sink. Pipeline calls `on_event` for every event matched
/// by the filter. `on_checkpoint` fires once per processed checkpoint after
/// all its events — use it to commit batches.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn on_event(&self, event: Event) -> anyhow::Result<()>;

    async fn on_checkpoint(&self, _seq: u64) -> anyhow::Result<()> {
        Ok(())
    }
}
