//! Sinks for wiretap. Each sink is a [`Handler`] that the user can plug into
//! [`wiretap_core::Pipeline`]. The `Sink` trait is a tiny wrapper that adds a
//! `flush` hook for batched sinks; for single-event sinks just implement
//! `Handler` directly.

use async_trait::async_trait;
use wiretap_core::Handler;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteSink;

#[cfg(feature = "stdout")]
pub mod stdout;
#[cfg(feature = "stdout")]
pub use stdout::StdoutSink;

#[cfg(feature = "webhook")]
pub mod webhook;
#[cfg(feature = "webhook")]
pub use webhook::WebhookSink;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresSink;

/// Marker trait — anything `Sink` is also a `Handler`. Useful when you want
/// to add buffering/flush semantics to your own sink.
#[async_trait]
pub trait Sink: Handler {
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

// (Implement `Sink` directly on your type; we don't blanket-impl over Handler
// to keep the trait open for downstream coherence.)

/// Build a sink from a `[sink]` config block. Returns a boxed Handler so the
/// CLI can dispatch on `type` at runtime.
pub fn build_from_config(
    cfg: &wiretap_core::SinkConfig,
) -> anyhow::Result<Box<dyn Handler>> {
    match cfg.kind.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let path = cfg
                .params
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("events.db");
            Ok(Box::new(SqliteSink::open(path)?))
        }
        #[cfg(feature = "stdout")]
        "stdout" => Ok(Box::new(StdoutSink::new())),
        #[cfg(feature = "webhook")]
        "webhook" => {
            let url = cfg
                .params
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("[sink] type=webhook requires `url`"))?;
            let batch = cfg
                .params
                .get("batch_size")
                .and_then(|v| v.as_integer())
                .unwrap_or(50) as usize;
            Ok(Box::new(WebhookSink::new(url.to_string(), batch)))
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let url = cfg
                .params
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("[sink] type=postgres requires `url`"))?;
            Ok(Box::new(futures::executor::block_on(
                PostgresSink::connect(url),
            )?))
        }
        other => anyhow::bail!("unknown sink type `{other}`"),
    }
}

// Re-export Event/Handler so downstream users don't have to also depend on
// wiretap-core directly when they're only writing a sink.
pub use wiretap_core::{Event as _Event, Handler as _Handler};
