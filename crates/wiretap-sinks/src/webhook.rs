use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::Mutex;
use wiretap_core::{Event, Handler};

/// Buffers events up to `batch_size`, flushes on checkpoint boundary, and POSTs
/// JSON `{ "events": [...] }` to `url`. Exponential backoff on transient errors.
pub struct WebhookSink {
    url: String,
    batch_size: usize,
    buf: Mutex<Vec<Event>>,
    client: reqwest::Client,
}

impl WebhookSink {
    pub fn new(url: String, batch_size: usize) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        Self {
            url,
            batch_size: batch_size.max(1),
            buf: Mutex::new(Vec::with_capacity(batch_size.max(1))),
            client,
        }
    }

    async fn flush_locked(&self, buf: &mut Vec<Event>) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let body = serde_json::json!({ "events": &buf });
        let mut delay = Duration::from_millis(250);
        for attempt in 0..6u32 {
            let res = self.client.post(&self.url).json(&body).send().await;
            match res {
                Ok(r) if r.status().is_success() => {
                    buf.clear();
                    return Ok(());
                }
                Ok(r) if r.status().is_client_error() => {
                    // 4xx — don't retry. Drop the batch but surface the error.
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    buf.clear();
                    anyhow::bail!("webhook {status}: {body}");
                }
                Ok(r) => {
                    tracing::warn!(attempt, status = %r.status(), "webhook 5xx, retrying");
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "webhook transport error, retrying");
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(15));
        }
        anyhow::bail!("webhook: giving up after 6 attempts")
    }
}

#[async_trait]
impl Handler for WebhookSink {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        let mut buf = self.buf.lock().await;
        buf.push(e);
        if buf.len() >= self.batch_size {
            self.flush_locked(&mut buf).await?;
        }
        Ok(())
    }

    async fn on_checkpoint(&self, _seq: u64) -> anyhow::Result<()> {
        let mut buf = self.buf.lock().await;
        self.flush_locked(&mut buf).await
    }
}
