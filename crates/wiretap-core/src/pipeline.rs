//! The pipeline glues Source → catch-up → filter → decode → handler →
//! cursor commit. Two state machines run concurrently:
//!
//! * Producer: catches up from cursor → tip via `fetch_range`, then opens a
//!   `subscribe` stream. On reconnect, detects gaps in the new stream and
//!   fills them via `fetch_range` before forwarding new checkpoints.
//! * Consumer: filter + decode each transaction's events, dispatch to the
//!   handler, then commit the cursor. At-least-once semantics: cursor only
//!   advances after handlers return success.

use crate::cursor::Cursor;
use crate::decoder::decode_event;
use crate::filter::{CompiledFilter, FilterSpec};
use crate::handler::Handler;
use crate::layout::LayoutResolver;
use crate::source::{CheckpointBatch, Source};
use crate::{Error, Result};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy)]
pub enum Start {
    /// Start at the server's current tip — the natural Sui v2 subscribe behavior.
    Latest,
    /// Start at a specific historical checkpoint. The producer backfills from
    /// `n` up to the server's current tip via `LedgerService.GetCheckpoint`
    /// before opening the subscription.
    At(u64),
}

pub struct Pipeline<S: Source> {
    source: Arc<S>,
    cursor: Arc<Cursor>,
    filter: CompiledFilter,
    start: Start,
    reconnect_base: Duration,
    channel_depth: usize,
    resolver: Option<Arc<LayoutResolver>>,
}

impl<S: Source> Pipeline<S> {
    pub fn new(source: S, cursor: Cursor, filter: FilterSpec) -> Self {
        Self {
            source: Arc::new(source),
            cursor: Arc::new(cursor),
            filter: filter.compile(),
            start: Start::Latest,
            reconnect_base: Duration::from_millis(500),
            channel_depth: 256,
            resolver: None,
        }
    }

    pub fn start_at(mut self, s: Start) -> Self {
        self.start = s;
        self
    }

    pub fn channel_depth(mut self, n: usize) -> Self {
        self.channel_depth = n;
        self
    }

    /// Attach a layout resolver. When set, events whose server-supplied
    /// `json` field is null will be decoded locally from BCS — fullnodes
    /// vary in whether they populate `json`, so attaching a resolver makes
    /// output consistent across providers.
    pub fn with_resolver(mut self, r: Arc<LayoutResolver>) -> Self {
        self.resolver = Some(r);
        self
    }

    pub async fn run<H: Handler>(self, handler: H) -> anyhow::Result<()> {
        let handler = Arc::new(handler);
        let (tx, mut rx) = mpsc::channel::<CheckpointBatch>(self.channel_depth);

        let producer = {
            let source = self.source.clone();
            let cursor = self.cursor.clone();
            let start = self.start;
            let base = self.reconnect_base;
            tokio::spawn(async move {
                producer_loop(source, cursor, start, base, tx).await
            })
        };

        let mut processed: u64 = 0;
        let mut last_progress = std::time::Instant::now();
        while let Some(batch) = rx.recv().await {
            let cp = &batch.checkpoint;
            let seq = batch.seq();

            let mut matched: usize = 0;
            for txn in &cp.transactions {
                let events = match txn.events.as_ref() {
                    Some(e) => &e.events,
                    None => continue,
                };
                for (i, ev) in events.iter().enumerate() {
                    if self.filter.matches(txn, ev) {
                        let event = decode_event(
                            cp,
                            txn,
                            ev,
                            i as u64,
                            self.resolver.as_deref(),
                        )
                        .await;
                        handler.on_event(event).await?;
                        matched += 1;
                    }
                }
            }
            handler.on_checkpoint(seq).await?;
            self.cursor.commit(seq).map_err(anyhow::Error::from)?;
            processed += 1;
            if matched > 0 {
                debug!(checkpoint = seq, matched, "wiretap: processed");
            }
            // Periodic progress + lag-vs-tip. Roughly every 100 checkpoints
            // or every 5s, whichever comes first. We sample tip on the log
            // path only, so steady-state cost is one extra gRPC every ~5s.
            if processed % 100 == 0 || last_progress.elapsed() >= Duration::from_secs(5) {
                let tip = self.source.latest_checkpoint().await.ok();
                let lag = tip.map(|t| t.saturating_sub(seq));
                info!(
                    checkpoint = seq,
                    tip = ?tip,
                    lag_checkpoints = ?lag,
                    processed_total = processed,
                    "wiretap: progress"
                );
                last_progress = std::time::Instant::now();
            }
        }

        match producer.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow::Error::from(e)),
            Err(join) => Err(anyhow::Error::from(join)),
        }
    }
}

async fn producer_loop<S: Source>(
    source: Arc<S>,
    cursor: Arc<Cursor>,
    start: Start,
    base: Duration,
    tx: mpsc::Sender<CheckpointBatch>,
) -> Result<()> {
    let mut last_sent: Option<u64> = cursor.last()?;
    let mut attempt: u32 = 0;

    // ── Phase 1: initial catch-up from (last cursor or configured start) to tip.
    // Sui v2 SubscribeCheckpoints always begins at the server's current latest
    // checkpoint, so we must drain any historical gap via GetCheckpoint first
    // — both on a fresh run with --start=N and on every restart with a cursor.
    let catch_up_start = match (last_sent, start) {
        (Some(n), _) => Some(n + 1),
        (None, Start::At(n)) => Some(n),
        (None, Start::Latest) => None,
    };
    if let Some(begin) = catch_up_start {
        match source.latest_checkpoint().await {
            Ok(tip) if tip >= begin => {
                info!(
                    from = begin,
                    to = tip,
                    span = tip - begin + 1,
                    "wiretap: catching up before subscribe"
                );
                // Chunk so the consumer can commit cursor + progress between
                // pages — a single fetch_range(begin, tip) would block until
                // every GetCheckpoint completed.
                const CHUNK: u64 = 10;
                let mut cursor = begin;
                while cursor <= tip {
                    let end = (cursor + CHUNK - 1).min(tip);
                    let fill = source.fetch_range(cursor, end).await.map_err(|e| {
                        Error::Backfill {
                            checkpoint: cursor,
                            source: Box::new(e),
                        }
                    })?;
                    for f in fill {
                        let seq = f.seq();
                        if tx.send(f).await.is_err() {
                            return Ok(());
                        }
                        last_sent = Some(seq);
                    }
                    cursor = end + 1;
                }
            }
            Ok(_) => {} // we're already past tip
            Err(e) => warn!(error = %e, "wiretap: couldn't fetch tip for catch-up; will rely on subscribe"),
        }
    }

    // ── Phase 2: stream + reconnect-with-gap-fill loop.
    loop {
        debug!(?last_sent, "wiretap: subscribing");
        let stream_res = source.subscribe().await;
        let mut stream = match stream_res {
            Ok(s) => s,
            Err(e) => {
                let delay = backoff(base, attempt);
                warn!(error = %e, ?delay, "wiretap: subscribe failed, retrying");
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };
        attempt = 0;

        while let Some(item) = stream.next().await {
            let batch = match item {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "wiretap: stream error, will reconnect + backfill");
                    break;
                }
            };

            if let Some(prev) = last_sent {
                let expected = prev + 1;
                if batch.seq() > expected {
                    let (from, to) = (expected, batch.seq() - 1);
                    info!(from, to, "wiretap: backfilling gap via LedgerService.GetCheckpoint");
                    let fill = source.fetch_range(from, to).await.map_err(|e| {
                        Error::Backfill {
                            checkpoint: from,
                            source: Box::new(e),
                        }
                    })?;
                    for f in fill {
                        if tx.send(f).await.is_err() {
                            return Ok(());
                        }
                    }
                } else if batch.seq() < expected {
                    debug!(skip = batch.seq(), "wiretap: skipping already-seen checkpoint");
                    continue;
                }
            }

            last_sent = Some(batch.seq());
            if tx.send(batch).await.is_err() {
                return Ok(());
            }
        }

        let delay = backoff(base, attempt);
        debug!(?delay, "wiretap: stream ended, reconnecting");
        tokio::time::sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

fn backoff(base: Duration, attempt: u32) -> Duration {
    let mult = 1u64 << attempt.min(6);
    base.saturating_mul(mult as u32)
        .min(Duration::from_secs(30))
}
