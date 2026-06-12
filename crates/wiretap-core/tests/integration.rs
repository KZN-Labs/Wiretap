#![allow(clippy::result_large_err, clippy::type_complexity)]

//! End-to-end pipeline test using an in-process MockSource that scripts:
//!   * canned checkpoints with events (real Sui v2 proto message shapes),
//!   * a forced disconnect (stream end at a midpoint),
//!   * resume — verifying gap backfill via fetch_range and cursor monotonicity.
//!
//! No network, no protoc at runtime — proto compilation already happened at
//! build time via tonic-build + the vendored protoc.

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Mutex as AsyncMutex;
use wiretap_core::layout::{LayoutProvider, LayoutResolver};
use wiretap_core::proto::{
    open_signature_body::Type as OpenType, BalanceChange, Bcs, Checkpoint, CheckpointSummary,
    DatatypeDescriptor, Event as ProtoEvent, ExecutedTransaction, FieldDescriptor,
    OpenSignatureBody, Transaction, TransactionEvents,
};
use wiretap_core::source::{CheckpointBatch, Source};
use wiretap_core::{Cursor, Event, FilterSpec, Handler, Pipeline};

/// Build a Checkpoint with `events` as `(event_type, package_id, sender)` tuples,
/// all wrapped into a single ExecutedTransaction.
fn cp(seq: u64, events: Vec<(&str, &str, &str)>) -> Checkpoint {
    let evs: Vec<ProtoEvent> = events
        .into_iter()
        .map(|(t, pkg, sender)| ProtoEvent {
            event_type: Some(t.into()),
            package_id: Some(pkg.into()),
            module: Some(t.split("::").nth(1).unwrap_or("").into()),
            sender: Some(sender.into()),
            contents: Some(Bcs {
                name: Some(t.into()),
                value: Some(vec![]),
            }),
            json: None,
        })
        .collect();
    Checkpoint {
        sequence_number: Some(seq),
        digest: Some(format!("d-{seq}")),
        summary: Some(CheckpointSummary {
            sequence_number: Some(seq),
            epoch: Some(1),
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000 + seq as i64,
                nanos: 0,
            }),
            ..Default::default()
        }),
        transactions: vec![ExecutedTransaction {
            digest: Some(format!("tx-{seq}")),
            transaction: Some(Transaction {
                sender: Some("0xdeadbeef".into()),
                ..Default::default()
            }),
            events: Some(TransactionEvents {
                events: evs,
                ..Default::default()
            }),
            balance_changes: vec![
                BalanceChange {
                    address: Some("0xalice".into()),
                    ..Default::default()
                },
                BalanceChange {
                    address: Some("0xbob".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[derive(Clone, Default)]
struct MockSource {
    /// FIFO queue of scripts. Each script is a stream's worth of checkpoints.
    scripts: Arc<Mutex<std::collections::VecDeque<Vec<Checkpoint>>>>,
    subscribe_calls: Arc<AtomicUsize>,
    fetch_range_calls: Arc<AtomicUsize>,
    fetch_range_log: Arc<Mutex<Vec<(u64, u64)>>>,
    tip: Arc<AtomicUsize>,
}

impl MockSource {
    fn push_script(&self, cps: Vec<Checkpoint>) {
        if let Some(last) = cps.last() {
            if let Some(s) = last.sequence_number {
                self.tip.store(s as usize, Ordering::SeqCst);
            }
        }
        self.scripts.lock().unwrap().push_back(cps);
    }
}

#[async_trait]
impl Source for MockSource {
    async fn subscribe(
        &self,
    ) -> wiretap_core::Result<BoxStream<'static, wiretap_core::Result<CheckpointBatch>>> {
        self.subscribe_calls.fetch_add(1, Ordering::SeqCst);
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let items: Vec<wiretap_core::Result<CheckpointBatch>> = script
            .into_iter()
            .map(|c| Ok(CheckpointBatch { checkpoint: c }))
            .collect();
        Ok(Box::pin(stream::iter(items)))
    }

    async fn fetch_range(&self, from: u64, to: u64) -> wiretap_core::Result<Vec<CheckpointBatch>> {
        self.fetch_range_calls.fetch_add(1, Ordering::SeqCst);
        self.fetch_range_log.lock().unwrap().push((from, to));
        Ok((from..=to)
            .map(|s| CheckpointBatch {
                checkpoint: cp(s, vec![("0xpkg::pool::SwapEvent", "0xpkg", "0xdeadbeef")]),
            })
            .collect())
    }

    async fn latest_checkpoint(&self) -> wiretap_core::Result<u64> {
        Ok(self.tip.load(Ordering::SeqCst) as u64)
    }
}

#[derive(Default, Clone)]
struct Collect {
    events: Arc<AsyncMutex<Vec<Event>>>,
    checkpoints: Arc<AsyncMutex<Vec<u64>>>,
    stop_after: usize,
}

#[async_trait]
impl Handler for Collect {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        self.events.lock().await.push(e);
        Ok(())
    }
    async fn on_checkpoint(&self, seq: u64) -> anyhow::Result<()> {
        let mut cps = self.checkpoints.lock().await;
        cps.push(seq);
        if self.stop_after > 0 && cps.len() >= self.stop_after {
            anyhow::bail!("stop_after reached");
        }
        Ok(())
    }
}

#[tokio::test]
async fn filter_passes_only_matching_events() {
    let src = MockSource::default();
    src.push_script(vec![
        cp(
            1,
            vec![
                ("0xpkg::pool::SwapEvent", "0xpkg", "0xdeadbeef"),
                ("0xpkg::farm::Harvest", "0xpkg", "0xdeadbeef"),
            ],
        ),
        cp(2, vec![("0xother::nft::Mint", "0xother", "0xdeadbeef")]),
        cp(
            3,
            vec![("0xpkg::pool::AddLiquidity", "0xpkg", "0xdeadbeef")],
        ),
    ]);

    let cursor = Cursor::in_memory().unwrap();
    let filter = FilterSpec::default().with_event("0xpkg::pool::*");
    let collect = Collect {
        stop_after: 3,
        ..Default::default()
    };

    let pipeline = Pipeline::new(src.clone(), cursor, filter);
    let _ = pipeline.run(collect.clone()).await;

    let events = collect.events.lock().await;
    let kinds: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        kinds,
        vec!["0xpkg::pool::SwapEvent", "0xpkg::pool::AddLiquidity"]
    );
}

#[tokio::test]
async fn gap_backfill_on_reconnect() {
    let src = MockSource::default();
    // FIFO: first push → first popped → first stream yielded.
    src.push_script(vec![
        cp(1, vec![("0xpkg::pool::SwapEvent", "0xpkg", "0xs")]),
        cp(2, vec![("0xpkg::pool::SwapEvent", "0xpkg", "0xs")]),
    ]);
    src.push_script(vec![cp(
        5,
        vec![("0xpkg::pool::SwapEvent", "0xpkg", "0xs")],
    )]);

    let cursor = Cursor::in_memory().unwrap();
    let filter = FilterSpec::default().with_event("0xpkg::pool::*");
    let collect = Collect {
        stop_after: 5,
        ..Default::default()
    };

    let pipeline = Pipeline::new(src.clone(), cursor, filter).channel_depth(8);
    let _ = pipeline.run(collect.clone()).await;

    let cps = collect.checkpoints.lock().await.clone();
    assert_eq!(cps, vec![1, 2, 3, 4, 5], "checkpoints must be contiguous");

    let log = src.fetch_range_log.lock().unwrap().clone();
    assert!(
        log.contains(&(3, 4)),
        "expected gap backfill of (3,4) via LedgerService.GetCheckpoint, got {log:?}"
    );
}

/// Mock layout provider keyed by full (`package`, `module`, `name`).
#[derive(Default, Clone)]
struct MockLayoutProvider {
    table: Arc<Mutex<std::collections::HashMap<(String, String, String), DatatypeDescriptor>>>,
}
impl MockLayoutProvider {
    fn insert(&self, pkg: &str, m: &str, n: &str, d: DatatypeDescriptor) {
        self.table
            .lock()
            .unwrap()
            .insert((pkg.into(), m.into(), n.into()), d);
    }
}
#[async_trait]
impl LayoutProvider for MockLayoutProvider {
    async fn get_datatype(
        &self,
        package: &str,
        module: &str,
        name: &str,
    ) -> Result<DatatypeDescriptor, wiretap_core::layout::LayoutError> {
        self.table
            .lock()
            .unwrap()
            .get(&(package.into(), module.into(), name.into()))
            .cloned()
            .ok_or_else(|| {
                wiretap_core::layout::LayoutError::NotFound(
                    format!("{package}::{module}::{name}"),
                    "not in mock".into(),
                )
            })
    }
}

/// End-to-end: server emits the BCS bytes but leaves `json = null`. With a
/// LayoutResolver attached, the pipeline's decoder must fill `json` locally
/// using the layout fetched via the (mock) MovePackageService.
#[tokio::test]
async fn local_decode_when_server_json_absent() {
    // Layout: struct Swap { amount: u64, ok: bool }
    let prim = |t: OpenType| OpenSignatureBody {
        r#type: Some(t as i32),
        ..Default::default()
    };
    let field = |n: &str, body: OpenSignatureBody| FieldDescriptor {
        name: Some(n.into()),
        position: Some(0),
        r#type: Some(body),
    };
    let datatype = DatatypeDescriptor {
        type_name: Some("0xabc::pool::Swap".into()),
        defining_id: Some("0xabc".into()),
        module: Some("pool".into()),
        name: Some("Swap".into()),
        kind: Some(wiretap_core::proto::datatype_descriptor::DatatypeKind::Struct as i32),
        fields: vec![
            field("amount", prim(OpenType::U64)),
            field("ok", prim(OpenType::Bool)),
        ],
        ..Default::default()
    };
    let provider = MockLayoutProvider::default();
    provider.insert("0xabc", "pool", "Swap", datatype);
    let resolver = Arc::new(LayoutResolver::new(Arc::new(provider), 16));

    // Cook a checkpoint with one event whose BCS matches the layout but
    // whose proto `json` field is None — simulates the v2 subscribe path.
    let bcs_bytes = bcs::to_bytes(&(1234567890u64, true)).unwrap();
    let event = ProtoEvent {
        event_type: Some("0xabc::pool::Swap".into()),
        package_id: Some("0xabc".into()),
        module: Some("pool".into()),
        sender: Some("0xsender".into()),
        contents: Some(Bcs {
            name: Some("0xabc::pool::Swap".into()),
            value: Some(bcs_bytes),
        }),
        json: None,
    };
    let checkpoint = Checkpoint {
        sequence_number: Some(42),
        digest: Some("d-42".into()),
        summary: Some(CheckpointSummary {
            sequence_number: Some(42),
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        }),
        transactions: vec![ExecutedTransaction {
            digest: Some("tx-42".into()),
            transaction: Some(Transaction {
                sender: Some("0xsender".into()),
                ..Default::default()
            }),
            events: Some(TransactionEvents {
                events: vec![event],
                ..Default::default()
            }),
            balance_changes: vec![],
            ..Default::default()
        }],
        ..Default::default()
    };

    let src = MockSource::default();
    src.push_script(vec![checkpoint]);

    let collect = Collect {
        stop_after: 1,
        ..Default::default()
    };
    let pipeline = Pipeline::new(
        src.clone(),
        Cursor::in_memory().unwrap(),
        FilterSpec::default(),
    )
    .with_resolver(resolver);
    let _ = pipeline.run(collect.clone()).await;

    let events = collect.events.lock().await;
    assert_eq!(events.len(), 1, "expected the event to flow through");
    let e = &events[0];
    assert!(!e.json.is_null(), "json must be locally decoded, got null");
    // u64 emitted as decimal string (see bcs_decode.rs).
    assert_eq!(e.json["amount"], serde_json::json!("1234567890"));
    assert_eq!(e.json["ok"], serde_json::json!(true));
}

#[tokio::test]
async fn cursor_resume_drives_catch_up() {
    let cursor = Cursor::in_memory().unwrap();
    cursor.commit(100).unwrap();

    let src = MockSource::default();
    // Simulate "server tip is 102" so the catch-up phase fetches (101,102),
    // then the subscribe stream is empty (server has nothing new).
    src.tip.store(102, Ordering::SeqCst);
    src.push_script(vec![]); // empty subscribe stream

    let collect = Collect {
        stop_after: 2,
        ..Default::default()
    };
    let filter = FilterSpec::default().with_event("0xpkg::pool::*");
    let pipeline = Pipeline::new(src.clone(), cursor, filter);
    let _ = pipeline.run(collect.clone()).await;

    let cps = collect.checkpoints.lock().await.clone();
    assert_eq!(cps, vec![101, 102]);

    let log = src.fetch_range_log.lock().unwrap().clone();
    assert!(
        log.contains(&(101, 102)),
        "expected initial catch-up fetch_range(101,102), got {log:?}"
    );
}
