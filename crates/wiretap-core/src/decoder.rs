use crate::bcs_decode;
use crate::handler::Event;
use crate::layout::LayoutResolver;
use crate::proto::{Checkpoint, Event as ProtoEvent, ExecutedTransaction};
use crate::type_tag::TypeTag;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use prost_types::value::Kind;
use serde_json::Value;
use tracing::debug;

/// Per-event decode, with this priority:
///   1. server-rendered `json` (if present),
///   2. local layout-driven BCS decode (if a resolver is provided),
///   3. `{ "_undecoded": true, "type": <tag>, "bcs_hex": ... }` as a last resort.
pub async fn decode_event(
    cp: &Checkpoint,
    tx: &ExecutedTransaction,
    ev: &ProtoEvent,
    event_seq: u64,
    resolver: Option<&LayoutResolver>,
) -> Event {
    let tx_sender = tx
        .transaction
        .as_ref()
        .and_then(|t| t.sender.clone())
        .unwrap_or_default();
    let sender = ev.sender.clone().unwrap_or(tx_sender);
    let tx_digest = tx.digest.clone().unwrap_or_default();

    let bcs_bytes_owned: Vec<u8> = ev
        .contents
        .as_ref()
        .and_then(|b| b.value.clone())
        .unwrap_or_default();

    let event_type = ev.event_type.clone().unwrap_or_default();

    let json = resolve_json(ev, &event_type, &bcs_bytes_owned, resolver).await;

    let cp_seq = cp.sequence_number.unwrap_or(0);
    let cp_ts_ms = cp
        .summary
        .as_ref()
        .and_then(|s| s.timestamp.as_ref())
        .map(|ts| ts.seconds as i128 * 1000 + (ts.nanos as i128) / 1_000_000)
        .and_then(|ms| u64::try_from(ms.max(0)).ok())
        .unwrap_or(0);

    Event {
        checkpoint: cp_seq,
        checkpoint_timestamp_ms: cp_ts_ms,
        tx_digest,
        event_seq,
        event_type,
        package_id: ev.package_id.clone().unwrap_or_default(),
        module: ev.module.clone().unwrap_or_default(),
        sender,
        json,
        bcs_b64: B64.encode(&bcs_bytes_owned),
    }
}

async fn resolve_json(
    ev: &ProtoEvent,
    event_type: &str,
    bcs_bytes: &[u8],
    resolver: Option<&LayoutResolver>,
) -> Value {
    // 1. Server-rendered json.
    if let Some(v) = ev.json.as_ref().map(prost_value_to_json) {
        if !matches!(v, Value::Null) {
            return v;
        }
    }
    // 2. Local layout-driven BCS decode.
    if let Some(r) = resolver {
        match TypeTag::parse(event_type) {
            Ok(tag) => match r.resolve(&tag).await {
                Ok(layout) => match bcs_decode::decode(layout.as_ref(), bcs_bytes) {
                    Ok(v) => return v,
                    Err(e) => debug!(event_type, error = %e, "wiretap: local BCS decode failed"),
                },
                Err(e) => debug!(event_type, error = %e, "wiretap: layout resolution failed"),
            },
            Err(e) => debug!(event_type, error = %e, "wiretap: type tag parse failed"),
        }
    }
    // 3. Last resort.
    serde_json::json!({
        "_undecoded": true,
        "type": event_type,
        "bcs_hex": hex::encode(bcs_bytes),
    })
}

fn prost_value_to_json(v: &prost_types::Value) -> Value {
    use Value as J;
    match &v.kind {
        None | Some(Kind::NullValue(_)) => J::Null,
        Some(Kind::BoolValue(b)) => J::Bool(*b),
        Some(Kind::NumberValue(n)) => serde_json::Number::from_f64(*n)
            .map(J::Number)
            .unwrap_or(J::Null),
        Some(Kind::StringValue(s)) => J::String(s.clone()),
        Some(Kind::ListValue(l)) => J::Array(l.values.iter().map(prost_value_to_json).collect()),
        Some(Kind::StructValue(s)) => J::Object(
            s.fields
                .iter()
                .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
                .collect(),
        ),
    }
}
