//! Layout-driven BCS decoder.
//!
//! Walks a [`ResolvedLayout`] tree against a byte slice and emits
//! `serde_json::Value`. Primitives go through the `bcs` crate's
//! `Deserializer` via a small `DeserializeSeed` impl, so the BCS wire format
//! (varints, vec-length prefixes, little-endian integers) is handled by
//! Mysten's BCS crate — we never touch the bytes directly.

use crate::layout::{ResolvedLayout, StructKind};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("bcs: {0}")]
    Bcs(#[from] bcs::Error),
    #[error("layout error: {0}")]
    Layout(String),
}

/// Decode a BCS blob into JSON under the given layout, using the canonical
/// `bcs` crate's seed-based deserializer (so we never touch the wire format
/// directly).
pub fn decode(layout: &ResolvedLayout, bytes: &[u8]) -> Result<Value, DecodeError> {
    let value: Value = bcs::from_bytes_seed(Seed(layout), bytes)?;
    Ok(value)
}

#[derive(Clone, Copy)]
struct Seed<'a>(&'a ResolvedLayout);

impl<'de, 'a> DeserializeSeed<'de> for Seed<'a> {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        match self.0 {
            ResolvedLayout::Bool => bool::deserialize(d).map(Value::Bool),
            ResolvedLayout::U8 => u8::deserialize(d).map(|n| json!(n)),
            ResolvedLayout::U16 => u16::deserialize(d).map(|n| json!(n)),
            ResolvedLayout::U32 => u32::deserialize(d).map(|n| json!(n)),
            ResolvedLayout::U64 => {
                // u64 in JSON: emit as decimal string to avoid precision loss
                // in downstream JS/JSON parsers (Sui pkg tags often exceed 2^53).
                u64::deserialize(d).map(|n| Value::String(n.to_string()))
            }
            ResolvedLayout::U128 => u128::deserialize(d).map(|n| Value::String(n.to_string())),
            ResolvedLayout::U256 => U256Bytes::deserialize(d).map(|b| {
                // BCS for u256 is 32 LE bytes. Render as 0x-hex (lowercase, no
                // leading-zero trim) — easier for JSON consumers than a 78-digit decimal.
                let mut hex = String::with_capacity(2 + 64);
                hex.push_str("0x");
                for byte in b.0.iter().rev() {
                    hex.push_str(&format!("{:02x}", byte));
                }
                Value::String(hex)
            }),
            ResolvedLayout::Address | ResolvedLayout::Signer => {
                AddressBytes::deserialize(d).map(|a| Value::String(a.hex()))
            }
            ResolvedLayout::Vector(inner) => match inner.as_ref() {
                // vector<u8>: keep as base64 (cheap, lossless, JSON-safe).
                ResolvedLayout::U8 => {
                    let bytes = ByteVec::deserialize(d)?;
                    Ok(json!({ "_bytes_b64": B64.encode(&bytes.0) }))
                }
                _ => d.deserialize_seq(VecVisitor { elem: inner }),
            },
            ResolvedLayout::Struct { fields, kind, .. } => match kind {
                StructKind::Utf8String => {
                    // 0x1::string::String / 0x1::ascii::String wrap a vector<u8>;
                    // one field, decoded as raw bytes -> UTF-8 string when valid.
                    let bytes = ByteVec::deserialize(d)?;
                    match std::str::from_utf8(&bytes.0) {
                        Ok(s) => Ok(Value::String(s.to_string())),
                        Err(_) => Ok(json!({ "_bytes_b64": B64.encode(&bytes.0) })),
                    }
                }
                StructKind::ObjectId => {
                    // ID/UID wraps an `address` (UID wraps an inner ID which wraps address;
                    // since BCS for nested single-field structs is identity-equivalent
                    // to the leaf, decoding 32 bytes works for both).
                    let addr = AddressBytes::deserialize(d)?;
                    Ok(Value::String(addr.hex()))
                }
                StructKind::Regular => d.deserialize_tuple(fields.len(), StructVisitor { fields }),
            },
        }
    }
}

struct VecVisitor<'a> {
    elem: &'a ResolvedLayout,
}

impl<'de, 'a> Visitor<'de> for VecVisitor<'a> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BCS sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element_seed(Seed(self.elem))? {
            out.push(v);
        }
        Ok(Value::Array(out))
    }
}

struct StructVisitor<'a> {
    fields: &'a [(String, ResolvedLayout)],
}

impl<'de, 'a> Visitor<'de> for StructVisitor<'a> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BCS struct with {} fields", self.fields.len())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut obj = serde_json::Map::with_capacity(self.fields.len());
        for (name, layout) in self.fields {
            let v: Value = seq
                .next_element_seed(Seed(layout))?
                .ok_or_else(|| serde::de::Error::custom(format!("missing field {name}")))?;
            obj.insert(name.clone(), v);
        }
        Ok(Value::Object(obj))
    }
}

// ── byte-slice helpers ──────────────────────────────────────────────────────

/// 32-byte LE u256.
struct U256Bytes([u8; 32]);
impl<'de> serde::Deserialize<'de> for U256Bytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let arr = <[u8; 32]>::deserialize(d)?;
        Ok(U256Bytes(arr))
    }
}

/// Sui addresses are 32 bytes in BCS, rendered as 0x-padded-hex.
struct AddressBytes([u8; 32]);
impl<'de> serde::Deserialize<'de> for AddressBytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let arr = <[u8; 32]>::deserialize(d)?;
        Ok(AddressBytes(arr))
    }
}
impl AddressBytes {
    fn hex(&self) -> String {
        let mut s = String::with_capacity(2 + 64);
        s.push_str("0x");
        for b in &self.0 {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }
}

struct ByteVec(Vec<u8>);
impl<'de> serde::Deserialize<'de> for ByteVec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Vec::<u8>::deserialize(d)?;
        Ok(ByteVec(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{ResolvedLayout, StructKind};
    use crate::type_tag::StructTag;

    fn s(tag: &str, fields: Vec<(&str, ResolvedLayout)>, kind: StructKind) -> ResolvedLayout {
        ResolvedLayout::Struct {
            type_tag: StructTag {
                package: "abc".into(),
                module: "m".into(),
                name: tag.into(),
                type_params: vec![],
            },
            fields: fields
                .into_iter()
                .map(|(n, l)| (n.to_string(), l))
                .collect(),
            kind,
        }
    }

    #[test]
    fn primitives_round_trip() {
        // BCS-encode a canned (u32, bool, String<u8 vec>) and decode it.
        let layout = s(
            "T",
            vec![
                ("x", ResolvedLayout::U32),
                ("ok", ResolvedLayout::Bool),
                ("msg", ResolvedLayout::Vector(Box::new(ResolvedLayout::U8))),
            ],
            StructKind::Regular,
        );
        let bytes = bcs::to_bytes(&(42u32, true, b"hi".to_vec())).unwrap();
        let v = decode(&layout, &bytes).unwrap();
        assert_eq!(v["x"], json!(42));
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["msg"]["_bytes_b64"], json!("aGk="));
    }

    #[test]
    fn u64_emitted_as_string() {
        let layout = s(
            "T",
            vec![("amount", ResolvedLayout::U64)],
            StructKind::Regular,
        );
        let bytes = bcs::to_bytes(&(u64::MAX,)).unwrap();
        let v = decode(&layout, &bytes).unwrap();
        assert_eq!(v["amount"], json!("18446744073709551615"));
    }

    #[test]
    fn address_renders_as_hex() {
        let layout = s(
            "T",
            vec![("addr", ResolvedLayout::Address)],
            StructKind::Regular,
        );
        let mut addr = [0u8; 32];
        addr[31] = 0x42;
        let bytes = bcs::to_bytes(&(addr,)).unwrap();
        let v = decode(&layout, &bytes).unwrap();
        assert_eq!(
            v["addr"].as_str().unwrap(),
            "0x0000000000000000000000000000000000000000000000000000000000000042"
        );
    }

    #[test]
    fn utf8_string_wrapper_decodes_to_string() {
        let layout = ResolvedLayout::Struct {
            type_tag: StructTag {
                package: "1".into(),
                module: "string".into(),
                name: "String".into(),
                type_params: vec![],
            },
            fields: vec![(
                "bytes".into(),
                ResolvedLayout::Vector(Box::new(ResolvedLayout::U8)),
            )],
            kind: StructKind::Utf8String,
        };
        let bytes = bcs::to_bytes(&b"hello".to_vec()).unwrap();
        let v = decode(&layout, &bytes).unwrap();
        assert_eq!(v, json!("hello"));
    }

    #[test]
    fn nested_struct_and_vector() {
        let inner = s(
            "Inner",
            vec![("v", ResolvedLayout::U16)],
            StructKind::Regular,
        );
        let outer = s(
            "Outer",
            vec![("items", ResolvedLayout::Vector(Box::new(inner)))],
            StructKind::Regular,
        );
        let bytes = bcs::to_bytes(&(vec![(1u16,), (2u16,), (3u16,)],)).unwrap();
        let v = decode(&outer, &bytes).unwrap();
        assert_eq!(v["items"][0]["v"], json!(1));
        assert_eq!(v["items"][2]["v"], json!(3));
    }
}
