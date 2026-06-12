//! Layout resolution for Move types.
//!
//! Given a parsed [`TypeTag`] for an event, this module walks
//! `DatatypeDescriptor`s fetched via [`LayoutProvider`] and produces a
//! [`ResolvedLayout`] — a fully type-parameter-substituted tree the BCS
//! decoder can walk against the event's `contents` bytes.
//!
//! Two LRU caches:
//!   * `descriptors` — keyed by `(package, module, name)`, holds the raw
//!     proto `DatatypeDescriptor`. These are immutable per published package
//!     version, so cache-forever-LRU is safe.
//!   * `resolved` — keyed by the full type-tag string (including generics).
//!     Holds the post-substitution layout so a repeating event type pays
//!     resolution cost exactly once.

use crate::proto::{
    move_package_service_client::MovePackageServiceClient, open_signature_body::Type as OpenType,
    DatatypeDescriptor, GetDatatypeRequest, OpenSignatureBody,
};
use crate::type_tag::{StructTag, TypeTag};
use async_trait::async_trait;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tonic::transport::Channel;

#[derive(Debug, Error)]
pub enum LayoutError {
    #[error("layout provider: {0}")]
    Provider(String),
    #[error("type {0} not found: {1}")]
    NotFound(String, String),
    #[error("malformed proto descriptor for {0}: {1}")]
    Malformed(String, String),
    #[error("type-parameter index {0} out of bounds (have {1})")]
    TypeParamOob(u32, usize),
    #[error("max recursion depth exceeded while resolving {0}")]
    RecursionLimit(String),
    #[error("enums not yet supported in this build (encountered {0})")]
    EnumUnsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedLayout {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    U256,
    Address,
    Signer,
    Vector(Box<ResolvedLayout>),
    Struct {
        type_tag: StructTag,
        fields: Vec<(String, ResolvedLayout)>,
        /// A hint for nicer JSON rendering of well-known wrapper structs.
        kind: StructKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructKind {
    Regular,
    /// `0x1::string::String` / `0x1::ascii::String` — render as JSON string.
    Utf8String,
    /// `0x2::object::ID` — render as 0x-hex.
    ObjectId,
}

/// Fetches `DatatypeDescriptor`s. The real impl talks to
/// `MovePackageService.GetDatatype`; tests can provide canned descriptors.
#[async_trait]
pub trait LayoutProvider: Send + Sync + 'static {
    async fn get_datatype(
        &self,
        package: &str,
        module: &str,
        name: &str,
    ) -> Result<DatatypeDescriptor, LayoutError>;
}

pub struct LayoutResolver {
    provider: Arc<dyn LayoutProvider>,
    descriptors: Mutex<LruCache<(String, String, String), Arc<DatatypeDescriptor>>>,
    resolved: Mutex<LruCache<String, Arc<ResolvedLayout>>>,
}

impl LayoutResolver {
    pub fn new(provider: Arc<dyn LayoutProvider>, cache_capacity: usize) -> Self {
        let cap = NonZeroUsize::new(cache_capacity.max(16)).unwrap();
        Self {
            provider,
            descriptors: Mutex::new(LruCache::new(cap)),
            resolved: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Resolve a parsed type tag to a fully substituted layout. Returns an
    /// `Arc` so the caller can share with the BCS decoder cheaply.
    pub async fn resolve(&self, tag: &TypeTag) -> Result<Arc<ResolvedLayout>, LayoutError> {
        let key = tag.to_string();
        if let Some(hit) = self.resolved.lock().unwrap().get(&key).cloned() {
            return Ok(hit);
        }
        let layout = self.resolve_inner(tag, 0).await?;
        let arc = Arc::new(layout);
        self.resolved.lock().unwrap().put(key, arc.clone());
        Ok(arc)
    }

    async fn fetch_descriptor(
        &self,
        package: &str,
        module: &str,
        name: &str,
    ) -> Result<Arc<DatatypeDescriptor>, LayoutError> {
        let key = (package.to_string(), module.to_string(), name.to_string());
        if let Some(hit) = self.descriptors.lock().unwrap().get(&key).cloned() {
            return Ok(hit);
        }
        let d = self.provider.get_datatype(package, module, name).await?;
        let arc = Arc::new(d);
        self.descriptors.lock().unwrap().put(key, arc.clone());
        Ok(arc)
    }

    fn resolve_inner<'a>(
        &'a self,
        tag: &'a TypeTag,
        depth: usize,
    ) -> futures::future::BoxFuture<'a, Result<ResolvedLayout, LayoutError>> {
        const MAX_DEPTH: usize = 64;
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Err(LayoutError::RecursionLimit(tag.to_string()));
            }
            match tag {
                TypeTag::Bool => Ok(ResolvedLayout::Bool),
                TypeTag::U8 => Ok(ResolvedLayout::U8),
                TypeTag::U16 => Ok(ResolvedLayout::U16),
                TypeTag::U32 => Ok(ResolvedLayout::U32),
                TypeTag::U64 => Ok(ResolvedLayout::U64),
                TypeTag::U128 => Ok(ResolvedLayout::U128),
                TypeTag::U256 => Ok(ResolvedLayout::U256),
                TypeTag::Address => Ok(ResolvedLayout::Address),
                TypeTag::Signer => Ok(ResolvedLayout::Signer),
                TypeTag::Vector(inner) => {
                    let inner = self.resolve_inner(inner, depth + 1).await?;
                    Ok(ResolvedLayout::Vector(Box::new(inner)))
                }
                TypeTag::Struct(s) => self.resolve_struct(s, depth).await,
            }
        })
    }

    async fn resolve_struct(
        &self,
        s: &StructTag,
        depth: usize,
    ) -> Result<ResolvedLayout, LayoutError> {
        // s.package is already 0x-prefixed when produced by our parser; try
        // also the prefix-stripped form for providers that key without it.
        let desc = match self.fetch_descriptor(&s.package, &s.module, &s.name).await {
            Ok(d) => d,
            Err(LayoutError::NotFound(..)) => {
                let alt = s.package.strip_prefix("0x").unwrap_or(&s.package);
                self.fetch_descriptor(alt, &s.module, &s.name).await?
            }
            Err(e) => return Err(e),
        };

        // Reject enums (rare in events); we explicitly surface this to the
        // caller so the decoder can fall back to _undecoded rather than
        // producing wrong JSON.
        if desc.kind() == crate::proto::datatype_descriptor::DatatypeKind::Enum {
            return Err(LayoutError::EnumUnsupported(s.to_string()));
        }

        let mut fields = Vec::with_capacity(desc.fields.len());
        for f in &desc.fields {
            let name = f.name.clone().unwrap_or_default();
            let body = f.r#type.as_ref().ok_or_else(|| {
                LayoutError::Malformed(s.to_string(), format!("field {name} missing type"))
            })?;
            let resolved = self.resolve_body(body, &s.type_params, depth + 1).await?;
            fields.push((name, resolved));
        }

        let kind = classify_struct(s);
        Ok(ResolvedLayout::Struct {
            type_tag: s.clone(),
            fields,
            kind,
        })
    }

    fn resolve_body<'a>(
        &'a self,
        body: &'a OpenSignatureBody,
        type_params: &'a [TypeTag],
        depth: usize,
    ) -> futures::future::BoxFuture<'a, Result<ResolvedLayout, LayoutError>> {
        Box::pin(async move {
            const MAX_DEPTH: usize = 64;
            if depth > MAX_DEPTH {
                return Err(LayoutError::RecursionLimit("<open signature body>".into()));
            }
            match body.r#type() {
                OpenType::Bool => Ok(ResolvedLayout::Bool),
                OpenType::U8 => Ok(ResolvedLayout::U8),
                OpenType::U16 => Ok(ResolvedLayout::U16),
                OpenType::U32 => Ok(ResolvedLayout::U32),
                OpenType::U64 => Ok(ResolvedLayout::U64),
                OpenType::U128 => Ok(ResolvedLayout::U128),
                OpenType::U256 => Ok(ResolvedLayout::U256),
                OpenType::Address => Ok(ResolvedLayout::Address),
                OpenType::Vector => {
                    let inner = body.type_parameter_instantiation.first().ok_or_else(|| {
                        LayoutError::Malformed("vector".into(), "missing element type".into())
                    })?;
                    let inner = self.resolve_body(inner, type_params, depth + 1).await?;
                    Ok(ResolvedLayout::Vector(Box::new(inner)))
                }
                OpenType::Datatype => {
                    let tname = body.type_name.clone().unwrap_or_default();
                    let (pkg, module, name) = parse_qualified_name(&tname).ok_or_else(|| {
                        LayoutError::Malformed(
                            tname.clone(),
                            "expected <pkg>::<module>::<name>".into(),
                        )
                    })?;
                    let mut nested_params = Vec::new();
                    for inst in &body.type_parameter_instantiation {
                        nested_params.push(
                            self.resolve_body_as_tag(inst, type_params, depth + 1)
                                .await?,
                        );
                    }
                    let s = StructTag {
                        package: with_0x(&pkg),
                        module,
                        name,
                        type_params: nested_params,
                    };
                    self.resolve_struct(&s, depth + 1).await
                }
                OpenType::Parameter => {
                    let idx = body.type_parameter.unwrap_or_default() as usize;
                    let t = type_params
                        .get(idx)
                        .ok_or(LayoutError::TypeParamOob(idx as u32, type_params.len()))?;
                    self.resolve_inner(t, depth + 1).await
                }
                OpenType::Unknown => Err(LayoutError::Malformed(
                    "unknown".into(),
                    "OpenSignatureBody.type == TYPE_UNKNOWN".into(),
                )),
            }
        })
    }

    /// Used to build the `type_params` list for a nested DATATYPE — we need
    /// the substituted [`TypeTag`] (not just the resolved layout) so further
    /// recursive resolution can substitute again.
    fn resolve_body_as_tag<'a>(
        &'a self,
        body: &'a OpenSignatureBody,
        type_params: &'a [TypeTag],
        depth: usize,
    ) -> futures::future::BoxFuture<'a, Result<TypeTag, LayoutError>> {
        Box::pin(async move {
            const MAX_DEPTH: usize = 64;
            if depth > MAX_DEPTH {
                return Err(LayoutError::RecursionLimit(
                    "<open signature body, tag pass>".into(),
                ));
            }
            match body.r#type() {
                OpenType::Bool => Ok(TypeTag::Bool),
                OpenType::U8 => Ok(TypeTag::U8),
                OpenType::U16 => Ok(TypeTag::U16),
                OpenType::U32 => Ok(TypeTag::U32),
                OpenType::U64 => Ok(TypeTag::U64),
                OpenType::U128 => Ok(TypeTag::U128),
                OpenType::U256 => Ok(TypeTag::U256),
                OpenType::Address => Ok(TypeTag::Address),
                OpenType::Vector => {
                    let inner = body.type_parameter_instantiation.first().ok_or_else(|| {
                        LayoutError::Malformed("vector".into(), "missing element type".into())
                    })?;
                    let inner = self
                        .resolve_body_as_tag(inner, type_params, depth + 1)
                        .await?;
                    Ok(TypeTag::Vector(Box::new(inner)))
                }
                OpenType::Datatype => {
                    let tname = body.type_name.clone().unwrap_or_default();
                    let (pkg, module, name) = parse_qualified_name(&tname).ok_or_else(|| {
                        LayoutError::Malformed(
                            tname.clone(),
                            "expected <pkg>::<module>::<name>".into(),
                        )
                    })?;
                    let mut params = Vec::new();
                    for inst in &body.type_parameter_instantiation {
                        params.push(
                            self.resolve_body_as_tag(inst, type_params, depth + 1)
                                .await?,
                        );
                    }
                    Ok(TypeTag::Struct(StructTag {
                        package: with_0x(&pkg),
                        module,
                        name,
                        type_params: params,
                    }))
                }
                OpenType::Parameter => {
                    let idx = body.type_parameter.unwrap_or_default() as usize;
                    type_params
                        .get(idx)
                        .cloned()
                        .ok_or(LayoutError::TypeParamOob(idx as u32, type_params.len()))
                }
                OpenType::Unknown => Err(LayoutError::Malformed(
                    "unknown".into(),
                    "OpenSignatureBody.type == TYPE_UNKNOWN".into(),
                )),
            }
        })
    }
}

fn classify_struct(s: &StructTag) -> StructKind {
    let pkg = s.package.strip_prefix("0x").unwrap_or(&s.package);
    // Sui std/framework addresses are short ids — normalize by parsing as u128.
    let is_addr = |want_hex: &str| -> bool {
        let a = u128::from_str_radix(pkg, 16).ok();
        let b = u128::from_str_radix(want_hex, 16).ok();
        a.is_some() && a == b
    };
    if is_addr("1") && (s.module == "string" || s.module == "ascii") && s.name == "String" {
        return StructKind::Utf8String;
    }
    if is_addr("2") && s.module == "object" && (s.name == "ID" || s.name == "UID") {
        return StructKind::ObjectId;
    }
    StructKind::Regular
}

fn parse_qualified_name(s: &str) -> Option<(String, String, String)> {
    // Format is "<pkg>::<module>::<name>" with no generics.
    let parts: Vec<&str> = s.split("::").collect();
    if parts.len() != 3 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

#[cfg(test)]
fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

fn with_0x(s: &str) -> String {
    if s.starts_with("0x") || s.starts_with("0X") {
        s.to_string()
    } else {
        format!("0x{s}")
    }
}

/// gRPC-backed [`LayoutProvider`] using `MovePackageService.GetDatatype`.
pub struct GrpcLayoutProvider {
    channel: Channel,
}

impl GrpcLayoutProvider {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }
}

#[async_trait]
impl LayoutProvider for GrpcLayoutProvider {
    async fn get_datatype(
        &self,
        package: &str,
        module: &str,
        name: &str,
    ) -> Result<DatatypeDescriptor, LayoutError> {
        let mut client = MovePackageServiceClient::new(self.channel.clone());
        let req = GetDatatypeRequest {
            package_id: Some(package.to_string()),
            module_name: Some(module.to_string()),
            name: Some(name.to_string()),
        };
        let resp = client.get_datatype(req).await.map_err(|s| {
            if s.code() == tonic::Code::NotFound {
                LayoutError::NotFound(
                    format!("{}::{}::{}", package, module, name),
                    s.message().to_string(),
                )
            } else {
                LayoutError::Provider(s.message().to_string())
            }
        })?;
        resp.into_inner().datatype.ok_or_else(|| {
            LayoutError::Malformed(
                format!("{}::{}::{}", package, module, name),
                "GetDatatypeResponse missing datatype field".into(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FieldDescriptor;

    #[derive(Default)]
    struct MockProvider {
        table: std::collections::HashMap<(String, String, String), DatatypeDescriptor>,
    }

    #[async_trait]
    impl LayoutProvider for MockProvider {
        async fn get_datatype(
            &self,
            package: &str,
            module: &str,
            name: &str,
        ) -> Result<DatatypeDescriptor, LayoutError> {
            // Try both prefixed and unprefixed.
            for key in [
                (package.to_string(), module.into(), name.into()),
                (strip_0x(package).to_string(), module.into(), name.into()),
            ] {
                if let Some(d) = self.table.get(&key) {
                    return Ok(d.clone());
                }
            }
            Err(LayoutError::NotFound(
                format!("{package}::{module}::{name}"),
                "not in mock".into(),
            ))
        }
    }

    fn prim(t: OpenType) -> OpenSignatureBody {
        OpenSignatureBody {
            r#type: Some(t as i32),
            ..Default::default()
        }
    }
    fn field(name: &str, body: OpenSignatureBody) -> FieldDescriptor {
        FieldDescriptor {
            name: Some(name.into()),
            position: Some(0),
            r#type: Some(body),
        }
    }
    fn datatype_struct(
        pkg: &str,
        m: &str,
        n: &str,
        fields: Vec<FieldDescriptor>,
    ) -> DatatypeDescriptor {
        DatatypeDescriptor {
            type_name: Some(format!("{pkg}::{m}::{n}")),
            defining_id: Some(pkg.into()),
            module: Some(m.into()),
            name: Some(n.into()),
            kind: Some(crate::proto::datatype_descriptor::DatatypeKind::Struct as i32),
            fields,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn resolves_simple_struct() {
        let mut mock = MockProvider::default();
        mock.table.insert(
            ("abc".into(), "pool".into(), "Swap".into()),
            datatype_struct(
                "0xabc",
                "pool",
                "Swap",
                vec![
                    field("amount", prim(OpenType::U64)),
                    field("ok", prim(OpenType::Bool)),
                ],
            ),
        );
        let r = LayoutResolver::new(Arc::new(mock), 16);
        let tag = TypeTag::parse("0xabc::pool::Swap").unwrap();
        let layout = r.resolve(&tag).await.unwrap();
        match layout.as_ref() {
            ResolvedLayout::Struct { fields, .. } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "amount");
                assert!(matches!(fields[0].1, ResolvedLayout::U64));
                assert_eq!(fields[1].0, "ok");
                assert!(matches!(fields[1].1, ResolvedLayout::Bool));
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn resolves_generic_struct_with_type_param() {
        let mut mock = MockProvider::default();
        // struct Holder<T> { value: T }
        mock.table.insert(
            ("abc".into(), "pool".into(), "Holder".into()),
            datatype_struct(
                "0xabc",
                "pool",
                "Holder",
                vec![field(
                    "value",
                    OpenSignatureBody {
                        r#type: Some(OpenType::Parameter as i32),
                        type_parameter: Some(0),
                        ..Default::default()
                    },
                )],
            ),
        );
        let r = LayoutResolver::new(Arc::new(mock), 16);
        let tag = TypeTag::parse("0xabc::pool::Holder<u64>").unwrap();
        let layout = r.resolve(&tag).await.unwrap();
        match layout.as_ref() {
            ResolvedLayout::Struct { fields, .. } => {
                assert!(matches!(fields[0].1, ResolvedLayout::U64));
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn classifies_well_known_wrappers() {
        let mut mock = MockProvider::default();
        // 0x1::string::String { bytes: vector<u8> }
        mock.table.insert(
            ("1".into(), "string".into(), "String".into()),
            datatype_struct(
                "0x1",
                "string",
                "String",
                vec![field(
                    "bytes",
                    OpenSignatureBody {
                        r#type: Some(OpenType::Vector as i32),
                        type_parameter_instantiation: vec![prim(OpenType::U8)],
                        ..Default::default()
                    },
                )],
            ),
        );
        let r = LayoutResolver::new(Arc::new(mock), 16);
        let tag = TypeTag::parse("0x1::string::String").unwrap();
        let layout = r.resolve(&tag).await.unwrap();
        match layout.as_ref() {
            ResolvedLayout::Struct { kind, .. } => {
                assert_eq!(*kind, StructKind::Utf8String);
            }
            _ => panic!(),
        }
    }
}
