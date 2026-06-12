//! gRPC implementation of [`Source`] against Sui's v2 fullnode API.
//!
//! Two services are exercised:
//!   * `SubscriptionService.SubscribeCheckpoints` — streamed catch-up from the
//!     server's current tip.
//!   * `LedgerService.GetCheckpoint` — point lookups, used for initial catch-up
//!     from the cursor, gap-fill on reconnect, and the `backfill` CLI command.
//!   * `LedgerService.GetServiceInfo` — for the current tip height.

use crate::proto::{
    get_checkpoint_request::CheckpointId, ledger_service_client::LedgerServiceClient,
    subscription_service_client::SubscriptionServiceClient, GetCheckpointRequest,
    GetServiceInfoRequest, SubscribeCheckpointsRequest,
};
use crate::source::{CheckpointBatch, Source};
use crate::{Error, Result};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use prost_types::FieldMask;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, info};

/// FieldMask paths for catch-up / streaming. We request only what the filter
/// engine and the decoder need: the checkpoint sequence + timestamp, and for
/// each transaction the digest, sender, and full event records.
///
/// Paths follow Sui's read-mask convention (see proto comments on each
/// `read_mask` field): dotted paths into the response message.
fn read_mask() -> FieldMask {
    FieldMask {
        paths: [
            "sequence_number",
            "digest",
            "summary.timestamp",
            "summary.epoch",
            "summary.sequence_number",
            "transactions.digest",
            "transactions.transaction.sender",
            "transactions.events.events.package_id",
            "transactions.events.events.module",
            "transactions.events.events.sender",
            "transactions.events.events.event_type",
            "transactions.events.events.contents",
            "transactions.events.events.json",
            "transactions.balance_changes.address",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    }
}

/// FieldMask for subscribe: the server applies this mask directly to the
/// `Checkpoint` it puts into each response (see sui-rpc-api's
/// subscription_service: `Checkpoint::merge_from(..., &read_mask)`). Paths are
/// therefore identical to those used for GetCheckpoint.
fn subscribe_read_mask() -> FieldMask {
    read_mask()
}

#[derive(Clone)]
pub struct GrpcSource {
    channel: Channel,
}

impl GrpcSource {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint_str = endpoint.into();
        let mut ep = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| Error::Config(format!("invalid endpoint {endpoint_str}: {e}")))?
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .tcp_nodelay(true);

        if endpoint_str.starts_with("https://") {
            ep = ep
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(Error::Transport)?;
        }

        let channel = ep.connect().await?;
        info!(endpoint = %endpoint_str, "wiretap: gRPC channel established");
        Ok(Self { channel })
    }

    /// Shared underlying channel — useful for piggybacking other services
    /// (e.g. `MovePackageService` for the layout resolver) without opening
    /// a second TCP/HTTP2 connection.
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    fn sub_client(&self) -> SubscriptionServiceClient<Channel> {
        SubscriptionServiceClient::new(self.channel.clone())
    }

    fn ledger_client(&self) -> LedgerServiceClient<Channel> {
        LedgerServiceClient::new(self.channel.clone())
    }
}

#[async_trait]
impl Source for GrpcSource {
    async fn subscribe(&self) -> Result<BoxStream<'static, Result<CheckpointBatch>>> {
        let mut client = self.sub_client();
        let req = SubscribeCheckpointsRequest {
            read_mask: Some(subscribe_read_mask()),
        };
        tracing::debug!(mask = ?req.read_mask, "wiretap: subscribe req mask");
        debug!("wiretap: opening SubscribeCheckpoints");
        let stream = client.subscribe_checkpoints(req).await?.into_inner();
        let mapped = stream.map(|item| match item {
            Ok(resp) => {
                let cursor = resp.cursor;
                match resp.checkpoint {
                    Some(mut c) => {
                        // Server always fills `cursor`; ensure the Checkpoint
                        // carries the seq even if the user's mask didn't ask for it.
                        if c.sequence_number.is_none() {
                            c.sequence_number = cursor;
                        }
                        Ok(CheckpointBatch { checkpoint: c })
                    }
                    None => Err(Error::Decode(
                        "SubscribeCheckpointsResponse missing checkpoint".into(),
                    )),
                }
            }
            Err(s) => Err(Error::Status(s)),
        });
        Ok(mapped.boxed())
    }

    async fn fetch_range(&self, from: u64, to: u64) -> Result<Vec<CheckpointBatch>> {
        let mut client = self.ledger_client();
        let mut out = Vec::with_capacity((to.saturating_sub(from) + 1) as usize);
        let mask = Some(read_mask());
        for seq in from..=to {
            let req = GetCheckpointRequest {
                checkpoint_id: Some(CheckpointId::SequenceNumber(seq)),
                read_mask: mask.clone(),
            };
            let resp = client.get_checkpoint(req).await?.into_inner();
            let checkpoint = resp.checkpoint.ok_or_else(|| {
                Error::Decode(format!("GetCheckpoint {seq} returned no checkpoint"))
            })?;
            out.push(CheckpointBatch { checkpoint });
        }
        Ok(out)
    }

    async fn latest_checkpoint(&self) -> Result<u64> {
        let mut client = self.ledger_client();
        let resp = client
            .get_service_info(GetServiceInfoRequest {})
            .await?
            .into_inner();
        resp.checkpoint_height.ok_or_else(|| {
            Error::Decode("GetServiceInfo response missing checkpoint_height".into())
        })
    }
}
