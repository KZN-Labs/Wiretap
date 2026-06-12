//! wiretap-core: streaming Sui event indexer building blocks.
//!
//! The minimum embed:
//! ```no_run
//! use wiretap_core::{Pipeline, GrpcSource, Cursor, FilterSpec, Handler, Event};
//! use async_trait::async_trait;
//!
//! struct Print;
//! #[async_trait]
//! impl Handler for Print {
//!     async fn on_event(&self, e: Event) -> anyhow::Result<()> {
//!         println!("{} {}", e.checkpoint, e.event_type);
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let source = GrpcSource::connect("https://fullnode.testnet.sui.io:443").await?;
//! let cursor = Cursor::open_sqlite("wiretap.db")?;
//! let filter = FilterSpec::default().with_event("0xPKG::pool::SwapEvent");
//! Pipeline::new(source, cursor, filter).run(Print).await
//! # }
//! ```

pub mod bcs_decode;
pub mod config;
pub mod cursor;
pub mod decoder;
pub mod error;
pub mod filter;
pub mod grpc;
pub mod handler;
pub mod layout;
pub mod pipeline;
pub mod source;
pub mod type_tag;

// tonic-generated proto types.
pub mod proto {
    pub mod google {
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }
    }
    pub mod sui {
        pub mod rpc {
            pub mod v2 {
                tonic::include_proto!("sui.rpc.v2");
            }
        }
    }
    // Convenience re-export of the v2 namespace for the rest of the crate.
    pub use sui::rpc::v2::*;
}

pub use config::{Config, SinkConfig, SourceConfig, WatchSpec};
pub use cursor::Cursor;
pub use decoder::decode_event;
pub use error::{Error, Result};
pub use filter::{CompiledFilter, FilterSpec};
pub use grpc::GrpcSource;
pub use handler::{Event, Handler};
pub use layout::{GrpcLayoutProvider, LayoutProvider, LayoutResolver, ResolvedLayout};
pub use pipeline::Pipeline;
pub use source::{CheckpointBatch, Source};
pub use type_tag::{StructTag, TypeTag};
