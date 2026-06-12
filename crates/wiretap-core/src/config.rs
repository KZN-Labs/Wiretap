//! `wiretap.toml` schema.

use crate::FilterSpec;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub source: SourceConfig,
    #[serde(default)]
    pub watch: Vec<WatchSpec>,
    pub sink: SinkConfig,
    #[serde(default)]
    pub cursor: CursorConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    pub endpoint: String,
    /// `"latest"` or a u64. Stored as a string for forward compatibility.
    #[serde(default = "default_start")]
    pub start_checkpoint: String,
}

fn default_start() -> String {
    "latest".into()
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WatchSpec {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub senders: Vec<String>,
    #[serde(default)]
    pub affected: Vec<String>,
}

impl WatchSpec {
    pub fn into_filter(self) -> FilterSpec {
        FilterSpec {
            events: self.events,
            packages: self.packages,
            senders: self.senders,
            affected: self.affected,
        }
    }
}

/// Sink section is intentionally loose so individual sinks can grab whichever
/// keys they need without us having to enumerate every parameter here.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub params: toml::Table,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CursorConfig {
    #[serde(default = "default_cursor_path")]
    pub path: String,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            path: default_cursor_path(),
        }
    }
}

fn default_cursor_path() -> String {
    "wiretap.db".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> crate::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        toml::from_str(&raw).map_err(|e| crate::Error::Config(e.to_string()))
    }

    pub fn combined_filter(&self) -> FilterSpec {
        // Multiple [[watch]] tables OR together.
        let mut out = FilterSpec::default();
        for w in &self.watch {
            out.events.extend(w.events.iter().cloned());
            out.packages.extend(w.packages.iter().cloned());
            out.senders.extend(w.senders.iter().cloned());
            out.affected.extend(w.affected.iter().cloned());
        }
        out
    }
}

pub const EXAMPLE_TOML: &str = r#"# wiretap.toml — Sui gRPC event indexer config

[source]
endpoint = "https://fullnode.testnet.sui.io:443"
start_checkpoint = "latest"  # or a number, e.g. 12345678

# One or more [[watch]] tables. Filters within a block AND together; multiple
# blocks OR together. Omit a key to skip that dimension.
[[watch]]
events = [
    "0xPACKAGE::pool::SwapEvent",
    "0xPACKAGE::pool::*",   # wildcard at the struct slot
]
# packages = ["0xPACKAGE"]
# senders  = ["0xYOURADDR"]
# affected = ["0xYOURADDR"]

[sink]
type = "sqlite"
path = "events.db"

[cursor]
path = "wiretap.db"
"#;
