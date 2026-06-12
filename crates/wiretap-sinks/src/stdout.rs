use async_trait::async_trait;
use std::io::Write;
use std::sync::Mutex;
use wiretap_core::{Event, Handler};

/// NDJSON to stdout. Pipe into `jq` for demos / quick inspection.
pub struct StdoutSink {
    out: Mutex<std::io::Stdout>,
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

impl StdoutSink {
    pub fn new() -> Self {
        Self {
            out: Mutex::new(std::io::stdout()),
        }
    }
}

#[async_trait]
impl Handler for StdoutSink {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        let line = serde_json::to_string(&e)?;
        let mut out = self.out.lock().unwrap();
        writeln!(out, "{line}")?;
        Ok(())
    }
}
