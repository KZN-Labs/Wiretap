use async_trait::async_trait;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;
use wiretap_core::{Event, Handler};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    checkpoint    INTEGER NOT NULL,
    tx_digest     TEXT NOT NULL,
    event_seq     INTEGER NOT NULL,
    event_type    TEXT NOT NULL,
    package_id    TEXT NOT NULL,
    module        TEXT NOT NULL,
    sender        TEXT NOT NULL,
    timestamp_ms  INTEGER NOT NULL,
    json          TEXT NOT NULL,
    bcs_b64       TEXT NOT NULL,
    PRIMARY KEY (tx_digest, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_events_checkpoint ON events(checkpoint);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_sender ON events(sender);
"#;

pub struct SqliteSink {
    conn: Mutex<Connection>,
}

impl SqliteSink {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        // Sensible defaults for an append-mostly event log.
        conn.execute_batch("PRAGMA journal_mode = WAL;\nPRAGMA synchronous = NORMAL;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

#[async_trait]
impl Handler for SqliteSink {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO events
             (checkpoint, tx_digest, event_seq, event_type, package_id, module, sender, timestamp_ms, json, bcs_b64)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                e.checkpoint as i64,
                e.tx_digest,
                e.event_seq as i64,
                e.event_type,
                e.package_id,
                e.module,
                e.sender,
                e.checkpoint_timestamp_ms as i64,
                e.json.to_string(),
                e.bcs_b64,
            ],
        )?;
        Ok(())
    }
}
