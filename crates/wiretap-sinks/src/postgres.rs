use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use wiretap_core::{Event, Handler};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS wiretap_events (
    checkpoint    BIGINT NOT NULL,
    tx_digest     TEXT NOT NULL,
    event_seq     BIGINT NOT NULL,
    event_type    TEXT NOT NULL,
    package_id    TEXT NOT NULL,
    module        TEXT NOT NULL,
    sender        TEXT NOT NULL,
    timestamp_ms  BIGINT NOT NULL,
    json          JSONB NOT NULL,
    bcs_b64       TEXT NOT NULL,
    PRIMARY KEY (tx_digest, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_we_checkpoint ON wiretap_events(checkpoint);
CREATE INDEX IF NOT EXISTS idx_we_type ON wiretap_events(event_type);
CREATE INDEX IF NOT EXISTS idx_we_sender ON wiretap_events(sender);
"#;

pub struct PostgresSink {
    pool: PgPool,
}

impl PostgresSink {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        sqlx::query(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Handler for PostgresSink {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO wiretap_events
             (checkpoint, tx_digest, event_seq, event_type, package_id, module, sender, timestamp_ms, json, bcs_b64)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
             ON CONFLICT (tx_digest, event_seq) DO NOTHING",
        )
        .bind(e.checkpoint as i64)
        .bind(&e.tx_digest)
        .bind(e.event_seq as i64)
        .bind(&e.event_type)
        .bind(&e.package_id)
        .bind(&e.module)
        .bind(&e.sender)
        .bind(e.checkpoint_timestamp_ms as i64)
        .bind(e.json.to_string())
        .bind(&e.bcs_b64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
