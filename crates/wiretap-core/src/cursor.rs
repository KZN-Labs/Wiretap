//! Cursor persistence. The pipeline writes the highest *fully processed*
//! checkpoint sequence number here after each checkpoint's handlers return.
//! On restart we resume from `last + 1`, and on stream reconnect we backfill
//! the gap between `last + 1` and the first checkpoint the new stream yields.

use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS wiretap_cursor (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq     INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);
"#;

pub struct Cursor {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl Cursor {
    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    /// In-memory cursor — useful for tests and ephemeral runs.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            path: PathBuf::from(":memory:"),
            conn: Mutex::new(conn),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Last fully processed checkpoint, or `None` if nothing has been written yet.
    pub fn last(&self) -> Result<Option<u64>> {
        let conn = self.conn.lock().unwrap();
        let row: Option<i64> = conn
            .query_row(
                "SELECT last_seq FROM wiretap_cursor WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.map(|v| v as u64))
    }

    /// Persist the highest fully processed checkpoint. Must be monotonic — the
    /// pipeline guarantees this; we additionally assert it to catch handler bugs.
    pub fn commit(&self, seq: u64) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO wiretap_cursor(id, last_seq, updated_at) VALUES (1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET
                 last_seq = MAX(wiretap_cursor.last_seq, excluded.last_seq),
                 updated_at = excluded.updated_at",
            params![seq as i64, now],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_none() {
        let c = Cursor::in_memory().unwrap();
        assert_eq!(c.last().unwrap(), None);
    }

    #[test]
    fn commit_monotonic() {
        let c = Cursor::in_memory().unwrap();
        c.commit(10).unwrap();
        assert_eq!(c.last().unwrap(), Some(10));
        c.commit(15).unwrap();
        assert_eq!(c.last().unwrap(), Some(15));
        // Late commit must not regress.
        c.commit(12).unwrap();
        assert_eq!(c.last().unwrap(), Some(15));
    }
}
