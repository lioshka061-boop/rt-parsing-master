use async_trait::async_trait;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_rusqlite::Connection;

#[derive(Debug, Clone)]
pub struct ManualUrlEntry {
    pub id: i64,
    pub url: String,
    pub added_at: OffsetDateTime,
    pub note: Option<String>,
    pub enabled: bool,
}

#[async_trait]
pub trait ManualUrlRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<ManualUrlEntry>, anyhow::Error>;
    async fn list_urls(&self) -> Result<Vec<String>, anyhow::Error>;
    async fn add(&self, url: &str) -> Result<bool, anyhow::Error>;
    async fn remove(&self, id: i64) -> Result<(), anyhow::Error>;
    async fn remove_by_url(&self, url: &str) -> Result<(), anyhow::Error>;
}

pub struct SqliteManualUrlRepository {
    conn: Connection,
}

impl SqliteManualUrlRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            let _ = conn.pragma_update(None, "journal_mode", &"WAL");
            let _ = conn.pragma_update(None, "synchronous", &"NORMAL");
            let _ = conn.pragma_update(None, "busy_timeout", &5000i64);
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dt_manual_url (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    added_at INTEGER NOT NULL,
                    note TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1
                )",
                [],
            )?;
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl ManualUrlRepository for SqliteManualUrlRepository {
    async fn list(&self) -> Result<Vec<ManualUrlEntry>, anyhow::Error> {
        self.conn
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, url, added_at, note, enabled
                     FROM dt_manual_url
                     ORDER BY added_at DESC, id DESC",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        let ts: i64 = row.get(2)?;
                        let added_at = OffsetDateTime::from_unix_timestamp(ts)
                            .unwrap_or_else(|_| OffsetDateTime::now_utc());
                        Ok(ManualUrlEntry {
                            id: row.get(0)?,
                            url: row.get(1)?,
                            added_at,
                            note: row.get(3)?,
                            enabled: row.get::<_, i64>(4)? != 0,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
            .map_err(anyhow::Error::from)
    }

    async fn list_urls(&self) -> Result<Vec<String>, anyhow::Error> {
        self.conn
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT url FROM dt_manual_url WHERE enabled = 1 ORDER BY added_at DESC",
                )?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
            .map_err(anyhow::Error::from)
    }

    async fn add(&self, url: &str) -> Result<bool, anyhow::Error> {
        let url = url.trim().to_string();
        if url.is_empty() {
            return Ok(false);
        }
        let now = OffsetDateTime::now_utc().unix_timestamp();
        self.conn
            .call(move |conn| {
                let affected = conn.execute(
                    "INSERT OR IGNORE INTO dt_manual_url (url, added_at, enabled) VALUES (?1, ?2, 1)",
                    rusqlite::params![url, now],
                )?;
                Ok(affected > 0)
            })
            .await
            .map_err(anyhow::Error::from)
    }

    async fn remove(&self, id: i64) -> Result<(), anyhow::Error> {
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM dt_manual_url WHERE id = ?1", [id])?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
    }

    async fn remove_by_url(&self, url: &str) -> Result<(), anyhow::Error> {
        let url = url.trim().to_string();
        if url.is_empty() {
            return Ok(());
        }
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM dt_manual_url WHERE url = ?1", [url])?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
    }
}

pub type ManualUrlRepo = Arc<dyn ManualUrlRepository>;
