use async_trait::async_trait;
use rusqlite::params;
use tokio_rusqlite::Connection;
use uuid::Uuid;

use crate::SqlWrapper;

#[derive(Debug, Clone)]
pub struct QuickOrder {
    pub id: i64,
    pub shop_id: Uuid,
    pub phone: String,
    pub article: Option<String>,
    pub title: Option<String>,
    pub created_at: i64,
}

#[async_trait]
pub trait QuickOrderRepository: Send + Sync {
    async fn add(&self, item: NewQuickOrder) -> anyhow::Result<QuickOrder>;
    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<QuickOrder>>;
    async fn remove(&self, shop_id: Uuid, id: i64) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct NewQuickOrder {
    pub shop_id: Uuid,
    pub phone: String,
    pub article: Option<String>,
    pub title: Option<String>,
    pub created_at: i64,
}

pub struct SqliteQuickOrderRepository {
    conn: Connection,
}

impl SqliteQuickOrderRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS quick_order (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_id TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    article TEXT,
                    title TEXT,
                    created_at INTEGER NOT NULL
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
impl QuickOrderRepository for SqliteQuickOrderRepository {
    async fn add(&self, item: NewQuickOrder) -> anyhow::Result<QuickOrder> {
        let SqlWrapper(out) = self
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO quick_order (shop_id, phone, article, title, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        item.shop_id.to_string(),
                        item.phone,
                        item.article,
                        item.title,
                        item.created_at
                    ],
                )?;
                let id = conn.last_insert_rowid();
                Ok(SqlWrapper(QuickOrder {
                    id,
                    shop_id: item.shop_id,
                    phone: item.phone,
                    article: item.article,
                    title: item.title,
                    created_at: item.created_at,
                }))
            })
            .await?;
        Ok(out)
    }

    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<QuickOrder>> {
        let SqlWrapper(items) = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, shop_id, phone, article, title, created_at
                     FROM quick_order WHERE shop_id = ?1 ORDER BY created_at DESC",
                )?;
                let items = stmt
                    .query_map(params![shop_id.to_string()], |row| {
                        let id: i64 = row.get(0)?;
                        let shop_id: String = row.get(1)?;
                        let phone: String = row.get(2)?;
                        let article: Option<String> = row.get(3)?;
                        let title: Option<String> = row.get(4)?;
                        let created_at: i64 = row.get(5)?;
                        let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                        Ok(QuickOrder {
                            id,
                            shop_id,
                            phone,
                            article,
                            title,
                            created_at,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SqlWrapper(items))
            })
            .await?;
        Ok(items)
    }

    async fn remove(&self, shop_id: Uuid, id: i64) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM quick_order WHERE shop_id = ?1 AND id = ?2",
                    params![shop_id.to_string(), id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}
