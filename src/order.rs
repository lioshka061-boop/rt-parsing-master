use async_trait::async_trait;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tokio_rusqlite::Connection;
use uuid::Uuid;

use crate::SqlWrapper;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderItem {
    pub article: String,
    pub title: String,
    pub price: Option<usize>,
    pub quantity: usize,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: i64,
    pub shop_id: Uuid,
    pub customer_name: String,
    pub phone: String,
    pub email: Option<String>,
    pub delivery: String,
    pub city_name: Option<String>,
    pub branch_name: Option<String>,
    pub payment: String,
    pub total: i64,
    pub items_count: usize,
    pub items_json: String,
    pub comment: Option<String>,
    pub created_at: i64,
}

#[async_trait]
pub trait OrderRepository: Send + Sync {
    async fn add(&self, item: NewOrder) -> anyhow::Result<Order>;
    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<Order>>;
    async fn remove(&self, shop_id: Uuid, id: i64) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct NewOrder {
    pub shop_id: Uuid,
    pub customer_name: String,
    pub phone: String,
    pub email: Option<String>,
    pub delivery: String,
    pub city_name: Option<String>,
    pub branch_name: Option<String>,
    pub payment: String,
    pub total: i64,
    pub items_count: usize,
    pub items_json: String,
    pub comment: Option<String>,
    pub created_at: i64,
}

pub struct SqliteOrderRepository {
    conn: Connection,
}

impl SqliteOrderRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS shop_order (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_id TEXT NOT NULL,
                    customer_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    email TEXT,
                    delivery TEXT NOT NULL,
                    city_name TEXT,
                    branch_name TEXT,
                    payment TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    items_count INTEGER NOT NULL,
                    items_json TEXT NOT NULL,
                    comment TEXT,
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
impl OrderRepository for SqliteOrderRepository {
    async fn add(&self, item: NewOrder) -> anyhow::Result<Order> {
        let SqlWrapper(out) = self
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO shop_order (
                        shop_id, customer_name, phone, email, delivery,
                        city_name, branch_name, payment, total, items_count,
                        items_json, comment, created_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        item.shop_id.to_string(),
                        item.customer_name,
                        item.phone,
                        item.email,
                        item.delivery,
                        item.city_name,
                        item.branch_name,
                        item.payment,
                        item.total,
                        item.items_count as i64,
                        item.items_json,
                        item.comment,
                        item.created_at
                    ],
                )?;
                let id = conn.last_insert_rowid();
                Ok(SqlWrapper(Order {
                    id,
                    shop_id: item.shop_id,
                    customer_name: item.customer_name,
                    phone: item.phone,
                    email: item.email,
                    delivery: item.delivery,
                    city_name: item.city_name,
                    branch_name: item.branch_name,
                    payment: item.payment,
                    total: item.total,
                    items_count: item.items_count,
                    items_json: item.items_json,
                    comment: item.comment,
                    created_at: item.created_at,
                }))
            })
            .await?;
        Ok(out)
    }

    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<Order>> {
        let SqlWrapper(items) = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, shop_id, customer_name, phone, email, delivery,
                        city_name, branch_name, payment, total, items_count,
                        items_json, comment, created_at
                     FROM shop_order WHERE shop_id = ?1 ORDER BY created_at DESC",
                )?;
                let items = stmt
                    .query_map(params![shop_id.to_string()], |row| {
                        let id: i64 = row.get(0)?;
                        let shop_id: String = row.get(1)?;
                        let customer_name: String = row.get(2)?;
                        let phone: String = row.get(3)?;
                        let email: Option<String> = row.get(4)?;
                        let delivery: String = row.get(5)?;
                        let city_name: Option<String> = row.get(6)?;
                        let branch_name: Option<String> = row.get(7)?;
                        let payment: String = row.get(8)?;
                        let total: i64 = row.get(9)?;
                        let items_count: i64 = row.get(10)?;
                        let items_json: String = row.get(11)?;
                        let comment: Option<String> = row.get(12)?;
                        let created_at: i64 = row.get(13)?;
                        let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                        Ok(Order {
                            id,
                            shop_id,
                            customer_name,
                            phone,
                            email,
                            delivery,
                            city_name,
                            branch_name,
                            payment,
                            total,
                            items_count: items_count.max(0) as usize,
                            items_json,
                            comment,
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
                    "DELETE FROM shop_order WHERE shop_id = ?1 AND id = ?2",
                    params![shop_id.to_string(), id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}
