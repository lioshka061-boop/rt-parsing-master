use async_trait::async_trait;
use rusqlite::params;
use tokio_rusqlite::Connection;
use uuid::Uuid;

use crate::SqlWrapper;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewStatus {
    Published,
    Pending,
}

impl ReviewStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReviewStatus::Published => "published",
            ReviewStatus::Pending => "pending",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "pending" => ReviewStatus::Pending,
            _ => ReviewStatus::Published,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Review {
    pub id: i64,
    pub shop_id: Uuid,
    pub product_key: Option<String>,
    pub name: String,
    pub text: String,
    pub rating: i64,
    pub photos: Vec<String>,
    pub status: ReviewStatus,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewReview {
    pub shop_id: Uuid,
    pub product_key: Option<String>,
    pub name: String,
    pub text: String,
    pub rating: i64,
    pub photos: Vec<String>,
    pub status: ReviewStatus,
    pub created_at: i64,
}

#[async_trait]
pub trait ReviewRepository: Send + Sync {
    async fn add(&self, item: NewReview) -> anyhow::Result<Review>;
    async fn list(
        &self,
        shop_id: Uuid,
        product_key: Option<String>,
        limit: usize,
        offset: usize,
        status: ReviewStatus,
    ) -> anyhow::Result<Vec<Review>>;
}

pub struct SqliteReviewRepository {
    conn: Connection,
}

impl SqliteReviewRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS review (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_id TEXT NOT NULL,
                    product_key TEXT,
                    author TEXT NOT NULL,
                    text TEXT NOT NULL,
                    rating INTEGER NOT NULL,
                    photos TEXT,
                    status TEXT NOT NULL DEFAULT 'published',
                    created_at INTEGER NOT NULL
                )",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS review_shop_idx ON review(shop_id)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS review_shop_product_idx ON review(shop_id, product_key)",
                [],
            )?;
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

fn photos_to_db(list: &[String]) -> Option<String> {
    if list.is_empty() {
        None
    } else {
        serde_json::to_string(list).ok()
    }
}

fn photos_from_db(data: Option<String>) -> Vec<String> {
    data.and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

#[async_trait]
impl ReviewRepository for SqliteReviewRepository {
    async fn add(&self, item: NewReview) -> anyhow::Result<Review> {
        let SqlWrapper(out) = self
            .conn
            .call(move |conn| {
                let photos = photos_to_db(&item.photos);
                conn.execute(
                    "INSERT INTO review (shop_id, product_key, author, text, rating, photos, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        item.shop_id.to_string(),
                        item.product_key,
                        item.name,
                        item.text,
                        item.rating,
                        photos,
                        item.status.as_str(),
                        item.created_at
                    ],
                )?;
                let id = conn.last_insert_rowid();
                Ok(SqlWrapper(Review {
                    id,
                    shop_id: item.shop_id,
                    product_key: item.product_key,
                    name: item.name,
                    text: item.text,
                    rating: item.rating,
                    photos: item.photos,
                    status: item.status,
                    created_at: item.created_at,
                }))
            })
            .await?;
        Ok(out)
    }

    async fn list(
        &self,
        shop_id: Uuid,
        product_key: Option<String>,
        limit: usize,
        offset: usize,
        status: ReviewStatus,
    ) -> anyhow::Result<Vec<Review>> {
        let shop_id = shop_id.to_string();
        let status_raw = status.as_str().to_string();
        let SqlWrapper(items) = self
            .conn
            .call(move |conn| {
                let mut items = Vec::new();
                if let Some(product_key) = product_key {
                    let mut stmt = conn.prepare(
                        "SELECT id, shop_id, product_key, author, text, rating, photos, status, created_at
                         FROM review
                         WHERE shop_id = ?1 AND product_key = ?2 AND status = ?3
                         ORDER BY created_at DESC
                         LIMIT ?4 OFFSET ?5",
                    )?;
                    let rows = stmt.query_map(
                        params![shop_id, product_key, status_raw, limit as i64, offset as i64],
                        |row| {
                            let created_at: i64 = row.get(8)?;
                            let shop_id: String = row.get(1)?;
                            let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                            Ok(Review {
                                id: row.get(0)?,
                                shop_id,
                                product_key: row.get(2)?,
                                name: row.get(3)?,
                                text: row.get(4)?,
                                rating: row.get(5)?,
                                photos: photos_from_db(row.get(6)?),
                                status: ReviewStatus::from_str(row.get::<_, String>(7)?.as_str()),
                                created_at,
                            })
                        },
                    )?;
                    for row in rows {
                        items.push(row?);
                    }
                } else {
                    let mut stmt = conn.prepare(
                        "SELECT id, shop_id, product_key, author, text, rating, photos, status, created_at
                         FROM review
                         WHERE shop_id = ?1 AND status = ?2
                         ORDER BY created_at DESC
                         LIMIT ?3 OFFSET ?4",
                    )?;
                    let rows = stmt.query_map(
                        params![shop_id, status_raw, limit as i64, offset as i64],
                        |row| {
                            let created_at: i64 = row.get(8)?;
                            let shop_id: String = row.get(1)?;
                            let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                            Ok(Review {
                                id: row.get(0)?,
                                shop_id,
                                product_key: row.get(2)?,
                                name: row.get(3)?,
                                text: row.get(4)?,
                                rating: row.get(5)?,
                                photos: photos_from_db(row.get(6)?),
                                status: ReviewStatus::from_str(row.get::<_, String>(7)?.as_str()),
                                created_at,
                            })
                        },
                    )?;
                    for row in rows {
                        items.push(row?);
                    }
                }
                Ok(SqlWrapper(items))
            })
            .await?;
        Ok(items)
    }
}

fn truncate_chars(input: &str, max_len: usize) -> String {
    let trimmed = input.trim();
    if trimmed.chars().count() <= max_len {
        return trimmed.to_string();
    }
    let mut end = 0usize;
    for (idx, _) in trimmed.char_indices() {
        if idx >= max_len {
            break;
        }
        end = idx;
    }
    trimmed[..=end].to_string()
}

pub(crate) fn normalize_product_key(input: Option<&str>) -> Option<String> {
    let value = input?.trim();
    if value.is_empty() {
        return None;
    }
    let lowered = value.to_lowercase();
    let normalized = truncate_chars(&lowered, 120);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(crate) fn normalize_name(input: &str) -> String {
    truncate_chars(input, 60)
}

pub(crate) fn normalize_text(input: &str) -> String {
    truncate_chars(input, 2000)
}

pub(crate) fn clamp_rating(value: i64) -> i64 {
    value.clamp(1, 5)
}

#[cfg(test)]
mod tests {
    use super::{clamp_rating, normalize_product_key, normalize_text};

    #[test]
    fn normalize_product_key_trims_and_lowercases() {
        assert_eq!(
            normalize_product_key(Some("  BMW G30  ")).as_deref(),
            Some("bmw g30")
        );
        assert_eq!(normalize_product_key(Some(" ")), None);
        assert_eq!(normalize_product_key(None), None);
    }

    #[test]
    fn normalize_text_limits_length() {
        let text = "a".repeat(2100);
        assert_eq!(normalize_text(&text).chars().count(), 2000);
    }

    #[test]
    fn clamp_rating_bounds() {
        assert_eq!(clamp_rating(0), 1);
        assert_eq!(clamp_rating(3), 3);
        assert_eq!(clamp_rating(6), 5);
    }
}
