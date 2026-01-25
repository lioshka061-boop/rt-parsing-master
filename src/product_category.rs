use async_trait::async_trait;
use regex::Regex;
use rusqlite::params;
use rusqlite::types::Type;
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::{Get, Remove, Save, Select};
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{IdentityOf, SelectBy, Selector};
use uuid::Uuid;

#[derive(Clone, Debug, Id)]
#[Id(ref_id, get_id)]
pub struct ProductCategory {
    pub name: String,
    #[id]
    pub id: Uuid,
    pub parent_id: Option<IdentityOf<ProductCategory>>,
    pub regex: Option<Regex>,
    pub shop_id: Uuid,
    pub status: CategoryStatus,
    pub visibility_on_site: Visibility,
    pub indexing_status: IndexingStatus,
    pub seo_title: Option<String>,
    pub seo_description: Option<String>,
    pub seo_text: Option<String>,
    pub image_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CategoryStatus {
    Draft,
    PublishedNoIndex,
    SeoReady,
}

impl CategoryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CategoryStatus::Draft => "draft",
            CategoryStatus::PublishedNoIndex => "published_noindex",
            CategoryStatus::SeoReady => "seo_ready",
        }
    }
    pub fn from_str(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "published_noindex" => CategoryStatus::PublishedNoIndex,
            "seo_ready" => CategoryStatus::SeoReady,
            _ => CategoryStatus::Draft,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Visibility {
    Hidden,
    Visible,
}

impl Visibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Visibility::Hidden => "hidden",
            Visibility::Visible => "visible",
        }
    }
    pub fn from_str(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "visible" => Visibility::Visible,
            _ => Visibility::Hidden,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexingStatus {
    NoIndex,
    Index,
}

impl IndexingStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexingStatus::NoIndex => "noindex",
            IndexingStatus::Index => "index",
        }
    }
    pub fn from_str(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "index" => IndexingStatus::Index,
            _ => IndexingStatus::NoIndex,
        }
    }
}

pub struct ByShop(pub Uuid);
pub struct ByParentId(pub Uuid);
pub struct TopLevel(pub Uuid);

impl Selector for ByShop {}
impl Selector for ByParentId {}
impl Selector for TopLevel {}

impl SelectBy<ByShop> for ProductCategory {}
impl SelectBy<ByParentId> for ProductCategory {}
impl SelectBy<TopLevel> for ProductCategory {}

#[async_trait]
pub trait ProductCategoryRepository:
    Repository<ProductCategory, Error = anyhow::Error>
    + Save<ProductCategory>
    + Get<ProductCategory>
    + Select<ProductCategory, ByShop>
    + Select<ProductCategory, ByParentId>
    + Select<ProductCategory, TopLevel>
    + Remove<ProductCategory>
    + Send
    + Sync
{
    async fn clear(&self, shop_id: Uuid) -> Result<(), Self::Error>;
}

pub struct SqliteProductCategoryRepository {
    conn: Connection,
}

impl SqliteProductCategoryRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS product_category (
                    id BLOB PRIMARY KEY,
                    parent_id BLOB,
                    name TEXT,
                    regex TEXT,
                    shop_id BLOB,
                    status TEXT NOT NULL DEFAULT 'draft',
                    visibility_on_site TEXT NOT NULL DEFAULT 'hidden',
                    indexing_status TEXT NOT NULL DEFAULT 'noindex',
                    seo_title TEXT,
                    seo_description TEXT,
                    seo_text TEXT,
                    image_url TEXT
                )",
                [],
            )?;
            // add columns if missing
            let _ = conn.execute(
                "ALTER TABLE product_category ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE product_category ADD COLUMN visibility_on_site TEXT NOT NULL DEFAULT 'hidden'",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE product_category ADD COLUMN indexing_status TEXT NOT NULL DEFAULT 'noindex'",
                [],
            );
            let _ = conn.execute("ALTER TABLE product_category ADD COLUMN seo_title TEXT", []);
            let _ = conn.execute("ALTER TABLE product_category ADD COLUMN seo_description TEXT", []);
            let _ = conn.execute("ALTER TABLE product_category ADD COLUMN seo_text TEXT", []);
            let _ = conn.execute("ALTER TABLE product_category ADD COLUMN image_url TEXT", []);
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

impl Repository<ProductCategory> for SqliteProductCategoryRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Select<ProductCategory, ByShop> for SqliteProductCategoryRepository {
    async fn select(&self, ByShop(shop_id): &ByShop) -> Result<Vec<ProductCategory>, Self::Error> {
        let shop_id = *shop_id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, status, visibility_on_site, indexing_status, seo_title, seo_description, seo_text, image_url
                     FROM product_category WHERE shop_id = ?1 ORDER BY name",
                )?;
                let items = stmt
                    .query_map([shop_id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(ProductCategory {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<ProductCategory>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            status: CategoryStatus::from_str(row.get::<_, String>(5)?.as_str()),
                            visibility_on_site: Visibility::from_str(
                                row.get::<_, String>(6)?.as_str(),
                            ),
                            indexing_status: IndexingStatus::from_str(
                                row.get::<_, String>(7)?.as_str(),
                            ),
                            seo_title: row.get(8)?,
                            seo_description: row.get(9)?,
                            seo_text: row.get(10)?,
                            image_url: row.get(11)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await?)
    }
}

#[async_trait]
impl Select<ProductCategory, TopLevel> for SqliteProductCategoryRepository {
    async fn select(
        &self,
        TopLevel(shop_id): &TopLevel,
    ) -> Result<Vec<ProductCategory>, Self::Error> {
        let shop_id = *shop_id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, status, visibility_on_site, indexing_status, seo_title, seo_description, seo_text, image_url
                     FROM product_category WHERE shop_id = ?1 AND parent_id IS NULL ORDER BY name",
                )?;
                let items = stmt
                    .query_map([shop_id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(ProductCategory {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<ProductCategory>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            status: CategoryStatus::from_str(row.get::<_, String>(5)?.as_str()),
                            visibility_on_site: Visibility::from_str(
                                row.get::<_, String>(6)?.as_str(),
                            ),
                            indexing_status: IndexingStatus::from_str(
                                row.get::<_, String>(7)?.as_str(),
                            ),
                            seo_title: row.get(8)?,
                            seo_description: row.get(9)?,
                            seo_text: row.get(10)?,
                            image_url: row.get(11)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await?)
    }
}

#[async_trait]
impl Select<ProductCategory, ByParentId> for SqliteProductCategoryRepository {
    async fn select(
        &self,
        ByParentId(parent_id): &ByParentId,
    ) -> Result<Vec<ProductCategory>, Self::Error> {
        let parent_id = *parent_id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, status, visibility_on_site, indexing_status, seo_title, seo_description, seo_text, image_url
                     FROM product_category WHERE parent_id = ?1 ORDER BY name",
                )?;
                let items = stmt
                    .query_map([parent_id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(ProductCategory {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<ProductCategory>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            status: CategoryStatus::from_str(row.get::<_, String>(5)?.as_str()),
                            visibility_on_site: Visibility::from_str(
                                row.get::<_, String>(6)?.as_str(),
                            ),
                            indexing_status: IndexingStatus::from_str(
                                row.get::<_, String>(7)?.as_str(),
                            ),
                            seo_title: row.get(8)?,
                            seo_description: row.get(9)?,
                            seo_text: row.get(10)?,
                            image_url: row.get(11)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await?)
    }
}

#[async_trait]
impl Get<ProductCategory> for SqliteProductCategoryRepository {
    async fn get_one(
        &self,
        id: &IdentityOf<ProductCategory>,
    ) -> Result<Option<ProductCategory>, Self::Error> {
        let id = *id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, status, visibility_on_site, indexing_status, seo_title, seo_description, seo_text, image_url
                     FROM product_category WHERE id = ?1",
                )?;
                let mut rows = stmt.query([id])?;
                let row = match rows.next()? {
                    Some(r) => r,
                    None => return Ok(None),
                };
                let regex = row
                    .get::<_, Option<String>>(3)?
                    .as_deref()
                    .map(Regex::new)
                    .transpose()
                    .map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                    })?;
                Ok(Some(ProductCategory {
                    id: row.get(0)?,
                    parent_id: row.get::<_, Option<IdentityOf<ProductCategory>>>(1)?,
                    name: row.get(2)?,
                    regex,
                    shop_id: row.get(4)?,
                    status: CategoryStatus::from_str(row.get::<_, String>(5)?.as_str()),
                    visibility_on_site: Visibility::from_str(row.get::<_, String>(6)?.as_str()),
                    indexing_status: IndexingStatus::from_str(row.get::<_, String>(7)?.as_str()),
                    seo_title: row.get(8)?,
                    seo_description: row.get(9)?,
                    seo_text: row.get(10)?,
                    image_url: row.get(11)?,
                }))
            })
            .await?)
    }
}

#[async_trait]
impl Save<ProductCategory> for SqliteProductCategoryRepository {
    async fn save(&self, c: ProductCategory) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO product_category (id, parent_id, name, regex, shop_id, status, visibility_on_site, indexing_status, seo_title, seo_description, seo_text, image_url)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                     ON CONFLICT(id) DO UPDATE SET parent_id=?2, name=?3, regex=?4, shop_id=?5, status=?6, visibility_on_site=?7, indexing_status=?8, seo_title=?9, seo_description=?10, seo_text=?11, image_url=?12",
                    params![
                        c.id,
                        c.parent_id,
                        c.name,
                        c.regex
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                        c.shop_id,
                        c.status.as_str(),
                        c.visibility_on_site.as_str(),
                        c.indexing_status.as_str(),
                        c.seo_title,
                        c.seo_description,
                        c.seo_text,
                        c.image_url,
                    ],
                )?;
                Ok(())
            })
            .await?)
    }
}

#[async_trait]
impl Remove<ProductCategory> for SqliteProductCategoryRepository {
    async fn remove(&self, id: &IdentityOf<ProductCategory>) -> Result<(), Self::Error> {
        let id = *id;
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM product_category WHERE id = ?1", params![id])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ProductCategoryRepository for SqliteProductCategoryRepository {
    async fn clear(&self, shop_id: Uuid) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM product_category WHERE shop_id = ?1",
                    params![shop_id],
                )?;
                Ok(())
            })
            .await?)
    }
}
