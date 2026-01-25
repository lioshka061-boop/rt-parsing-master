use async_trait::async_trait;
use lazy_regex::regex;
use regex::Regex;
use rt_types::category::{By, ByParentId, Category, CategoryRepository, TopLevel};
use rt_types::shop::Shop;
use rusqlite::params;
use rusqlite::types::Type;
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::{Get, Remove, Save, Select};
use typesafe_repository::prelude::*;

pub struct SqliteCategoryRepository {
    conn: Connection,
}

impl SqliteCategoryRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS category (
                    id BLOB PRIMARY KEY,
                    parent_id BLOB,
                    name TEXT,
                    regex TEXT,
                    shop_id BLOB,
                    seo_title TEXT,
                    seo_description TEXT,
                    seo_text TEXT
                )",
                [],
            )?;
            let _ = conn.execute("ALTER TABLE category ADD COLUMN seo_title TEXT", []);
            let _ = conn.execute("ALTER TABLE category ADD COLUMN seo_description TEXT", []);
            let _ = conn.execute("ALTER TABLE category ADD COLUMN seo_text TEXT", []);
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
    pub async fn update_by_name(
        &self,
        name: String,
        id: IdentityOf<Category>,
        parent_id: Option<IdentityOf<Category>>,
    ) -> Result<(), anyhow::Error> {
        self.conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE category SET id = ?1, parent_id = ?2 WHERE name = ?3",
                    params![id, parent_id, name],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

impl Repository<Category> for SqliteCategoryRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Select<Category, ByParentId> for SqliteCategoryRepository {
    async fn select(&self, ByParentId(id): &ByParentId) -> Result<Vec<Category>, Self::Error> {
        let id = id.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, seo_title, seo_description, seo_text FROM category WHERE parent_id = ?1 ORDER BY name",
                )?;
                let p = stmt
                    .query_map([id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(Category {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<Category>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            seo_title: row.get(5)?,
                            seo_description: row.get(6)?,
                            seo_text: row.get(7)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p)
            })
            .await?)
    }
}

#[async_trait]
impl Select<Category, By<IdentityOf<Shop>>> for SqliteCategoryRepository {
    async fn select(
        &self,
        By(shop_id): &By<IdentityOf<Shop>>,
    ) -> Result<Vec<Category>, Self::Error> {
        let shop_id = shop_id.clone();
        Ok(self
            .conn
            .call(move |conn| {
             let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, seo_title, seo_description, seo_text FROM category WHERE shop_id = ?1 ORDER BY name",
                )?;
                let p = stmt
                    .query_map([shop_id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(Category {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<Category>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            seo_title: row.get(5)?,
                            seo_description: row.get(6)?,
                            seo_text: row.get(7)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p)
            })
        .await?)
    }
}

#[async_trait]
impl Select<Category, TopLevel<By<IdentityOf<Shop>>>> for SqliteCategoryRepository {
    async fn select(
        &self,
        TopLevel(By(shop_id)): &TopLevel<By<IdentityOf<Shop>>>,
    ) -> Result<Vec<Category>, Self::Error> {
        let shop_id = shop_id.clone();
        Ok(self
            .conn
            .call(move |conn| {
             let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, seo_title, seo_description, seo_text FROM category WHERE shop_id = ?1 AND parent_id IS NULL ORDER BY name",
                )?;
                let p = stmt
                    .query_map([shop_id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(Category {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<Category>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            seo_title: row.get(5)?,
                            seo_description: row.get(6)?,
                            seo_text: row.get(7)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p)
            })
        .await?)
    }
}

#[async_trait]
impl Get<Category> for SqliteCategoryRepository {
    async fn get_one(&self, id: &IdentityOf<Category>) -> Result<Option<Category>, Self::Error> {
        let id = id.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, parent_id, name, regex, shop_id, seo_title, seo_description, seo_text FROM category WHERE id = ?1",
                )?;
                let p = stmt
                    .query_map([id], |row| {
                        let regex = row
                            .get::<_, Option<String>>(3)?
                            .as_deref()
                            .map(Regex::new)
                            .transpose()
                            .map_err(|err| {
                                rusqlite::Error::FromSqlConversionFailure(3, Type::Text, err.into())
                            })?;
                        Ok(Category {
                            id: row.get(0)?,
                            parent_id: row.get::<_, Option<IdentityOf<Category>>>(1)?,
                            name: row.get(2)?,
                            regex,
                            shop_id: row.get(4)?,
                            seo_title: row.get(5)?,
                            seo_description: row.get(6)?,
                            seo_text: row.get(7)?,
                        })
                    })?
                    .next()
                    .transpose()?;
                Ok(p)
            })
            .await?)
    }
}

#[async_trait]
impl Save<Category> for SqliteCategoryRepository {
    async fn save(&self, c: Category) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO category (id, parent_id, name, regex, shop_id, seo_title, seo_description, seo_text) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(id) DO UPDATE SET parent_id=?2, name=?3, regex=?4, shop_id=?5, seo_title=?6, seo_description=?7, seo_text=?8",
                    params![
                        c.id,
                        c.parent_id,
                        c.name,
                        c.regex
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                        c.shop_id,
                        c.seo_title,
                        c.seo_description,
                        c.seo_text,
                    ],
                )?;
                Ok(())
            })
            .await?)
    }
}

#[async_trait]
impl Remove<Category> for SqliteCategoryRepository {
    async fn remove(&self, id: &IdentityOf<Category>) -> Result<(), Self::Error> {
        let id = id.clone();
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM category WHERE id = ?1", params![id])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl CategoryRepository for SqliteCategoryRepository {
    async fn clear(&self) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                conn.execute("DELETE FROM category", [])?;
                Ok(())
            })
            .await?)
    }
    async fn count_by(&self, By(shop_id): &By<IdentityOf<Shop>>) -> Result<usize, Self::Error> {
        let shop_id = shop_id.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare("SELECT COUNT(*) FROM category WHERE shop_id = ?1")?;
                let p = stmt
                    .query_map([shop_id], |row| row.get(0))?
                    .next()
                    .transpose()?
                    .unwrap_or_default();
                Ok(p)
            })
            .await?)
    }
}
