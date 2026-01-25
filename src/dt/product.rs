#![allow(clippy::let_and_return)]

use crate::{Model, Url};
use async_trait::async_trait;
use rt_types::Availability;
use rusqlite::{Transaction, TransactionBehavior};
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::*;
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{SelectBy, Selector};

#[derive(Id, Clone, Debug)]
#[Id(ref_id, get_id)]
pub struct Product {
    pub title: String,
    pub description: Option<String>,
    pub title_ua: Option<String>,
    pub description_ua: Option<String>,
    pub price: Option<usize>,
    pub source_price: Option<usize>,
    #[id]
    pub article: String,
    pub brand: String,
    #[id_by]
    pub model: Model,
    pub category: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub available: Availability,
    pub quantity: Option<usize>,
    #[id_by]
    pub url: Url,
    pub supplier: Option<String>,
    pub discount_percent: Option<usize>,
    pub last_visited: OffsetDateTime,
    pub images: Vec<String>,
    pub upsell: Option<String>,
}

impl Product {
    fn img_as_str(&self) -> String {
        itertools::intersperse(self.images.iter().cloned(), ",".to_string()).collect()
    }
    fn img_from_str<T: AsRef<str>>(s: T) -> Vec<String> {
        s.as_ref().split(',').map(ToString::to_string).collect()
    }
    fn attrs_to_db(attrs: &Option<HashMap<String, String>>) -> Option<String> {
        attrs
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok())
            .filter(|s| !s.is_empty())
    }
    fn attrs_from_db(raw: Option<String>) -> Option<HashMap<String, String>> {
        raw.and_then(|s| serde_json::from_str(&s).ok())
    }
    pub fn is_outdated(&self) -> bool {
        let now = OffsetDateTime::now_utc();
        self.last_visited
            .checked_add(Duration::hours(24))
            .map(|x| x < now)
            .unwrap_or(true)
    }
    pub fn format_model(&self) -> Option<String> {
        let Model(model) = &self.model;
        let brand = &self.brand;
        if model.contains(brand) {
            Some(model.clone())
        } else if model.contains(&brand.to_uppercase()) {
            Some(model.replace(&brand.to_uppercase(), brand))
        } else if model.contains(&brand.to_lowercase()) {
            Some(model.replace(&brand.to_lowercase(), brand))
        } else {
            Some(format!("{brand} {model}"))
        }
    }

    pub fn slug(&self) -> String {
        let url = self.url.0.trim_matches('/');
        let candidate = url.rsplit('/').next().unwrap_or(url);
        if candidate.is_empty() {
            self.article.clone()
        } else {
            candidate.to_string()
        }
    }

    pub fn images_csv(&self) -> String {
        self.images.join(", ")
    }

    pub fn description_text(&self) -> String {
        self.description.clone().unwrap_or_default()
    }

    pub fn upsell_text(&self) -> String {
        self.upsell.clone().unwrap_or_default()
    }
}

fn row_to_product(row: &rusqlite::Row<'_>) -> rusqlite::Result<Product> {
    let images = row
        .get::<_, Option<String>>(17)?
        .map(Product::img_from_str)
        .unwrap_or_default();
    let quantity: Option<i64> = row.get(11)?;
    let discount_percent: Option<i64> = row.get(14)?;
    Ok(Product {
        title: row.get(0)?,
        description: row.get(1)?,
        title_ua: row.get(2)?,
        description_ua: row.get(3)?,
        price: row.get(4)?,
        source_price: row.get(5)?,
        article: row.get(6)?,
        model: Model(row.get(7)?),
        category: row.get(8)?,
        attributes: Product::attrs_from_db(row.get(9)?),
        available: row.get::<_, u8>(10)?.into(),
        quantity: quantity.map(|q| q.max(0) as usize),
        url: Url(row.get(12)?),
        supplier: row.get(13)?,
        discount_percent: discount_percent.map(|d| d.max(0) as usize),
        last_visited: row.get(15)?,
        brand: row.get(16)?,
        images,
        upsell: row.get(18).unwrap_or(None),
    })
}

pub struct FromDateAvailableSelector(pub OffsetDateTime);
pub struct AvailableSelector;

impl Selector for FromDateAvailableSelector {}
impl Selector for AvailableSelector {}

impl SelectBy<FromDateAvailableSelector> for Product {}
impl SelectBy<AvailableSelector> for Product {}

#[async_trait]
pub trait ProductRepository:
    Repository<Product, Error = anyhow::Error>
    + Save<Product>
    + Get<Product>
    + List<Product>
    + GetBy<Product, Url>
    + ListBy<Product, Model>
    + Select<Product, FromDateAvailableSelector>
    + Select<Product, AvailableSelector>
    + DeleteProducts
    + Send
    + Sync
{
}

#[async_trait]
pub trait DeleteProducts {
    async fn delete_articles(&self, articles: &[String]) -> Result<(), anyhow::Error>;
}

pub struct SqliteProductRepository {
    conn: Connection,
}

impl SqliteProductRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            let _ = conn.pragma_update(None, "journal_mode", &"WAL");
            let _ = conn.pragma_update(None, "synchronous", &"NORMAL");
            let _ = conn.pragma_update(None, "busy_timeout", &5000i64);
            let conn = Transaction::new(conn, TransactionBehavior::Deferred)?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS product (
                    article TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    title_ua TEXT,
                    description_ua TEXT,
                    price INTEGER,
                    source_price INTEGER,
                    model TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    category TEXT,
                    attributes TEXT,
                    available INTEGER,
                    quantity INTEGER,
                    url TEXT,
                    supplier TEXT,
                    discount_percent INTEGER,
                    last_visited INTEGER,
                    images TEXT,
                    upsell TEXT
                )",
                [],
            )?;
            // Можемо додати колонку, якщо база створена раніше
            let _ = conn.execute("ALTER TABLE product ADD COLUMN upsell TEXT", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN title_ua TEXT", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN description_ua TEXT", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN source_price INTEGER", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN attributes TEXT", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN quantity INTEGER", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN supplier TEXT", []);
            let _ = conn.execute("ALTER TABLE product ADD COLUMN discount_percent INTEGER", []);
            conn.commit()?;
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

impl Repository<Product> for SqliteProductRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Save<Product> for SqliteProductRepository {
    async fn save(&self, p: Product) -> Result<(), Self::Error> {
        self.conn
            .call(move |conn| {
                let img = p.img_as_str();
                let attrs = Product::attrs_to_db(&p.attributes);
                conn.execute(
                    "INSERT OR REPLACE INTO product 
                    (title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell) 
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
                    rusqlite::params![
                        p.title,
                        p.description,
                        p.title_ua,
                        p.description_ua,
                        p.price,
                        p.source_price,
                        p.article,
                        p.model.0,
                        p.category,
                        attrs,
                        p.available as u8,
                        p.quantity.map(|q| q as i64),
                        p.url.0,
                        p.supplier,
                        p.discount_percent.map(|d| d as i64),
                        p.last_visited,
                        p.brand,
                        img,
                        p.upsell,
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Get<Product> for SqliteProductRepository {
    async fn get_one(&self, id: &IdentityOf<Product>) -> Result<Option<Product>, Self::Error> {
        let id = id.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let p = {
                    let mut stmt = conn.prepare(
                        "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell
                        FROM product WHERE article = ?1",
                    )?;
                    let p = stmt
                        .query_map([&id], row_to_product)?
                        .collect::<Result<Vec<_>, _>>();
                        p
                };
                Ok(p?.pop())
            })
            .await?)
    }
}

#[async_trait]
impl GetBy<Product, Url> for SqliteProductRepository {
    async fn get_by(&self, url: &Url) -> Result<Option<Product>, Self::Error> {
        let url = url.clone();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell
                    FROM product WHERE url = ?1",
                )?;
                let mut p = stmt
                    .query_map([url.0], row_to_product)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(p.pop())
            })
            .await?)
    }
}

#[async_trait]
impl List<Product> for SqliteProductRepository {
    async fn list(&self) -> Result<Vec<Product>, Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let p = {
                    let mut stmt = conn.prepare(
                        "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell
                        FROM product",
                    )?;
                    let p = stmt
                        .query_map([], row_to_product)?
                        .collect::<Result<Vec<_>, _>>()?;
                    p
                };
                Ok(p)
            })
            .await?)
    }
}

#[async_trait]
impl ListBy<Product, Model> for SqliteProductRepository {
    async fn list_by(&self, model: &Model) -> Result<Vec<Product>, Self::Error> {
        let Model(model) = model.clone();
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell
                    FROM product WHERE model = ?1",
                )?;
                let p = stmt
                    .query_map([model], row_to_product)?
                    .collect::<Result<Vec<_>, _>>()?;
                    p
            };
            Ok(p)
        }).await?
        )
    }
}

#[async_trait]
impl Select<Product, AvailableSelector> for SqliteProductRepository {
    async fn select(&self, _: &AvailableSelector) -> Result<Vec<Product>, Self::Error> {
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell 
                    FROM product WHERE available > 0",
                )?;
                let p = stmt
                    .query_map([], row_to_product)?
                    .collect::<Result<Vec<_>, _>>()?;
                p
            };
            Ok(p)
        }).await?)
    }
}

#[async_trait]
impl Select<Product, FromDateAvailableSelector> for SqliteProductRepository {
    async fn select(
        &self,
        FromDateAvailableSelector(date): &FromDateAvailableSelector,
    ) -> Result<Vec<Product>, Self::Error> {
        let date = *date;
        Ok(self.conn.call(move |conn| {
            let p = {
                let mut stmt = conn.prepare(
                    "SELECT title, description, title_ua, description_ua, price, source_price, article, model, category, attributes, available, quantity, url, supplier, discount_percent, last_visited, brand, images, upsell 
                    FROM product WHERE last_visited >= ?1 AND available > 0",
                )?;
                let p = stmt
                    .query_map([date], row_to_product)?
                    .collect::<Result<Vec<_>, _>>()?;
                p
            };
            Ok(p)
        }).await?)
    }
}

impl ProductRepository for SqliteProductRepository {}

#[async_trait]
impl DeleteProducts for SqliteProductRepository {
    async fn delete_articles(&self, articles: &[String]) -> Result<(), anyhow::Error> {
        if articles.is_empty() {
            return Ok(());
        }
        let items = articles.to_owned();
        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                for a in items.iter() {
                    let _ = tx.execute("DELETE FROM product WHERE article = ?1", [&a]);
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

impl TryInto<rt_types::product::Product> for Product {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<rt_types::product::Product, Self::Error> {
        let url = self.url.0.to_lowercase();
        let vendor = if url.contains("op-tuning") || url.contains("op_tuning") {
            "O&P Tuning"
        } else {
            "design-tuning"
        };
        let images = self
            .images
            .iter()
            .map(|i| {
                if i.starts_with('/') {
                    format!("https://design-tuning.com{}", i.replace("mini_", ""))
                } else {
                    i.clone()
                }
            })
            .collect();

        let keywords = match &self.category {
            Some(category) => format!("{}, {}, {}", self.brand, self.model.0, category),
            None => format!("{}, {}", self.brand, self.model.0),
        };
        let keywords = Some(keywords);
        let mut params = HashMap::new();
        params.insert("Марка".to_string(), self.brand.clone());
        params.insert("Модель".to_string(), self.model.clone().0);
        if let Some(attrs) = &self.attributes {
            for (k, v) in attrs {
                if !k.trim().is_empty() && !v.trim().is_empty() {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(ref upsell) = self.upsell {
            params.insert("Upsell".to_string(), upsell.clone());
        }
        let ua_translation = self
            .title_ua
            .clone()
            .or_else(|| self.description_ua.clone().map(|_| self.title.clone()))
            .map(|title| rt_types::product::UaTranslation {
                title,
                description: self.description_ua.clone(),
            });
        Ok(rt_types::product::Product {
            id: rt_types::product::generate_id(&self.article, &vendor, &keywords),
            title: self.title,
            params,
            ua_translation,
            description: self.description,
            price: self
                .price
                .map(Into::into)
                .ok_or(anyhow::anyhow!("self must contain price"))?,
            in_stock: self.quantity,
            currency: "UAH".to_string(),
            article: self.article,
            brand: self.brand,
            model: self.model.0,
            category: None,
            available: self.available,
            vendor: vendor.into(),
            keywords,
            images,
        })
    }
}

pub struct MaxtonProduct(pub Product);

impl Identity for MaxtonProduct {
    type Id = <Product as Identity>::Id;
}

impl RefIdentity for MaxtonProduct {
    fn id_ref(&self) -> &Self::Id {
        self.0.id_ref()
    }
}

impl TryInto<rt_types::product::Product> for MaxtonProduct {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<rt_types::product::Product, Self::Error> {
        let vendor = "maxton";
        let images = self
            .0
            .images
            .iter()
            .map(|i| {
                if i.starts_with('/') {
                    format!("https://design-tuning.com{}", i.replace("mini_", ""))
                } else {
                    i.clone()
                }
            })
            .collect();

        let keywords = match &self.0.category {
            Some(category) => format!("{}, {}, {}", self.0.brand, self.0.model.0, category),
            None => format!("{}, {}", self.0.brand, self.0.model.0),
        };
        let keywords = Some(keywords);
        let mut params = HashMap::new();
        params.insert("Марка".to_string(), self.0.brand.clone());
        params.insert("Модель".to_string(), self.0.model.clone().0);
        if let Some(attrs) = &self.0.attributes {
            for (k, v) in attrs {
                if !k.trim().is_empty() && !v.trim().is_empty() {
                    params.insert(k.clone(), v.clone());
                }
            }
        }
        let ua_translation = self
            .0
            .title_ua
            .clone()
            .or_else(|| self.0.description_ua.clone().map(|_| self.0.title.clone()))
            .map(|title| rt_types::product::UaTranslation {
                title,
                description: self.0.description_ua.clone(),
            });
        Ok(rt_types::product::Product {
            id: rt_types::product::generate_id(&self.0.article, &vendor, &keywords),
            title: self.0.title,
            params,
            ua_translation,
            description: self.0.description,
            price: self
                .0
                .price
                .map(Into::into)
                .ok_or(anyhow::anyhow!("self.0 must contain price"))?,
            in_stock: self.0.quantity,
            currency: "UAH".to_string(),
            article: self.0.article,
            brand: self.0.brand,
            model: self.0.model.0,
            category: None,
            available: self.0.available,
            vendor: vendor.into(),
            keywords,
            images,
        })
    }
}
