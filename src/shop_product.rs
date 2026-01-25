use anyhow::Context;
use async_trait::async_trait;
use rt_types::Availability;
use time::OffsetDateTime;
use tokio_rusqlite::Connection;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceType {
    Manual,
    Xml,
    Api,
    Parsing,
    LegacyProm,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Manual => "manual",
            SourceType::Xml => "xml",
            SourceType::Api => "api",
            SourceType::Parsing => "parsing",
            SourceType::LegacyProm => "legacy_prom",
        }
    }

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "manual" => SourceType::Manual,
            "xml" => SourceType::Xml,
            "api" => SourceType::Api,
            "legacy_prom" => SourceType::LegacyProm,
            _ => SourceType::Parsing,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecommendMode {
    Auto,
    Manual,
}

impl RecommendMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecommendMode::Auto => "auto",
            RecommendMode::Manual => "manual",
        }
    }

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "manual" => RecommendMode::Manual,
            _ => RecommendMode::Auto,
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

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
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

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "index" => IndexingStatus::Index,
            _ => IndexingStatus::NoIndex,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProductStatus {
    Draft,
    PublishedNoIndex,
    SeoReady,
}

impl ProductStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProductStatus::Draft => "draft",
            ProductStatus::PublishedNoIndex => "published_noindex",
            ProductStatus::SeoReady => "seo_ready",
        }
    }

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "published_noindex" => ProductStatus::PublishedNoIndex,
            "seo_ready" => ProductStatus::SeoReady,
            _ => ProductStatus::Draft,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShopProduct {
    pub shop_id: Uuid,
    pub article: String,
    pub internal_product_id: Uuid,
    pub title: Option<String>,
    pub description: Option<String>,
    pub price: Option<usize>,
    pub images: Option<Vec<String>>,
    pub available: Option<Availability>,
    pub site_category_id: Option<Uuid>,
    pub recommend_mode: RecommendMode,
    pub recommended_articles: Vec<String>,
    pub is_hit: bool,
    pub source_type: SourceType,
    pub visibility_on_site: Visibility,
    pub indexing_status: IndexingStatus,
    pub status: ProductStatus,
    pub seo_score: i32,
    pub h1: Option<String>,
    pub seo_text: Option<String>,
    pub canonical: Option<String>,
    pub robots: Option<String>,
    pub og_title: Option<String>,
    pub og_description: Option<String>,
    pub og_image: Option<String>,
    pub slug: Option<String>,
    pub faq: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl ShopProduct {
    fn images_to_db(images: &Option<Vec<String>>) -> Option<String> {
        images.as_ref().map(|v| v.join(","))
    }

    fn images_from_db(images: Option<String>) -> Option<Vec<String>> {
        images.map(|s| {
            s.split(',')
                .map(|x| x.trim())
                .filter(|x| !x.is_empty())
                .map(str::to_string)
                .collect()
        })
    }

    fn articles_to_db(list: &[String]) -> Option<String> {
        let joined = list
            .iter()
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
            .join(",");
        if joined.is_empty() {
            None
        } else {
            Some(joined)
        }
    }

    fn articles_from_db(s: Option<String>) -> Vec<String> {
        s.unwrap_or_default()
            .split(',')
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    }
}

fn ensure_columns(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // Додаємо нові колонки, якщо ще не додані. Ігноруємо помилки про дублювання.
    let alters = [
        "ALTER TABLE shop_product ADD COLUMN source_type TEXT NOT NULL DEFAULT 'parsing'",
        "ALTER TABLE shop_product ADD COLUMN visibility_on_site TEXT NOT NULL DEFAULT 'hidden'",
        "ALTER TABLE shop_product ADD COLUMN indexing_status TEXT NOT NULL DEFAULT 'noindex'",
        "ALTER TABLE shop_product ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'",
        "ALTER TABLE shop_product ADD COLUMN seo_score INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shop_product ADD COLUMN internal_product_id TEXT",
        "ALTER TABLE shop_product ADD COLUMN is_hit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shop_product ADD COLUMN h1 TEXT",
        "ALTER TABLE shop_product ADD COLUMN seo_text TEXT",
        "ALTER TABLE shop_product ADD COLUMN canonical TEXT",
        "ALTER TABLE shop_product ADD COLUMN robots TEXT",
        "ALTER TABLE shop_product ADD COLUMN og_title TEXT",
        "ALTER TABLE shop_product ADD COLUMN og_description TEXT",
        "ALTER TABLE shop_product ADD COLUMN og_image TEXT",
        "ALTER TABLE shop_product ADD COLUMN slug TEXT",
        "ALTER TABLE shop_product ADD COLUMN faq TEXT",
    ];
    for sql in alters {
        let _ = conn.execute(sql, []);
    }
    Ok(())
}

#[async_trait]
pub trait ShopProductRepository: Send + Sync {
    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<ShopProduct>>;
    async fn get(&self, shop_id: Uuid, article: &str) -> anyhow::Result<Option<ShopProduct>>;
    async fn upsert(&self, product: ShopProduct) -> anyhow::Result<()>;
    async fn ensure_exists(&self, shop_id: Uuid, article: &str) -> anyhow::Result<()>;
    async fn set_site_category(
        &self,
        shop_id: Uuid,
        article: &str,
        category_id: Option<Uuid>,
    ) -> anyhow::Result<()>;
    async fn bulk_set_visibility(
        &self,
        shop_id: Uuid,
        articles: &[String],
        visibility: Visibility,
        indexing_status: IndexingStatus,
        status: ProductStatus,
        robots: Option<String>,
        source_type: SourceType,
        ensure_missing: bool,
    ) -> anyhow::Result<usize>;
    async fn bulk_set_hit(
        &self,
        shop_id: Uuid,
        articles: &[String],
        is_hit: bool,
        ensure_missing: bool,
    ) -> anyhow::Result<usize>;
    async fn remove(&self, shop_id: Uuid, article: &str) -> anyhow::Result<()>;
    async fn remove_many(&self, shop_id: Uuid, articles: &[String]) -> anyhow::Result<()>;
}

pub struct SqliteShopProductRepository {
    conn: Connection,
}

impl SqliteShopProductRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            let _ = conn.pragma_update(None, "journal_mode", &"WAL");
            let _ = conn.pragma_update(None, "synchronous", &"NORMAL");
            let _ = conn.pragma_update(None, "busy_timeout", &5000i64);
            conn.execute(
                "CREATE TABLE IF NOT EXISTS shop_product (
                    shop_id TEXT NOT NULL,
                    article TEXT NOT NULL,
                    internal_product_id TEXT,
                    title TEXT,
                    description TEXT,
                    price INTEGER,
                    images TEXT,
                    available INTEGER,
                    site_category_id TEXT,
                    recommend_mode TEXT NOT NULL DEFAULT 'auto',
                    recommended_articles TEXT,
                    is_hit INTEGER NOT NULL DEFAULT 0,
                    source_type TEXT NOT NULL DEFAULT 'parsing',
                    visibility_on_site TEXT NOT NULL DEFAULT 'hidden',
                    indexing_status TEXT NOT NULL DEFAULT 'noindex',
                    status TEXT NOT NULL DEFAULT 'draft',
                    seo_score INTEGER NOT NULL DEFAULT 0,
                    h1 TEXT,
                    seo_text TEXT,
                    canonical TEXT,
                    robots TEXT,
                    og_title TEXT,
                    og_description TEXT,
                    og_image TEXT,
                    slug TEXT,
                    faq TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (shop_id, article)
                )",
                [],
            )?;
            ensure_columns(conn)?;
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl ShopProductRepository for SqliteShopProductRepository {
    async fn list_by_shop(&self, shop_id: Uuid) -> anyhow::Result<Vec<ShopProduct>> {
        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT shop_id, article, title, description, price, images, available, site_category_id,
                            recommend_mode, recommended_articles, is_hit, source_type, visibility_on_site,
                            indexing_status, status, seo_score, internal_product_id, h1, seo_text, canonical, robots,
                            og_title, og_description, og_image, slug, faq, created_at, updated_at
                     FROM shop_product WHERE shop_id = ?1 ORDER BY updated_at DESC",
                )?;
                let items = stmt
                    .query_map([shop_id.to_string()], |row| {
                        let shop_id: String = row.get(0)?;
                        let article: String = row.get(1)?;
                        let title: Option<String> = row.get(2)?;
                        let description: Option<String> = row.get(3)?;
                        let price: Option<i64> = row.get(4)?;
                        let images: Option<String> = row.get(5)?;
                        let available: Option<u8> = row.get(6)?;
                        let site_category_id: Option<String> = row.get(7)?;
                        let recommend_mode: String = row.get(8)?;
                        let recommended_articles: Option<String> = row.get(9)?;
                        let is_hit: i64 = row.get(10).unwrap_or(0);
                        let source_type: String =
                            row.get(11).unwrap_or_else(|_| "parsing".to_string());
                        let visibility_on_site: String =
                            row.get(12).unwrap_or_else(|_| "hidden".to_string());
                        let indexing_status: String =
                            row.get(13).unwrap_or_else(|_| "noindex".to_string());
                        let status: String = row.get(14).unwrap_or_else(|_| "draft".to_string());
                        let seo_score: i64 = row.get(15).unwrap_or(0);
                        let internal_product_id: Option<String> = row.get(16).ok();
                        let h1: Option<String> = row.get(17).ok();
                        let seo_text: Option<String> = row.get(18).ok();
                        let canonical: Option<String> = row.get(19).ok();
                        let robots: Option<String> = row.get(20).ok();
                        let og_title: Option<String> = row.get(21).ok();
                        let og_description: Option<String> = row.get(22).ok();
                        let og_image: Option<String> = row.get(23).ok();
                        let slug: Option<String> = row.get(24).ok();
                        let faq: Option<String> = row.get(25).ok();
                        let created_at: i64 = row.get(26)?;
                        let updated_at: i64 = row.get(27)?;
                        let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                        let site_category_id = site_category_id
                            .and_then(|s| Uuid::parse_str(&s).ok());
                        let created_at = OffsetDateTime::from_unix_timestamp(created_at).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                        let updated_at = OffsetDateTime::from_unix_timestamp(updated_at).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                        Ok(ShopProduct {
                            shop_id,
                            article,
                            title,
                            description,
                            price: price.map(|p| p.max(0) as usize),
                            images: ShopProduct::images_from_db(images),
                            available: available.map(Into::into),
                            site_category_id,
                            recommend_mode: RecommendMode::from_str(&recommend_mode),
                            recommended_articles: ShopProduct::articles_from_db(recommended_articles),
                            is_hit: is_hit != 0,
                            source_type: SourceType::from_str(&source_type),
                            visibility_on_site: Visibility::from_str(&visibility_on_site),
                            indexing_status: IndexingStatus::from_str(&indexing_status),
                            status: ProductStatus::from_str(&status),
                            seo_score: seo_score as i32,
                            internal_product_id: internal_product_id.and_then(|s| Uuid::parse_str(&s).ok()).unwrap_or_else(Uuid::new_v4),
                            h1,
                            seo_text,
                            canonical,
                            robots,
                            og_title,
                            og_description,
                            og_image,
                            slug,
                            faq,
                            created_at,
                            updated_at,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await
            .context("Unable to list shop products")
    }

    async fn get(&self, shop_id: Uuid, article: &str) -> anyhow::Result<Option<ShopProduct>> {
        let article = article.to_string();
        self.conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT shop_id, article, title, description, price, images, available, site_category_id,
                            recommend_mode, recommended_articles, is_hit, source_type, visibility_on_site,
                            indexing_status, status, seo_score, internal_product_id, h1, seo_text, canonical, robots,
                            og_title, og_description, og_image, slug, faq, created_at, updated_at
                     FROM shop_product WHERE shop_id = ?1 AND article = ?2",
                )?;
                let mut rows = stmt.query([shop_id.to_string(), article])?;
                let row = match rows.next()? {
                    Some(r) => r,
                    None => return Ok(None),
                };

                let shop_id: String = row.get(0)?;
                let article: String = row.get(1)?;
                let title: Option<String> = row.get(2)?;
                let description: Option<String> = row.get(3)?;
                let price: Option<i64> = row.get(4)?;
                let images: Option<String> = row.get(5)?;
                let available: Option<u8> = row.get(6)?;
                let site_category_id: Option<String> = row.get(7)?;
                let recommend_mode: String = row.get(8)?;
                let recommended_articles: Option<String> = row.get(9)?;
                let is_hit: i64 = row.get(10).unwrap_or(0);
                let source_type: String = row.get(11).unwrap_or_else(|_| "parsing".to_string());
                let visibility_on_site: String =
                    row.get(12).unwrap_or_else(|_| "hidden".to_string());
                let indexing_status: String =
                    row.get(13).unwrap_or_else(|_| "noindex".to_string());
                let status: String = row.get(14).unwrap_or_else(|_| "draft".to_string());
                let seo_score: i64 = row.get(15).unwrap_or(0);
                let internal_product_id: Option<String> = row.get(16).ok();
                let h1: Option<String> = row.get(17).ok();
                let seo_text: Option<String> = row.get(18).ok();
                let canonical: Option<String> = row.get(19).ok();
                let robots: Option<String> = row.get(20).ok();
                let og_title: Option<String> = row.get(21).ok();
                let og_description: Option<String> = row.get(22).ok();
                let og_image: Option<String> = row.get(23).ok();
                let slug: Option<String> = row.get(24).ok();
                let faq: Option<String> = row.get(25).ok();
                let created_at: i64 = row.get(26)?;
                let updated_at: i64 = row.get(27)?;

                let shop_id = Uuid::parse_str(&shop_id).unwrap_or(Uuid::nil());
                let site_category_id = site_category_id.and_then(|s| Uuid::parse_str(&s).ok());
                let created_at =
                    OffsetDateTime::from_unix_timestamp(created_at).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                let updated_at =
                    OffsetDateTime::from_unix_timestamp(updated_at).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                Ok(Some(ShopProduct {
                    shop_id,
                    article,
                    title,
                    description,
                    price: price.map(|p| p.max(0) as usize),
                    images: ShopProduct::images_from_db(images),
                    available: available.map(Into::into),
                    site_category_id,
                    recommend_mode: RecommendMode::from_str(&recommend_mode),
                    recommended_articles: ShopProduct::articles_from_db(recommended_articles),
                    is_hit: is_hit != 0,
                    source_type: SourceType::from_str(&source_type),
                    visibility_on_site: Visibility::from_str(&visibility_on_site),
                    indexing_status: IndexingStatus::from_str(&indexing_status),
                    status: ProductStatus::from_str(&status),
                    seo_score: seo_score as i32,
                    internal_product_id: internal_product_id.and_then(|s| Uuid::parse_str(&s).ok()).unwrap_or_else(Uuid::new_v4),
                    h1,
                    seo_text,
                    canonical,
                    robots,
                    og_title,
                    og_description,
                    og_image,
                    slug,
                    faq,
                    created_at,
                    updated_at,
                }))
            })
            .await
            .context("Unable to get shop product")
    }

    async fn upsert(&self, product: ShopProduct) -> anyhow::Result<()> {
        self.conn
            .call(move |conn| {
                let now = OffsetDateTime::now_utc().unix_timestamp();
                let created_at = product.created_at.unix_timestamp();
                // Імпортним джерелам не дозволяємо змінювати SEO поля та статуси публікації.
                let sanitize = |mut p: ShopProduct| {
                    if !matches!(p.source_type, SourceType::Manual) {
                        p.faq = None;
                        p.h1 = None;
                        p.seo_text = None;
                        p.canonical = None;
                        p.robots = None;
                        p.og_title = None;
                        p.og_description = None;
                        p.og_image = None;
                        p.slug = None;
                        p.status = ProductStatus::Draft;
                        p.visibility_on_site = Visibility::Hidden;
                        p.indexing_status = IndexingStatus::NoIndex;
                        p.seo_score = 0;
                    }
                    p
                };
                let product = sanitize(product);
                let images = ShopProduct::images_to_db(&product.images);
                let available = product.available.map(|a| a as u8);
                let site_category_id = product.site_category_id.map(|u| u.to_string());
                let recommended_articles = ShopProduct::articles_to_db(&product.recommended_articles);
                conn.execute(
                    "INSERT INTO shop_product
                        (shop_id, article, title, description, price, images, available, site_category_id,
                         recommend_mode, recommended_articles, is_hit, source_type, visibility_on_site,
                         indexing_status, status, seo_score, internal_product_id, h1, seo_text, canonical, robots,
                         og_title, og_description, og_image, slug, faq, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28)
                     ON CONFLICT(shop_id, article) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        price = excluded.price,
                        images = excluded.images,
                        available = excluded.available,
                        site_category_id = excluded.site_category_id,
                        recommend_mode = excluded.recommend_mode,
                        recommended_articles = excluded.recommended_articles,
                        is_hit = excluded.is_hit,
                        source_type = excluded.source_type,
                        visibility_on_site = excluded.visibility_on_site,
                        indexing_status = excluded.indexing_status,
                        status = excluded.status,
                        seo_score = excluded.seo_score,
                        internal_product_id = excluded.internal_product_id,
                        h1 = excluded.h1,
                        seo_text = excluded.seo_text,
                        canonical = excluded.canonical,
                        robots = excluded.robots,
                        og_title = excluded.og_title,
                        og_description = excluded.og_description,
                        og_image = excluded.og_image,
                        slug = excluded.slug,
                        faq = excluded.faq,
                        updated_at = excluded.updated_at",
                    rusqlite::params![
                        product.shop_id.to_string(),
                        product.article,
                        product.title,
                        product.description,
                        product.price.map(|p| p as i64),
                        images,
                        available,
                        site_category_id,
                        product.recommend_mode.as_str(),
                        recommended_articles,
                        if product.is_hit { 1i64 } else { 0i64 },
                        product.source_type.as_str(),
                        product.visibility_on_site.as_str(),
                        product.indexing_status.as_str(),
                        product.status.as_str(),
                        product.seo_score as i64,
                        product.internal_product_id.to_string(),
                        product.h1,
                        product.seo_text,
                        product.canonical,
                        product.robots,
                        product.og_title,
                        product.og_description,
                        product.og_image,
                        product.slug,
                        product.faq,
                        created_at.max(0),
                        now.max(0),
                    ],
                )?;
                Ok(())
            })
            .await
            .context("Unable to upsert shop product")
    }

    async fn ensure_exists(&self, shop_id: Uuid, article: &str) -> anyhow::Result<()> {
        let article = article.to_string();
        self.conn
            .call(move |conn| {
                let now = OffsetDateTime::now_utc().unix_timestamp().max(0);
                let internal_id = Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT OR IGNORE INTO shop_product (shop_id, article, recommend_mode, internal_product_id, created_at, updated_at)
                     VALUES (?1, ?2, 'auto', ?3, ?4, ?4)",
                    rusqlite::params![shop_id.to_string(), article, internal_id, now],
                )?;
                Ok(())
            })
            .await
            .context("Unable to ensure shop product exists")
    }

    async fn set_site_category(
        &self,
        shop_id: Uuid,
        article: &str,
        category_id: Option<Uuid>,
    ) -> anyhow::Result<()> {
        let article = article.to_string();
        self.conn
            .call(move |conn| {
                let now = OffsetDateTime::now_utc().unix_timestamp().max(0);
                conn.execute(
                    "INSERT INTO shop_product (shop_id, article, site_category_id, recommend_mode, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 'auto', ?4, ?4)
                     ON CONFLICT(shop_id, article)
                     DO UPDATE SET site_category_id=excluded.site_category_id, updated_at=excluded.updated_at",
                    rusqlite::params![
                        shop_id.to_string(),
                        article,
                        category_id.map(|c| c.to_string()),
                        now
                    ],
                )?;
                Ok(())
            })
            .await
            .context("Unable to set site category for product")
    }

    async fn bulk_set_visibility(
        &self,
        shop_id: Uuid,
        articles: &[String],
        visibility: Visibility,
        indexing_status: IndexingStatus,
        status: ProductStatus,
        robots: Option<String>,
        source_type: SourceType,
        ensure_missing: bool,
    ) -> anyhow::Result<usize> {
        if articles.is_empty() {
            return Ok(0);
        }
        let shop_id = shop_id.to_string();
        let articles = articles.to_vec();
        let visibility = visibility.as_str().to_string();
        let indexing_status = indexing_status.as_str().to_string();
        let status = status.as_str().to_string();
        let source_type = source_type.as_str().to_string();
        let robots = robots;
        let now = OffsetDateTime::now_utc().unix_timestamp().max(0);
        let chunk_size = 400usize;
        let mut updated = 0usize;

        if ensure_missing {
            for chunk in articles.chunks(chunk_size) {
                let shop_id = shop_id.clone();
                let chunk = chunk.to_vec();
                self.conn
                    .call(move |conn| {
                        let tx = conn.transaction()?;
                        {
                            let mut insert = tx.prepare(
                                "INSERT OR IGNORE INTO shop_product
                                    (shop_id, article, internal_product_id, created_at, updated_at)
                                 VALUES (?1, ?2, ?3, ?4, ?5)",
                            )?;
                            for article in chunk.iter() {
                                let internal_id = Uuid::new_v4().to_string();
                                insert.execute(rusqlite::params![
                                    &shop_id,
                                    article,
                                    internal_id,
                                    now,
                                    now
                                ])?;
                            }
                        }
                        tx.commit()?;
                        Ok(())
                    })
                    .await
                    .context("Unable to ensure shop products exist")?;
                tokio::task::yield_now().await;
            }
        }

        for chunk in articles.chunks(chunk_size) {
            let shop_id = shop_id.clone();
            let visibility = visibility.clone();
            let indexing_status = indexing_status.clone();
            let status = status.clone();
            let source_type = source_type.clone();
            let robots = robots.clone();
            let chunk = chunk.to_vec();
            let count = self
                .conn
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    let mut sql = String::from(
                        "UPDATE shop_product
                         SET visibility_on_site = ?,
                             indexing_status = ?,
                             status = ?,
                             robots = ?,
                             source_type = ?,
                             updated_at = ?
                         WHERE shop_id = ? AND article IN (",
                    );
                    for (idx, _) in chunk.iter().enumerate() {
                        if idx > 0 {
                            sql.push_str(", ");
                        }
                        sql.push('?');
                    }
                    sql.push(')');

                    let mut params: Vec<rusqlite::types::Value> =
                        Vec::with_capacity(7 + chunk.len());
                    params.push(visibility.into());
                    params.push(indexing_status.into());
                    params.push(status.into());
                    params.push(
                        robots
                            .as_ref()
                            .map(|r| r.clone().into())
                            .unwrap_or(rusqlite::types::Value::Null),
                    );
                    params.push(source_type.into());
                    params.push((now as i64).into());
                    params.push(shop_id.into());
                    for article in chunk.iter() {
                        params.push(article.clone().into());
                    }
                    let count = tx.execute(&sql, rusqlite::params_from_iter(params))?;
                    tx.commit()?;
                    Ok(count)
                })
                .await
                .context("Unable to bulk update shop products")?;
            updated += count;
            tokio::task::yield_now().await;
        }

        Ok(updated)
    }

    async fn bulk_set_hit(
        &self,
        shop_id: Uuid,
        articles: &[String],
        is_hit: bool,
        ensure_missing: bool,
    ) -> anyhow::Result<usize> {
        if articles.is_empty() {
            return Ok(0);
        }
        let shop_id = shop_id.to_string();
        let articles = articles.to_vec();
        let hit_value = if is_hit { 1i64 } else { 0i64 };
        let now = OffsetDateTime::now_utc().unix_timestamp().max(0);
        let chunk_size = 400usize;
        let mut updated = 0usize;

        if ensure_missing {
            for chunk in articles.chunks(chunk_size) {
                let shop_id = shop_id.clone();
                let chunk = chunk.to_vec();
                self.conn
                    .call(move |conn| {
                        let tx = conn.transaction()?;
                        {
                            let mut insert = tx.prepare(
                                "INSERT OR IGNORE INTO shop_product
                                    (shop_id, article, is_hit, internal_product_id, created_at, updated_at)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                            )?;
                            for article in chunk.iter() {
                                let internal_id = Uuid::new_v4().to_string();
                                insert.execute(rusqlite::params![
                                    &shop_id,
                                    article,
                                    hit_value,
                                    internal_id,
                                    now
                                ])?;
                            }
                        }
                        tx.commit()?;
                        Ok(())
                    })
                    .await
                    .context("Unable to ensure shop products exist for hit update")?;
                tokio::task::yield_now().await;
            }
        }

        for chunk in articles.chunks(chunk_size) {
            let shop_id = shop_id.clone();
            let chunk = chunk.to_vec();
            let count = self
                .conn
                .call(move |conn| {
                    let tx = conn.transaction()?;
                    let mut sql = String::from(
                        "UPDATE shop_product
                         SET is_hit = ?,
                             updated_at = ?
                         WHERE shop_id = ? AND article IN (",
                    );
                    for (idx, _) in chunk.iter().enumerate() {
                        if idx > 0 {
                            sql.push_str(", ");
                        }
                        sql.push('?');
                    }
                    sql.push(')');

                    let mut params: Vec<rusqlite::types::Value> =
                        Vec::with_capacity(3 + chunk.len());
                    params.push(hit_value.into());
                    params.push((now as i64).into());
                    params.push(shop_id.into());
                    for article in chunk.iter() {
                        params.push(article.clone().into());
                    }
                    let count = tx.execute(&sql, rusqlite::params_from_iter(params))?;
                    tx.commit()?;
                    Ok(count)
                })
                .await
                .context("Unable to bulk update hit status")?;
            updated += count;
            tokio::task::yield_now().await;
        }

        Ok(updated)
    }

    async fn remove(&self, shop_id: Uuid, article: &str) -> anyhow::Result<()> {
        let article = article.to_string();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM shop_product WHERE shop_id = ?1 AND article = ?2",
                    rusqlite::params![shop_id.to_string(), article],
                )?;
                Ok(())
            })
            .await
            .context("Unable to remove shop product")
    }

    async fn remove_many(&self, shop_id: Uuid, articles: &[String]) -> anyhow::Result<()> {
        if articles.is_empty() {
            return Ok(());
        }
        let shop_id_str = shop_id.to_string();
        let list = articles.to_owned();
        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                for a in list.iter() {
                    let _ = tx.execute(
                        "DELETE FROM shop_product WHERE shop_id = ?1 AND article = ?2",
                        rusqlite::params![shop_id_str, a],
                    );
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .context("Unable to bulk remove shop products")
    }
}
