use async_trait::async_trait;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio_rusqlite::Connection;
use typesafe_repository::async_ops::{Get, Remove, Save, Select};
use typesafe_repository::macros::Id;
use typesafe_repository::prelude::*;
use typesafe_repository::{IdentityOf, SelectBy, Selector};
use uuid::Uuid;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeoPageType {
    TuningModel,
    AccessoriesCar,
    HowToChoose,
}

impl SeoPageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SeoPageType::TuningModel => "tuning_model",
            SeoPageType::AccessoriesCar => "accessories_car",
            SeoPageType::HowToChoose => "how_to_choose",
        }
    }

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "accessories_car" => SeoPageType::AccessoriesCar,
            "how_to_choose" => SeoPageType::HowToChoose,
            _ => SeoPageType::TuningModel,
        }
    }

    pub fn from_path_segment(input: &str) -> Option<Self> {
        match input.trim().to_lowercase().as_str() {
            "tuning" => Some(SeoPageType::TuningModel),
            "accessories" => Some(SeoPageType::AccessoriesCar),
            "guides" => Some(SeoPageType::HowToChoose),
            _ => None,
        }
    }

    pub fn path_segment(&self) -> &'static str {
        match self {
            SeoPageType::TuningModel => "tuning",
            SeoPageType::AccessoriesCar => "accessories",
            SeoPageType::HowToChoose => "guides",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeoPageStatus {
    Draft,
    Published,
}

impl SeoPageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SeoPageStatus::Draft => "draft",
            SeoPageStatus::Published => "published",
        }
    }

    pub fn from_str(input: &str) -> Self {
        match input.trim().to_lowercase().as_str() {
            "published" => SeoPageStatus::Published,
            _ => SeoPageStatus::Draft,
        }
    }
}

#[derive(Clone, Debug, Id)]
#[Id(ref_id, get_id)]
pub struct SeoPage {
    #[id]
    pub id: Uuid,
    pub shop_id: Uuid,
    pub page_type: SeoPageType,
    pub slug: String,
    pub title: String,
    pub h1: Option<String>,
    pub meta_title: Option<String>,
    pub meta_description: Option<String>,
    pub seo_text: Option<String>,
    pub seo_text_auto: bool,
    pub faq: Option<String>,
    pub robots: Option<String>,
    pub status: SeoPageStatus,
    pub source_payload: Option<String>,
    pub related_links: Vec<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl SeoPage {
    pub fn path(&self) -> String {
        format!("/{}/{}", self.page_type.path_segment(), self.slug)
    }

    pub fn related_links_to_db(list: &[String]) -> Option<String> {
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

    pub fn related_links_from_db(list: Option<String>) -> Vec<String> {
        list.unwrap_or_default()
            .split(',')
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct SeoPageSlug {
    pub id: Uuid,
    pub page_id: Uuid,
    pub shop_id: Uuid,
    pub slug: String,
    pub created_at: OffsetDateTime,
}

pub struct ByShop(pub Uuid);

impl Selector for ByShop {}
impl SelectBy<ByShop> for SeoPage {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SeoPagePayload {
    pub brand: Option<String>,
    pub model: Option<String>,
    pub car: Option<String>,
    pub category: Option<String>,
    pub topic: Option<String>,
    pub brand_slug: Option<String>,
    pub model_slug: Option<String>,
    pub category_slug: Option<String>,
}

fn read_uuid(row: &rusqlite::Row, idx: usize) -> Result<Uuid, rusqlite::Error> {
    use rusqlite::types::{FromSqlError, Type, ValueRef};
    match row.get_ref(idx)? {
        ValueRef::Blob(bytes) => Uuid::from_slice(bytes).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(idx, Type::Blob, Box::new(err))
        }),
        ValueRef::Text(text) => {
            let text = std::str::from_utf8(text).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(idx, Type::Text, Box::new(err))
            })?;
            Uuid::from_str(text).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(idx, Type::Text, Box::new(err))
            })
        }
        ValueRef::Null => Err(rusqlite::Error::FromSqlConversionFailure(
            idx,
            Type::Null,
            Box::new(FromSqlError::Other("NULL uuid".into())),
        )),
        ValueRef::Integer(_) | ValueRef::Real(_) => Err(rusqlite::Error::FromSqlConversionFailure(
            idx,
            Type::Integer,
            Box::new(FromSqlError::Other("Invalid uuid type".into())),
        )),
    }
}

impl SeoPagePayload {
    pub fn from_json(input: Option<&str>) -> Self {
        input
            .and_then(|s| serde_json::from_str::<SeoPagePayload>(s).ok())
            .unwrap_or_default()
    }
}

pub struct GeneratedSeo {
    pub title: String,
    pub h1: String,
    pub meta_title: String,
    pub meta_description: String,
    pub seo_text: String,
}

pub fn generate_from_template(page_type: &SeoPageType, payload: &SeoPagePayload) -> GeneratedSeo {
    let brand_model = [payload.brand.as_deref(), payload.model.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    let car = payload.car.clone().unwrap_or_else(|| brand_model.clone());
    let topic = payload
        .topic
        .clone()
        .unwrap_or_else(|| "тюнінг".to_string());

    match page_type {
        SeoPageType::TuningModel => {
            let title = format!("Тюнінг {brand_model}");
            let h1 = title.clone();
            let meta_title = format!("Тюнінг {brand_model} — обвіс, спойлери, дифузори");
            let meta_description = format!(
                "Тюнінг {brand_model}: спліттери, дифузори, пороги, решітки та інші елементи. Актуальні ціни, фото і швидка доставка."
            );
            let seo_text = format!(
                "Тюнінг {brand_model} — це підбір обвісів та аеродинаміки, які підкреслюють стиль авто й покращують вигляд. \
Ми зібрали рішення для щоденної їзди та шоу‑проєктів: спліттери, дифузори, пороги, спойлери, решітки та елементи захисту. \
Каталог оновлюється, тому ви бачите актуальні позиції з реальними фото та цінами.\n\n\
Під час вибору тюнінгу для {brand_model} звертайте увагу на сумісність з комплектацією, рік випуску та тип кузова. \
Якщо потрібна допомога — ми підкажемо оптимальний варіант, комплектацію та строки постачання. \
Усі товари на сторінці відповідають конкретній моделі, що спрощує вибір і зменшує ризик помилок.",
            );
            let seo_text = format!(
                "{seo_text}\n\n\
Якщо ви плануєте встановлення, підберемо рішення з урахуванням матеріалів, типу кріплення та бажаного ефекту. \
Для {brand_model} доступні як базові елементи, так і розширені комплекти тюнінгу — \
це дозволяє зібрати індивідуальну конфігурацію без зайвих витрат."
            );
            GeneratedSeo {
                title,
                h1,
                meta_title,
                meta_description,
                seo_text,
            }
        }
        SeoPageType::AccessoriesCar => {
            let title = format!("Аксесуари для {car}");
            let h1 = title.clone();
            let meta_title = format!("Аксесуари для {car} — підбір і ціни");
            let meta_description = format!(
                "Аксесуари для {car}: обвіс, захист, декоративні елементи та корисні доповнення. Зручно підібрані товари для вашого авто."
            );
            let seo_text = format!(
                "Аксесуари для {car} — це практичні та стильні доповнення, які роблять автомобіль більш зручним та виразним. \
На сторінці зібрані позиції для {car}: накладки, захист, декоративні елементи й аксесуари салону. \
Усі товари підібрані з урахуванням сумісності та реальних розмірів.\n\n\
Якщо ви плануєте оновити зовнішній вигляд або додати корисні деталі — обирайте категорію та дивіться варіанти з фото. \
Ми підкажемо, які аксесуари підходять саме під вашу комплектацію та організуємо доставку у зручні терміни.\n\n\
Додатково можна підібрати аксесуари під сезон або стиль експлуатації: \
від захисту й накладок до декоративних елементів, які підкреслюють індивідуальність автомобіля.",
            );
            GeneratedSeo {
                title,
                h1,
                meta_title,
                meta_description,
                seo_text,
            }
        }
        SeoPageType::HowToChoose => {
            let title = format!("Як обрати {topic}");
            let h1 = title.clone();
            let meta_title = format!("Як обрати {topic} — поради та критерії");
            let meta_description = format!(
                "Як обрати {topic}: ключові критерії, сумісність, матеріали та поради щодо монтажу. Практичний гайд для покупки."
            );
            let seo_text = format!(
                "Вибір {topic} — це не лише про дизайн, а й про сумісність, якість та безпеку. \
Перед покупкою важливо перевірити точну модель авто, рік випуску та тип кузова. \
Також звертайте увагу на матеріали, геометрію та спосіб кріплення.\n\n\
У цьому гіді ми пояснюємо, як порівнювати різні варіанти {topic}, на що впливає форма, \
як підібрати оптимальне рішення за бюджетом і як уникнути невідповідностей. \
Якщо потрібна консультація — ми допоможемо підібрати правильний варіант.\n\n\
Окремо зверніть увагу на сумісність з комплектацією та брендом виробника — \
це зменшує ризик невдалого монтажу та економить час. \
Гайд оновлюється разом із каталогом, тому ви отримуєте актуальні рекомендації.",
            );
            GeneratedSeo {
                title,
                h1,
                meta_title,
                meta_description,
                seo_text,
            }
        }
    }
}

pub fn slugify_latin(input: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in input.to_lowercase().chars() {
        let mapped = match ch {
            'а' => "a",
            'б' => "b",
            'в' => "v",
            'г' => "h",
            'ґ' => "g",
            'д' => "d",
            'е' => "e",
            'є' => "ie",
            'ж' => "zh",
            'з' => "z",
            'и' => "y",
            'і' => "i",
            'ї' => "i",
            'й' => "i",
            'к' => "k",
            'л' => "l",
            'м' => "m",
            'н' => "n",
            'о' => "o",
            'п' => "p",
            'р' => "r",
            'с' => "s",
            'т' => "t",
            'у' => "u",
            'ф' => "f",
            'х' => "kh",
            'ц' => "ts",
            'ч' => "ch",
            'ш' => "sh",
            'щ' => "shch",
            'ю' => "iu",
            'я' => "ia",
            'ь' | 'ъ' => "",
            'ы' => "y",
            'э' => "e",
            _ => "",
        };
        if !mapped.is_empty() {
            out.push_str(mapped);
            prev_dash = false;
            continue;
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            prev_dash = false;
            continue;
        }
        if ch.is_whitespace() || ch == '-' || ch == '_' {
            if !prev_dash && !out.is_empty() {
                out.push('-');
                prev_dash = true;
            }
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

pub fn build_auto_slug(page_type: &SeoPageType, payload: &SeoPagePayload) -> String {
    let base = match page_type {
        SeoPageType::TuningModel => {
            let brand = payload.brand.clone().unwrap_or_default();
            let model = payload.model.clone().unwrap_or_default();
            format!("{brand} {model}")
        }
        SeoPageType::AccessoriesCar => payload
            .car
            .clone()
            .or_else(|| {
                let brand = payload.brand.clone().unwrap_or_default();
                let model = payload.model.clone().unwrap_or_default();
                let combined = format!("{brand} {model}");
                if combined.trim().is_empty() {
                    None
                } else {
                    Some(combined)
                }
            })
            .unwrap_or_default(),
        SeoPageType::HowToChoose => payload.topic.clone().unwrap_or_default(),
    };
    slugify_latin(&base)
}

pub fn seo_page_indexable(
    page_type: &SeoPageType,
    status: &SeoPageStatus,
    meta_title: &Option<String>,
    meta_description: &Option<String>,
    seo_text: &Option<String>,
    product_count: usize,
) -> bool {
    if !matches!(status, SeoPageStatus::Published) {
        return false;
    }
    let seo_len = seo_text
        .as_deref()
        .map(|s| s.chars().count())
        .unwrap_or(0);
    if seo_len < 500 {
        return false;
    }
    if meta_title.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
        return false;
    }
    if meta_description
        .as_ref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        return false;
    }
    if matches!(page_type, SeoPageType::HowToChoose) {
        return true;
    }
    product_count > 0
}

#[async_trait]
pub trait SeoPageRepository:
    Repository<SeoPage, Error = anyhow::Error>
    + Save<SeoPage>
    + Get<SeoPage>
    + Select<SeoPage, ByShop>
    + Remove<SeoPage>
    + Send
    + Sync
{
    async fn get_by_slug(
        &self,
        shop_id: Uuid,
        slug: &str,
    ) -> Result<Option<SeoPage>, Self::Error>;
    async fn get_slug_history(
        &self,
        shop_id: Uuid,
        slug: &str,
    ) -> Result<Option<SeoPageSlug>, Self::Error>;
    async fn list_slug_history(
        &self,
        page_id: Uuid,
    ) -> Result<Vec<SeoPageSlug>, Self::Error>;
    async fn insert_slug_history(&self, entry: SeoPageSlug) -> Result<(), Self::Error>;
    async fn slug_exists(
        &self,
        shop_id: Uuid,
        slug: &str,
        exclude_page: Option<Uuid>,
    ) -> Result<bool, Self::Error>;
}

pub struct SqliteSeoPageRepository {
    conn: Connection,
}

impl SqliteSeoPageRepository {
    pub async fn init(conn: Connection) -> Result<Self, tokio_rusqlite::Error> {
        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS seo_page (
                    id BLOB PRIMARY KEY,
                    shop_id BLOB NOT NULL,
                    page_type TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    title TEXT NOT NULL,
                    h1 TEXT,
                    meta_title TEXT,
                    meta_description TEXT,
                    seo_text TEXT,
                    seo_text_auto INTEGER NOT NULL DEFAULT 1,
                    faq TEXT,
                    robots TEXT,
                    status TEXT NOT NULL DEFAULT 'draft',
                    source_payload TEXT,
                    related_links TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )",
                [],
            )?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS seo_page_slugs (
                    id BLOB PRIMARY KEY,
                    page_id BLOB NOT NULL,
                    shop_id BLOB NOT NULL,
                    slug TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )",
                [],
            )?;
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN h1 TEXT", []);
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN meta_title TEXT", []);
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN meta_description TEXT", []);
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN seo_text TEXT", []);
            let _ = conn.execute(
                "ALTER TABLE seo_page ADD COLUMN seo_text_auto INTEGER NOT NULL DEFAULT 1",
                [],
            );
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN faq TEXT", []);
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN robots TEXT", []);
            let _ = conn.execute(
                "ALTER TABLE seo_page ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'",
                [],
            );
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN source_payload TEXT", []);
            let _ = conn.execute("ALTER TABLE seo_page ADD COLUMN related_links TEXT", []);
            let _ = conn.execute(
                "ALTER TABLE seo_page ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE seo_page ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
                [],
            );
            Ok(())
        })
        .await?;
        Ok(Self { conn })
    }
}

impl Repository<SeoPage> for SqliteSeoPageRepository {
    type Error = anyhow::Error;
}

#[async_trait]
impl Select<SeoPage, ByShop> for SqliteSeoPageRepository {
    async fn select(&self, ByShop(shop_id): &ByShop) -> Result<Vec<SeoPage>, Self::Error> {
        let shop_id = *shop_id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, shop_id, page_type, slug, title, h1, meta_title, meta_description, seo_text,
                            seo_text_auto, faq, robots, status, source_payload, related_links, created_at, updated_at
                     FROM seo_page WHERE shop_id = ?1 ORDER BY updated_at DESC",
                )?;
                let items = stmt
                    .query_map([shop_id], |row| {
                        let created_at: i64 = row.get(15)?;
                        let updated_at: i64 = row.get(16)?;
                        Ok(SeoPage {
                            id: read_uuid(row, 0)?,
                            shop_id: read_uuid(row, 1)?,
                            page_type: SeoPageType::from_str(row.get::<_, String>(2)?.as_str()),
                            slug: row.get(3)?,
                            title: row.get(4)?,
                            h1: row.get(5)?,
                            meta_title: row.get(6)?,
                            meta_description: row.get(7)?,
                            seo_text: row.get(8)?,
                            seo_text_auto: row.get::<_, i64>(9)? != 0,
                            faq: row.get(10)?,
                            robots: row.get(11)?,
                            status: SeoPageStatus::from_str(row.get::<_, String>(12)?.as_str()),
                            source_payload: row.get(13)?,
                            related_links: SeoPage::related_links_from_db(row.get(14)?),
                            created_at: OffsetDateTime::from_unix_timestamp(created_at)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                            updated_at: OffsetDateTime::from_unix_timestamp(updated_at)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await?)
    }
}

#[async_trait]
impl Get<SeoPage> for SqliteSeoPageRepository {
    async fn get_one(&self, id: &IdentityOf<SeoPage>) -> Result<Option<SeoPage>, Self::Error> {
        let id = *id;
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, shop_id, page_type, slug, title, h1, meta_title, meta_description, seo_text,
                            seo_text_auto, faq, robots, status, source_payload, related_links, created_at, updated_at
                     FROM seo_page WHERE id = ?1",
                )?;
                let item = stmt
                    .query_map([id], |row| {
                        let created_at: i64 = row.get(15)?;
                        let updated_at: i64 = row.get(16)?;
                        Ok(SeoPage {
                            id: read_uuid(row, 0)?,
                            shop_id: read_uuid(row, 1)?,
                            page_type: SeoPageType::from_str(row.get::<_, String>(2)?.as_str()),
                            slug: row.get(3)?,
                            title: row.get(4)?,
                            h1: row.get(5)?,
                            meta_title: row.get(6)?,
                            meta_description: row.get(7)?,
                            seo_text: row.get(8)?,
                            seo_text_auto: row.get::<_, i64>(9)? != 0,
                            faq: row.get(10)?,
                            robots: row.get(11)?,
                            status: SeoPageStatus::from_str(row.get::<_, String>(12)?.as_str()),
                            source_payload: row.get(13)?,
                            related_links: SeoPage::related_links_from_db(row.get(14)?),
                            created_at: OffsetDateTime::from_unix_timestamp(created_at)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                            updated_at: OffsetDateTime::from_unix_timestamp(updated_at)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        })
                    })?
                    .next()
                    .transpose()?;
                Ok(item)
            })
            .await?)
    }
}

#[async_trait]
impl Save<SeoPage> for SqliteSeoPageRepository {
    async fn save(&self, page: SeoPage) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let created_at = page.created_at.unix_timestamp();
                let updated_at = page.updated_at.unix_timestamp();
                let related = SeoPage::related_links_to_db(&page.related_links);
                conn.execute(
                    "INSERT INTO seo_page (id, shop_id, page_type, slug, title, h1, meta_title, meta_description, seo_text,
                        seo_text_auto, faq, robots, status, source_payload, related_links, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                     ON CONFLICT(id) DO UPDATE SET shop_id=?2, page_type=?3, slug=?4, title=?5, h1=?6, meta_title=?7,
                        meta_description=?8, seo_text=?9, seo_text_auto=?10, faq=?11, robots=?12, status=?13,
                        source_payload=?14, related_links=?15, created_at=?16, updated_at=?17",
                    params![
                        page.id,
                        page.shop_id,
                        page.page_type.as_str(),
                        page.slug,
                        page.title,
                        page.h1,
                        page.meta_title,
                        page.meta_description,
                        page.seo_text,
                        if page.seo_text_auto { 1 } else { 0 },
                        page.faq,
                        page.robots,
                        page.status.as_str(),
                        page.source_payload,
                        related,
                        created_at.max(0),
                        updated_at.max(0),
                    ],
                )?;
                Ok(())
            })
            .await?)
    }
}

#[async_trait]
impl Remove<SeoPage> for SqliteSeoPageRepository {
    async fn remove(&self, id: &IdentityOf<SeoPage>) -> Result<(), Self::Error> {
        let id = *id;
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM seo_page WHERE id = ?1", params![id])?;
                conn.execute("DELETE FROM seo_page_slugs WHERE page_id = ?1", params![id])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SeoPageRepository for SqliteSeoPageRepository {
    async fn get_by_slug(
        &self,
        shop_id: Uuid,
        slug: &str,
    ) -> Result<Option<SeoPage>, Self::Error> {
        let slug = slug.to_string();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, shop_id, page_type, slug, title, h1, meta_title, meta_description, seo_text,
                            seo_text_auto, faq, robots, status, source_payload, related_links, created_at, updated_at
                     FROM seo_page WHERE shop_id = ?1",
                )?;
                let mut rows = stmt.query([shop_id])?;
                while let Some(row) = rows.next()? {
                    let row_slug: String = row.get(3)?;
                    if !row_slug.eq_ignore_ascii_case(&slug) {
                        continue;
                    }
                    let created_at: i64 = row.get(15)?;
                    let updated_at: i64 = row.get(16)?;
                    let page = SeoPage {
                        id: read_uuid(row, 0)?,
                        shop_id: read_uuid(row, 1)?,
                        page_type: SeoPageType::from_str(row.get::<_, String>(2)?.as_str()),
                        slug: row_slug,
                        title: row.get(4)?,
                        h1: row.get(5)?,
                        meta_title: row.get(6)?,
                        meta_description: row.get(7)?,
                        seo_text: row.get(8)?,
                        seo_text_auto: row.get::<_, i64>(9)? != 0,
                        faq: row.get(10)?,
                        robots: row.get(11)?,
                        status: SeoPageStatus::from_str(row.get::<_, String>(12)?.as_str()),
                        source_payload: row.get(13)?,
                        related_links: SeoPage::related_links_from_db(row.get(14)?),
                        created_at: OffsetDateTime::from_unix_timestamp(created_at)
                            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        updated_at: OffsetDateTime::from_unix_timestamp(updated_at)
                            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                    };
                    return Ok(Some(page));
                }
                Ok(None)
            })
            .await?)
    }

    async fn get_slug_history(
        &self,
        shop_id: Uuid,
        slug: &str,
    ) -> Result<Option<SeoPageSlug>, Self::Error> {
        let slug = slug.to_string();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, page_id, shop_id, slug, created_at FROM seo_page_slugs WHERE shop_id = ?1",
                )?;
                let mut rows = stmt.query([shop_id])?;
                while let Some(row) = rows.next()? {
                    let row_slug: String = row.get(3)?;
                    if !row_slug.eq_ignore_ascii_case(&slug) {
                        continue;
                    }
                    let created_at: i64 = row.get(4)?;
                    return Ok(Some(SeoPageSlug {
                        id: read_uuid(row, 0)?,
                        page_id: read_uuid(row, 1)?,
                        shop_id: read_uuid(row, 2)?,
                        slug: row_slug,
                        created_at: OffsetDateTime::from_unix_timestamp(created_at)
                            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                    }));
                }
                Ok(None)
            })
            .await?)
    }

    async fn list_slug_history(
        &self,
        page_id: Uuid,
    ) -> Result<Vec<SeoPageSlug>, Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, page_id, shop_id, slug, created_at FROM seo_page_slugs WHERE page_id = ?1 ORDER BY created_at DESC",
                )?;
                let items = stmt
                    .query_map([page_id], |row| {
                        let created_at: i64 = row.get(4)?;
                        Ok(SeoPageSlug {
                            id: read_uuid(row, 0)?,
                            page_id: read_uuid(row, 1)?,
                            shop_id: read_uuid(row, 2)?,
                            slug: row.get(3)?,
                            created_at: OffsetDateTime::from_unix_timestamp(created_at)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(items)
            })
            .await?)
    }

    async fn insert_slug_history(&self, entry: SeoPageSlug) -> Result<(), Self::Error> {
        Ok(self
            .conn
            .call(move |conn| {
                let created_at = entry.created_at.unix_timestamp();
                conn.execute(
                    "INSERT INTO seo_page_slugs (id, page_id, shop_id, slug, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        entry.id,
                        entry.page_id,
                        entry.shop_id,
                        entry.slug,
                        created_at.max(0),
                    ],
                )?;
                Ok(())
            })
            .await?)
    }

    async fn slug_exists(
        &self,
        shop_id: Uuid,
        slug: &str,
        exclude_page: Option<Uuid>,
    ) -> Result<bool, Self::Error> {
        let slug = slug.to_lowercase();
        let exclude_page = exclude_page.map(|v| v.to_string()).unwrap_or_default();
        Ok(self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, slug FROM seo_page WHERE shop_id = ?1",
                )?;
                let mut rows = stmt.query([shop_id])?;
                while let Some(row) = rows.next()? {
                    let id: String = row.get(0)?;
                    let row_slug: String = row.get(1)?;
                    if !exclude_page.is_empty() && id == exclude_page {
                        continue;
                    }
                    if row_slug.to_lowercase() == slug {
                        return Ok(true);
                    }
                }

                let mut stmt = conn.prepare(
                    "SELECT slug FROM seo_page_slugs WHERE shop_id = ?1",
                )?;
                let mut rows = stmt.query([shop_id])?;
                while let Some(row) = rows.next()? {
                    let row_slug: String = row.get(0)?;
                    if row_slug.to_lowercase() == slug {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .await?)
    }
}
