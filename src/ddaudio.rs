use anyhow::anyhow;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Deserialize, Clone)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: T,
    #[serde(default)]
    pub total: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default, rename = "totalResults")]
    pub total_results: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CategoryNode {
    pub title: String,
    #[serde(default)]
    pub children: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CategoriesResponse {
    pub success: bool,
    pub data: HashMap<String, CategoryNode>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WarehousesResponse {
    pub success: bool,
    pub data: Vec<String>,
}

const DEFAULT_CACHE_TTL_SECS: u64 = 6 * 60 * 60;

static DDAUDIO_CACHE_TTL: Lazy<Duration> = Lazy::new(|| {
    std::env::var("DDAUDIO_API_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_CACHE_TTL_SECS))
});

#[derive(Clone)]
struct CacheEntry<T> {
    cached_at: Instant,
    value: T,
}

static CATEGORIES_CACHE: Lazy<RwLock<HashMap<String, CacheEntry<CategoriesResponse>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static WAREHOUSES_CACHE: Lazy<RwLock<HashMap<String, CacheEntry<WarehousesResponse>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Deserialize, Clone)]
pub struct ParentInfo {
    #[serde(default, deserialize_with = "de_opt_string")]
    pub id: Option<String>,
    #[serde(deserialize_with = "de_string")]
    pub title: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProductItem {
    pub id: Option<usize>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub mark: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub category: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub subcategory: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub manufacturer: Option<String>,
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub country: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub material: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub installation: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub kit: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub sku: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub currency: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    pub quantity: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    pub available_in_stock: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub warehouse: Option<String>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub sale_price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub sale_start_at: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub sale_end_at: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub short_title: Option<String>,
    pub parent: Option<ParentInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceKind {
    Retail,
    Wholesale,
}

impl PriceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Retail => "retail",
            Self::Wholesale => "wholesale",
        }
    }
}

fn api_base() -> &'static str {
    "https://ddaudio.com.ua/api"
}

fn normalize_token(token: &str) -> String {
    let trimmed = token.trim();
    let lower = trimmed.to_lowercase();
    if let Some(rest) = lower.strip_prefix("bearer ") {
        let offset = trimmed.len().saturating_sub(rest.len());
        trimmed[offset..].trim().to_string()
    } else {
        trimmed.to_string()
    }
}

async fn get_json<T: for<'de> Deserialize<'de>>(
    token: &str,
    url: &str,
) -> anyhow::Result<T> {
    let token = normalize_token(token);
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .bearer_auth(token)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if text.trim().is_empty() {
        return Err(anyhow!("DD Audio API empty response"));
    }
    if !status.is_success() {
        return Err(anyhow!(
            "DD Audio API {}: {}",
            status,
            truncate_body(&text)
        ));
    }
    serde_json::from_str::<T>(&text)
        .map_err(|err| anyhow!("DD Audio API decode error: {err}. Body: {}", truncate_body(&text)))
}

pub async fn fetch_categories(token: &str, lang: Option<&str>) -> anyhow::Result<CategoriesResponse> {
    let token = normalize_token(token);
    let lang_key = lang
        .map(|l| l.trim().to_lowercase())
        .filter(|l| !l.is_empty());
    let cache_key = format!(
        "{}|{}",
        token,
        lang_key.clone().unwrap_or_default()
    );
    if let Some(entry) = {
        let cache = CATEGORIES_CACHE.read().await;
        cache.get(&cache_key).cloned()
    } {
        if entry.cached_at.elapsed() < *DDAUDIO_CACHE_TTL {
            return Ok(entry.value);
        }
    }
    let mut url = format!("{}/categories", api_base());
    if let Some(lang) = lang_key.as_deref() {
        url.push_str(&format!("?lang={}", lang));
    }
    match get_json::<CategoriesResponse>(&token, &url).await {
        Ok(value) => {
            let mut cache = CATEGORIES_CACHE.write().await;
            cache.insert(
                cache_key,
                CacheEntry {
                    cached_at: Instant::now(),
                    value: value.clone(),
                },
            );
            Ok(value)
        }
        Err(err) => {
            if let Some(entry) = {
                let cache = CATEGORIES_CACHE.read().await;
                cache.get(&cache_key).cloned()
            } {
                log::warn!("DD Audio categories: using cached data due to error: {err}");
                return Ok(entry.value);
            }
            Err(err)
        }
    }
}

pub async fn fetch_warehouses(token: &str) -> anyhow::Result<WarehousesResponse> {
    let token = normalize_token(token);
    if let Some(entry) = {
        let cache = WAREHOUSES_CACHE.read().await;
        cache.get(&token).cloned()
    } {
        if entry.cached_at.elapsed() < *DDAUDIO_CACHE_TTL {
            return Ok(entry.value);
        }
    }
    let url = format!("{}/warehouses", api_base());
    match get_json::<WarehousesResponse>(&token, &url).await {
        Ok(value) => {
            let mut cache = WAREHOUSES_CACHE.write().await;
            cache.insert(
                token,
                CacheEntry {
                    cached_at: Instant::now(),
                    value: value.clone(),
                },
            );
            Ok(value)
        }
        Err(err) => {
            if let Some(entry) = {
                let cache = WAREHOUSES_CACHE.read().await;
                cache.get(&token).cloned()
            } {
                log::warn!("DD Audio warehouses: using cached data due to error: {err}");
                return Ok(entry.value);
            }
            Err(err)
        }
    }
}

pub async fn fetch_prices(
    token: &str,
    kind: PriceKind,
    lang: Option<&str>,
    offset: usize,
    limit: usize,
) -> anyhow::Result<ApiResponse<Vec<ProductItem>>> {
    let mut url = format!("{}/price/{}", api_base(), kind.as_str());
    let mut query = Vec::new();
    if offset > 0 {
        query.push(format!("offset={offset}"));
    }
    if limit > 0 {
        query.push(format!("limit={limit}"));
    }
    if let Some(lang) = lang {
        if !lang.trim().is_empty() {
            query.push(format!("lang={}", lang.trim()));
        }
    }
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query.join("&"));
    }
    get_json(token, &url).await
}

fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
        Int(i64),
        Bool(bool),
        Other(serde_json::Value),
    }
    let v = Option::<NumOrStr>::deserialize(deserializer)?;
    Ok(v.and_then(|val| match val {
        NumOrStr::Num(n) => Some(n),
        NumOrStr::Int(i) => Some(i as f64),
        NumOrStr::Str(s) => s.replace(',', ".").parse::<f64>().ok(),
        NumOrStr::Bool(b) => Some(if b { 1.0 } else { 0.0 }),
        NumOrStr::Other(v) => match v {
            serde_json::Value::Number(n) => n.as_f64(),
            serde_json::Value::String(s) => s.replace(',', ".").parse::<f64>().ok(),
            serde_json::Value::Bool(b) => Some(if b { 1.0 } else { 0.0 }),
            _ => None,
        },
    }))
}

fn de_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
        Int(i64),
        Bool(bool),
        Other(serde_json::Value),
    }
    let v = Option::<NumOrStr>::deserialize(deserializer)?;
    Ok(v.and_then(|val| match val {
        NumOrStr::Num(n) => Some(n.round() as i64),
        NumOrStr::Int(i) => Some(i),
        NumOrStr::Str(s) => s.replace(',', ".").parse::<f64>().ok().map(|n| n.round() as i64),
        NumOrStr::Bool(b) => Some(if b { 1 } else { 0 }),
        NumOrStr::Other(v) => match v {
            serde_json::Value::Number(n) => n.as_f64().map(|n| n.round() as i64),
            serde_json::Value::String(s) => s
                .replace(',', ".")
                .parse::<f64>()
                .ok()
                .map(|n| n.round() as i64),
            serde_json::Value::Bool(b) => Some(if b { 1 } else { 0 }),
            _ => None,
        },
    }))
}

fn de_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(de_opt_string(deserializer)?.unwrap_or_default())
}

fn de_opt_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrNum {
        Str(String),
        Int(i64),
        Float(f64),
        Bool(bool),
    }
    let v = Option::<StrOrNum>::deserialize(deserializer)?;
    let out = v.map(|val| match val {
        StrOrNum::Str(s) => s,
        StrOrNum::Int(i) => i.to_string(),
        StrOrNum::Float(f) => {
            let mut s = f.to_string();
            if s.ends_with(".0") {
                s.truncate(s.len() - 2);
            }
            s
        }
        StrOrNum::Bool(b) => b.to_string(),
    });
    Ok(out.and_then(|s| {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    }))
}

fn truncate_body(body: &str) -> String {
    const LIMIT: usize = 220;
    let trimmed = body.trim();
    if trimmed.len() <= LIMIT {
        return trimmed.to_string();
    }
    let mut end = 0usize;
    for (idx, _) in trimmed.char_indices() {
        if idx > LIMIT {
            break;
        }
        end = idx;
    }
    if end == 0 {
        return trimmed.to_string();
    }
    format!("{}…", &trimmed[..end])
}

#[cfg(test)]
mod tests {
    use super::normalize_token;

    #[test]
    fn normalize_token_strips_bearer_prefix() {
        assert_eq!(normalize_token("Bearer abc123"), "abc123");
        assert_eq!(normalize_token("bearer abc123"), "abc123");
        assert_eq!(normalize_token("  Bearer abc123  "), "abc123");
        assert_eq!(normalize_token("abc123"), "abc123");
    }
}
