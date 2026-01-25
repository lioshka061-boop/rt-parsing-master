use serde::{Deserialize, Serialize};

const DEFAULT_API_KEY: &str = "272d3015b8655098f2ef525732f153a9";

fn api_key() -> String {
    std::env::var("RESTAL_API_KEY").unwrap_or_else(|_| DEFAULT_API_KEY.to_string())
}

fn api_key_or(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        api_key()
    } else {
        trimmed.to_string()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestalCategory {
    pub category_id: Option<String>,
    pub name: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestalProduct {
    pub product_id: Option<String>,
    pub name: Option<String>,
    pub sku: Option<String>,
    pub model: Option<String>,
    pub category_id: Option<String>,
    pub image: Option<String>,
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(deserialize_with = "de_opt_string")]
    pub price: Option<String>,
    #[serde(deserialize_with = "de_opt_string")]
    pub quantity: Option<String>,
    pub description: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestalStock {
    pub product_id: Option<String>,
    #[serde(deserialize_with = "de_opt_string")]
    pub price: Option<String>,
    #[serde(deserialize_with = "de_opt_string")]
    pub quantity: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

async fn post_form<T: for<'de> Deserialize<'de>>(
    endpoint: &str,
    mut form: Vec<(String, String)>,
) -> anyhow::Result<T> {
    let client = reqwest::Client::new();
    form.push(("apikey".into(), api_key()));
    let resp = client
        .post(endpoint)
        .form(&form)
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.json::<T>().await?)
}

async fn post_form_with_key<T: for<'de> Deserialize<'de>>(
    endpoint: &str,
    mut form: Vec<(String, String)>,
    key: &str,
) -> anyhow::Result<T> {
    let client = reqwest::Client::new();
    form.push(("apikey".into(), api_key_or(key)));
    let resp = client
        .post(endpoint)
        .form(&form)
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.json::<T>().await?)
}

pub async fn fetch_categories(parent_ids: Option<&str>) -> anyhow::Result<Vec<RestalCategory>> {
    let mut form = Vec::new();
    if let Some(ids) = parent_ids {
        form.push(("categories".to_string(), ids.to_string()));
    }
    post_form(
        "https://restal-auto.com.ua/index.php?route=account/api/getcategories",
        form,
    )
    .await
}

pub async fn fetch_products_by_category(category_id: &str) -> anyhow::Result<Vec<RestalProduct>> {
    post_form(
        "https://restal-auto.com.ua/index.php?route=account/api/getproductsbycategory",
        vec![("category_id".into(), category_id.to_string())],
    )
    .await
}

pub async fn fetch_products(start: usize, limit: usize) -> anyhow::Result<Vec<RestalProduct>> {
    post_form(
        "https://restal-auto.com.ua/index.php?route=account/api/getproducts",
        vec![
            ("start".into(), start.to_string()),
            ("limit".into(), limit.to_string()),
        ],
    )
    .await
}

pub async fn fetch_products_with_key(
    key: &str,
    start: usize,
    limit: usize,
) -> anyhow::Result<Vec<RestalProduct>> {
    post_form_with_key(
        "https://restal-auto.com.ua/index.php?route=account/api/getproducts",
        vec![
            ("start".into(), start.to_string()),
            ("limit".into(), limit.to_string()),
        ],
        key,
    )
    .await
}

pub async fn fetch_stock() -> anyhow::Result<Vec<RestalStock>> {
    post_form(
        "https://restal-auto.com.ua/index.php?route=account/api/getstock",
        vec![],
    )
    .await
}

fn de_opt_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrNum {
        Str(String),
        Num(f64),
        Int(i64),
    }

    let v = Option::<StrOrNum>::deserialize(deserializer)?;
    Ok(v.map(|x| match x {
        StrOrNum::Str(s) => s,
        StrOrNum::Num(n) => n.to_string(),
        StrOrNum::Int(i) => i.to_string(),
    }))
}
