use crate::external_import;
use async_zip::base::read::mem::ZipFileReader;
use external_import::{Item, Offer};
use reqwest::multipart::Part;
use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ImportResponse {
    pub id: Option<String>,
    pub status: String,
    pub message: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ImportStatus {
    pub status: String,
    pub not_changed: usize,
    pub updated: usize,
    pub not_in_file: usize,
    pub imported: usize,
    pub created: usize,
    pub actualized: usize,
    pub created_active: usize,
    pub created_hidden: usize,
    pub total: usize,
    pub with_errors_count: usize,
}

pub async fn upload_products(
    path: &str,
    token: &str,
    client: Client,
) -> Result<String, anyhow::Error> {
    const URL: &str = "https://my.prom.ua/api/v1/products/import_file";
    let file = tokio::fs::read(path).await?;
    let form = reqwest::multipart::Form::new()
        .part("file", Part::bytes(file).file_name(path.to_string()))
        .text(
            "data",
            "
{
  \"force_update\": false,
  \"only_available\": false,
  \"mark_missing_product_as\": \"not_available\",
  \"updated_fields\": [
    \"name\",
    \"sku\",
    \"price\",
    \"image_urls\",
    \"presence\",
    \"quantity_in_stock\",
    \"description\",
    \"discount\",
    \"attributes\",
    \"translations\"
  ]
}
",
        );
    let resp = client
        .post(URL)
        .multipart(form)
        .bearer_auth(token)
        .send()
        .await?;
    log::info!("{resp:?}");
    let text = resp.text().await?;
    log::info!("{text:?}");
    let resp: ImportResponse = serde_yaml::from_str(&text)?;
    match (resp.id.as_ref(), resp.message.as_ref()) {
        (Some(id), _) => Ok(id.to_string()),
        (None, None) => Err(anyhow::anyhow!("No id: {resp:?}")),
        (None, Some(msg)) => Err(anyhow::anyhow!("{}: {}", resp.status, msg)),
    }
}

pub enum DownloadResult {
    Offers(Vec<Offer>),
    Items(Vec<Item>),
}

#[derive(Debug)]
pub enum DownloadFromLinkError {
    UnableToParse { err: anyhow::Error, content: String },
    Other(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for DownloadFromLinkError {
    fn from(err: E) -> Self {
        Self::Other(err.into())
    }
}

pub async fn download_from_link(
    url: &str,
    client: Client,
) -> Result<DownloadResult, DownloadFromLinkError> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(DownloadFromLinkError::Other(anyhow::anyhow!(
            "HTTP {status} for {url}"
        )));
    }
    let bytes = response.bytes().await?;
    if bytes.is_empty() {
        return Err(DownloadFromLinkError::Other(anyhow::anyhow!(
            "Empty response for {url}"
        )));
    }
    let content = if looks_like_zip(&bytes) {
        unzip_first_text(bytes.to_vec()).await?
    } else {
        String::from_utf8_lossy(&bytes).to_string()
    };
    let c: Result<external_import::Shop, _> = quick_xml::de::from_str(&content);
    match c.map(|c| c.items) {
        Ok(Some(items)) => Ok(DownloadResult::Items(items.items)),
        _ => {
            let c: Result<external_import::YmlCatalog, _> = quick_xml::de::from_str(&content);
            match c {
                Ok(c) => Ok(DownloadResult::Offers(
                    c.shop
                        .offers
                        .ok_or(anyhow::anyhow!("No offers found"))?
                        .offers
                        .into_iter()
                        .collect::<Vec<_>>(),
                )),
                Err(err) => Err(DownloadFromLinkError::UnableToParse {
                    err: err.into(),
                    content,
                }),
            }
        }
    }
}

fn looks_like_zip(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes.starts_with(b"PK\x03\x04")
}

async fn unzip_first_text(bytes: Vec<u8>) -> Result<String, DownloadFromLinkError> {
    let zip = ZipFileReader::new(bytes)
        .await
        .map_err(|err| DownloadFromLinkError::Other(err.into()))?;
    let entry_index = zip
        .file()
        .entries()
        .iter()
        .enumerate()
        .find_map(|(idx, entry)| entry.dir().ok().map(|is_dir| (!is_dir).then_some(idx)).flatten())
        .ok_or_else(|| {
            DownloadFromLinkError::Other(anyhow::anyhow!("ZIP archive has no files"))
        })?;
    let mut reader = zip
        .reader_with_entry(entry_index)
        .await
        .map_err(|err| DownloadFromLinkError::Other(err.into()))?;
    let mut output = String::new();
    reader
        .read_to_string_checked(&mut output)
        .await
        .map_err(|err| DownloadFromLinkError::Other(err.into()))?;
    Ok(output)
}

pub async fn upload_by_link(
    url: &str,
    token: &str,
    client: Client,
) -> Result<String, anyhow::Error> {
    const URL: &str = "https://my.prom.ua/api/v1/products/import_url";
    let resp = client
        .post(URL)
        .header("Content-Type", "application/json")
        .body(format!(
            "{{
  \"url\": \"{url}\",
  \"force_update\": false,
  \"only_available\": false,
  \"mark_missing_product_as\": \"none\",
  \"updated_fields\": [
    \"name\",
    \"sku\",
    \"price\",
    \"image_urls\",
    \"presence\",
    \"quantity_in_stock\",
    \"description\",
    \"keywords\",
    \"attributes\",
    \"translations\"
  ]
}}
"
        ))
        .bearer_auth(token)
        .send()
        .await?;
    log::info!("{resp:?}");
    let text = resp.text().await?;
    log::info!("{:?}", text);
    let resp: ImportResponse = serde_yaml::from_str(&text)?;
    match (resp.id.as_ref(), resp.message.as_ref()) {
        (Some(id), _) => Ok(id.to_string()),
        (None, None) => Err(anyhow::anyhow!("No id: {resp:?}")),
        (None, Some(msg)) => Err(anyhow::anyhow!("{}: {}", resp.status, msg)),
    }
}

pub async fn import_status(
    id: &str,
    token: &str,
    client: Client,
) -> Result<ImportStatus, anyhow::Error> {
    const URL: &str = "https://my.prom.ua/api/v1/products/import/status";
    let url = format!("{URL}/{id}");
    let resp = client.get(url).bearer_auth(token).send().await?;
    log::info!("{resp:?}");
    Ok(resp.json().await?)
}
