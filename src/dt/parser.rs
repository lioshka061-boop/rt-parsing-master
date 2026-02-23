use crate::cache;
use crate::dt::{
    product::{Product, ProductRepository},
    manual_queue::{ManualUrlRepository, ManualUrlRepo},
    selectors,
};
use crate::{format_raw_html, Model, Url};
use actix::prelude::*;
use actix_broker::BrokerSubscribe;
use anyhow::anyhow;
use derive_more::Display;
use derive_more::Error;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use lazy_regex::regex;
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use rt_types::shop::ConfigurationChanged;
use rt_types::{Availability, Pause, Resume};
use scraper::{node::Node, Html};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use time::Duration as TimeDuration;
use tokio::signal;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio_util::sync::CancellationToken;
use typesafe_repository::IdentityOf;

#[derive(Message)]
#[rtype(result = "Result<ParsingProgress, anyhow::Error>")]
pub struct GetProgress;

#[derive(Message)]
#[rtype(result = "Result<Product, ProductParsingError>")]
pub struct Parse(pub String);

#[derive(Message)]
#[rtype(result = "Result<Vec<String>, ProductParsingError>")]
pub struct ParsePage(pub String);

#[derive(Message)]
#[rtype(result = "Result<Option<Product>, anyhow::Error>")]
pub struct ProductInfo(pub IdentityOf<Product>);

#[derive(Message)]
#[rtype(result = "Result<usize, anyhow::Error>")]
pub struct CleanupMissing;

#[derive(Message)]
#[rtype(result = "CleanupStatusInfo")]
pub struct GetCleanupStatus;

pub struct ParsingProgress {
    pub ready: u64,
    pub total: u64,
    pub stage: ParsingStage,
    pub started_at: Option<String>,
    pub elapsed: Option<String>,
    pub speed_per_min: Option<String>,
    pub eta: Option<String>,
    pub eta_at: Option<String>,
}

#[derive(Clone, Debug)]
enum CleanupState {
    Idle,
    Running,
    Done,
    Error,
}

#[derive(Clone, Debug)]
struct CleanupStatus {
    state: CleanupState,
    started_at: Option<OffsetDateTime>,
    finished_at: Option<OffsetDateTime>,
    removed: Option<usize>,
    error: Option<String>,
}

#[derive(Clone)]
pub struct CleanupStatusInfo {
    pub state: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub removed: Option<usize>,
    pub error: Option<String>,
}

fn cleanup_status_info(status: &CleanupStatus) -> CleanupStatusInfo {
    let state = match status.state {
        CleanupState::Idle => "idle",
        CleanupState::Running => "running",
        CleanupState::Done => "done",
        CleanupState::Error => "error",
    }
    .to_string();
    let started_at = status
        .started_at
        .and_then(|dt| dt.format(&time::format_description::well_known::Rfc3339).ok());
    let finished_at = status
        .finished_at
        .and_then(|dt| dt.format(&time::format_description::well_known::Rfc3339).ok());
    CleanupStatusInfo {
        state,
        started_at,
        finished_at,
        removed: status.removed,
        error: status.error.clone(),
    }
}

#[derive(Clone, Display)]
pub enum ParsingStage {
    #[display("Пауза")]
    Pause,
    #[display("Парсинг брендов")]
    Brands,
    #[display("Парсинг моделей")]
    Models,
    #[display("Парсинг списка товаров")]
    ProductList,
    #[display("Парсинг товаров")]
    Products,
}

#[derive(Debug, Display, Error)]
pub enum ProductParsingError {
    NoArticle,
    ParsingError(ParsingError),
}

impl From<anyhow::Error> for ProductParsingError {
    fn from(err: anyhow::Error) -> Self {
        ParsingError::from(err).into()
    }
}

impl From<reqwest::Error> for ProductParsingError {
    fn from(err: reqwest::Error) -> Self {
        ParsingError::from(err).into()
    }
}

impl From<ParsingError> for ProductParsingError {
    fn from(err: ParsingError) -> Self {
        ProductParsingError::ParsingError(err)
    }
}

#[derive(Debug, Display, Error)]
pub enum ParsingError {
    #[error(ignore)]
    BrowserCheck(String),
    #[error(ignore)]
    MissingHref(String),
    Network(reqwest::Error),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for ParsingError {
    fn from(err: anyhow::Error) -> Self {
        ParsingError::Other(err)
    }
}

impl From<reqwest::Error> for ParsingError {
    fn from(err: reqwest::Error) -> ParsingError {
        ParsingError::Network(err)
    }
}

impl From<reqwest_middleware::Error> for ParsingError {
    fn from(err: reqwest_middleware::Error) -> ParsingError {
        match err {
            reqwest_middleware::Error::Middleware(err) => ParsingError::Other(err),
            reqwest_middleware::Error::Reqwest(err) => ParsingError::Network(err),
        }
    }
}

#[derive(Clone)]
pub struct ParsingOptions {
    pub url: String,
    pub repo: Arc<dyn ProductRepository>,
    pub manual_repo: ManualUrlRepo,
    pub client: ClientWithMiddleware,
    pub progress_bar: Option<Arc<ProgressBar>>,
    pub parallel_downloads: usize,
    pub stage: ParsingStage,
    pub started_at: Option<OffsetDateTime>,
}

impl ParsingOptions {
    pub fn new(
        url: String,
        repo: Arc<dyn ProductRepository>,
        manual_repo: ManualUrlRepo,
        client: ClientWithMiddleware,
        progress_bar: Option<Arc<ProgressBar>>,
        parallel_downloads: usize,
    ) -> Self {
        Self {
            url,
            repo,
            manual_repo,
            client,
            progress_bar,
            parallel_downloads,
            stage: ParsingStage::Pause,
            started_at: None,
        }
    }
}

fn format_duration(seconds: i64) -> String {
    let mut secs = seconds.max(0);
    let hours = secs / 3600;
    secs %= 3600;
    let minutes = secs / 60;
    secs %= 60;
    if hours > 0 {
        format!("{hours}h {minutes}m {secs}s")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

fn format_speed(speed_per_min: f64) -> String {
    format!("{speed_per_min:.1} /min")
}

pub async fn parse_brands(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(&url).send().await?.text().await?;
    if is_browser_check(&body) {
        return Err(anyhow::anyhow!("Browser check detected, cannot proceed with parsing"));
    }
    let document = Html::parse_document(&body);
    let brands: Vec<_> = document
        .select(&selectors::BRANDS)
        .filter_map(
            |e| match (e.attr("href"), e.last_child().map(|c| c.value())) {
                (Some(url), Some(Node::Text(t))) => {
                    Some((Url(url.to_string()), format_raw_html(t.to_string())))
                }
                (None, _) => {
                    log::error!("Brand without link:\n{e:?}");
                    None
                }
                (_, _) => {
                    log::error!("Unable to parse brand name:\n{e:?}");
                    None
                }
            },
        )
        .collect();
    Ok(brands)
}

pub async fn parse_categories(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String)>, anyhow::Error> {
    let (url, client) = {
        let opts = options.read().await;
        (opts.url.clone(), opts.client.clone())
    };
    let body = client.get(&url).send().await?.text().await?;
    if is_browser_check(&body) {
        return Err(anyhow::anyhow!("Browser check detected, cannot proceed with parsing"));
    }
    let document = Html::parse_document(&body);
    let categories: Vec<_> = document
        .select(&selectors::CATEGORIES)
        .filter_map(
            |e| match (e.attr("href"), e.last_child().map(|c| c.value())) {
                (Some(url), Some(Node::Text(t))) => {
                    Some((Url(url.to_string()), format_raw_html(t.to_string())))
                }
                (None, _) => {
                    log::error!("Category without link:\n{e:?}");
                    None
                }
                (_, _) => {
                    log::error!("Unable to parse category name:\n{e:?}");
                    None
                }
            },
        )
        .collect();
    Ok(categories)
}

pub async fn parse_subcategories(
    categories: &[(Url, String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String, &String)>, ParsingError> {
    let (client, url, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    stream::iter(categories)
        .map(|(Url(link), category)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{link}").replace("///", "/"))
                    .send()
                    .await
                    .map(|body| (link, body, category))
            }
        })
        .buffer_unordered(parallel_downloads)
        .map(|res| {
            let pb = pb.clone();
            async move {
                let (link, body, category) = res?;
                let body = body.text().await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let document = Html::parse_document(&body);
                let subcategories: Vec<_> = document
                    .select(&selectors::SUBCATEGORY)
                    .map(|e| (e.attr("href"), e.inner_html()))
                    .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                    .collect();
                if subcategories.is_empty() {
                    return Ok(vec![(Url(link.clone()), category.clone(), category)]);
                }
                subcategories
                    .into_iter()
                    .map(|(url, v)| {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        Ok((Url(url), v, category))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn parse_models(
    brands: &[(Url, String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String, &String)>, ParsingError> {
    let (client, url, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    stream::iter(brands)
        .map(|(Url(link), brand)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{link}").replace("///", "/"))
                    .send()
                    .await
                    .map(|body| (link, body, brand))
            }
        })
        .buffer_unordered(parallel_downloads)
        .map(|res| {
            let pb = pb.clone();
            async move {
                let (link, body, brand) = res?;
                let body = body.text().await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let document = Html::parse_document(&body);
                let models: Vec<_> = document
                    .select(&selectors::MODEL)
                    .map(|e| (e.attr("href"), e.inner_html()))
                    .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                    .collect();
                models
                    .into_iter()
                    .map(|(url, v)| {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        Ok((Url(url), v, brand))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn parse_product_lists<'a>(
    models: &'a [(Url, String, &String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<(Url, String, String)>, ParsingError> {
    let (client, url, repo, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.repo.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    let url = if url.ends_with('/') {
        url[..url.len() - 1].to_string()
    } else {
        url
    };
    stream::iter(models)
        .map(|(Url(link), model, brand)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{}", format_link(link)))
                    .send()
                    .await
                    .map(|body| (link, body, model, brand))
            }
        })
        .buffered(parallel_downloads)
        .map(|res| {
            let client = client.clone();
            let url = url.clone();
            let repo = repo.clone();
            let pb = pb.clone();
            async move {
                let (link, body, model, &brand) = res?;
                let mut body = body.text().await?;
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let mut pages = vec![body.clone()];
                let mut seen_pages: HashSet<String> = HashSet::new();
                seen_pages.insert(normalize_url_key(link));
                let mut queue: VecDeque<String> = VecDeque::new();
                let mut doc = Html::parse_document(&body);
                for href in collect_pagination_links(&doc) {
                    let key = normalize_url_key(&href);
                    if !key.is_empty() && seen_pages.insert(key) {
                        queue.push_back(href);
                    }
                }
                while let Some(next_link) = queue.pop_front() {
                    let page_url = format!("{url}/{}", format_link(&next_link));
                    let response = match client.get(page_url.clone()).send().await {
                        Ok(response) => response,
                        Err(err) => {
                            log::error!("Unable to parse products list at {page_url}: {err}");
                            continue;
                        }
                    };
                    let text = match response.text().await {
                        Ok(text) => text,
                        Err(err) => {
                            log::error!("Unable to parse products list at {page_url}: {err}");
                            continue;
                        }
                    };
                    if is_browser_check(&text) {
                        log::warn!("Browser check for list page {page_url}");
                        continue;
                    }
                    pages.push(text.clone());
                    doc = Html::parse_document(&text);
                    for href in collect_pagination_links(&doc) {
                        let key = normalize_url_key(&href);
                        if !key.is_empty() && seen_pages.insert(key) {
                            queue.push_back(href);
                        }
                    }
                }
                let mut product_links: Vec<Url> = Vec::new();
                let mut seen_products: HashSet<String> = HashSet::new();
                for page_body in pages.iter() {
                    let document = Html::parse_document(page_body);
                    let items: Vec<_> = document
                        .select(&selectors::PRODUCT_ITEM)
                        .map(|e| (e.attr("href").map(str::to_string), e.inner_html()))
                        .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                        .collect();
                    for (url, _) in items {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        let key = normalize_url_key(&url);
                        if key.is_empty() {
                            continue;
                        }
                        if seen_products.insert(key) {
                            let rel = to_relative_path(&url).unwrap_or(url);
                            product_links.push(Url(rel));
                        }
                    }
                }
                let products: Vec<_> = repo.list_by(&Model(model.to_string())).await?;
                let mut products_by_url: HashMap<String, &Product> = HashMap::new();
                for product in &products {
                    products_by_url.insert(normalize_url_key(&product.url.0), product);
                }
                let mut urls: Vec<_> = product_links
                    .into_iter()
                    .map(|url| {
                        let key = normalize_url_key(&url.0);
                        (url, products_by_url.get(&key).copied())
                    })
                    .collect();
                urls.sort_by(|a, b| {
                    a.1.map(|x| x.last_visited)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                        .partial_cmp(
                            &b.1.map(|x| x.last_visited)
                                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                        )
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                let urls: Vec<_> = urls
                    .into_iter()
                    .filter_map(|(url, product)| match product {
                        Some(product) if product.is_outdated() => {
                            Some((url, model.clone(), brand.to_string()))
                        }
                        None => Some((url, model.clone(), brand.to_string())),
                        Some(product) => {
                            log::info!("Skipping up to date product parsing: {}", product.article);
                            None
                        }
                    })
                    .collect();
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                Ok(urls)
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect()
        .await
}

pub async fn collect_all_product_urls<'a>(
    models: &'a [(Url, String, &String)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<Url>, ParsingError> {
    let (client, url, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    let url = if url.ends_with('/') {
        url[..url.len() - 1].to_string()
    } else {
        url
    };
    stream::iter(models)
        .map(|(Url(link), model, brand)| {
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .get(format!("{url}/{}", format_link(link)))
                    .send()
                    .await
                    .map(|body| (link, body, model, brand))
            }
        })
        .buffered(parallel_downloads)
        .map(|res| {
            let client = client.clone();
            let url = url.clone();
            let pb = pb.clone();
            async move {
                let (link, body, _model, _brand) = res?;
                let body = body.text().await?;
                if is_browser_check(&body) {
                    return Err(ParsingError::BrowserCheck(link.clone()));
                }
                let mut pages = vec![body.clone()];
                let mut seen_pages: HashSet<String> = HashSet::new();
                seen_pages.insert(normalize_url_key(link));
                let mut queue: VecDeque<String> = VecDeque::new();
                let mut doc = Html::parse_document(&body);
                for href in collect_pagination_links(&doc) {
                    let key = normalize_url_key(&href);
                    if !key.is_empty() && seen_pages.insert(key) {
                        queue.push_back(href);
                    }
                }
                while let Some(next_link) = queue.pop_front() {
                    let page_url = format!("{url}/{}", format_link(&next_link));
                    let response = match client.get(page_url.clone()).send().await {
                        Ok(response) => response,
                        Err(err) => {
                            log::error!("Unable to parse products list at {page_url}: {err}");
                            continue;
                        }
                    };
                    let text = match response.text().await {
                        Ok(text) => text,
                        Err(err) => {
                            log::error!("Unable to parse products list at {page_url}: {err}");
                            continue;
                        }
                    };
                    if is_browser_check(&text) {
                        log::warn!("Browser check for list page {page_url}");
                        continue;
                    }
                    pages.push(text.clone());
                    doc = Html::parse_document(&text);
                    for href in collect_pagination_links(&doc) {
                        let key = normalize_url_key(&href);
                        if !key.is_empty() && seen_pages.insert(key) {
                            queue.push_back(href);
                        }
                    }
                }
                let mut product_links: Vec<Url> = Vec::new();
                let mut seen_products: HashSet<String> = HashSet::new();
                for page_body in pages.iter() {
                    let document = Html::parse_document(page_body);
                    let items: Vec<_> = document
                        .select(&selectors::PRODUCT_ITEM)
                        .map(|e| (e.attr("href").map(str::to_string), e.inner_html()))
                        .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
                        .collect();
                    for (url, _) in items {
                        let url = url
                            .ok_or(ParsingError::MissingHref(link.clone()))?
                            .to_string();
                        let key = normalize_url_key(&url);
                        if key.is_empty() {
                            continue;
                        }
                        if seen_products.insert(key) {
                            let rel = to_relative_path(&url).unwrap_or(url);
                            product_links.push(Url(rel));
                        }
                    }
                }
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                Ok(product_links)
            }
        })
        .buffered(2048)
        .flat_map(|i| match i {
            Ok(i) => stream::iter(i.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(err) => stream::iter(vec![Err(err)]),
        })
        .try_collect()
        .await
}

pub async fn parse_product_list_page(
    link: &String,
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<Vec<Url>, ParsingError> {
    let client = {
        let opts = options.read().await;
        opts.client.clone()
    };
    let client = client.clone();
    let res = client.get(link).send().await?;
    let body: String = res.text().await?;
    if is_browser_check(&body) {
        return Err(ParsingError::BrowserCheck(link.clone()));
    }
    let document = Html::parse_document(&body);
    let items: Vec<_> = document
        .select(&selectors::PRODUCT_ITEM)
        .map(|e| (e.attr("href").map(str::to_string), e.inner_html()))
        .map(|(url, v)| (url, v.replace('\n', "").trim().to_string()))
        .collect();
    let urls: Vec<Url> = items
        .into_iter()
        .map(|(url, _)| {
            let url = url
                .ok_or(ParsingError::MissingHref(link.clone()))?
                .to_string();
            Ok(Url(url))
        })
        .collect::<Result<Vec<_>, ParsingError>>()?;
    Ok(urls)
}

pub async fn parse_products<M, B>(
    links: &[(Url, M, B)],
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<usize, anyhow::Error>
where
    M: AsRef<str> + std::fmt::Display,
    B: AsRef<str> + std::fmt::Display,
{
    let (client, url, repo, pb, parallel_downloads) = {
        let opts = options.read().await;
        (
            opts.client.clone(),
            opts.url.clone(),
            opts.repo.clone(),
            opts.progress_bar.clone(),
            opts.parallel_downloads,
        )
    };
    let len = stream::iter(links)
        .map(|(Url(link), model, brand)| {
            client
                .get(format!("{url}/{link}").replace("///", "/"))
                .send()
                .map(move |res| res.map(|body| (link, body, model, brand)))
        })
        .buffer_unordered(parallel_downloads)
        .filter_map(|res| async {
            match res {
                Ok(res) => Some(res),
                Err(err) => {
                    log::error!("Unable to parse product: {err}");
                    None
                }
            }
        })
        .map(|(link, body, model, brand)| {
            let repo = repo.clone();
            let pb = pb.clone();
            async move {
                if body.status().as_u16() == 404 || body.status().as_u16() == 410 {
                    if let Ok(Some(existing)) = repo.get_by(&Url(link.to_string())).await {
                        let _ = repo.delete_articles(&[existing.article.clone()]).await;
                        log::info!("Removed missing product {}", existing.article);
                    }
                    if let Some(pb) = pb {
                        pb.inc(1);
                    }
                    return Ok(());
                }
                let body = body
                    .text()
                    .await
                    .map_err(|err| ParsingError::Other(err.into()))?;
                if is_browser_check(&body) {
                    return Err::<_, ProductParsingError>(
                        ParsingError::BrowserCheck(link.clone()).into(),
                    );
                }
                let product = {
                    let document = Html::parse_document(&body);

                    parse_product(brand, model, link, document)?
                };
                log::info!("Saved {}", product.article);
                repo.save(product).await?;
                if let Some(pb) = pb {
                    pb.inc(1);
                }
                Ok(())
            }
        })
        .buffered(2048)
        .collect::<Vec<_>>()
        .await
        .len();
    Ok(len)
}

pub fn parse_product<M, B>(
    brand: B,
    model: M,
    link: &str,
    document: Html,
) -> Result<Product, ProductParsingError>
where
    M: AsRef<str> + std::fmt::Display,
    B: AsRef<str> + std::fmt::Display,
{
    let select = |selector| {
        document
            .select(selector)
            .map(|v| format_raw_html(v.inner_html()).to_string())
            .next()
    };

    let article_regex = regex!(r"(?i)(арт:?)? *(<.*>)?(.*) *$");
    let article = select(&selectors::ARTICLE)
        .map(|a| {
            article_regex
                .captures(&a)
                .and_then(|c| c.get(3))
                .map(|c| c.as_str().to_string())
                .ok_or(a)
        })
        .ok_or_else(|| ProductParsingError::NoArticle)?
        .map_err(|raw| anyhow!("Unable to parse article {raw} for item at {link}"))?;
    let article = normalize_article(&article);
    let title = select(&selectors::TITLE)
        .ok_or_else(|| anyhow!("Missing title for item {article} at {link}"))?;
    let description = select(&selectors::DESCRIPTION);
    if description.is_none() {
        log::warn!("Missing description for item {article} at {link}");
    }
    let category = select(&selectors::CATEGORY);
    if category.is_none() {
        log::warn!("Missing category for item {article} at {link}");
    }
    let (schema_price, schema_availability) = parse_schema_org(&document);
    let mut price = select(&selectors::PRICE).and_then(|s| parse_price_str(&s));
    if price.is_none() && schema_price.is_some() {
        price = schema_price;
    }
    let available_text = document
        .select(&selectors::AVAILABLE)
        .next()
        .map(|x| format_raw_html(x.inner_html()).to_lowercase());
    let on_order_text = document
        .select(&selectors::AVAILABLE_ON_ORDER)
        .next()
        .map(|x| format_raw_html(x.inner_html()).to_lowercase());
    let mut available_dom: Option<Availability> = None;
    if let Some(text) = available_text {
        available_dom = parse_availability_text(&text).or(Some(Availability::Available));
    }
    if let Some(text) = on_order_text {
        if let Some(parsed) = parse_availability_text(&text) {
            available_dom = Some(parsed);
        }
    }
    let available = available_dom.or(schema_availability).unwrap_or(Availability::NotAvailable);
    let mut images = document
        .select(&selectors::GALLERY_IMAGES)
        .filter_map(|v| {
            let href = v.attr("src");
            if href.is_none() {
                log::warn!("Image without link for item {link}");
            }
            href
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let logo = document
        .select(&selectors::LOGO)
        .map(|v| v.attr("src"))
        .next()
        .flatten()
        .map(ToString::to_string);
    if let Some(logo) = logo {
        images.insert(0, logo);
    }
    Ok(Product {
        title,
        description,
        title_ua: None,
        description_ua: None,
        article,
        category,
        price,
        source_price: price,
        available,
        attributes: None,
        quantity: None,
        brand: brand.to_string(),
        model: Model(model.to_string()),
        url: Url(link.to_string()),
        supplier: None,
        discount_percent: None,
        last_visited: OffsetDateTime::now_utc(),
        images,
        upsell: None,
    })
}

fn normalize_article(raw: &str) -> String {
    let cleaned = raw.replace('\u{a0}', " ");
    let compact: String = cleaned.split_whitespace().collect();
    compact.trim().to_uppercase()
}

fn parse_price_str(raw: &str) -> Option<usize> {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn parse_availability_text(text: &str) -> Option<Availability> {
    let t = text.to_lowercase();
    if t.contains("под заказ") || t.contains("під замовлення") || t.contains("preorder") {
        Some(Availability::OnOrder)
    } else if t.contains("backorder") {
        Some(Availability::OnOrder)
    } else if t.contains("в наличии")
        || t.contains("есть")
        || t.contains("на складе")
        || t.contains("in stock")
        || t.contains("available")
    {
        Some(Availability::Available)
    } else if t.contains("нет в наличии")
        || t.contains("немає")
        || t.contains("нет")
        || t.contains("out of stock")
    {
        Some(Availability::NotAvailable)
    } else {
        None
    }
}

fn collect_pagination_links(document: &Html) -> Vec<String> {
    let mut links = HashSet::new();
    for anchor in document.select(&selectors::LINKS) {
        let Some(href) = anchor.attr("href") else {
            continue;
        };
        let href = href.trim();
        if href.is_empty() || href.starts_with('#') || href.starts_with("javascript") {
            continue;
        }
        let is_page = href.contains("page=") || href.contains("/page/") || href.contains("page/");
        if is_page {
            if let Some(rel) = to_relative_path(href) {
                links.insert(rel);
            }
        }
    }
    for anchor in document.select(&selectors::PAGINATION_NEXT) {
        let Some(href) = anchor.attr("href") else {
            continue;
        };
        let href = href.trim();
        if href.is_empty() || href.starts_with('#') || href.starts_with("javascript") {
            continue;
        }
        if let Some(rel) = to_relative_path(href) {
            links.insert(rel);
        }
    }
    links.into_iter().collect()
}

fn to_relative_path(href: &str) -> Option<String> {
    let href = href.trim();
    if href.is_empty() {
        return None;
    }
    let without_domain = if let Some(pos) = href.find("://") {
        let rest = &href[pos + 3..];
        match rest.find('/') {
            Some(idx) => &rest[idx..],
            None => "",
        }
    } else {
        href
    };
    let without_fragment = match without_domain.find('#') {
        Some(idx) => &without_domain[..idx],
        None => without_domain,
    };
    let without_query = match without_fragment.find('?') {
        Some(idx) => &without_fragment[..idx],
        None => without_fragment,
    };
    if without_query.is_empty() {
        None
    } else if without_query.starts_with('/') {
        Some(without_query.to_string())
    } else {
        Some(format!("/{without_query}"))
    }
}

fn normalize_url_key(href: &str) -> String {
    let rel = to_relative_path(href).unwrap_or_else(|| href.to_string());
    rel.trim_start_matches('/').to_string()
}

fn parse_schema_org(document: &Html) -> (Option<usize>, Option<Availability>) {
    for script in document.select(&selectors::JSON_LD) {
        let raw = script.inner_html();
        let parsed: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(product) = find_product_node(&parsed) {
            let offers = product.get("offers");
            if let Some(offers) = offers {
                if let Some((price, availability)) = parse_offers(offers) {
                    return (price, availability);
                }
            }
        }
    }
    (None, None)
}

fn find_product_node<'a>(value: &'a Value) -> Option<&'a Value> {
    match value {
        Value::Array(items) => items.iter().find_map(find_product_node),
        Value::Object(map) => {
            if map
                .get("@type")
                .and_then(|t| t.as_str())
                .is_some_and(|t| t.eq_ignore_ascii_case("product"))
            {
                return Some(value);
            }
            if let Some(graph) = map.get("@graph") {
                return find_product_node(graph);
            }
            None
        }
        _ => None,
    }
}

fn parse_offers(offers: &Value) -> Option<(Option<usize>, Option<Availability>)> {
    match offers {
        Value::Array(items) => items.iter().find_map(parse_offers),
        Value::Object(map) => {
            let price = map
                .get("price")
                .and_then(|v| v.as_str().map(parse_price_str).flatten())
                .or_else(|| map.get("price").and_then(|v| v.as_u64()).map(|v| v as usize));
            let availability = map
                .get("availability")
                .and_then(|v| v.as_str())
                .and_then(parse_schema_availability);
            Some((price, availability))
        }
        _ => None,
    }
}

fn parse_schema_availability(raw: &str) -> Option<Availability> {
    let t = raw.to_lowercase();
    if t.contains("instock") {
        Some(Availability::Available)
    } else if t.contains("preorder") || t.contains("backorder") {
        Some(Availability::OnOrder)
    } else if t.contains("outofstock") || t.contains("discontinued") {
        Some(Availability::NotAvailable)
    } else {
        None
    }
}

pub async fn cleanup_missing_products(
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<usize, anyhow::Error> {
    let brands = parse_brands(options.clone()).await?;
    let models = parse_models(&brands, options.clone()).await?;
    let mut urls = collect_all_product_urls(&models, options.clone()).await?;

    if let Ok(categories) = parse_categories(options.clone()).await {
        if let Ok(subcategories) = parse_subcategories(&categories, options.clone()).await {
            if let Ok(extra) = collect_all_product_urls(&subcategories, options.clone()).await {
                urls.extend(extra);
            }
        }
    }

    let mut present: HashSet<String> = HashSet::new();
    for url in urls {
        let key = normalize_url_key(&url.0);
        if !key.is_empty() {
            present.insert(key);
        }
    }
    let manual_urls = {
        let opts = options.read().await;
        opts.manual_repo.list_urls().await.unwrap_or_default()
    };
    for url in manual_urls {
        let key = normalize_url_key(&url);
        if !key.is_empty() {
            present.insert(key);
        }
    }
    let repo = { options.read().await.repo.clone() };
    let products = repo.list().await?;
    let mut to_delete: Vec<String> = Vec::new();
    for product in products {
        let url_lower = product.url.0.to_lowercase();
        let is_manual = url_lower.contains("/manual/")
            || product
                .supplier
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
        let is_dt_product = url_lower.contains("/item/")
            || url_lower.contains("design-tuning.com/item/");
        if is_manual || !is_dt_product {
            continue;
        }
        let key = normalize_url_key(&product.url.0);
        if !key.is_empty() && !present.contains(&key) {
            to_delete.push(product.article);
        }
    }
    if to_delete.is_empty() {
        return Ok(0);
    }
    repo.delete_articles(&to_delete).await?;
    Ok(to_delete.len())
}

pub fn is_browser_check(s: &str) -> bool {
    s.contains("<title>Browser check, please wait ...</title>")
}

pub struct ParserService {
    opts: Arc<RwLock<ParsingOptions>>,
    pb_style: Option<ProgressStyle>,
    token: CancellationToken,
    stop_notify: Arc<Notify>,
    start_notify: Arc<Notify>,
    start_paused: bool,
    cleanup_status: Arc<RwLock<CleanupStatus>>,
}

impl ParserService {
    pub fn new(
        opts: ParsingOptions,
        pb_style: Option<ProgressStyle>,
        token: CancellationToken,
        start_paused: bool,
    ) -> Self {
        Self {
            opts: Arc::new(RwLock::new(opts)),
            pb_style,
            token,
            stop_notify: Arc::new(Notify::new()),
            start_notify: Arc::new(Notify::new()),
            start_paused,
            cleanup_status: Arc::new(RwLock::new(CleanupStatus {
                state: CleanupState::Idle,
                started_at: None,
                finished_at: None,
                removed: None,
                error: None,
            })),
        }
    }
}

impl Actor for ParserService {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.subscribe_system_async::<ConfigurationChanged>(ctx);
        let opts = self.opts.clone();
        let pb_style = self.pb_style.clone();
        let token = self.token.clone();
        let stop_notify = self.stop_notify.clone();
        let start_notify = self.start_notify.clone();
        let start_paused = self.start_paused;
        tokio::task::spawn_local(async move {
            let stop = Arc::new(AtomicBool::new(start_paused));
            tokio::task::spawn({
                let stop = stop.clone();
                let stop_notify = stop_notify.clone();
                async move {
                    loop {
                        stop_notify.notified().await;
                        stop.store(true, Ordering::SeqCst);
                    }
                }
            });
            match crate::cache::read_links(LINKS_PATH) {
                Ok(res) if !res.is_empty() => {
                    log::info!("Found {} cached links", res.len());
                    loop {
                        if stop.load(Ordering::SeqCst) {
                            start_notify.notified().await;
                            stop.store(false, Ordering::SeqCst);
                        }
                        let res = tokio::select! {
                            res = products_parsing(res.clone(), pb_style.clone(), opts.clone()) => res,
                            _ = stop_notify.notified() => continue,
                        };
                        if let Err(err) = res {
                            log::error!("Unable to parse dt products: {err}");
                        }
                        break;
                    }
                }
                Ok(_) => (),
                Err(err) => {
                    log::error!("Unable to read links from file: {err}");
                }
            };
            loop {
                if stop.load(Ordering::SeqCst) {
                    start_notify.notified().await;
                    stop.store(false, Ordering::SeqCst);
                }
                let res = tokio::select! {
                    res = work_cycle(opts.clone(), pb_style.clone(), token.clone()) => res,
                    _ = stop_notify.notified() => continue,
                };
                match res {
                    Ok(ControlFlow::Continue(())) => continue,
                    Ok(ControlFlow::Break(())) => break,
                    Err(err) => {
                        log::error!("Unable to parse dt products: {err}");
                        continue;
                    }
                }
            }
        });
        log::info!("DT parser started");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        log::info!("DT parser stopped");
    }
}

impl Handler<ConfigurationChanged> for ParserService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _msg: ConfigurationChanged, _ctx: &mut Self::Context) -> Self::Result {
        let fut = async move {};
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Pause> for ParserService {
    type Result = ();

    fn handle(&mut self, _: Pause, _ctx: &mut Self::Context) {
        let opts = self.opts.clone();
        actix::spawn(async move {
            let mut opts = opts.write().await;
            opts.stage = ParsingStage::Pause;
            opts.started_at = None;
        });
        self.stop_notify.notify_waiters();
    }
}

impl Handler<Resume> for ParserService {
    type Result = ();

    fn handle(&mut self, _: Resume, _ctx: &mut Self::Context) {
        self.start_notify.notify_waiters();
    }
}

impl Handler<GetProgress> for ParserService {
    type Result = ResponseActFuture<Self, Result<ParsingProgress, anyhow::Error>>;

    fn handle(&mut self, _: GetProgress, _ctx: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let fut = async move {
            let opts = opts.read().await;
            let now = OffsetDateTime::now_utc();
            let (started_at, elapsed, speed_per_min, eta, eta_at) = match (
                opts.started_at,
                &opts.stage,
                opts.progress_bar.as_ref().map(|p| p.position()).unwrap_or(0),
                opts.progress_bar.as_ref().and_then(|p| p.length()).unwrap_or(0),
            ) {
                (Some(started_at), stage, ready, total) if !matches!(stage, ParsingStage::Pause) => {
                    let elapsed_seconds = (now - started_at).whole_seconds();
                    let started_at_str = started_at.format(&time::format_description::well_known::Rfc3339).ok();
                    let elapsed_str = if elapsed_seconds > 0 {
                        Some(format_duration(elapsed_seconds))
                    } else {
                        None
                    };
                    if elapsed_seconds > 0 && ready > 0 {
                        let speed_per_sec = ready as f64 / elapsed_seconds as f64;
                        let speed_per_min = speed_per_sec * 60.0;
                        let remaining = total.saturating_sub(ready) as f64;
                        let eta_seconds = if speed_per_sec > 0.0 {
                            (remaining / speed_per_sec).ceil() as i64
                        } else {
                            0
                        };
                        let eta_str = Some(format_duration(eta_seconds));
                        let eta_at_str = (now + TimeDuration::seconds(eta_seconds))
                            .format(&time::format_description::well_known::Rfc3339)
                            .ok();
                        (
                            started_at_str,
                            elapsed_str,
                            Some(format_speed(speed_per_min)),
                            eta_str,
                            eta_at_str,
                        )
                    } else {
                        (started_at_str, elapsed_str, None, None, None)
                    }
                }
                _ => (None, None, None, None, None),
            };
            Ok(ParsingProgress {
                ready: opts
                    .progress_bar
                    .as_ref()
                    .map(|p| p.position())
                    .unwrap_or(0),
                total: opts
                    .progress_bar
                    .as_ref()
                    .and_then(|p| p.length())
                    .unwrap_or(0),
                stage: opts.stage.clone(),
                started_at,
                elapsed,
                speed_per_min,
                eta,
                eta_at,
            })
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Parse> for ParserService {
    type Result = ResponseActFuture<Self, Result<Product, ProductParsingError>>;

    fn handle(&mut self, Parse(link): Parse, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                let body = opts
                    .client
                    .get(format!("{link}"))
                    .send()
                    .await
                    .map_err(|err| ParsingError::Other(err.into()))?;
                let body: String = body.text().await?;
                let document = Html::parse_document(&body);
                let product = parse_product("", "", &link, document)?;
                opts.repo.save(product.clone()).await?;
                Ok(product)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ParsePage> for ParserService {
    type Result = ResponseActFuture<Self, Result<Vec<String>, ProductParsingError>>;

    fn handle(&mut self, ParsePage(link): ParsePage, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                let client = reqwest::Client::new();
                let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
                let url = &opts.url;
                let opts = Arc::new(RwLock::new(ParsingOptions {
                    client: reqwest_middleware::ClientBuilder::new(client)
                        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                        .build(),
                    ..opts.clone()
                }));
                let res = parse_product_list_page(&link, opts)
                    .await?
                    .into_iter()
                    .map(|a| a.0)
                    .collect();
                Ok(res)
            }
            .into_actor(self),
        )
    }
}

impl Handler<ProductInfo> for ParserService {
    type Result = ResponseActFuture<Self, Result<Option<Product>, anyhow::Error>>;

    fn handle(&mut self, ProductInfo(id): ProductInfo, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        Box::pin(
            async move {
                let opts = opts.read().await;
                opts.repo.get_one(&id).await
            }
            .into_actor(self),
        )
    }
}

impl Handler<CleanupMissing> for ParserService {
    type Result = ResponseActFuture<Self, Result<usize, anyhow::Error>>;

    fn handle(&mut self, _: CleanupMissing, _: &mut Self::Context) -> Self::Result {
        let opts = self.opts.clone();
        let cleanup_status = self.cleanup_status.clone();
        Box::pin(
            async move {
                let now = OffsetDateTime::now_utc();
                let stage = { opts.read().await.stage.clone() };
                if !matches!(stage, ParsingStage::Pause) {
                    let mut status = cleanup_status.write().await;
                    status.state = CleanupState::Error;
                    status.started_at = Some(now);
                    status.finished_at = Some(now);
                    status.removed = None;
                    status.error = Some("DT parsing is running. Pause it before cleanup.".to_string());
                    return Err(anyhow!(
                        "DT parsing is running. Pause it before cleanup."
                    ));
                }
                {
                    let mut status = cleanup_status.write().await;
                    status.state = CleanupState::Running;
                    status.started_at = Some(now);
                    status.finished_at = None;
                    status.removed = None;
                    status.error = None;
                }
                let result = cleanup_missing_products(opts).await;
                let finished_at = OffsetDateTime::now_utc();
                let mut status = cleanup_status.write().await;
                match result {
                    Ok(removed) => {
                        status.state = CleanupState::Done;
                        status.finished_at = Some(finished_at);
                        status.removed = Some(removed);
                        status.error = None;
                        Ok(removed)
                    }
                    Err(err) => {
                        status.state = CleanupState::Error;
                        status.finished_at = Some(finished_at);
                        status.removed = None;
                        status.error = Some(err.to_string());
                        Err(err)
                    }
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetCleanupStatus> for ParserService {
    type Result = ResponseActFuture<Self, CleanupStatusInfo>;

    fn handle(&mut self, _: GetCleanupStatus, _: &mut Self::Context) -> Self::Result {
        let cleanup_status = self.cleanup_status.clone();
        Box::pin(
            async move {
                let status = cleanup_status.read().await;
                cleanup_status_info(&status)
            }
            .into_actor(self),
        )
    }
}

pub async fn work_cycle(
    options: Arc<RwLock<ParsingOptions>>,
    pb_style: Option<ProgressStyle>,
    token: CancellationToken,
) -> Result<ControlFlow<(), ()>, anyhow::Error> {
    {
        let mut options = options.write().await;
        options.stage = ParsingStage::Brands;
        options.started_at = Some(OffsetDateTime::now_utc());
    }
    let brands = tokio::select! {
        brands = parse_brands(options.clone()) => brands?,
        _ = token.cancelled() => return Ok(ControlFlow::Break(())),
    };

    log::info!("{} brands", brands.len());
    log::info!("{:?}", brands.iter().map(|(_, v)| v).collect::<Vec<_>>());
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(brands.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);

    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::Models;
    }

    let models = match cache::read_models(MODELS_PATH) {
        Ok(m) if m.is_empty() => {
            let models;
            loop {
                let res = tokio::select! {
                    res = parse_models(&brands, options.clone()) => res,
                    _ = token.cancelled() => return Ok(ControlFlow::Break(())),
                };
                match res {
                    Ok(m) => {
                        models = m;
                        break;
                    }
                    Err(err) => {
                        log::error!("Unable to parse models: {err:?}");
                        continue;
                    }
                }
            }
            if let Err(err) = cache::write_models(
                MODELS_PATH,
                models
                    .clone()
                    .into_iter()
                    .map(|(Url(url), model, brand)| cache::Model {
                        url,
                        model,
                        brand: brand.clone(),
                    })
                    .collect(),
            ) {
                log::warn!("Unable to write models: {err}");
            }
            models
        }
        Ok(models) => models
            .into_iter()
            .filter_map(|m| {
                let brand = brands.iter().map(|(_, b)| b).find(|&b| *b == m.brand);
                match brand {
                    Some(brand) => Some((Url(m.url), m.model, brand)),
                    None => {
                        log::warn!("Unable to find brand for model {}", m.model);
                        None
                    }
                }
            })
            .collect(),
        Err(err) => {
            log::error!("Unable to read models: {err}");
            return Ok(ControlFlow::Continue(()));
        }
    };
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    log::info!("{} total models", models.len());
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(models.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);

    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::ProductList;
    }
    let res;
    loop {
        let r = tokio::select! {
            r = parse_product_lists(&models, options.clone()) => r,
            _ = token.cancelled() => return Ok(ControlFlow::Break(())),
        };
        match r {
            Ok(r) => {
                res = r;
                break;
            }
            Err(err) => {
                log::error!("Unable to parse product lists: {err:?}");
                if let Some(pb) = &pb {
                    pb.reset();
                }
                continue;
            }
        }
    }
    if let Some(pb) = pb.clone() {
        pb.finish_and_clear();
    }
    let (repo, manual_repo) = {
        let opts = options.read().await;
        (opts.repo.clone(), opts.manual_repo.clone())
    };
    let mut res = res
        .into_iter()
        .map(|(url, model, brand)| (url, model.clone(), brand.clone()))
        .collect::<Vec<_>>();
    let mut seen = HashSet::with_capacity(res.len());
    for (url, _, _) in &res {
        seen.insert(normalize_url_key(&url.0));
    }
    let all_products = repo.list().await?;
    let mut url_map: HashMap<String, (String, String)> = HashMap::new();
    for p in &all_products {
        let key = normalize_url_key(&p.url.0);
        if key.is_empty() {
            continue;
        }
        url_map
            .entry(key)
            .or_insert_with(|| (p.model.0.clone(), p.brand.clone()));
    }
    let stale_products = all_products
        .iter()
        .filter(|p| p.is_outdated())
        .filter(|p| !p.url.0.trim().is_empty());
    for product in stale_products {
        let key = normalize_url_key(&product.url.0);
        if seen.insert(key) {
            res.push((product.url.clone(), product.model.0.clone(), product.brand.clone()));
        }
    }
    if let Ok(manual_urls) = manual_repo.list_urls().await {
        for raw in manual_urls {
            let key = normalize_url_key(&raw);
            if key.is_empty() || !seen.insert(key.clone()) {
                continue;
            }
            let (model, brand) = url_map
                .get(&key)
                .cloned()
                .unwrap_or_else(|| ("Універсальна".to_string(), "O&P Tuning".to_string()));
            let rel = to_relative_path(&raw).unwrap_or_else(|| raw.clone());
            res.push((Url(rel), model, brand));
        }
    }
    let fut = async {
        let categories = parse_categories(options.clone()).await?;
        let subcategories = parse_subcategories(&categories, options.clone()).await;
        match subcategories {
            Ok(r) => match parse_product_lists(&r, options.clone()).await {
                Ok(res) => {
                    products_parsing(res, pb_style.clone(), options.clone()).await?;
                }
                Err(err) => {
                    log::error!("Unable to parse product lists in subcategory: {err:?}");
                }
            },
            Err(err) => {
                log::error!("Unable to parse subcategories: {err:?}");
            }
        };
        log::info!("{} total links", res.len());
        if products_parsing(res, pb_style.clone(), options).await? {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    };
    tokio::select! {
        r = fut => r,
        _ = token.cancelled() => Ok(ControlFlow::Break(())),
    }
}

pub async fn products_parsing<M, B>(
    res: Vec<(Url, M, B)>,
    pb_style: Option<ProgressStyle>,
    options: Arc<RwLock<ParsingOptions>>,
) -> Result<bool, anyhow::Error>
where
    M: AsRef<str> + std::fmt::Display + Into<String> + Clone,
    B: AsRef<str> + std::fmt::Display + Into<String> + Clone,
{
    let token = CancellationToken::new();
    let (tx, mut rx) = mpsc::channel(100);
    let r = res
        .iter()
        .cloned()
        .map(|(u, m, b)| (u.clone(), m.into(), b.into()))
        .collect::<Vec<_>>();
    let t = token.clone();
    tokio::spawn(async move {
        let mut res = r;
        let token = t;
        tokio::select! {
            sig = signal::ctrl_c() => match sig {
                Ok(()) => {
                    token.cancel();
                    let count = rx.recv().await;
                    if let Some(count) = count {
                        if res.len() > count {
                            res.drain(0..count);
                        } else {
                            res.drain(0..);
                        }
                    }
                    #[allow(clippy::unwrap_used)]
                    cache::write_links(LINKS_PATH, &res).unwrap();
                },
                Err(err) => {
                    log::error!("Unable to listen for shutdown: {err}");
                }
            },
            count = (rx.recv()) => {
                if let Some(count) = count {
                    if res.len() > count {
                        res.drain(0..count);
                    } else {
                        res.drain(0..);
                    }
                }
                #[allow(clippy::unwrap_used)]
                cache::write_links(LINKS_PATH, &res).unwrap();
            }
        }
    });
    let total_chunks = res.len() / CHUNK_SIZE;
    let pb = pb_style
        .clone()
        .map(|s| {
            let p = ProgressBar::new(res.len() as u64).with_style(s);
            p.enable_steady_tick(Duration::from_millis(500));
            p
        })
        .map(Arc::new);
    {
        let mut options = options.write().await;
        options.progress_bar = pb.clone();
        options.stage = ParsingStage::Products;
        if options.started_at.is_none() {
            options.started_at = Some(OffsetDateTime::now_utc());
        }
    }
    for (i, links) in res.chunks(CHUNK_SIZE).enumerate() {
        let r = tokio::select! {
            r = parse_products(links, options.clone()) => r,
            _ = token.cancelled() => {
                if let Err(err) = tx.send(CHUNK_SIZE).await {
                    log::error!("Unable to send chunk via tx: {err}");
                }
                return Ok(true)
            },
        };
        if let Err(err) = r {
            log::error!("Unable to parse products chunk {i}: {err}");
        } else if pb.as_ref().map(|pb| pb.is_hidden()).unwrap_or(true) {
            log::info!("Chunk {i} of {total_chunks} done");
        }
    }
    if let Err(err) = cache::clean_links(LINKS_PATH) {
        log::error!("Unable to clean links: {err}");
    }
    if let Some(pb) = pb {
        pb.finish_and_clear();
    }
    if let Err(err) = tx.send(res.len()).await {
        log::error!("Unable to send chunk via tx: {err}");
    }
    Ok(false)
}

static MODELS_PATH: &str = "models.yml";
static LINKS_PATH: &str = "links.yml";
static CHUNK_SIZE: usize = 50;

pub fn format_link(s: &str) -> &str {
    if let Some(s) = s.strip_prefix('/') {
        s
    } else {
        s
    }
}
