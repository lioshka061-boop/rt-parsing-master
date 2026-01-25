use actix::prelude::*;
use anyhow::anyhow;
use currency_service::{CurrencyService, ListRates};
use log_error::LogError;
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{broadcast, Notify, RwLock};
use tokio::time::sleep;
use typesafe_repository::IdentityOf;

use crate::dt;
use crate::category_auto;
use crate::external_import::Vendored;
use crate::product_category;
use crate::product_category_auto;
use crate::restal;
use crate::shop_product;
use crate::site_publish;
use crate::import_throttle;
use crate::uploader;
use crate::xlsx;
use crate::{Model, Url};
use rt_types::category::{By, Category, CategoryRepository};
use rt_types::shop::service::ShopService;
use rt_types::shop::{
    MissingProductPolicy, SiteImportEntry, SiteImportOptions, SiteImportSource,
    SiteImportUpdateFields,
};
use rt_types::{Availability, DescriptionOptions};

const MAX_RETRY_COUNT: usize = 3;

#[derive(Debug, Clone, Serialize)]
pub struct ProgressInfo {
    pub stage: String,
    pub done: usize,
    pub total: usize,
}

#[derive(Clone, Debug, Serialize)]
pub enum SiteImportStatus {
    Enqueued,
    InProgress,
    Success,
    Suspended,
    Failure(String),
}

impl std::fmt::Display for SiteImportStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enqueued => write!(f, "В очереди"),
            Self::InProgress => write!(f, "В процессе"),
            Self::Success => write!(f, "Импорт успешно завершен"),
            Self::Suspended => write!(f, "Импорт приостановлен"),
            Self::Failure(msg) => write!(f, "Импорт завершен с ошибкой: {msg}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SiteImport {
    pub shop: IdentityOf<rt_types::shop::Shop>,
    pub status: SiteImportStatus,
    pub entry: SiteImportEntry,
    pub progress: Option<ProgressInfo>,
    start: Arc<Notify>,
    suspend_tx: broadcast::Sender<bool>,
    stop: Arc<Notify>,
    armed: bool,
}

impl SiteImport {
    pub fn status(&self) -> &SiteImportStatus {
        &self.status
    }
}

pub struct SiteImportService {
    client: Client,
    entries: Vec<(IdentityOf<rt_types::shop::Shop>, SiteImportEntry)>,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    category_repo: Arc<dyn CategoryRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
    shop_service: Addr<ShopService>,
    currency_service: Addr<CurrencyService>,
    import: HashMap<String, Arc<RwLock<SiteImport>>>,
}

impl SiteImportService {
    pub fn new(
        client: Client,
        entries: Vec<(IdentityOf<rt_types::shop::Shop>, SiteImportEntry)>,
        dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
        shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
        category_repo: Arc<dyn CategoryRepository>,
        product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
        shop_service: Addr<ShopService>,
        currency_service: Addr<CurrencyService>,
    ) -> Self {
        Self {
            client,
            entries,
            dt_repo,
            shop_product_repo,
            category_repo,
            product_category_repo,
            shop_service,
            currency_service,
            import: HashMap::new(),
        }
    }

    async fn set_progress(
        import: &Arc<RwLock<SiteImport>>,
        stage: impl Into<String>,
        done: usize,
        total: usize,
    ) {
        let mut ex = import.write().await;
        ex.progress = Some(ProgressInfo {
            stage: stage.into(),
            done,
            total,
        });
    }

    pub async fn start_import_cycle(
        client: Client,
        import: Arc<RwLock<SiteImport>>,
        dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
        shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
        category_repo: Arc<dyn CategoryRepository>,
        product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
        currency_service: Addr<CurrencyService>,
    ) {
        let (mut entry, start_notify, stop_notify, shop, mut rx) = {
            let e = import.read().await;
            (
                e.entry.clone(),
                e.start.clone(),
                e.stop.clone(),
                e.shop.clone(),
                e.suspend_tx.subscribe(),
            )
        };
        let mut retry_count = 0;
        loop {
            {
                let mut state = import.write().await;
                state.status = SiteImportStatus::Enqueued;
                if entry != state.entry {
                    entry = state.entry.clone();
                }
                if let Some(true) = rx.try_recv().log_error("Unable to read suspend rx") {
                    state.status = SiteImportStatus::Suspended;
                    drop(state);
                    loop {
                        match rx.recv().await.log_error("Unable to read suspend rx") {
                            Some(false) => break,
                            _ => continue,
                        }
                    }
                    continue;
                }
                if !state.armed {
                    state.progress = None;
                    drop(state);
                    tokio::select! {
                        _ = start_notify.notified() => {
                            let mut state = import.write().await;
                            state.armed = true;
                        }
                        _ = stop_notify.notified() => return,
                    }
                    continue;
                }
            }

            let _permit = import_throttle::acquire_import_permit().await;
            let (res, _) = tokio::join!(
                do_import(
                    &entry,
                    shop,
                    client.clone(),
                    dt_repo.clone(),
                    shop_product_repo.clone(),
                    category_repo.clone(),
                    product_category_repo.clone(),
                    currency_service.clone(),
                    import.clone(),
                ),
                async {
                    let mut import = import.write().await;
                    import.status = SiteImportStatus::InProgress;
                }
            );

            let status = match res {
                Ok(_) => {
                    retry_count = 0;
                    SiteImportStatus::Success
                }
                Err(err) => {
                    log::error!("Site import failed: {err}");
                    if retry_count < MAX_RETRY_COUNT {
                        retry_count += 1;
                        continue;
                    }
                    retry_count = 0;
                    SiteImportStatus::Failure(err.to_string())
                }
            };

            {
                let mut import = import.write().await;
                import.status = status;
                import.progress = None;
            }

            tokio::select! {
                _ = sleep(entry.update_rate) => (),
                _ = start_notify.notified() => (),
                _ = stop_notify.notified() => return,
            }
        }
    }
}

impl Actor for SiteImportService {
    type Context = Context<Self>;

    fn start(mut self) -> Addr<Self>
    where
        Self: Actor<Context = Context<Self>>,
    {
        for (shop, entry) in &self.entries {
            let (suspend_tx, _) = broadcast::channel(20);
            self.import.insert(
                entry.generate_hash().to_string(),
                Arc::new(RwLock::new(SiteImport {
                    shop: *shop,
                    entry: entry.clone(),
                    progress: None,
                    start: Arc::new(Notify::new()),
                    stop: Arc::new(Notify::new()),
                    suspend_tx,
                    status: SiteImportStatus::Enqueued,
                    armed: true,
                })),
            );
        }
        for i in self.import.values() {
            tokio::task::spawn_local(SiteImportService::start_import_cycle(
                self.client.clone(),
                i.clone(),
                self.dt_repo.clone(),
                self.shop_product_repo.clone(),
                self.category_repo.clone(),
                self.product_category_repo.clone(),
                self.currency_service.clone(),
            ));
        }
        Context::new().run(self)
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {}
}

#[derive(Message)]
#[rtype(result = "Option<SiteImport>")]
pub struct GetStatus(pub String);

#[derive(Message)]
#[rtype(result = "HashMap<String, SiteImport>")]
pub struct GetAllStatus(pub IdentityOf<rt_types::shop::Shop>);

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct Add(pub IdentityOf<rt_types::shop::Shop>, pub SiteImportEntry);

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct Update(pub IdentityOf<rt_types::shop::Shop>, pub String, pub SiteImportEntry);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Remove(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Start(pub String);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct SuspendByShop(pub IdentityOf<rt_types::shop::Shop>, pub bool);

impl Handler<GetStatus> for SiteImportService {
    type Result = ResponseActFuture<Self, Option<SiteImport>>;

    fn handle(&mut self, GetStatus(hash): GetStatus, _ctx: &mut Self::Context) -> Self::Result {
        let import = self.import.get(&hash).cloned();
        let fut = async move {
            if let Some(import) = import {
                Some(import.read().await.clone())
            } else {
                None
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetAllStatus> for SiteImportService {
    type Result = ResponseActFuture<Self, HashMap<String, SiteImport>>;

    fn handle(
        &mut self,
        GetAllStatus(shop): GetAllStatus,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let import = self.import.clone();
        let fut = async move {
            let mut res = HashMap::new();
            for (h, e) in import.iter() {
                let e = e.read().await;
                if e.shop == shop {
                    res.insert(h.clone(), e.clone());
                }
            }
            res
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Add> for SiteImportService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(&mut self, Add(shop_id, mut entry): Add, _ctx: &mut Self::Context) -> Self::Result {
        let shop_service = self.shop_service.clone();
        let dt_repo = self.dt_repo.clone();
        let shop_product_repo = self.shop_product_repo.clone();
        let category_repo = self.category_repo.clone();
        let product_category_repo = self.product_category_repo.clone();
        let client = self.client.clone();
        let currency_service = self.currency_service.clone();

        let fut = async move {
            let mut shop = shop_service
                .send(rt_types::shop::service::Get(shop_id))
                .await??
                .ok_or(anyhow!("Shop not found"))?;
            let supplier_key = entry.supplier_key();
            if let Some(key) = supplier_key.as_ref() {
                if shop
                    .site_import_entries
                    .iter()
                    .any(|e| e.supplier_key().as_deref() == Some(key))
                {
                    return Err(anyhow!("Постачальник уже налаштований"));
                }
            }
            let now = OffsetDateTime::now_utc();
            entry.created_time = now;
            entry.edited_time = now;
            shop.site_import_entries.push(entry.clone());
            shop_service.send(rt_types::shop::service::Update(shop)).await??;
            let hash = entry.generate_hash().to_string();
            Ok((hash, entry, shop_id))
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _ctx| {
            let (hash, entry, shop_id) = match res {
                Ok(val) => val,
                Err(err) => return Err(err),
            };
            let (suspend_tx, _) = broadcast::channel(20);
            let import_entry = Arc::new(RwLock::new(SiteImport {
                shop: shop_id,
                entry,
                progress: None,
                start: Arc::new(Notify::new()),
                stop: Arc::new(Notify::new()),
                suspend_tx,
                status: SiteImportStatus::Enqueued,
                armed: true,
            }));
            act.import.insert(hash.clone(), import_entry.clone());
            tokio::task::spawn_local(SiteImportService::start_import_cycle(
                client,
                import_entry,
                dt_repo,
                shop_product_repo,
                category_repo,
                product_category_repo,
                currency_service,
            ));
            Ok(hash)
        }))
    }
}

impl Handler<Update> for SiteImportService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(
        &mut self,
        Update(shop_id, hash, mut entry): Update,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let shop_service = self.shop_service.clone();
        let hash_for_async = hash.clone();
        let fut = async move {
            let mut shop = shop_service
                .send(rt_types::shop::service::Get(shop_id))
                .await??
                .ok_or(anyhow!("Shop not found"))?;
            let supplier_key = entry.supplier_key();
            if let Some(key) = supplier_key.as_ref() {
                if shop
                    .site_import_entries
                    .iter()
                    .any(|e| {
                        e.supplier_key().as_deref() == Some(key)
                            && e.generate_hash().to_string() != hash_for_async
                    })
                {
                    return Err(anyhow!("Постачальник уже налаштований"));
                }
            }
            entry.edited_time = OffsetDateTime::now_utc();
            let mut updated_hash = None;
            for e in shop.site_import_entries.iter_mut() {
                if e.generate_hash().to_string() == hash_for_async {
                    *e = entry.clone();
                    updated_hash = Some(e.generate_hash().to_string());
                    break;
                }
            }
            let updated_hash = updated_hash.ok_or(anyhow!("Site import entry not found"))?;
            shop_service.send(rt_types::shop::service::Update(shop)).await??;
            Ok((updated_hash, entry))
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _ctx| {
            let (updated_hash, entry) = match res {
                Ok(val) => val,
                Err(err) => return Err(err),
            };
            let import_entry = act
                .import
                .get(&hash)
                .cloned()
                .ok_or_else(|| anyhow!("Import entry not found"))?;
            actix::spawn(async move {
                let mut i = import_entry.write().await;
                i.entry = entry;
            });
            if updated_hash != hash {
                if let Some(entry) = act.import.remove(&hash) {
                    act.import.insert(updated_hash.clone(), entry);
                }
            }
            Ok(updated_hash)
        }))
    }
}

impl Handler<Remove> for SiteImportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Remove(hash): Remove, _ctx: &mut Self::Context) -> Self::Result {
        let shop_service = self.shop_service.clone();
        let entry = self.import.remove(&hash);
        let fut = async move {
            let entry = entry.ok_or(anyhow!("Entry not found"))?;
            let shop_id = { entry.read().await.shop };
            let mut shop = shop_service
                .send(rt_types::shop::service::Get(shop_id))
                .await??
                .ok_or(anyhow!("Shop not found"))?;
            shop.site_import_entries.retain(|e| e.generate_hash().to_string() != hash);
            shop_service.send(rt_types::shop::service::Update(shop)).await??;
            entry.write().await.stop.notify_one();
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Start> for SiteImportService {
    type Result = ();

    fn handle(&mut self, Start(hash): Start, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(import) = self.import.get(&hash).cloned() {
            actix::spawn(async move {
                let start = {
                    let mut import = import.write().await;
                    import.armed = true;
                    import.start.clone()
                };
                start.notify_one();
            });
        }
    }
}

impl Handler<SuspendByShop> for SiteImportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(
        &mut self,
        SuspendByShop(shop_id, suspend): SuspendByShop,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let import = self.import.clone();
        let fut = async move {
            for entry in import.values() {
                let entry = entry.read().await;
                if entry.shop == shop_id {
                    entry.suspend_tx.send(suspend).ok();
                }
            }
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

#[derive(Clone, Debug)]
struct ImportProduct {
    product: dt::product::Product,
}

async fn do_import(
    entry: &SiteImportEntry,
    shop_id: IdentityOf<rt_types::shop::Shop>,
    client: Client,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    category_repo: Arc<dyn CategoryRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
    currency_service: Addr<CurrencyService>,
    import_handle: Arc<RwLock<SiteImport>>,
) -> Result<(), anyhow::Error> {
    let supplier_key = entry.supplier_key();
    let rates = match currency_service.send(ListRates).await {
        Ok(rates) => rates
            .into_iter()
            .map(|(k, v)| (k, v * dec!(1.07)))
            .collect::<HashMap<_, _>>(),
        Err(err) => {
            log::error!("Unable to list rates: {err}");
            HashMap::new()
        }
    };
    let categories = category_repo.select(&By(shop_id)).await.unwrap_or_default();
    let categories_by_id: HashMap<_, _> = categories
        .iter()
        .map(|c| (c.id, c.name.clone()))
        .collect();
    let brand_set = categories
        .iter()
        .filter(|c| c.parent_id.is_none())
        .map(|c| normalize_key(&c.name))
        .collect::<HashSet<_>>();
    let product_categories = product_category_repo
        .select(&product_category::ByShop(shop_id))
        .await
        .unwrap_or_default();
    let category_matcher = if product_categories.is_empty() {
        None
    } else {
        Some(product_category_auto::CategoryMatcher::new(&product_categories))
    };
    let mut site_category_by_article = if category_matcher.is_some() {
        shop_product_repo
            .list_by_shop(shop_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p.article.to_lowercase(), p.site_category_id))
            .collect::<HashMap<_, _>>()
    } else {
        HashMap::new()
    };

    let existing_products = if matches!(entry.source, SiteImportSource::Parsing { .. })
        || !matches!(entry.options.missing_policy, MissingProductPolicy::Keep)
    {
        Some(dt_repo.list().await.unwrap_or_default())
    } else {
        None
    };

    let import_products = match &entry.source {
        SiteImportSource::Parsing { supplier } => {
            let supplier = supplier.trim().to_lowercase();
            let source = existing_products.clone().unwrap_or_default();
            source
                .into_iter()
                .filter(|p| matches_supplier(p, Some(&supplier)))
                .filter(|p| !entry.options.transform.only_available || p.available == Availability::Available)
                .map(|p| {
                    let mut p = apply_transform_dt_product(
                        p,
                        entry.options.clone(),
                        &rates,
                        shop_id,
                    );
                    p.supplier = Some(supplier.clone());
                    ImportProduct { product: p }
                })
                .collect::<Vec<_>>()
        }
        SiteImportSource::Xml { link, vendor_name } => {
            let vendor = vendor_name
                .as_ref()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .or_else(|| supplier_key.clone())
                .unwrap_or_else(|| "xml".to_string());
            let parsed = uploader::download_from_link(link, client.clone())
                .await
                .map_err(|err| anyhow!("Unable to load XML: {err:?}"))?;
            let items = match parsed {
                uploader::DownloadResult::Offers(offers) => {
                    rt_types::product::convert(
                        offers
                            .into_iter()
                            .map(Vendored::with_vendor(vendor.clone())),
                    )
                    .collect::<Vec<_>>()
                }
                uploader::DownloadResult::Items(items) => {
                    rt_types::product::convert(
                        items
                            .into_iter()
                            .map(Vendored::with_vendor(vendor.clone())),
                    )
                    .collect::<Vec<_>>()
                }
            };
            items
                .into_iter()
                .filter(|p| !entry.options.transform.only_available || p.available == Availability::Available)
                .filter_map(|p| {
                    build_import_product(
                        p,
                        supplier_key.clone(),
                        &entry.options,
                        &rates,
                        &categories,
                        &categories_by_id,
                        shop_id,
                    )
                })
                .map(|product| ImportProduct { product })
                .collect::<Vec<_>>()
        }
        SiteImportSource::RestalApi => {
            let key = site_publish::load_restal_key(&shop_id).unwrap_or_default();
            let mut start = 0usize;
            let limit = 500usize;
            let mut items = Vec::new();
            loop {
                let batch = restal::fetch_products_with_key(&key, start, limit).await?;
                if batch.is_empty() {
                    break;
                }
                let batch_len = batch.len();
                for p in batch.iter() {
                    if let Some(mapped) = map_restal_product(&p, &categories, &entry.options, &rates, shop_id) {
                        items.push(ImportProduct { product: mapped });
                    }
                }
                if batch_len < limit {
                    break;
                }
                start += limit;
            }
            items
        }
    };

    let total = import_products.len();
    SiteImportService::set_progress(&import_handle, "Імпорт товарів", 0, total).await;

    let mut existing_map = existing_products
        .unwrap_or_default()
        .into_iter()
        .map(|p| (p.article.to_lowercase(), p))
        .collect::<HashMap<_, _>>();

    let append_images = entry.options.append_images;
    for (idx, item) in import_products.into_iter().enumerate() {
        let mut incoming = item.product;
        let article_key = incoming.article.to_lowercase();
        let existing = existing_map.remove(&article_key);
        let mut merged =
            merge_product(existing, &mut incoming, &entry.options.update_fields, append_images);
        apply_car_meta_autofill(&mut merged, &categories, &brand_set);
        if let Some(matcher) = category_matcher.as_ref() {
            let has_site_category = site_category_by_article
                .get(&article_key)
                .and_then(|id| *id)
                .is_some();
            if !has_site_category {
                let haystack = product_category_auto::build_haystack(
                    &merged.title,
                    merged.description.as_deref().unwrap_or_default(),
                );
                if let Some(cat_id) = matcher.guess(&haystack) {
                    shop_product_repo
                        .set_site_category(shop_id, &merged.article, Some(cat_id))
                        .await?;
                    site_category_by_article.insert(article_key.clone(), Some(cat_id));
                }
            }
        }
        dt_repo.save(merged).await?;
        if idx % 50 == 0 || idx + 1 == total {
            SiteImportService::set_progress(&import_handle, "Імпорт товарів", idx + 1, total)
                .await;
        }
    }

    if let (Some(supplier), policy) = (supplier_key.as_ref(), &entry.options.missing_policy) {
        if !matches!(policy, MissingProductPolicy::Keep) {
            let missing = existing_map
                .into_values()
                .filter(|p| matches_supplier(p, Some(supplier)))
                .map(|p| p.article)
                .collect::<Vec<_>>();
            apply_missing_policy(
                policy,
                shop_id,
                supplier,
                missing,
                dt_repo,
                shop_product_repo,
            )
            .await?;
        }
    }

    Ok(())
}

fn matches_supplier(p: &dt::product::Product, supplier: Option<&str>) -> bool {
    let supplier = match supplier {
        Some(s) if !s.trim().is_empty() => s.trim().to_lowercase(),
        _ => return false,
    };
    if let Some(p_supplier) = p.supplier.as_ref().map(|s| s.trim().to_lowercase()) {
        return p_supplier == supplier;
    }
    site_publish::detect_supplier(p)
        .map(|s| s == supplier)
        .unwrap_or(false)
}

fn merge_product(
    existing: Option<dt::product::Product>,
    incoming: &mut dt::product::Product,
    update_fields: &SiteImportUpdateFields,
    append_images: bool,
) -> dt::product::Product {
    match existing {
        None => incoming.clone(),
        Some(mut current) => {
            if update_fields.title_ru {
                current.title = incoming.title.clone();
            }
            if update_fields.title_ua {
                current.title_ua = incoming.title_ua.clone();
            }
            if update_fields.description_ru {
                current.description = incoming.description.clone();
            }
            if update_fields.description_ua {
                current.description_ua = incoming.description_ua.clone();
            }
            if update_fields.price {
                current.price = incoming.price;
            }
            if incoming.source_price.is_some() {
                current.source_price = incoming.source_price;
            }
            if update_fields.images {
                current.images = if append_images {
                    merge_images(&current.images, &incoming.images)
                } else {
                    incoming.images.clone()
                };
            }
            if update_fields.availability {
                current.available = incoming.available.clone();
            }
            if update_fields.quantity {
                current.quantity = incoming.quantity;
            }
            if update_fields.attributes {
                current.attributes = incoming.attributes.clone();
            }
            if update_fields.discounts {
                current.discount_percent = incoming.discount_percent;
            }
            current.supplier = incoming.supplier.clone().or(current.supplier.clone());
            current.last_visited = incoming.last_visited;
            current
        }
    }
}

fn merge_images(existing: &[String], incoming: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut res = Vec::new();
    for img in existing {
        if seen.insert(img.clone()) {
            res.push(img.clone());
        }
    }
    for img in incoming {
        if seen.insert(img.clone()) {
            res.push(img.clone());
        }
    }
    res
}

const CAR_HINT_LIMIT: usize = 800;
const BRAND_ATTR_KEYS: [&str; 8] = [
    "марка",
    "бренд",
    "brand",
    "make",
    "manufacturer",
    "mfr",
    "makeauto",
    "brandauto",
];
const MODEL_ATTR_KEYS: [&str; 6] = ["модель", "model", "modelauto", "vehiclemodel", "carmodel", "modelcar"];
const CAR_ATTR_KEYS: [&str; 8] = [
    "авто",
    "автомобиль",
    "автомобіль",
    "vehicle",
    "car",
    "compatibility",
    "vehiclefit",
    "carfit",
];

fn normalize_key(input: &str) -> String {
    input
        .to_lowercase()
        .replace(['_', '/', '\\', '—', '-', '–'], " ")
        .replace(|c: char| !c.is_alphanumeric() && !c.is_whitespace(), " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
}

fn truncate_chars(input: &str, max: usize) -> &str {
    if input.is_empty() || input.len() <= max {
        return input;
    }
    let mut end = 0usize;
    let mut count = 0usize;
    for (idx, ch) in input.char_indices() {
        if count >= max {
            break;
        }
        end = idx + ch.len_utf8();
        count += 1;
    }
    &input[..end]
}

fn attr_lookup(attrs: &HashMap<String, String>, needles: &[&str]) -> Option<String> {
    for (key, value) in attrs {
        let key_norm = normalize_key(key);
        if needles.iter().any(|needle| key_norm.contains(needle)) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn is_brand_invalid(brand: &str, supplier: Option<&str>, brand_set: &HashSet<String>) -> bool {
    let brand_norm = normalize_key(brand);
    if brand_norm.is_empty() || brand_norm == "інше" || brand_norm == "other" {
        return true;
    }
    if let Some(supplier) = supplier {
        if normalize_key(supplier) == brand_norm {
            return true;
        }
    }
    if !brand_set.is_empty() && !brand_set.contains(&brand_norm) {
        return true;
    }
    false
}

fn is_model_invalid(model: &str, brand: &str) -> bool {
    let model_norm = normalize_key(model);
    if model_norm.is_empty() || model_norm == "інше" || model_norm == "other" {
        return true;
    }
    let brand_norm = normalize_key(brand);
    if !brand_norm.is_empty() && model_norm == brand_norm {
        return true;
    }
    false
}

fn build_car_hint(
    product: &dt::product::Product,
    brand_set: &HashSet<String>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(desc) = product.description.as_deref() {
        let desc = truncate_chars(desc.trim(), CAR_HINT_LIMIT);
        if !desc.is_empty() {
            parts.push(desc.to_string());
        }
    }
    if let Some(attrs) = product.attributes.as_ref() {
        if let Some(val) = attr_lookup(attrs, &BRAND_ATTR_KEYS) {
            parts.push(val);
        }
        if let Some(val) = attr_lookup(attrs, &MODEL_ATTR_KEYS) {
            parts.push(val);
        }
        if let Some(val) = attr_lookup(attrs, &CAR_ATTR_KEYS) {
            parts.push(val);
        }
    }
    let brand_norm = normalize_key(&product.brand);
    if !product.brand.trim().is_empty()
        && (brand_set.is_empty() || brand_set.contains(&brand_norm))
    {
        parts.push(product.brand.trim().to_string());
    }
    let model = product.model.0.trim();
    if !model.is_empty() && normalize_key(model) != brand_norm {
        parts.push(model.to_string());
    }
    parts.join(" ")
}

fn apply_car_meta_autofill(
    product: &mut dt::product::Product,
    categories: &[Category],
    brand_set: &HashSet<String>,
) {
    let brand_invalid = is_brand_invalid(
        &product.brand,
        product.supplier.as_deref(),
        brand_set,
    );
    let model_invalid = is_model_invalid(&product.model.0, &product.brand);
    if !brand_invalid && !model_invalid {
        return;
    }
    let hint = build_car_hint(product, brand_set);
    let hint = if hint.trim().is_empty() {
        None
    } else {
        Some(hint.as_str())
    };
    if let Some((brand, model, _category_id)) =
        category_auto::guess_brand_model(&product.title, hint, categories)
    {
        if brand_invalid {
            product.brand = brand;
        }
        if model_invalid {
            product.model = Model(model);
        }
    }
}

fn apply_transform_dt_product(
    mut p: dt::product::Product,
    opts: SiteImportOptions,
    rates: &HashMap<String, Decimal>,
    shop_id: IdentityOf<rt_types::shop::Shop>,
) -> dt::product::Product {
    let base_price = p.price.map(|v| Decimal::from(v as i64));
    let (final_price, source_price, discount_percent) =
        compute_prices(base_price, "UAH", &opts, rates);
    p.source_price = source_price;
    if opts.update_fields.price {
        p.price = final_price;
    }
    if opts.update_fields.discounts {
        p.discount_percent = discount_percent;
    }
    p.title = xlsx::build_title(&opts.transform, &p.title, false);
    if let Some(title_ua) = p.title_ua.clone() {
        p.title_ua = Some(xlsx::build_title(&opts.transform, &title_ua, true));
    }
    p.description = apply_description(p.description.clone(), &opts, &shop_id, false);
    p.description_ua = apply_description(p.description_ua.clone(), &opts, &shop_id, true);
    if let Some(av) = opts.transform.set_availability {
        p.available = av;
    }
    p.last_visited = OffsetDateTime::now_utc();
    p
}

fn build_import_product(
    p: rt_types::product::Product,
    supplier_key: Option<String>,
    opts: &SiteImportOptions,
    rates: &HashMap<String, Decimal>,
    categories: &[Category],
    categories_by_id: &HashMap<IdentityOf<Category>, String>,
    shop_id: IdentityOf<rt_types::shop::Shop>,
) -> Option<dt::product::Product> {
    let mut attrs = if p.params.is_empty() {
        None
    } else {
        Some(p.params.clone())
    };
    let mut brand = p.brand.clone();
    if brand.trim().is_empty() {
        if let Some(val) = attrs.as_ref().and_then(|m| m.get("Марка")) {
            brand = val.clone();
        }
    }
    let mut model = p.model.clone();
    if model.trim().is_empty() {
        if let Some(val) = attrs.as_ref().and_then(|m| m.get("Модель")) {
            model = val.clone();
        }
    }

    let mut category = p
        .category
        .and_then(|id| categories_by_id.get(&id).cloned())
        .or_else(|| {
            attrs
                .as_ref()
                .and_then(|m| m.get("Категория").cloned())
        });

    if brand.trim().is_empty()
        || model.trim().is_empty()
        || category.as_ref().map(|c| c.trim().is_empty()).unwrap_or(true)
    {
        if let Some((guess_brand, guess_model, guess_category)) =
            categorize_to_brand_model(categories, &p.title, p.description.as_deref())
        {
            if brand.trim().is_empty() {
                brand = guess_brand;
            }
            if model.trim().is_empty() {
                model = guess_model;
            }
            if category
                .as_ref()
                .map(|c| c.trim().is_empty())
                .unwrap_or(true)
            {
                category = guess_category;
            }
        }
    }

    if brand.trim().is_empty() {
        brand = "Інше".to_string();
    }
    if model.trim().is_empty() {
        model = brand.clone();
    }

    let title_ru = xlsx::build_title(&opts.transform, &p.title, false);
    let title_ua = p
        .ua_translation
        .as_ref()
        .map(|t| xlsx::build_title(&opts.transform, &t.title, true));
    let desc_ru = apply_description(p.description.clone(), opts, &shop_id, false);
    let desc_ua = apply_description(
        p.ua_translation.as_ref().and_then(|t| t.description.clone()),
        opts,
        &shop_id,
        true,
    );
    let (final_price, source_price, discount_percent) =
        compute_prices(Some(p.price), &p.currency, opts, rates);
    let mut attributes = attrs.clone();
    if opts.transform.add_vendor {
        let supplier_label = supplier_key
            .clone()
            .unwrap_or_else(|| p.vendor.to_string());
        let map = attributes.get_or_insert_with(HashMap::new);
        map.entry("Постачальник".to_string())
            .or_insert_with(|| supplier_label);
    }
    let url = Url(format!(
        "/{}/{}.html",
        supplier_key.clone().unwrap_or_else(|| "import".to_string()),
        p.article
    ));
    Some(dt::product::Product {
        title: title_ru,
        description: desc_ru,
        title_ua,
        description_ua: desc_ua,
        price: final_price,
        source_price,
        article: p.article,
        brand,
        model: Model(model),
        category,
        attributes,
        available: opts
            .transform
            .set_availability
            .clone()
            .unwrap_or(p.available),
        quantity: p.in_stock,
        url,
        supplier: supplier_key,
        discount_percent,
        last_visited: OffsetDateTime::now_utc(),
        images: p.images,
        upsell: None,
    })
}

fn apply_description(
    base: Option<String>,
    opts: &SiteImportOptions,
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    is_ua: bool,
) -> Option<String> {
    let option = if is_ua {
        opts.transform.description_ua.as_ref()
    } else {
        opts.transform.description.as_ref()
    };
    let option = option.and_then(|o| resolve_description(shop_id, o));
    match option {
        Some(DescriptionOptions::Replace(value)) => Some(value),
        Some(DescriptionOptions::Append(extra)) => {
            let base = base.unwrap_or_default();
            if base.is_empty() {
                Some(extra)
            } else {
                Some(format!("{base}\n{extra}"))
            }
        }
        None => base.map(|d| xlsx::trim_images(&d)),
    }
}

fn resolve_description(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    opt: &DescriptionOptions,
) -> Option<DescriptionOptions> {
    let path = opt.value();
    let full_path = format!("./description/{shop_id}/{path}");
    match std::fs::read_to_string(&full_path) {
        Ok(content) => match opt {
            DescriptionOptions::Replace(_) => Some(DescriptionOptions::Replace(content)),
            DescriptionOptions::Append(_) => Some(DescriptionOptions::Append(content)),
        },
        Err(err) => {
            log::error!("Unable to read description {path}: {err}");
            None
        }
    }
}

fn compute_prices(
    base: Option<Decimal>,
    currency: &str,
    opts: &SiteImportOptions,
    rates: &HashMap<String, Decimal>,
) -> (Option<usize>, Option<usize>, Option<usize>) {
    let mut price = match base {
        Some(p) => p,
        None => return (None, None, None),
    };
    let mut currency = currency.trim().to_uppercase();
    if opts.transform.convert_to_uah && currency != "UAH" {
        if let Some(rate) = rates.get(&currency) {
            price *= *rate;
            currency = "UAH".to_string();
        }
    }
    let base_uah = price.round().to_i64().unwrap_or(0).max(0) as usize;
    let mut final_price = Decimal::from(base_uah as i64);
    if let Some(mult) = opts.transform.adjust_price {
        final_price *= mult;
    }
    let mut discount_percent = None;
    if opts.update_fields.discounts {
        if let Some(discount) = &opts.transform.discount {
            let percent = discount.percent.min(100);
            discount_percent = Some(percent);
            let factor = Decimal::from(100u64.saturating_sub(percent as u64)) / Decimal::from(100u64);
            final_price *= factor;
        }
    }
    let mut final_price = final_price.round().to_i64().unwrap_or(0).max(0) as usize;
    if opts.round_to_9 {
        final_price = round_price_to_9(final_price);
    }
    (Some(final_price), Some(base_uah), discount_percent)
}

fn round_price_to_9(value: usize) -> usize {
    if value == 0 {
        return 0;
    }
    if value < 10 {
        return 9;
    }
    let base = value - (value % 10);
    base + 9
}

async fn apply_missing_policy(
    policy: &MissingProductPolicy,
    shop_id: IdentityOf<rt_types::shop::Shop>,
    supplier: &str,
    articles: Vec<String>,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
) -> Result<(), anyhow::Error> {
    if articles.is_empty() {
        return Ok(());
    }
    match policy {
        MissingProductPolicy::Keep => Ok(()),
        MissingProductPolicy::NotAvailable => {
            for article in articles {
                if let Ok(Some(mut product)) = dt_repo.get_one(&article).await {
                    product.available = Availability::NotAvailable;
                    product.last_visited = OffsetDateTime::now_utc();
                    dt_repo.save(product).await?;
                }
            }
            Ok(())
        }
        MissingProductPolicy::Hidden => {
            let visibility = shop_product::Visibility::Hidden;
            let indexing = shop_product::IndexingStatus::NoIndex;
            let status = shop_product::ProductStatus::Draft;
            let source_type = map_source_type(supplier);
            let _ = shop_product_repo
                .bulk_set_visibility(
                    shop_id,
                    &articles,
                    visibility,
                    indexing,
                    status,
                    Some("noindex,follow".to_string()),
                    source_type,
                    true,
                )
                .await?;
            Ok(())
        }
        MissingProductPolicy::Deleted => {
            dt_repo.delete_articles(&articles).await?;
            shop_product_repo.remove_many(shop_id, &articles).await?;
            Ok(())
        }
    }
}

fn map_source_type(supplier: &str) -> shop_product::SourceType {
    let supplier = supplier.trim().to_lowercase();
    match supplier.as_str() {
        "restal" => shop_product::SourceType::Api,
        "restal_xml" => shop_product::SourceType::Xml,
        _ => shop_product::SourceType::Parsing,
    }
}

fn map_restal_product(
    src: &restal::RestalProduct,
    categories: &[Category],
    opts: &SiteImportOptions,
    rates: &HashMap<String, Decimal>,
    shop_id: IdentityOf<rt_types::shop::Shop>,
) -> Option<dt::product::Product> {
    let article = src
        .sku
        .clone()
        .or_else(|| src.product_id.clone())
        .or_else(|| src.model.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let title = src.name.clone().unwrap_or_else(|| article.clone());
    let base_price = src
        .price
        .as_ref()
        .and_then(|s| s.parse::<Decimal>().ok());
    let (final_price, source_price, discount_percent) =
        compute_prices(base_price, "UAH", opts, rates);
    let available = match src.quantity.as_ref().and_then(|s| s.parse::<i64>().ok()) {
        Some(q) if q > 0 => Availability::Available,
        Some(_) => Availability::NotAvailable,
        None => Availability::OnOrder,
    };
    if opts.transform.only_available && available != Availability::Available {
        return None;
    }
    let (brand, model, category) =
        categorize_to_brand_model(categories, &title, src.description.as_deref()).unwrap_or_else(
            || {
                let fallback_brand = "Інше".to_string();
                let fallback_model = src
                    .model
                    .clone()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| fallback_brand.clone());
                (fallback_brand, fallback_model, None)
            },
        );
    let model = Model(model);
    let url = Url(format!("/restal/{}.html", article));
    let mut images = src.images.clone();
    if images.is_empty() {
        if let Some(i) = src.image.as_ref() {
            images.push(i.clone());
        }
    }
    let title_ru = xlsx::build_title(&opts.transform, &title, false);
    let desc_ru = apply_description(src.description.clone(), opts, &shop_id, false);
    Some(dt::product::Product {
        title: title_ru,
        description: desc_ru,
        title_ua: None,
        description_ua: None,
        price: final_price,
        source_price,
        article,
        brand,
        model,
        category,
        attributes: None,
        available: opts
            .transform
            .set_availability
            .clone()
            .unwrap_or(available),
        quantity: src
            .quantity
            .as_ref()
            .and_then(|s| s.parse::<i64>().ok())
            .map(|q| q.max(0) as usize),
        url,
        supplier: Some("restal".to_string()),
        discount_percent,
        last_visited: OffsetDateTime::now_utc(),
        images,
        upsell: None,
    })
}

fn categorize_to_brand_model(
    categories: &[Category],
    title: &str,
    description: Option<&str>,
) -> Option<(String, String, Option<String>)> {
    let (brand, model, category_id) =
        category_auto::guess_brand_model(title, description, categories)?;
    let category = category_id.and_then(|id| {
        categories
            .iter()
            .find(|c| c.id == id)
            .map(|c| c.name.clone())
    });
    Some((brand, model, category))
}
