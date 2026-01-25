use crate::category_auto;
use crate::ddaudio;
use crate::dt;
use crate::product_category;
use crate::product_category_auto;
use crate::shop_product;
use crate::site_publish::{self, DDAudioCategoryRule, DDAudioConfig, DDAudioPriceType, DDAudioTarget, ZeroStockPolicy};
use crate::import_throttle;
use crate::{Model, Url};
use anyhow::anyhow;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use time::{Duration as TimeDuration, OffsetDateTime, Time};
use time_tz::timezones::db::europe::KYIV;
use time_tz::OffsetDateTimeExt;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use uuid::Uuid;
use rt_types::shop::MissingProductPolicy;
use rt_types::Availability;

const PAGE_LIMIT: usize = 10_000;
static REQUEST_DELAY_SECS: Lazy<u64> = Lazy::new(|| {
    std::env::var("DDAUDIO_REQUEST_DELAY_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10)
});

#[derive(Clone, Debug, serde::Serialize)]
pub struct ProgressInfo {
    pub stage: String,
    pub done: usize,
    pub total: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
pub enum ImportStatus {
    Idle,
    InProgress,
    Success,
    Failure(String),
}

impl std::fmt::Display for ImportStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Очікує"),
            Self::InProgress => write!(f, "В процесі"),
            Self::Success => write!(f, "Успішно"),
            Self::Failure(_) => write!(f, "Помилка"),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ImportState {
    pub status: ImportStatus,
    pub progress: Option<ProgressInfo>,
    pub last_error: Option<String>,
    pub last_log: Option<String>,
    pub last_started: Option<OffsetDateTime>,
    pub last_finished: Option<OffsetDateTime>,
    pub target: Option<DDAudioTarget>,
}

impl Default for ImportState {
    fn default() -> Self {
        Self {
            status: ImportStatus::Idle,
            progress: None,
            last_error: None,
            last_log: None,
            last_started: None,
            last_finished: None,
            target: None,
        }
    }
}

#[derive(Clone)]
struct ImportTask {
    notify: Arc<Notify>,
    stop: Arc<Notify>,
}

static IMPORT_STATE: Lazy<RwLock<HashMap<Uuid, ImportState>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static IMPORT_TASKS: Lazy<RwLock<HashMap<Uuid, ImportTask>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static AUTO_UPDATE_ARMED: Lazy<RwLock<HashSet<Uuid>>> =
    Lazy::new(|| RwLock::new(HashSet::new()));

pub async fn get_status(shop_id: Uuid) -> ImportState {
    let map = IMPORT_STATE.read().await;
    map.get(&shop_id).cloned().unwrap_or_default()
}

async fn set_state(shop_id: Uuid, state: ImportState) {
    let mut map = IMPORT_STATE.write().await;
    map.insert(shop_id, state);
}

async fn update_progress(shop_id: Uuid, stage: impl Into<String>, done: usize, total: usize) {
    let mut map = IMPORT_STATE.write().await;
    let entry = map.entry(shop_id).or_insert_with(ImportState::default);
    entry.progress = Some(ProgressInfo {
        stage: stage.into(),
        done,
        total,
    });
}

pub async fn trigger_import(
    shop_id: Uuid,
    target: DDAudioTarget,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    category_repo: Arc<dyn rt_types::category::CategoryRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
    manual_start: bool,
) -> anyhow::Result<()> {
    let config = site_publish::load_ddaudio_config(&shop_id);
    if config.token.trim().is_empty() {
        return Err(anyhow!("DD Audio token is empty"));
    }
    if manual_start {
        AUTO_UPDATE_ARMED.write().await.insert(shop_id);
    }
    let mut state = get_status(shop_id).await;
    if matches!(state.status, ImportStatus::InProgress) {
        return Err(anyhow!("Import already in progress"));
    }
    state.status = ImportStatus::InProgress;
    state.progress = None;
    state.last_error = None;
    state.last_log = None;
    state.last_started = Some(OffsetDateTime::now_utc());
    state.target = Some(target);
    set_state(shop_id, state).await;
    if manual_start {
        trigger_scheduled_run(shop_id).await;
    }

    tokio::spawn(async move {
        let _permit = import_throttle::acquire_import_permit().await;
        let res = run_import(
            shop_id,
            target,
            config,
            dt_repo,
            shop_product_repo,
            category_repo,
            product_category_repo,
        )
        .await;
        let mut state = get_status(shop_id).await;
        match res {
            Ok(msg) => {
                state.status = ImportStatus::Success;
                state.last_log = msg;
            }
            Err(err) => {
                state.status = ImportStatus::Failure(err.to_string());
                state.last_error = Some(err.to_string());
            }
        }
        state.progress = None;
        state.last_finished = Some(OffsetDateTime::now_utc());
        set_state(shop_id, state).await;
    });
    Ok(())
}

pub async fn sync_scheduler(
    shop_id: Uuid,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    category_repo: Arc<dyn rt_types::category::CategoryRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
) {
    let config = site_publish::load_ddaudio_config(&shop_id);
    let mut tasks = IMPORT_TASKS.write().await;
    if !config.auto_update {
        AUTO_UPDATE_ARMED.write().await.remove(&shop_id);
        if let Some(task) = tasks.remove(&shop_id) {
            task.stop.notify_waiters();
        }
        return;
    }
    AUTO_UPDATE_ARMED.write().await.insert(shop_id);
    if tasks.contains_key(&shop_id) {
        return;
    }
    let notify = Arc::new(Notify::new());
    let stop = Arc::new(Notify::new());
    tasks.insert(
        shop_id,
        ImportTask {
            notify: notify.clone(),
            stop: stop.clone(),
        },
    );
    tokio::spawn(async move {
        loop {
            let cfg = site_publish::load_ddaudio_config(&shop_id);
            let armed = AUTO_UPDATE_ARMED.read().await.contains(&shop_id);
            if !armed {
                tokio::select! {
                    _ = stop.notified() => break,
                    _ = notify.notified() => continue,
                }
            }

            let state = get_status(shop_id).await;
            if matches!(state.status, ImportStatus::InProgress) {
                tokio::select! {
                    _ = stop.notified() => break,
                    _ = notify.notified() => continue,
                    _ = sleep(cfg.update_rate) => (),
                }
                continue;
            }
            if let Some(last_started) = state.last_started {
                let elapsed = OffsetDateTime::now_utc() - last_started;
                let elapsed_secs = elapsed.as_seconds_f64();
                if elapsed_secs >= 0.0 {
                    let elapsed = std::time::Duration::from_secs_f64(elapsed_secs);
                    if elapsed < cfg.update_rate {
                        let wait_for = cfg.update_rate - elapsed;
                        tokio::select! {
                            _ = stop.notified() => break,
                            _ = notify.notified() => continue,
                            _ = sleep(wait_for) => (),
                        }
                        continue;
                    }
                }
            }

            if cfg.token.trim().is_empty() {
                let mut state = get_status(shop_id).await;
                state.status = ImportStatus::Failure("DD Audio token is empty".to_string());
                state.last_error = Some("DD Audio token is empty".to_string());
                set_state(shop_id, state).await;
            } else {
                let _ = trigger_import(
                    shop_id,
                    cfg.auto_target,
                    dt_repo.clone(),
                    shop_product_repo.clone(),
                    category_repo.clone(),
                    product_category_repo.clone(),
                    false,
                )
                .await;
            }
            tokio::select! {
                _ = stop.notified() => break,
                _ = notify.notified() => continue,
                _ = sleep(cfg.update_rate) => (),
            }
        }
    });
}

pub async fn trigger_scheduled_run(shop_id: Uuid) {
    let tasks = IMPORT_TASKS.read().await;
    if let Some(task) = tasks.get(&shop_id) {
        task.notify.notify_waiters();
    }
}

#[derive(Clone)]
struct AggregatedItem {
    article: String,
    brand: String,
    model: String,
    title: String,
    category: Option<String>,
    subcategory: Option<String>,
    manufacturer: Option<String>,
    images: Vec<String>,
    country: Option<String>,
    material: Option<String>,
    installation: Option<String>,
    kit: Option<String>,
    attrs: HashMap<String, String>,
    quantity: Option<i64>,
    available_in_stock: Option<i64>,
    price: Option<f64>,
    sale_price: Option<f64>,
    currency: Option<String>,
    warehouse: Option<String>,
    short_title: Option<String>,
    parent_title: Option<String>,
}

#[derive(Clone)]
struct ImportedProduct {
    article: String,
    brand: String,
    model: String,
    category: Option<String>,
    images: Vec<String>,
    attributes: HashMap<String, String>,
    price: Option<usize>,
    source_price: Option<usize>,
    available: rt_types::Availability,
    quantity: Option<usize>,
    discount_percent: Option<usize>,
    title_ru: Option<String>,
    title_ua: Option<String>,
    description_ru: Option<String>,
    description_ua: Option<String>,
    base_ready: bool,
}

impl Default for ImportedProduct {
    fn default() -> Self {
        Self {
            article: String::new(),
            brand: String::new(),
            model: String::new(),
            category: None,
            images: Vec::new(),
            attributes: HashMap::new(),
            price: None,
            source_price: None,
            available: rt_types::Availability::NotAvailable,
            quantity: None,
            discount_percent: None,
            title_ru: None,
            title_ua: None,
            description_ru: None,
            description_ua: None,
            base_ready: false,
        }
    }
}

fn normalize_langs(raw: &[String]) -> Vec<String> {
    raw.iter()
        .map(|l| l.trim().to_lowercase())
        .filter(|l| matches!(l.as_str(), "ru" | "ua" | "en"))
        .collect::<Vec<_>>()
}

fn currency_rates_paths() -> Vec<PathBuf> {
    if let Ok(path) = std::env::var("CURRENCY_RATES_PATH") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return vec![PathBuf::from(trimmed)];
        }
    }
    let mut paths = vec![PathBuf::from("currency_rates.csv")];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            paths.push(dir.join("currency_rates.csv"));
        }
    }
    paths
}

fn load_currency_rates() -> HashMap<String, f64> {
    let mut rates = HashMap::new();
    let mut content = None;
    for path in currency_rates_paths() {
        match fs::read_to_string(&path) {
            Ok(data) => {
                content = Some(data);
                break;
            }
            Err(_) => continue,
        }
    }
    let Some(content) = content else {
        log::warn!("Unable to read currency rates from any known path");
        return rates;
    };
    for line in content.lines() {
        let mut parts = line.split(',');
        let code = parts.next().unwrap_or("").trim().to_uppercase();
        let rate_raw = parts.next().unwrap_or("").trim().replace(',', ".");
        if code.is_empty() {
            continue;
        }
        if let Ok(rate) = rate_raw.parse::<f64>() {
            if rate > 0.0 {
                rates.insert(code, rate);
            }
        }
    }
    rates
}

fn convert_to_uah(value: f64, currency: Option<&str>, rates: &HashMap<String, f64>) -> f64 {
    let code = currency
        .unwrap_or("")
        .trim()
        .to_uppercase();
    let code = if code.is_empty() {
        std::env::var("DDAUDIO_DEFAULT_CURRENCY")
            .unwrap_or_else(|_| "USD".to_string())
            .trim()
            .to_uppercase()
    } else {
        code
    };
    if code.is_empty() || matches!(code.as_str(), "UAH" | "ГРН" | "UA") {
        return value;
    }
    if let Some(rate) = rates.get(&code) {
        value * rate
    } else {
        value
    }
}

fn selected_price_types(config: &DDAudioConfig) -> HashSet<DDAudioPriceType> {
    let mut types = HashSet::new();
    types.insert(config.default_rule.price_type);
    for rule in config.category_rules.values() {
        types.insert(rule.price_type);
    }
    for rule in config.subcategory_rules.values() {
        types.insert(rule.price_type);
    }
    types
}

fn build_attr_map(item: &AggregatedItem) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    if let Some(v) = item.category.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Категорія".to_string(), v.to_string());
    }
    if let Some(v) = item.subcategory.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Підкатегорія".to_string(), v.to_string());
    }
    if let Some(v) = item.short_title.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Характеристика".to_string(), v.to_string());
    }
    if let Some(v) = item.parent_title.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Група".to_string(), v.to_string());
    }
    if let Some(v) = item.material.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Матеріал".to_string(), v.to_string());
    }
    if let Some(v) = item.country.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Країна".to_string(), v.to_string());
    }
    if let Some(v) = item.manufacturer.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Виробник".to_string(), v.to_string());
    }
    if let Some(v) = item.installation.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Монтаж".to_string(), v.to_string());
    }
    if let Some(v) = item.kit.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Комплект".to_string(), v.to_string());
    }
    if let Some(v) = item.warehouse.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        attrs.insert("Склад".to_string(), v.to_string());
    }
    attrs
}

fn build_description(attrs: &HashMap<String, String>, lang: &str) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    let mut lines = Vec::new();
    for (key, value) in attrs {
        let label = match (lang, key.as_str()) {
            ("ru", "Категорія") => "Категория",
            ("ru", "Підкатегорія") => "Подкатегория",
            ("ru", "Характеристика") => "Характеристика",
            ("ru", "Група") => "Группа",
            ("ru", "Матеріал") => "Материал",
            ("ru", "Країна") => "Страна",
            ("ru", "Виробник") => "Производитель",
            ("ru", "Монтаж") => "Монтаж",
            ("ru", "Комплект") => "Комплект",
            ("ru", "Склад") => "Склад",
            _ => key,
        };
        lines.push(format!("{label}: {value}"));
    }
    Some(lines.join("<br>"))
}

fn apply_replacements(mut title: String, replacements: &[(String, String)]) -> String {
    for (from, to) in replacements {
        if !from.trim().is_empty() {
            title = title.replace(from, to);
        }
    }
    title
}

fn normalize_model_for_title(model: &str, lang: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let mut out = trimmed.to_string();
    match lang {
        "ua" => {
            out = out.replace("рр.", "року").replace("рр", "року");
        }
        "ru" => {
            out = out.replace("гг.", "года").replace("гг", "года");
        }
        _ => {}
    }
    out
}

fn append_model_to_title(title: String, model: &str) -> String {
    let model = model.trim();
    if model.is_empty() {
        return title;
    }
    let title_norm = title.to_lowercase();
    let model_norm = model.to_lowercase();
    if title_norm.contains(&model_norm) {
        return title;
    }
    format!("{} {}", title.trim_end(), model)
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

fn resolve_rule<'a>(
    config: &'a DDAudioConfig,
    category_id: Option<&str>,
    sub_id: Option<&str>,
) -> &'a DDAudioCategoryRule {
    if let Some(sub_id) = sub_id {
        if let Some(rule) = config.subcategory_rules.get(sub_id) {
            return rule;
        }
    }
    if let Some(category_id) = category_id {
        if let Some(rule) = config.category_rules.get(category_id) {
            return rule;
        }
    }
    &config.default_rule
}

fn should_include(
    config: &DDAudioConfig,
    category_id: Option<&str>,
    sub_id: Option<&str>,
) -> bool {
    if config.selected_categories.is_empty() && config.selected_subcategories.is_empty() {
        return true;
    }
    if let Some(sub_id) = sub_id {
        if !config.selected_subcategories.is_empty() {
            return config.selected_subcategories.iter().any(|v| v == sub_id);
        }
    }
    if let Some(cat_id) = category_id {
        if config.selected_categories.iter().any(|v| v == cat_id) {
            return true;
        }
    }
    if let Some(sub_id) = sub_id {
        if config.selected_subcategories.iter().any(|v| v == sub_id) {
            return true;
        }
    }
    false
}

fn price_from_item(
    item: &AggregatedItem,
    rule: &DDAudioCategoryRule,
    rates: &HashMap<String, f64>,
) -> (Option<usize>, Option<usize>, Option<usize>) {
    let base_raw = item.price.unwrap_or(0.0);
    if base_raw <= 0.0 {
        return (None, None, None);
    }
    let base = convert_to_uah(base_raw, item.currency.as_deref(), rates);
    if base <= 0.0 {
        return (None, None, None);
    }
    let base_uah = base.round().max(0.0) as usize;
    let mut price = base;
    if let Some(markup) = rule.markup_percent {
        price *= 1.0 + (markup.max(0.0) / 100.0);
    }
    let mut discount_percent = None;
    if let Some(discount) = rule.discount_percent {
        let mut apply_discount = true;
        if let Some(hours) = rule.discount_hours {
            let now = OffsetDateTime::now_utc().to_timezone(KYIV);
            let start = now.replace_time(Time::MIDNIGHT);
            let end = start + TimeDuration::hours(hours.max(1) as i64);
            apply_discount = now >= start && now < end;
        }
        if apply_discount {
            let percent = discount.min(100);
            discount_percent = Some(percent);
            price *= (100.0 - percent as f64) / 100.0;
        }
    }
    let mut final_price = price.round().max(0.0) as usize;
    if rule.round_to_9 {
        final_price = round_price_to_9(final_price);
    }
    (Some(final_price), Some(base_uah), discount_percent)
}

fn resolve_availability(rule: &DDAudioCategoryRule, qty: i64, default_rule: &DDAudioCategoryRule) -> rt_types::Availability {
    if qty > 0 {
        return rt_types::Availability::Available;
    }
    let policy = match rule.zero_stock_policy {
        ZeroStockPolicy::Inherit => default_rule.zero_stock_policy,
        other => other,
    };
    match policy {
        ZeroStockPolicy::OnOrder => rt_types::Availability::OnOrder,
        ZeroStockPolicy::NotAvailable => rt_types::Availability::NotAvailable,
        ZeroStockPolicy::Inherit => rt_types::Availability::NotAvailable,
    }
}

fn availability_with_warehouse_policy(
    policy: Option<&ZeroStockPolicy>,
    rule: &DDAudioCategoryRule,
    qty: i64,
    default_rule: &DDAudioCategoryRule,
) -> rt_types::Availability {
    match policy {
        Some(ZeroStockPolicy::OnOrder) => rt_types::Availability::OnOrder,
        Some(ZeroStockPolicy::NotAvailable) => rt_types::Availability::NotAvailable,
        Some(ZeroStockPolicy::Inherit) | None => resolve_availability(rule, qty, default_rule),
    }
}

fn merge_product(
    existing: Option<dt::product::Product>,
    incoming: &dt::product::Product,
    update_fields: &rt_types::shop::SiteImportUpdateFields,
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

fn is_ddaudio_product(product: &dt::product::Product) -> bool {
    product
        .supplier
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("ddaudio"))
        .unwrap_or(false)
}

async fn apply_missing_policy(
    policy: &MissingProductPolicy,
    shop_id: Uuid,
    articles: Vec<String>,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
) -> anyhow::Result<()> {
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
            let source_type = shop_product::SourceType::Api;
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

async fn load_price_map(
    token: &str,
    kind: ddaudio::PriceKind,
    lang: &str,
    warehouses: &[String],
    shop_id: Uuid,
    known_warehouses: &mut HashSet<String>,
) -> anyhow::Result<HashMap<String, AggregatedItem>> {
    let mut offset = 0usize;
    let mut total = None;
    let mut collected: HashMap<String, AggregatedItem> = HashMap::new();
    loop {
        let resp = ddaudio::fetch_prices(token, kind, Some(lang), offset, PAGE_LIMIT).await?;
        if !resp.success {
            return Err(anyhow!("DD Audio API returned error"));
        }
        if total.is_none() {
            total = resp.total_results.or(resp.total);
        }
        let total_val = total.unwrap_or(resp.data.len());
        update_progress(
            shop_id,
            format!("Завантаження {lang} / {}", kind.as_str()),
            offset.min(total_val),
            total_val,
        )
        .await;
        if resp.data.is_empty() {
            break;
        }
        let batch_len = resp.data.len();
        for item in resp.data.into_iter() {
            let warehouse = item.warehouse.clone().unwrap_or_default();
            if !warehouse.trim().is_empty() {
                known_warehouses.insert(warehouse.clone());
            }
            if !warehouses.is_empty() && !warehouses.iter().any(|w| w == &warehouse) {
                continue;
            }
            let article = item
                .sku
                .clone()
                .filter(|s| !s.trim().is_empty())
                .or_else(|| item.id.map(|v| v.to_string()))
                .unwrap_or_default();
            if article.is_empty() {
                continue;
            }
            let entry = collected.entry(article.clone()).or_insert_with(|| AggregatedItem {
                article: article.clone(),
                brand: item.mark.clone().unwrap_or_default(),
                model: item.model.clone().unwrap_or_default(),
                title: item.title.clone().unwrap_or_else(|| article.clone()),
                category: item.category.clone(),
                subcategory: item.subcategory.clone(),
                manufacturer: item.manufacturer.clone(),
                images: item.images.clone(),
                country: item.country.clone(),
                material: item.material.clone(),
                installation: item.installation.clone(),
                kit: item.kit.clone(),
                attrs: HashMap::new(),
                quantity: item.quantity,
                available_in_stock: item.available_in_stock,
                price: item.price,
                sale_price: item.sale_price,
                currency: item.currency.clone(),
                warehouse: item.warehouse.clone(),
                short_title: item.short_title.clone(),
                parent_title: item.parent.as_ref().map(|p| p.title.clone()),
            });
            if entry.brand.trim().is_empty() {
                entry.brand = item.mark.clone().unwrap_or_default();
            }
            if entry.model.trim().is_empty() {
                entry.model = item.model.clone().unwrap_or_default();
            }
            if entry.title.trim().is_empty() {
                entry.title = item.title.clone().unwrap_or_else(|| article.clone());
            }
            if entry.category.is_none() {
                entry.category = item.category.clone();
            }
            if entry.subcategory.is_none() {
                entry.subcategory = item.subcategory.clone();
            }
            if entry.manufacturer.is_none() {
                entry.manufacturer = item.manufacturer.clone();
            }
            if entry.country.is_none() {
                entry.country = item.country.clone();
            }
            if entry.material.is_none() {
                entry.material = item.material.clone();
            }
            if entry.installation.is_none() {
                entry.installation = item.installation.clone();
            }
            if entry.kit.is_none() {
                entry.kit = item.kit.clone();
            }
            if entry.images.is_empty() {
                entry.images = item.images.clone();
            } else {
                entry.images = merge_images(&entry.images, &item.images);
            }
            if let Some(q) = item.quantity {
                entry.quantity = Some(entry.quantity.unwrap_or(0) + q);
            }
            if let Some(q) = item.available_in_stock {
                entry.available_in_stock = Some(entry.available_in_stock.unwrap_or(0).max(q));
            }
        }
        if batch_len < PAGE_LIMIT {
            break;
        }
        offset += PAGE_LIMIT;
        sleep(std::time::Duration::from_secs(*REQUEST_DELAY_SECS)).await;
    }
    Ok(collected)
}

async fn run_import(
    shop_id: Uuid,
    target: DDAudioTarget,
    config: DDAudioConfig,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    category_repo: Arc<dyn rt_types::category::CategoryRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
) -> anyhow::Result<Option<String>> {
    let token = config.token.trim().to_string();
    if token.is_empty() {
        return Err(anyhow!("DD Audio token is empty"));
    }
    site_publish::upsert_known_supplier(&shop_id, "ddaudio", "DD Audio")?;

    let target_cfg = match target {
        DDAudioTarget::Site => config.site.clone(),
        DDAudioTarget::Prom => config.prom.clone(),
    };
    let languages = normalize_langs(&target_cfg.languages);
    if languages.is_empty() {
        return Err(anyhow!("No languages selected"));
    }
    let rates = load_currency_rates();

    let categories = category_repo.select(&rt_types::category::By(shop_id)).await?;
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

    let price_types = selected_price_types(&config);
    let mut known_warehouses = HashSet::new();
    let mut imported: HashMap<String, ImportedProduct> = HashMap::new();
    for lang in languages {
        let categories_resp = ddaudio::fetch_categories(&token, Some(&lang)).await?;
        if !categories_resp.success {
            return Err(anyhow!("Unable to load DD Audio categories"));
        }
        let mut category_name_map = HashMap::new();
        let mut subcategory_name_map = HashMap::new();
        for (cat_id, node) in &categories_resp.data {
            category_name_map.insert(node.title.trim().to_lowercase(), cat_id.clone());
            for (sub_id, name) in &node.children {
                subcategory_name_map.insert(name.trim().to_lowercase(), sub_id.clone());
            }
        }

        let mut retail_map = HashMap::new();
        let mut wholesale_map = HashMap::new();
        if price_types.contains(&DDAudioPriceType::Retail) {
            retail_map = load_price_map(
                &token,
                ddaudio::PriceKind::Retail,
                &lang,
                &config.selected_warehouses,
                shop_id,
                &mut known_warehouses,
            )
            .await?;
        }
        if price_types.contains(&DDAudioPriceType::Wholesale) {
            wholesale_map = load_price_map(
                &token,
                ddaudio::PriceKind::Wholesale,
                &lang,
                &config.selected_warehouses,
                shop_id,
                &mut known_warehouses,
            )
            .await?;
        }
        let mut keys = HashSet::new();
        keys.extend(retail_map.keys().cloned());
        keys.extend(wholesale_map.keys().cloned());
        let total = keys.len();
        let mut idx = 0usize;
        for key in keys {
            idx += 1;
            if idx % 200 == 0 || idx == total {
                update_progress(
                    shop_id,
                    format!("Підготовка {lang}"),
                    idx,
                    total,
                )
                .await;
            }
            let base = retail_map
                .get(&key)
                .or_else(|| wholesale_map.get(&key));
            let Some(base) = base else { continue; };
            let category_id = base
                .category
                .as_ref()
                .and_then(|c| category_name_map.get(&c.trim().to_lowercase()).cloned());
            let sub_id = base
                .subcategory
                .as_ref()
                .and_then(|c| subcategory_name_map.get(&c.trim().to_lowercase()).cloned());
            if !should_include(&config, category_id.as_deref(), sub_id.as_deref()) {
                continue;
            }
            let rule = resolve_rule(&config, category_id.as_deref(), sub_id.as_deref());
            let price_item = match rule.price_type {
                DDAudioPriceType::Retail => retail_map.get(&key).or_else(|| wholesale_map.get(&key)),
                DDAudioPriceType::Wholesale => wholesale_map.get(&key).or_else(|| retail_map.get(&key)),
            }
            .unwrap_or(base);

            let entry = imported.entry(key.clone()).or_insert_with(|| ImportedProduct {
                article: key.clone(),
                ..ImportedProduct::default()
            });
            if !entry.base_ready {
                let attrs = build_attr_map(price_item);
                entry.brand = price_item.brand.clone();
                entry.model = price_item.model.clone();
                entry.category = price_item
                    .subcategory
                    .clone()
                    .or_else(|| price_item.category.clone());
                entry.images = price_item.images.clone();
                entry.attributes = attrs;
                let qty = price_item
                    .quantity
                    .or(price_item.available_in_stock)
                    .unwrap_or(0)
                    .max(0);
                entry.quantity = Some(qty as usize);
                let warehouse_policy = price_item
                    .warehouse
                    .as_ref()
                    .and_then(|w| config.warehouse_statuses.get(w));
                entry.available = availability_with_warehouse_policy(
                    warehouse_policy,
                    rule,
                    qty,
                    &config.default_rule,
                );
                let (price, source, discount) = price_from_item(price_item, rule, &rates);
                entry.price = price;
                entry.source_price = source;
                entry.discount_percent = discount;
                entry.base_ready = true;
            }
            let mut title = price_item.title.clone();
            if lang == "ru" {
                title = apply_replacements(title, &config.title_replacements_ru);
            } else if lang == "ua" {
                title = apply_replacements(title, &config.title_replacements_ua);
            }
            if !price_item.model.trim().is_empty() {
                let model_label = normalize_model_for_title(&price_item.model, &lang);
                if !model_label.is_empty() {
                    title = append_model_to_title(title, &model_label);
                }
            }
            let description = if config.append_attributes {
                build_description(&entry.attributes, &lang)
            } else {
                None
            };
            if lang == "ru" {
                entry.title_ru = Some(title);
                entry.description_ru = description;
            } else if lang == "ua" {
                entry.title_ua = Some(title);
                entry.description_ua = description;
            } else {
                entry.title_ru = Some(title);
                entry.description_ru = description;
            }
        }
    }

    let existing = dt_repo.list().await.unwrap_or_default();
    let mut existing_map = existing
        .into_iter()
        .map(|p| (p.article.to_lowercase(), p))
        .collect::<HashMap<_, _>>();

    let total = imported.len();
    let mut done = 0usize;
    for (_, item) in imported {
        done += 1;
        if done % 100 == 0 || done == total {
            update_progress(shop_id, "Імпорт товарів", done, total).await;
        }
        let mut brand = String::new();
        let mut model = String::new();
        let title_hint = item
            .title_ua
            .as_deref()
            .or(item.title_ru.as_deref())
            .unwrap_or("");
        let desc_hint = item
            .description_ua
            .as_deref()
            .or(item.description_ru.as_deref());
        if let Some((b, m, id)) = category_auto::guess_brand_model(title_hint, desc_hint, &categories) {
            brand = b;
            if id.is_some() {
                model = m;
            }
        }
        let mut attrs = item.attributes.clone();
        if let Some(v) = item.attributes.get("Категорія") {
            if !v.trim().is_empty() {
                attrs.insert("Категорія".to_string(), v.clone());
            }
        }
        let product = dt::product::Product {
            title: item.title_ru.clone().unwrap_or_else(|| item.title_ua.clone().unwrap_or_default()),
            description: if matches!(target, DDAudioTarget::Site) {
                item.description_ua.clone().or(item.description_ru.clone())
            } else {
                item.description_ru.clone().or(item.description_ua.clone())
            },
            title_ua: item.title_ua.clone(),
            description_ua: item.description_ua.clone(),
            price: item.price,
            source_price: item.source_price,
            article: item.article.clone(),
            brand,
            model: Model(model),
            category: item.category.clone(),
            attributes: if attrs.is_empty() { None } else { Some(attrs) },
            available: item.available,
            quantity: item.quantity,
            url: Url(format!("/ddaudio/{}", item.article)),
            supplier: Some("ddaudio".to_string()),
            discount_percent: item.discount_percent,
            last_visited: OffsetDateTime::now_utc(),
            images: item.images.clone(),
            upsell: None,
        };
        let existing = existing_map.remove(&item.article.to_lowercase());
        let mut update_fields = target_cfg.update_fields.clone();
        if matches!(target, DDAudioTarget::Site) {
            if update_fields.description_ua && !update_fields.description_ru {
                update_fields.description_ru = true;
            }
            if update_fields.title_ua && !update_fields.title_ru {
                update_fields.title_ru = true;
            }
        }
        let mut merged = merge_product(existing, &product, &update_fields, config.append_images);
        merged.brand = product.brand.clone();
        merged.model = product.model.clone();
        let article_key = merged.article.to_lowercase();
        if let Some(matcher) = category_matcher.as_ref() {
            let has_site_category = site_category_by_article
                .get(&article_key)
                .and_then(|id| *id)
                .is_some();
            if !has_site_category {
                let mut attr_hint = String::new();
                if let Some(attrs) = merged.attributes.as_ref() {
                    for (key, value) in attrs {
                        if key.trim().is_empty() && value.trim().is_empty() {
                            continue;
                        }
                        attr_hint.push_str(key);
                        attr_hint.push(' ');
                        attr_hint.push_str(value);
                        attr_hint.push('\n');
                    }
                }
                let description = merged.description.as_deref().unwrap_or_default();
                let haystack = if attr_hint.is_empty() {
                    product_category_auto::build_haystack(&merged.title, description)
                } else {
                    let combined = format!("{description}\n{attr_hint}");
                    product_category_auto::build_haystack(&merged.title, &combined)
                };
                if let Some(cat_id) = matcher.guess(&haystack) {
                    shop_product_repo
                        .set_site_category(shop_id, &merged.article, Some(cat_id))
                        .await?;
                    site_category_by_article.insert(article_key.clone(), Some(cat_id));
                }
            }
        }
        dt_repo.save(merged).await?;
    }

    if !matches!(target_cfg.missing_policy, MissingProductPolicy::Keep) {
        let missing = existing_map
            .into_values()
            .filter(|p| is_ddaudio_product(p))
            .map(|p| p.article)
            .collect::<Vec<_>>();
        apply_missing_policy(
            &target_cfg.missing_policy,
            shop_id,
            missing,
            dt_repo,
            shop_product_repo,
        )
        .await?;
    }
    if !known_warehouses.is_empty() {
        let mut sorted = known_warehouses.into_iter().collect::<Vec<_>>();
        sorted.sort();
        let mut updated_config = config.clone();
        updated_config.known_warehouses = sorted;
        site_publish::save_ddaudio_config(&shop_id, &updated_config)?;
    }
    Ok(Some(format!("Імпорт завершено: {total} товарів")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_price_to_9() {
        assert_eq!(round_price_to_9(0), 0);
        assert_eq!(round_price_to_9(3), 9);
        assert_eq!(round_price_to_9(10), 19);
        assert_eq!(round_price_to_9(149), 149);
        assert_eq!(round_price_to_9(151), 159);
    }

    #[test]
    fn test_apply_replacements() {
        let input = "BMW X5 DEFLECTOR".to_string();
        let out = apply_replacements(
            input,
            &[("DEFLECTOR".to_string(), "дефлектор".to_string())],
        );
        assert_eq!(out, "BMW X5 дефлектор");
    }
}
