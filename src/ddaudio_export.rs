use crate::category_auto;
use crate::ddaudio;
use crate::export::{Export, ProgressInfo};
use anyhow::anyhow;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use time::{Duration as TimeDuration, OffsetDateTime, Time};
use time_tz::timezones::db::europe::KYIV;
use time_tz::OffsetDateTimeExt;
use tokio::sync::RwLock;
use tokio::time::sleep;

use rt_types::category::Category;
use rt_types::product::{generate_id, Product, UaTranslation};
use rt_types::shop::{DDAudioCategoryRule, DDAudioExportOptions, DDAudioPriceType, ZeroStockPolicy};
use rt_types::Availability;

const PAGE_LIMIT: usize = 10_000;
const REQUEST_DELAY_SECS: u64 = 6;

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
    quantity: Option<i64>,
    available_in_stock: Option<i64>,
    price: Option<f64>,
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
    available: Availability,
    quantity: Option<usize>,
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
            available: Availability::NotAvailable,
            quantity: None,
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

fn selected_price_types(config: &DDAudioExportOptions) -> HashSet<DDAudioPriceType> {
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

#[cfg(test)]
mod tests {
    use super::round_price_to_9;

    #[test]
    fn round_price_to_9_rounds_to_ending_nine() {
        assert_eq!(round_price_to_9(0), 0);
        assert_eq!(round_price_to_9(1), 9);
        assert_eq!(round_price_to_9(9), 9);
        assert_eq!(round_price_to_9(10), 19);
        assert_eq!(round_price_to_9(11), 19);
        assert_eq!(round_price_to_9(20), 29);
        assert_eq!(round_price_to_9(99), 99);
        assert_eq!(round_price_to_9(101), 109);
    }
}

fn resolve_rule<'a>(
    config: &'a DDAudioExportOptions,
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
    config: &DDAudioExportOptions,
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

fn markup_to_f64(markup: Option<Decimal>) -> Option<f64> {
    markup.and_then(|m| m.to_f64())
}

fn price_from_item(
    item: &AggregatedItem,
    rule: &DDAudioCategoryRule,
    rates: &HashMap<String, f64>,
) -> (Option<usize>, Option<usize>) {
    let base_raw = item.price.unwrap_or(0.0);
    if base_raw <= 0.0 {
        return (None, None);
    }
    let base = convert_to_uah(base_raw, item.currency.as_deref(), rates);
    if base <= 0.0 {
        return (None, None);
    }
    let base_uah = base.round().max(0.0) as usize;
    let mut price = base;
    if let Some(markup) = markup_to_f64(rule.markup_percent) {
        price *= 1.0 + (markup.max(0.0) / 100.0);
    }
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
            price *= (100.0 - percent as f64) / 100.0;
        }
    }
    let mut final_price = price.round().max(0.0) as usize;
    if rule.round_to_9 {
        final_price = round_price_to_9(final_price);
    }
    (Some(final_price), Some(base_uah))
}

fn resolve_availability(
    rule: &DDAudioCategoryRule,
    qty: i64,
    default_rule: &DDAudioCategoryRule,
) -> Availability {
    if qty > 0 {
        return Availability::Available;
    }
    let policy = match rule.zero_stock_policy {
        ZeroStockPolicy::Inherit => default_rule.zero_stock_policy,
        other => other,
    };
    match policy {
        ZeroStockPolicy::OnOrder => Availability::OnOrder,
        ZeroStockPolicy::NotAvailable => Availability::NotAvailable,
        ZeroStockPolicy::Inherit => Availability::NotAvailable,
    }
}

fn availability_with_warehouse_policy(
    policy: Option<&ZeroStockPolicy>,
    rule: &DDAudioCategoryRule,
    qty: i64,
    default_rule: &DDAudioCategoryRule,
) -> Availability {
    match policy {
        Some(ZeroStockPolicy::OnOrder) => Availability::OnOrder,
        Some(ZeroStockPolicy::NotAvailable) => Availability::NotAvailable,
        Some(ZeroStockPolicy::Inherit) | None => resolve_availability(rule, qty, default_rule),
    }
}

async fn update_progress(
    handle: &Option<Arc<RwLock<Export>>>,
    stage: impl Into<String>,
    done: usize,
    total: usize,
) {
    if let Some(handle) = handle.as_ref() {
        let mut export = handle.write().await;
        export.progress = Some(ProgressInfo {
            stage: stage.into(),
            done,
            total,
        });
    }
}

async fn load_price_map(
    token: &str,
    kind: ddaudio::PriceKind,
    lang: &str,
    warehouses: &[String],
    progress: &Option<Arc<RwLock<Export>>>,
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
            progress,
            format!("DD Audio: {lang} / {}", kind.as_str()),
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
                quantity: item.quantity,
                available_in_stock: item.available_in_stock,
                price: item.price,
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
            if entry.currency.is_none() {
                entry.currency = item.currency.clone();
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
        sleep(std::time::Duration::from_secs(REQUEST_DELAY_SECS)).await;
    }
    Ok(collected)
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

pub async fn fetch_products(
    config: &DDAudioExportOptions,
    categories: &[Category],
    progress: Option<Arc<RwLock<Export>>>,
) -> anyhow::Result<Vec<Product>> {
    let token = config.token.trim();
    if token.is_empty() {
        return Err(anyhow!("DD Audio token is empty"));
    }
    let mut languages = normalize_langs(&config.languages);
    if languages.is_empty() {
        languages = vec!["ru".to_string()];
    }
    let rates = load_currency_rates();

    let price_types = selected_price_types(config);
    let mut imported: HashMap<String, ImportedProduct> = HashMap::new();
    for lang in languages {
        let categories_resp = ddaudio::fetch_categories(token, Some(&lang)).await?;
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
                token,
                ddaudio::PriceKind::Retail,
                &lang,
                &config.selected_warehouses,
                &progress,
            )
            .await?;
        }
        if price_types.contains(&DDAudioPriceType::Wholesale) {
            wholesale_map = load_price_map(
                token,
                ddaudio::PriceKind::Wholesale,
                &lang,
                &config.selected_warehouses,
                &progress,
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
                update_progress(&progress, format!("DD Audio: {lang}"), idx, total).await;
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
            if !should_include(config, category_id.as_deref(), sub_id.as_deref()) {
                continue;
            }
            let rule = resolve_rule(config, category_id.as_deref(), sub_id.as_deref());
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
                let (price, _source) = price_from_item(price_item, rule, &rates);
                entry.price = price;
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

    let mut out = Vec::new();
    for (_, item) in imported {
        let title = item
            .title_ru
            .clone()
            .or(item.title_ua.clone())
            .unwrap_or_default();
        if title.trim().is_empty() {
            continue;
        }
        let mut brand = String::new();
        let mut model = String::new();
        let desc_hint = item
            .description_ua
            .as_deref()
            .or(item.description_ru.as_deref());
        if let Some((b, m, id)) = category_auto::guess_brand_model(&title, desc_hint, categories) {
            brand = b;
            if id.is_some() {
                model = m;
            }
        }
        let price = match item.price {
            Some(price) if price > 0 => Decimal::from(price as i64),
            _ => continue,
        };
        let mut params = HashMap::new();
        if !brand.trim().is_empty() {
            params.insert("Марка".to_string(), brand.clone());
        }
        if !model.trim().is_empty() {
            params.insert("Модель".to_string(), model.clone());
        }
        for (k, v) in &item.attributes {
            if !k.trim().is_empty() && !v.trim().is_empty() {
                params.insert(k.clone(), v.clone());
            }
        }
        let mut keyword_parts = Vec::new();
        if !brand.trim().is_empty() {
            keyword_parts.push(brand.clone());
        }
        if !model.trim().is_empty() {
            keyword_parts.push(model.clone());
        }
        if let Some(category) = &item.category {
            if !category.trim().is_empty() {
                keyword_parts.push(category.clone());
            }
        }
        let keywords = if keyword_parts.is_empty() {
            None
        } else {
            Some(keyword_parts.join(", "))
        };
        let vendor = "DD Audio".to_string();
        let id = generate_id(&item.article, &vendor, &None);
        let ua_translation = item.title_ua.clone().map(|title| UaTranslation {
            title,
            description: item.description_ua.clone(),
        });
        let product = Product {
            id,
            title,
            ua_translation,
            description: item.description_ru.clone(),
            price,
            article: item.article.clone(),
            in_stock: item.quantity,
            currency: "UAH".to_string(),
            keywords,
            params,
            brand,
            model,
            category: None,
            available: item.available.clone(),
            vendor,
            images: item.images.clone(),
        };
        out.push(product);
    }

    update_progress(&progress, "DD Audio: готово", out.len(), out.len()).await;
    Ok(out)
}
