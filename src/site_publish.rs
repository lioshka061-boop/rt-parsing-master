use crate::dt::product::Product;
use crate::parse_vendor_from_link;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use typesafe_repository::IdentityOf;
use uuid::Uuid;

const SUPPLIERS: [&str; 9] = [
    "dt",
    "maxton",
    "jgd",
    "skm",
    "tt",
    "restal",
    "restal_xml",
    "op_tuning",
    "ddaudio",
];

pub fn list_suppliers() -> Vec<String> {
    SUPPLIERS.iter().map(|s| s.to_string()).collect()
}

fn bool_true() -> bool {
    true
}

fn bool_false() -> bool {
    false
}

fn default_update_rate() -> Duration {
    Duration::from_secs(6 * 60 * 60)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KnownSupplier {
    pub key: String,
    pub label: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SitePublishConfig {
    pub site_publish_suppliers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExportTarget {
    Prom,
    Site,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PriceRules {
    pub markup_percent: Option<f64>,
    pub round_to: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ExportConfig {
    pub category_map: HashMap<String, String>,
    pub brand_map: HashMap<String, String>,
    pub model_map: HashMap<String, String>,
    pub price_rules: PriceRules,
    #[serde(default)]
    pub availability_rules: HashMap<String, String>,
    #[serde(default)]
    pub image_rules: HashMap<String, String>,
    #[serde(default)]
    pub description_rules: HashMap<String, String>,
    #[serde(default)]
    pub slug_template: Option<String>,
    #[serde(default)]
    pub seo_title_template: Option<String>,
    #[serde(default)]
    pub seo_description_template: Option<String>,
    #[serde(default)]
    pub publish_enabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum DDAudioPriceType {
    Retail,
    Wholesale,
}

impl Default for DDAudioPriceType {
    fn default() -> Self {
        Self::Retail
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ZeroStockPolicy {
    Inherit,
    OnOrder,
    NotAvailable,
}

impl Default for ZeroStockPolicy {
    fn default() -> Self {
        Self::Inherit
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DDAudioCategoryRule {
    #[serde(default)]
    pub price_type: DDAudioPriceType,
    #[serde(default)]
    pub markup_percent: Option<f64>,
    #[serde(default)]
    pub discount_percent: Option<usize>,
    #[serde(default)]
    pub discount_hours: Option<u32>,
    #[serde(default = "bool_true")]
    pub round_to_9: bool,
    #[serde(default)]
    pub zero_stock_policy: ZeroStockPolicy,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DDAudioTargetConfig {
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub update_fields: rt_types::shop::SiteImportUpdateFields,
    #[serde(default)]
    pub missing_policy: rt_types::shop::MissingProductPolicy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DDAudioConfig {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub selected_categories: Vec<String>,
    #[serde(default)]
    pub selected_subcategories: Vec<String>,
    #[serde(default)]
    pub selected_warehouses: Vec<String>,
    #[serde(default)]
    pub known_warehouses: Vec<String>,
    #[serde(default)]
    pub warehouse_statuses: HashMap<String, ZeroStockPolicy>,
    #[serde(default)]
    pub category_rules: HashMap<String, DDAudioCategoryRule>,
    #[serde(default)]
    pub subcategory_rules: HashMap<String, DDAudioCategoryRule>,
    #[serde(default)]
    pub default_rule: DDAudioCategoryRule,
    #[serde(default)]
    pub title_replacements_ru: Vec<(String, String)>,
    #[serde(default)]
    pub title_replacements_ua: Vec<(String, String)>,
    #[serde(default = "bool_false")]
    pub append_images: bool,
    #[serde(default = "bool_true")]
    pub append_attributes: bool,
    #[serde(default)]
    pub site: DDAudioTargetConfig,
    #[serde(default)]
    pub prom: DDAudioTargetConfig,
    #[serde(default = "bool_false")]
    pub auto_update: bool,
    #[serde(
        default = "default_update_rate",
        deserialize_with = "rt_types::shop::deserialize_duration_from_string",
        serialize_with = "rt_types::shop::serialize_duration_into_string"
    )]
    pub update_rate: std::time::Duration,
    #[serde(default = "default_ddaudio_target")]
    pub auto_target: DDAudioTarget,
}

impl Default for DDAudioConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            selected_categories: Vec::new(),
            selected_subcategories: Vec::new(),
            selected_warehouses: Vec::new(),
            known_warehouses: Vec::new(),
            warehouse_statuses: HashMap::new(),
            category_rules: HashMap::new(),
            subcategory_rules: HashMap::new(),
            default_rule: DDAudioCategoryRule {
                price_type: DDAudioPriceType::Retail,
                markup_percent: None,
                discount_percent: None,
                discount_hours: Some(24),
                round_to_9: true,
                zero_stock_policy: ZeroStockPolicy::Inherit,
            },
            title_replacements_ru: Vec::new(),
            title_replacements_ua: Vec::new(),
            append_images: false,
            append_attributes: true,
            site: DDAudioTargetConfig {
                languages: vec!["ua".to_string()],
                update_fields: rt_types::shop::SiteImportUpdateFields::default(),
                missing_policy: rt_types::shop::MissingProductPolicy::Keep,
            },
            prom: DDAudioTargetConfig {
                languages: vec!["ru".to_string(), "ua".to_string()],
                update_fields: rt_types::shop::SiteImportUpdateFields::default(),
                missing_policy: rt_types::shop::MissingProductPolicy::Keep,
            },
            auto_update: false,
            update_rate: default_update_rate(),
            auto_target: DDAudioTarget::Site,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DDAudioTarget {
    Site,
    Prom,
}

impl Default for DDAudioTarget {
    fn default() -> Self {
        Self::Site
    }
}

fn default_ddaudio_target() -> DDAudioTarget {
    DDAudioTarget::Site
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct XmlSupplier {
    pub id: Uuid,
    pub shop_id: IdentityOf<rt_types::shop::Shop>,
    pub xml_url: String,
    pub target: ExportTarget,
    pub config: ExportConfig,
    #[serde(default)]
    pub status: SupplierStatus,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub progress_percent: Option<u8>,
    #[serde(default)]
    pub last_log: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SupplierStatus {
    Parsed,
    Ready,
    Published,
    Error,
}

impl Default for SupplierStatus {
    fn default() -> Self {
        SupplierStatus::Ready
    }
}

impl fmt::Display for SupplierStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SupplierStatus::Parsed => write!(f, "parsed"),
            SupplierStatus::Ready => write!(f, "ready"),
            SupplierStatus::Published => write!(f, "published"),
            SupplierStatus::Error => write!(f, "error"),
        }
    }
}

fn cfg_path(shop_id: &IdentityOf<rt_types::shop::Shop>) -> PathBuf {
    PathBuf::from("cfg.d").join(format!("site_publish_{shop_id}.yml"))
}

fn restal_key_path(shop_id: &IdentityOf<rt_types::shop::Shop>) -> PathBuf {
    PathBuf::from("cfg.d").join(format!("restal_key_{shop_id}.txt"))
}

pub fn save_restal_key(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    key: &str,
) -> anyhow::Result<()> {
    let path = restal_key_path(shop_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, key)?;
    Ok(())
}

pub fn load_restal_key(shop_id: &IdentityOf<rt_types::shop::Shop>) -> Option<String> {
    fs::read_to_string(restal_key_path(shop_id))
        .ok()
        .map(|s| s.trim().to_string())
}

pub fn load_site_publish_suppliers(shop_id: &IdentityOf<rt_types::shop::Shop>) -> Vec<String> {
    let path = cfg_path(shop_id);
    let data = match fs::read_to_string(&path) {
        Ok(d) => d,
        Err(_) => return vec![],
    };

    // 1) основной формат { site_publish_suppliers: [..] }
    if let Ok(cfg) = serde_json::from_str::<SitePublishConfig>(&data)
        .or_else(|_| serde_yaml::from_str::<SitePublishConfig>(&data))
    {
        return normalize_suppliers(cfg.site_publish_suppliers);
    }

    // 2) прямой список ["dt", "maxton"]
    if let Ok(list) = serde_yaml::from_str::<Vec<String>>(&data)
        .or_else(|_| serde_json::from_str::<Vec<String>>(&data))
    {
        return normalize_suppliers(list);
    }

    // 3) одиночная строка "dt"
    if let Ok(single) =
        serde_yaml::from_str::<String>(&data).or_else(|_| serde_json::from_str::<String>(&data))
    {
        return normalize_suppliers(vec![single]);
    }

    // 4) YAML object, но значение — строка (site_publish_suppliers: "dt")
    if let Ok(val) = serde_yaml::from_str::<serde_yaml::Value>(&data) {
        if let Some(map) = val.as_mapping() {
            if let Some(v) = map.get(&serde_yaml::Value::from("site_publish_suppliers")) {
                if let Some(s) = v.as_str() {
                    return normalize_suppliers(vec![s.to_string()]);
                }
                if let Some(seq) = v.as_sequence() {
                    let list = seq
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>();
                    return normalize_suppliers(list);
                }
            }
        }
    }

    vec![]
}

fn suppliers_cfg_path(shop_id: &IdentityOf<rt_types::shop::Shop>) -> PathBuf {
    PathBuf::from("cfg.d").join(format!("site_publish_suppliers_{shop_id}.json"))
}

fn ddaudio_cfg_path(shop_id: &IdentityOf<rt_types::shop::Shop>) -> PathBuf {
    PathBuf::from("cfg.d").join(format!("ddaudio_{shop_id}.json"))
}

pub fn load_site_publish_configs(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
) -> anyhow::Result<Vec<XmlSupplier>> {
    let path = suppliers_cfg_path(shop_id);
    let data = fs::read_to_string(&path).unwrap_or_default();
    if data.is_empty() {
        return Ok(vec![]);
    }
    let mut parsed: Vec<XmlSupplier> =
        serde_json::from_str(&data).or_else(|_| serde_yaml::from_str(&data))?;
    // ensure shop_id set correctly
    for s in parsed.iter_mut() {
        s.shop_id = *shop_id;
    }
    Ok(parsed)
}

pub fn save_site_publish_configs(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    suppliers: &[XmlSupplier],
) -> anyhow::Result<()> {
    let path = suppliers_cfg_path(shop_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Unable to create dir {parent:?}"))?;
    }
    let payload = serde_json::to_string_pretty(suppliers)?;
    fs::write(&path, payload)
        .with_context(|| format!("Unable to write site publish suppliers to {path:?}"))?;
    Ok(())
}

pub fn load_ddaudio_config(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
) -> DDAudioConfig {
    let path = ddaudio_cfg_path(shop_id);
    let data = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return DDAudioConfig::default(),
    };
    if data.trim().is_empty() {
        return DDAudioConfig::default();
    }
    serde_json::from_str(&data)
        .or_else(|_| serde_yaml::from_str(&data))
        .unwrap_or_default()
}

pub fn save_ddaudio_config(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    config: &DDAudioConfig,
) -> anyhow::Result<()> {
    let path = ddaudio_cfg_path(shop_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(config)?;
    fs::write(&path, payload)?;
    Ok(())
}

pub fn load_all_ddaudio_configs() -> Vec<(IdentityOf<rt_types::shop::Shop>, DDAudioConfig)> {
    let dir = PathBuf::from("cfg.d");
    let mut out = Vec::new();
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.starts_with("ddaudio_") || !name.ends_with(".json") {
            continue;
        }
        let shop_id = name
            .trim_start_matches("ddaudio_")
            .trim_end_matches(".json")
            .to_string();
        let shop_id = match shop_id.parse::<uuid::Uuid>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let data = fs::read_to_string(&path).unwrap_or_default();
        if data.trim().is_empty() {
            continue;
        }
        if let Ok(cfg) = serde_json::from_str::<DDAudioConfig>(&data)
            .or_else(|_| serde_yaml::from_str::<DDAudioConfig>(&data))
        {
            out.push((shop_id, cfg));
        }
    }
    out
}

pub fn upsert_site_supplier(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    xml_url: String,
    config: ExportConfig,
) -> anyhow::Result<XmlSupplier> {
    let mut current = load_site_publish_configs(shop_id).unwrap_or_default();
    if let Some(existing) = current
        .iter_mut()
        .find(|s| s.xml_url == xml_url && matches!(s.target, ExportTarget::Site))
    {
        existing.config = config;
        let clone = existing.clone();
        save_site_publish_configs(shop_id, &current)?;
        return Ok(clone);
    }
    let supplier = XmlSupplier {
        id: Uuid::new_v4(),
        shop_id: *shop_id,
        xml_url,
        target: ExportTarget::Site,
        config,
        status: SupplierStatus::Ready,
        last_error: None,
        title: None,
        progress_percent: None,
        last_log: None,
    };
    current.push(supplier.clone());
    save_site_publish_configs(shop_id, &current)?;
    Ok(supplier)
}

pub fn update_supplier_status(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    supplier_id: Uuid,
    status: SupplierStatus,
    error: Option<String>,
    progress: Option<u8>,
    log: Option<String>,
) -> anyhow::Result<XmlSupplier> {
    let mut suppliers = load_site_publish_configs(shop_id)?;
    if let Some(s) = suppliers.iter_mut().find(|s| s.id == supplier_id) {
        s.status = status;
        s.last_error = error;
        if let Some(p) = progress {
            s.progress_percent = Some(p.min(100));
        }
        if let Some(msg) = log {
            s.last_log = Some(msg);
        }
        let clone = s.clone();
        save_site_publish_configs(shop_id, &suppliers)?;
        return Ok(clone);
    }
    anyhow::bail!("Supplier not found");
}

pub fn save_site_publish_suppliers(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    suppliers: Vec<String>,
) -> anyhow::Result<()> {
    let normalized = normalize_suppliers(suppliers);
    let cfg = SitePublishConfig {
        site_publish_suppliers: normalized,
    };
    let path = cfg_path(shop_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Unable to create dir {parent:?}"))?;
    }
    let data = serde_json::to_string_pretty(&cfg)?;
    fs::write(&path, data)
        .with_context(|| format!("Unable to write site publish cfg to {path:?}"))?;
    Ok(())
}

fn normalize_suppliers(list: Vec<String>) -> Vec<String> {
    list.into_iter()
        .filter_map(|s| normalize_supplier_key(&s))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

fn normalize_supplier_key(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let mut out = String::new();
    let mut last_sep = false;
    for ch in raw.to_lowercase().chars() {
        if ch.is_alphanumeric() {
            out.push(ch);
            last_sep = false;
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            if !last_sep {
                out.push('_');
                last_sep = true;
            }
        }
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn known_suppliers_cfg_path(shop_id: &IdentityOf<rt_types::shop::Shop>) -> PathBuf {
    PathBuf::from("cfg.d").join(format!("site_publish_known_suppliers_{shop_id}.json"))
}

pub fn load_known_suppliers(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
) -> Vec<KnownSupplier> {
    let path = known_suppliers_cfg_path(shop_id);
    let data = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return vec![],
        Err(err) => {
            log::error!("Unable to read known suppliers {path:?}: {err}");
            return vec![];
        }
    };
    let parsed: Vec<KnownSupplier> = serde_json::from_str(&data).unwrap_or_default();
    let mut by_key: HashMap<String, KnownSupplier> = HashMap::new();
    for supplier in parsed.into_iter() {
        let key = match normalize_supplier_key(&supplier.key) {
            Some(k) => k,
            None => continue,
        };
        let label = supplier.label.trim();
        let label = if label.is_empty() {
            key.clone()
        } else {
            label.to_string()
        };
        by_key.insert(key.clone(), KnownSupplier { key, label });
    }
    by_key.into_values().collect()
}

pub fn upsert_known_supplier(
    shop_id: &IdentityOf<rt_types::shop::Shop>,
    key: &str,
    label: &str,
) -> anyhow::Result<()> {
    let key = match normalize_supplier_key(key) {
        Some(k) => k,
        None => return Ok(()),
    };
    let label = label.trim();
    let label = if label.is_empty() {
        key.clone()
    } else {
        label.to_string()
    };
    let mut suppliers = load_known_suppliers(shop_id);
    if let Some(existing) = suppliers.iter_mut().find(|s| s.key == key) {
        existing.label = label;
    } else {
        suppliers.push(KnownSupplier {
            key: key.clone(),
            label,
        });
    }
    let path = known_suppliers_cfg_path(shop_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Unable to create dir {parent:?}"))?;
    }
    let payload = serde_json::to_string_pretty(&suppliers)?;
    fs::write(&path, payload)
        .with_context(|| format!("Unable to write known suppliers to {path:?}"))?;
    Ok(())
}

pub fn detect_supplier(product: &Product) -> Option<String> {
    if let Some(supplier) = product
        .supplier
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
    {
        return Some(supplier);
    }
    let url = product.url.0.to_lowercase();
    let title = product.title.to_lowercase();
    let brand = product.brand.to_lowercase();
    if url.contains("op-tuning")
        || url.contains("op_tuning")
        || title.contains("o&p")
        || brand.contains("o&p")
        || title.contains("op tuning")
        || brand.contains("op tuning")
    {
        return Some("op_tuning".to_string());
    }
    let article = product.article.to_uppercase();

    // Heuristics for suppliers when links are relative and do not contain domain info.
    if article.ends_with("-M") {
        return Some("maxton".to_string());
    }
    if title.contains("maxton") {
        return Some("maxton".to_string());
    }
    if article.starts_with("JGD") {
        return Some("jgd".to_string());
    }
    if article.starts_with("SKM") {
        return Some("skm".to_string());
    }
    if article.starts_with("TT") {
        return Some("tt".to_string());
    }

    if let Some(supplier) = detect_supplier_from_link(&product.url.0) {
        return Some(supplier);
    }
    if product.url.0.starts_with('/') {
        // Design-tuning URLs are stored as relative paths.
        return Some("dt".to_string());
    }
    None
}

pub fn filter_products_for_site(
    products: Vec<Product>,
    allowed_suppliers: &[String],
) -> Vec<Product> {
    let allowed: HashSet<String> = allowed_suppliers
        .iter()
        .map(|s| s.trim().to_lowercase())
        .collect();
    if allowed.is_empty() {
        return products;
    }

    products
        .into_iter()
        .filter(|p| {
            detect_supplier(p)
                .map(|s| allowed.contains(&s))
                .unwrap_or(false)
        })
        .collect()
}

pub fn detect_supplier_from_link(link: &str) -> Option<String> {
    let url = link.to_lowercase();
    if url.contains("op-tuning") || url.contains("op_tuning") || url.contains("optuning") {
        return Some("op_tuning".to_string());
    }
    if url.contains("restalauto.com.ua") || url.contains("restalauto") {
        return Some("restal_xml".to_string());
    }
    if url.contains("restal-auto") || url.contains("/restal/") {
        return Some("restal".to_string());
    }
    if url.contains("maxton") {
        return Some("maxton".to_string());
    }
    if url.contains("design-tuning")
        || url.contains("davi.com.ua")
        || url.contains("restal-auto")
        || url.contains("tuning-tec")
    {
        return Some("dt".to_string());
    }
    if url.contains("jgd") {
        return Some("jgd".to_string());
    }
    if url.contains("skm") {
        return Some("skm".to_string());
    }
    if url.contains("tt") || url.contains("dt-tt") {
        return Some("tt".to_string());
    }

    // fallback на домен
    parse_vendor_from_link(link).map(|v| {
        let v = v.to_lowercase();
        if v.contains("maxton") {
            "maxton".to_string()
        } else if v.contains("design") || v.contains("tuning") || v.contains("restal") {
            "dt".to_string()
        } else if v.contains("jgd") {
            "jgd".to_string()
        } else if v.contains("skm") {
            "skm".to_string()
        } else if v.contains("tt") {
            "tt".to_string()
        } else {
            v
        }
    })
}
