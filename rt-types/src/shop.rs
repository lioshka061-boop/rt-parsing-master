use crate::access::UserCredentials;
use crate::watermark::WatermarkOptions;
use crate::{Availability, DescriptionOptions};
use actix::prelude::*;
use derive_more::Display;
use lazy_regex::regex;
use rust_decimal::Decimal;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_aux::field_attributes::{
    deserialize_number_from_string, deserialize_option_number_from_string,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::time::Duration;
use time::OffsetDateTime;
use typesafe_repository::async_ops::{Get, List, Remove, Save};
use typesafe_repository::macros::Id;
use typesafe_repository::{GetIdentity, Identity, IdentityOf, RefIdentity, Repository};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

pub mod service;

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct ConfigurationChanged(pub Shop);

pub trait ShopRepository:
    Repository<Shop, Error = anyhow::Error>
    + Get<Shop>
    + List<Shop>
    + Remove<Shop>
    + Save<Shop>
    + Send
    + Sync
{
}

#[derive(Id, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[Id(ref_id, get_id)]
pub struct Shop {
    pub id: Uuid,
    #[serde(default)]
    pub is_suspended: bool,
    pub name: String,
    pub owner: IdentityOf<UserCredentials>,
    pub export_entries: Vec<ExportEntry>,
    #[serde(default)]
    pub site_import_entries: Vec<SiteImportEntry>,
    pub limits: Option<ShopLimits>,
    pub default_custom_options: Option<CustomOptions>,
    #[serde(default)]
    pub image_proxy: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CustomOptions {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShopLimits {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub maximum_exports: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub links_per_export: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub unique_links: u32,
    #[serde(deserialize_with = "deserialize_option_number_from_string")]
    pub descriptions: Option<NonZeroU32>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub maximum_description_size: u32,
    #[serde(deserialize_with = "deserialize_option_number_from_string")]
    pub categories: Option<NonZeroU32>,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    pub minimum_update_rate: Duration,
}

impl Default for ShopLimits {
    fn default() -> Self {
        Self {
            maximum_exports: 2,
            links_per_export: 2,
            unique_links: 4,
            descriptions: NonZeroU32::new(2),
            maximum_description_size: 10 * 1024,
            categories: NonZeroU32::new(10),
            minimum_update_rate: Duration::from_millis(1000 * 60 * 60 * 6),
        }
    }
}

impl Shop {
    pub fn conforms_limits(&self) -> bool {
        let limits = match &self.limits {
            Some(l) => l,
            None => return true,
        };
        self.export_entries.len() <= limits.maximum_exports as usize
            && self.export_entries.iter().all(|e| {
                e.links
                    .as_ref()
                    .map(|l| l.len() <= limits.links_per_export as usize)
                    .unwrap_or(true)
                    && e.tt_parsing.is_none()
                    && e.dt_parsing.is_none()
                    && e.jgd_parsing.is_none()
                    && e.pl_parsing.is_none()
                    && e.dt_tt_parsing.is_none()
                    && e.skm_parsing.is_none()
                    && e.update_rate >= limits.minimum_update_rate
                    && e.links
                        .as_ref()
                        .map(|l| {
                            l.iter().all(|l| {
                                limits.categories.is_some()
                                    || !l.options.as_ref().is_some_and(|o| o.categories)
                            })
                        })
                        .unwrap_or(true)
            })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DDAudioCategoryRule {
    #[serde(default)]
    pub price_type: DDAudioPriceType,
    #[serde(default)]
    pub markup_percent: Option<Decimal>,
    #[serde(default)]
    pub discount_percent: Option<usize>,
    #[serde(default)]
    pub discount_hours: Option<u32>,
    #[serde(default = "bool_true")]
    pub round_to_9: bool,
    #[serde(default)]
    pub zero_stock_policy: ZeroStockPolicy,
}

impl Default for DDAudioCategoryRule {
    fn default() -> Self {
        Self {
            price_type: DDAudioPriceType::Retail,
            markup_percent: None,
            discount_percent: None,
            discount_hours: Some(24),
            round_to_9: true,
            zero_stock_policy: ZeroStockPolicy::Inherit,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExportEntry {
    #[serde(default = "default_time")]
    pub created_time: OffsetDateTime,
    #[serde(default = "default_time")]
    pub edited_time: OffsetDateTime,
    pub file_name: Option<String>,
    pub links: Option<Vec<ExportEntryLink>>,
    pub tt_parsing: Option<TtParsingOptions>,
    pub dt_parsing: Option<DtParsingOptions>,
    pub op_tuning_parsing: Option<DtParsingOptions>,
    pub jgd_parsing: Option<DtParsingOptions>,
    pub pl_parsing: Option<DtParsingOptions>,
    pub dt_tt_parsing: Option<DtParsingOptions>,
    pub skm_parsing: Option<DtParsingOptions>,
    pub maxton_parsing: Option<DtParsingOptions>,
    pub davi_parsing: Option<ExportOptions>,
    #[serde(default)]
    pub ddaudio_api: Option<DDAudioExportOptions>,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    #[serde(default = "default_update_rate")]
    pub update_rate: Duration,
}

impl ExportEntry {
    pub fn uses_watermark(&self, watermark: &str) -> bool {
        self.tt_parsing
            .as_ref()
            .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .dt_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .op_tuning_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .maxton_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .jgd_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .pl_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .dt_tt_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self
                .skm_parsing
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
            || self.links.iter().flatten().any(|l| {
                l.options
                    .as_ref()
                    .is_some_and(|opts| opts.watermarks.iter().any(|(w, _)| w == watermark))
            })
            || self
                .ddaudio_api
                .as_ref()
                .is_some_and(|o| o.options.watermarks.iter().any(|(w, _)| w == watermark))
    }

    pub fn generate_hash(&self) -> u64 {
        let mut hasher =
            Xxh3::with_seed(self.links.as_ref().map(Vec::len).unwrap_or_default() as u64);
        self.hash(&mut hasher);
        hasher.digest()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SiteImportSource {
    Parsing { supplier: String },
    Xml { link: String, vendor_name: Option<String> },
    RestalApi,
}

impl SiteImportSource {
    pub fn supplier_key(&self) -> Option<String> {
        match self {
            Self::Parsing { supplier } => normalize_supplier_key(supplier),
            Self::Xml {
                link,
                vendor_name,
            } => vendor_name
                .as_ref()
                .and_then(|s| normalize_supplier_key(s))
                .or_else(|| parse_vendor_from_link(link).and_then(|v| normalize_supplier_key(&v))),
            Self::RestalApi => Some("restal".to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MissingProductPolicy {
    Keep,
    NotAvailable,
    Hidden,
    Deleted,
}

impl Default for MissingProductPolicy {
    fn default() -> Self {
        Self::Keep
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SiteImportUpdateFields {
    #[serde(default = "bool_true")]
    pub title_ru: bool,
    #[serde(default = "bool_true")]
    pub title_ua: bool,
    #[serde(default = "bool_true")]
    pub description_ru: bool,
    #[serde(default = "bool_true")]
    pub description_ua: bool,
    #[serde(default = "bool_true")]
    pub sku: bool,
    #[serde(default = "bool_true")]
    pub price: bool,
    #[serde(default = "bool_true")]
    pub images: bool,
    #[serde(default = "bool_true")]
    pub availability: bool,
    #[serde(default = "bool_true")]
    pub quantity: bool,
    #[serde(default = "bool_true")]
    pub attributes: bool,
    #[serde(default = "bool_true")]
    pub discounts: bool,
}

impl Default for SiteImportUpdateFields {
    fn default() -> Self {
        Self {
            title_ru: true,
            title_ua: true,
            description_ru: true,
            description_ua: true,
            sku: true,
            price: true,
            images: true,
            availability: true,
            quantity: true,
            attributes: true,
            discounts: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SiteImportOptions {
    #[serde(default)]
    pub update_fields: SiteImportUpdateFields,
    #[serde(default)]
    pub missing_policy: MissingProductPolicy,
    #[serde(default = "bool_false")]
    pub append_images: bool,
    #[serde(default = "bool_true")]
    pub round_to_9: bool,
    #[serde(flatten)]
    pub transform: ExportOptions,
}

impl Default for SiteImportOptions {
    fn default() -> Self {
        Self {
            update_fields: SiteImportUpdateFields::default(),
            missing_policy: MissingProductPolicy::Keep,
            append_images: false,
            round_to_9: true,
            transform: ExportOptions::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SiteImportEntry {
    #[serde(default = "default_time")]
    pub created_time: OffsetDateTime,
    #[serde(default = "default_time")]
    pub edited_time: OffsetDateTime,
    pub name: Option<String>,
    pub source: SiteImportSource,
    #[serde(default)]
    pub options: SiteImportOptions,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    #[serde(default = "default_update_rate")]
    pub update_rate: Duration,
}

impl Default for SiteImportEntry {
    fn default() -> Self {
        Self {
            created_time: default_time(),
            edited_time: default_time(),
            name: None,
            source: SiteImportSource::Xml {
                link: String::new(),
                vendor_name: None,
            },
            options: SiteImportOptions::default(),
            update_rate: default_update_rate(),
        }
    }
}

impl SiteImportEntry {
    pub fn supplier_key(&self) -> Option<String> {
        self.source
            .supplier_key()
            .or_else(|| self.name.as_ref().and_then(|n| normalize_supplier_key(n)))
    }

    pub fn generate_hash(&self) -> u64 {
        let mut hasher = Xxh3::with_seed(0);
        self.hash(&mut hasher);
        hasher.digest()
    }
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

impl Hash for SiteImportEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.source.hash(state);
        self.created_time.hash(state);
        self.edited_time.hash(state);
    }
}

impl Hash for ExportEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.links.hash(state);
        self.file_name.hash(state);
        self.created_time.hash(state);
        self.edited_time.hash(state);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct DtParsingOptions {
    #[serde(flatten)]
    pub options: ExportOptions,
}

impl DtParsingOptions {
    pub fn options(&self) -> &ExportOptions {
        &self.options
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct TtParsingOptions {
    #[serde(flatten)]
    pub options: ExportOptions,
    pub append_categories: Option<ParsingCategoriesAction>,
}

impl TtParsingOptions {
    pub fn options(&self) -> &ExportOptions {
        &self.options
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParsingCategoriesAction {
    BeforeTitle { separator: String },
    AfterTitle { separator: String },
}

impl Default for ExportEntry {
    fn default() -> Self {
        Self {
            created_time: default_time(),
            edited_time: default_time(),
            file_name: None,
            links: None,
            tt_parsing: None,
            dt_parsing: None,
            op_tuning_parsing: None,
            davi_parsing: None,
            jgd_parsing: None,
            pl_parsing: None,
            dt_tt_parsing: None,
            skm_parsing: None,
            maxton_parsing: None,
            ddaudio_api: None,
            update_rate: default_update_rate(),
        }
    }
}

fn default_time() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

impl ExportEntry {
    pub fn get_link_by_hash_mut(&mut self, hash: String) -> Option<&mut ExportEntryLink> {
        self.links.as_mut().and_then(|l| {
            l.iter_mut()
                .enumerate()
                .find(|(i, l)| l.hash_with_index(*i).to_string() == hash)
                .map(|(_, l)| l)
        })
    }
    pub fn remove_link_by_hash(&mut self, hash: String) -> Option<ExportEntryLink> {
        self.links
            .as_mut()
            .and_then(|l| {
                l.iter()
                    .enumerate()
                    .find(|(i, l)| l.hash_with_index(*i).to_string() == hash)
                    .unzip()
                    .0
                    .map(|i| (l, i))
            })
            .map(|(l, i)| l.remove(i))
    }
    pub fn file_name<T: Into<Option<FileFormat>>>(&self, file_format: T) -> String {
        if let Some(file_name) = self.file_name.as_ref() {
            // actix-web не дозволяє сегменти шляху, що починаються з '.', тому прибираємо її
            // і замінюємо розділювачі, щоб уникнути 400 при скачуванні.
            let mut file_name = file_name.trim().to_string();
            file_name = file_name.trim_start_matches('.').to_string();
            file_name = file_name.replace(['/', '\\'], "_");
            if file_name.is_empty() {
                file_name = "export".to_string();
            }
            return match file_format.into() {
                Some(FileFormat::Xlsx) => format!("{file_name}.{}", FileFormat::Xlsx.extension()),
                Some(FileFormat::HoroshopCsv) => {
                    format!("{file_name}_hs.{}.zip", FileFormat::HoroshopCsv.extension())
                }
                Some(FileFormat::HoroshopCategories) => {
                    format!(
                        "{file_name}_hs_categories.{}.zip",
                        FileFormat::HoroshopCategories.extension()
                    )
                }
                Some(format) => format!("{file_name}.{}.zip", format.extension()),
                None => file_name,
            };
        }
        let mut file_name = match &self.links {
            Some(links) => itertools::intersperse(
                links
                    .iter()
                    .map(|l| &l.link)
                    .filter_map(parse_vendor_from_link),
                "_".to_string(),
            )
            .collect::<String>(),
            None => "".to_string(),
        };
        if self.dt_parsing.is_some() {
            if !file_name.is_empty() {
                file_name = itertools::intersperse([file_name, "dt".to_string()], "_".to_string())
                    .collect();
            } else {
                file_name = "dt".to_string();
            }
        }
        if self.tt_parsing.is_some() {
            if !file_name.is_empty() {
                file_name = itertools::intersperse([file_name, "tt".to_string()], "_".to_string())
                    .collect();
            } else {
                file_name = "tt".to_string();
            }
        }
        let file_format = file_format.into();
        if let Some(file_format) = &file_format {
            file_name.push_str(&format!(".{}", file_format.extension()));
        }
        if let Some(FileFormat::Csv | FileFormat::Xml) = file_format {
            file_name.push_str(".zip");
        }
        file_name
    }
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Display)]
pub enum FileFormat {
    #[display("xlsx")]
    Xlsx,
    #[display("csv")]
    Csv,
    #[display("xml")]
    Xml,
    #[display("horoshop csv")]
    HoroshopCsv,
    #[display("horoshop categories")]
    HoroshopCategories,
}

impl FileFormat {
    pub fn extension(&self) -> &str {
        match self {
            Self::Xlsx => "xlsx",
            Self::Csv => "csv",
            Self::Xml => "xml",
            Self::HoroshopCsv => "csv",
            Self::HoroshopCategories => "csv",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExportEntryLink {
    pub vendor_name: Option<String>,
    pub link: String,
    #[serde(default = "bool_true")]
    pub publish: bool,
    #[serde(flatten)]
    pub options: Option<ExportOptions>,
}

impl Hash for ExportEntryLink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.link.hash(state)
    }
}

impl ExportEntryLink {
    pub fn hash_with_index(&self, i: usize) -> u64 {
        let mut hasher = Xxh3::with_seed(i as u64);
        self.hash(&mut hasher);
        hasher.digest()
    }
}

fn parse_vendor_from_link<S: AsRef<str>>(l: S) -> Option<String> {
    let regex = regex!(r"(?U)https?:\/\/(([^\.]+)\.)*(com|net|ua|org|one)(\.[^\.]+)*");
    regex
        .captures(l.as_ref())
        .and_then(|c| c.get(2))
        .map(|m| m.as_str().to_string())
}

impl ExportEntryLink {
    pub fn vendor_name(&self) -> String {
        self.vendor_name
            .as_ref()
            .filter(|n| !n.is_empty())
            .cloned()
            .unwrap_or_else(|| parse_vendor_from_link(&self.link).unwrap_or_default())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExportOptions {
    pub title_prefix: Option<String>,
    pub title_prefix_ua: Option<String>,
    pub title_suffix: Option<String>,
    pub title_suffix_ua: Option<String>,
    #[serde(default)]
    pub title_replacements: Option<Vec<(String, String)>>,
    #[serde(default = "bool_false")]
    pub only_available: bool,
    pub discount: Option<Discount>,
    #[serde(default = "bool_false")]
    pub format_years: bool,
    #[serde(default = "bool_false")]
    pub add_vendor: bool,
    #[serde(default = "bool_true")]
    pub publish: bool,
    #[serde(default)]
    pub description: Option<DescriptionOptions>,
    #[serde(default)]
    pub description_ua: Option<DescriptionOptions>,
    pub delivery_time: Option<usize>,
    pub adjust_price: Option<Decimal>,
    #[serde(default = "bool_false")]
    pub categories: bool,
    #[serde(default = "bool_false")]
    pub convert_to_uah: bool,
    pub custom_options: Option<CustomOptions>,
    pub watermarks: Option<(String, Option<WatermarkOptions>)>,
    pub set_availability: Option<Availability>,
}

impl ExportOptions {
    pub fn has_watermark<'a>(&'a self, watermark: impl Into<&'a str>) -> bool {
        match &self.watermarks {
            Some((x, _)) => x == watermark.into(),
            None => false,
        }
    }
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            title_prefix: None,
            title_prefix_ua: None,
            title_suffix: None,
            title_suffix_ua: None,
            title_replacements: None,
            only_available: false,
            discount: None,
            format_years: false,
            add_vendor: false,
            publish: true,
            description: None,
            description_ua: None,
            delivery_time: None,
            adjust_price: None,
            categories: false,
            convert_to_uah: false,
            custom_options: None,
            watermarks: None,
            set_availability: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DDAudioExportOptions {
    #[serde(flatten)]
    pub options: ExportOptions,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub languages: Vec<String>,
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
    #[serde(default = "bool_true")]
    pub append_attributes: bool,
}

impl PartialOrd for DDAudioExportOptions {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DDAudioExportOptions {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut self_category_rules: Vec<_> = self
            .category_rules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self_category_rules.sort_by(|a, b| a.0.cmp(&b.0));
        let mut other_category_rules: Vec<_> = other
            .category_rules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        other_category_rules.sort_by(|a, b| a.0.cmp(&b.0));

        let mut self_sub_rules: Vec<_> = self
            .subcategory_rules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self_sub_rules.sort_by(|a, b| a.0.cmp(&b.0));
        let mut other_sub_rules: Vec<_> = other
            .subcategory_rules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        other_sub_rules.sort_by(|a, b| a.0.cmp(&b.0));
        let mut self_warehouse_statuses: Vec<_> = self
            .warehouse_statuses
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        self_warehouse_statuses.sort_by(|a, b| a.0.cmp(&b.0));
        let mut other_warehouse_statuses: Vec<_> = other
            .warehouse_statuses
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        other_warehouse_statuses.sort_by(|a, b| a.0.cmp(&b.0));

        let mut ord = self.options.cmp(&other.options);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.token.cmp(&other.token);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.languages.cmp(&other.languages);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.selected_categories.cmp(&other.selected_categories);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.selected_subcategories.cmp(&other.selected_subcategories);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.selected_warehouses.cmp(&other.selected_warehouses);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self_warehouse_statuses.cmp(&other_warehouse_statuses);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.default_rule.cmp(&other.default_rule);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.title_replacements_ru.cmp(&other.title_replacements_ru);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.title_replacements_ua.cmp(&other.title_replacements_ua);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self.append_attributes.cmp(&other.append_attributes);
        if ord != Ordering::Equal {
            return ord;
        }
        ord = self_category_rules.cmp(&other_category_rules);
        if ord != Ordering::Equal {
            return ord;
        }
        self_sub_rules.cmp(&other_sub_rules)
    }
}

impl Default for DDAudioExportOptions {
    fn default() -> Self {
        Self {
            options: ExportOptions::default(),
            token: String::new(),
            languages: vec!["ru".to_string(), "ua".to_string()],
            selected_categories: Vec::new(),
            selected_subcategories: Vec::new(),
            selected_warehouses: Vec::new(),
            known_warehouses: Vec::new(),
            warehouse_statuses: HashMap::new(),
            category_rules: HashMap::new(),
            subcategory_rules: HashMap::new(),
            default_rule: DDAudioCategoryRule::default(),
            title_replacements_ru: Vec::new(),
            title_replacements_ua: Vec::new(),
            append_attributes: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Discount {
    pub percent: usize,
    #[serde(deserialize_with = "deserialize_duration_from_string")]
    #[serde(serialize_with = "serialize_duration_into_string")]
    pub duration: Duration,
}

fn bool_false() -> bool {
    false
}

fn bool_true() -> bool {
    true
}

fn default_update_rate() -> Duration {
    Duration::from_secs(6 * 60 * 60)
}

pub fn deserialize_duration_from_string<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.replace(" ", "");
    crate::parse_duration(&s).map_err(serde::de::Error::custom)
}

pub fn serialize_duration_into_string<S: Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{}s", duration.as_secs()))
}
