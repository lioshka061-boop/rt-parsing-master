use crate::control::{Record, Response};
use crate::category_auto;
use crate::dt;
use crate::product_category;
use crate::product_category_auto;
use crate::quick_order;
use crate::review;
use crate::order;
use crate::seo_page;
use crate::shop_product;
use crate::site_publish;
use actix_web::{get, post};
use actix_web::web::{Data, Json, Path, Query};
use actix_web::HttpRequest;
use anyhow::anyhow;
use regex::Regex;
use rt_types::category::{By, Category, CategoryRepository};
use serde::Deserialize;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use once_cell::sync::Lazy;

mod rate_limit {
    use actix_web::HttpRequest;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;
    use once_cell::sync::Lazy;

    #[derive(Clone)]
    struct RateLimitEntry {
        count: u32,
        reset_at: Instant,
    }

    struct RateLimiter {
        entries: Arc<RwLock<HashMap<String, RateLimitEntry>>>,
        max_requests: u32,
        window_secs: u64,
    }

    impl RateLimiter {
        fn new(max_requests: u32, window_secs: u64) -> Self {
            Self {
                entries: Arc::new(RwLock::new(HashMap::new())),
                max_requests,
                window_secs,
            }
        }

        async fn check(&self, key: &str) -> Result<(), RateLimitError> {
            let now = Instant::now();
            let mut entries = self.entries.write().await;
            
            // Очищаємо застарілі записи
            entries.retain(|_, entry| entry.reset_at > now);
            
            let entry = entries.entry(key.to_string()).or_insert_with(|| RateLimitEntry {
                count: 0,
                reset_at: now + Duration::from_secs(self.window_secs),
            });
            
            if entry.reset_at <= now {
                // Окно закінчилося, скидаємо лічильник
                entry.count = 1;
                entry.reset_at = now + Duration::from_secs(self.window_secs);
                return Ok(());
            }
            
            entry.count += 1;
            if entry.count > self.max_requests {
                let retry_after = (entry.reset_at - now).as_secs().max(1);
                return Err(RateLimitError {
                    retry_after,
                    message: format!("Rate limit exceeded. Max {} requests per {} seconds", self.max_requests, self.window_secs),
                });
            }
            
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct RateLimitError {
        pub retry_after: u64,
        pub message: String,
    }

    impl std::fmt::Display for RateLimitError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl std::error::Error for RateLimitError {}

    // Rate limiter для API endpoints: 100 запитів на 60 секунд на IP
    static API_RATE_LIMITER: Lazy<RateLimiter> = Lazy::new(|| {
        let max = std::env::var("API_RATE_LIMIT_MAX")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(100);
        let window = std::env::var("API_RATE_LIMIT_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60);
        RateLimiter::new(max, window)
    });

    fn get_client_ip(req: &HttpRequest) -> String {
        // Перевіряємо заголовки для отримання реального IP
        if let Some(forwarded) = req.headers().get("x-forwarded-for") {
            if let Ok(forwarded_str) = forwarded.to_str() {
                if let Some(first_ip) = forwarded_str.split(',').next() {
                    return first_ip.trim().to_string();
                }
            }
        }
        
        if let Some(real_ip) = req.headers().get("x-real-ip") {
            if let Ok(ip_str) = real_ip.to_str() {
                return ip_str.to_string();
            }
        }
        
        // Fallback на peer address
        req.peer_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub async fn check_api_rate_limit(req: &HttpRequest) -> Result<(), RateLimitError> {
        let ip = get_client_ip(req);
        let key = format!("api:{}", ip);
        API_RATE_LIMITER.check(&key).await
    }
}

use rate_limit::check_api_rate_limit;

#[derive(Clone, Debug)]
struct SeoTemplates {
    product_title: String,
    product_h1: String,
    product_description: String,
    category_title: String,
    category_h1: String,
    category_description: String,
    global_title: String,
    global_h1: String,
    global_description: String,
}

#[derive(Deserialize)]
pub struct QuickOrderRequest {
    pub phone: String,
    pub article: Option<String>,
    pub title: Option<String>,
}

#[derive(Deserialize)]
pub struct OrderItemRequest {
    pub article: String,
    pub title: String,
    pub price: Option<usize>,
    pub quantity: Option<usize>,
}

#[derive(Deserialize)]
pub struct OrderRequest {
    pub email: Option<String>,
    pub phone: String,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: Option<String>,
    pub delivery: String,
    pub city_name: Option<String>,
    pub branch_name: Option<String>,
    pub comment: Option<String>,
    pub payment: String,
    pub news: Option<bool>,
    pub items: Vec<OrderItemRequest>,
}

impl Default for SeoTemplates {
    fn default() -> Self {
        SeoTemplates {
            product_title: "{brand} {model} {category} {article}".to_string(),
            product_h1: "{title}".to_string(),
            product_description:
                "Купити {title} для {brand} {model}. Категорія: {category}. Артикул: {article}."
                    .to_string(),
            category_title: "{category} для {brand} {model}".to_string(),
            category_h1: "{category}".to_string(),
            category_description:
                "Категорія {category} для {brand} {model}. Популярні позиції: {title}.".to_string(),
            global_title: "Тюнінг та запчастини для авто".to_string(),
            global_h1: "Каталог тюнінгу".to_string(),
            global_description:
                "Каталог тюнінгу, спліттери, дифузори, спойлери та обвіси з доставкою.".to_string(),
        }
    }
}

struct ProductsCache {
    cached_at: Instant,
    items: Arc<Vec<dt::product::Product>>,
}

fn cache_ttl_from_env(key: &str, default_secs: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(default_secs))
}

static DT_PRODUCTS_CACHE_TTL: Lazy<Duration> =
    Lazy::new(|| cache_ttl_from_env("DT_PRODUCTS_CACHE_TTL_SECS", 600));

static DT_PRODUCTS_CACHE: Lazy<RwLock<Option<ProductsCache>>> = Lazy::new(|| RwLock::new(None));

pub(crate) struct CachedProduct {
    pub(crate) dto: ProductDto,
    pub(crate) brand_slug: String,
    pub(crate) model_slug: String,
    pub(crate) category_slug: String,
    pub(crate) search_blob: String,
    pub(crate) article_lower: String,
    pub(crate) category_id: Option<uuid::Uuid>,
    pub(crate) lastmod: OffsetDateTime,
    pub(crate) is_hit: bool,
}

struct SiteProductsCache {
    cached_at: Instant,
    shop_id: String,
    allowed_suppliers: Vec<String>,
    items: Arc<Vec<CachedProduct>>,
    by_article: Arc<HashMap<String, usize>>,
    by_brand_slug: Arc<HashMap<String, Vec<usize>>>,
    by_model_slug: Arc<HashMap<String, Vec<usize>>>,
    by_category_slug: Arc<HashMap<String, Vec<usize>>>,
    hit_indices: Arc<HashSet<usize>>,
}

static SITE_PRODUCTS_CACHE_TTL: Lazy<Duration> =
    Lazy::new(|| cache_ttl_from_env("SITE_PRODUCTS_CACHE_TTL_SECS", 600));

static SITE_PRODUCTS_CACHE: Lazy<RwLock<Option<SiteProductsCache>>> =
    Lazy::new(|| RwLock::new(None));

static PRIMARY_SHOP_CACHE_TTL: Lazy<Duration> =
    Lazy::new(|| cache_ttl_from_env("PRIMARY_SHOP_CACHE_TTL_SECS", 60));

static PRIMARY_SHOP_CACHE: Lazy<RwLock<Option<(Instant, rt_types::shop::Shop)>>> =
    Lazy::new(|| RwLock::new(None));

async fn load_dt_products_cached(
    dt_repo: &Arc<dyn dt::product::ProductRepository + Send>,
) -> Arc<Vec<dt::product::Product>> {
    {
        let cache = DT_PRODUCTS_CACHE.read().await;
        if let Some(entry) = cache.as_ref() {
            if entry.cached_at.elapsed() < *DT_PRODUCTS_CACHE_TTL {
                return entry.items.clone();
            }
        }
    }
    let mut items = dt_repo.list().await.unwrap_or_default();
    items.sort_by_key(|p| p.last_visited);
    items.reverse();
    let items = Arc::new(items);
    let mut cache = DT_PRODUCTS_CACHE.write().await;
    *cache = Some(ProductsCache {
        cached_at: Instant::now(),
        items: items.clone(),
    });
    items
}

pub(crate) async fn load_site_products_cached(
    shop: &rt_types::shop::Shop,
    allowed_suppliers: &[String],
    dt_repo: &Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: &Arc<dyn shop_product::ShopProductRepository>,
    category_repo: &Arc<dyn CategoryRepository>,
    product_category_repo: &Arc<dyn product_category::ProductCategoryRepository>,
) -> (Arc<Vec<CachedProduct>>, Arc<HashMap<String, usize>>) {
    let mut allowed_key = allowed_suppliers
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    allowed_key.sort();

    {
        let cache = SITE_PRODUCTS_CACHE.read().await;
        if let Some(entry) = cache.as_ref() {
            if entry.cached_at.elapsed() < *SITE_PRODUCTS_CACHE_TTL
                && entry.shop_id == shop.id.to_string()
                && entry.allowed_suppliers == allowed_key
            {
                return (entry.items.clone(), entry.by_article.clone());
            }
        }
    }

    let shop_id = shop.id;
    let product_categories_filter = product_category::ByShop(shop_id);
    let categories_filter = By(shop_id);
    let (overrides_result, product_categories_result, categories_result) = tokio::join!(
        shop_product_repo.list_by_shop(shop_id),
        product_category_repo.select(&product_categories_filter),
        category_repo.select(&categories_filter)
    );

    let overrides = overrides_result.unwrap_or_default();
    let mut overrides_by_article: HashMap<String, shop_product::ShopProduct> = HashMap::new();
    for o in overrides.into_iter() {
        overrides_by_article.insert(o.article.to_lowercase(), o);
    }

    let product_categories = product_categories_result.unwrap_or_default();
    let category_matcher = product_category_auto::CategoryMatcher::new(&product_categories);
    let mut product_category_by_id = HashMap::<uuid::Uuid, String>::new();
    for c in product_categories.iter() {
        product_category_by_id.insert(c.id, c.name.clone());
    }
    let categories = categories_result.unwrap_or_default();
    let mut category_by_id = HashMap::<uuid::Uuid, &Category>::new();
    for c in categories.iter() {
        category_by_id.insert(c.id, c);
    }
    let mut model_slugs_by_brand: HashMap<String, HashSet<String>> = HashMap::new();
    for c in categories.iter().filter(|c| c.parent_id.is_some()) {
        let mut current = c;
        let mut backtrace = HashSet::<uuid::Uuid>::new();
        while let Some(parent_id) = current.parent_id {
            if !backtrace.insert(parent_id) {
                break;
            }
            if let Some(parent) = category_by_id.get(&parent_id) {
                current = parent;
            } else {
                break;
            }
        }
        if current.parent_id.is_some() {
            continue;
        }
        let brand_slug = slugify_latin(&current.name);
        let model_slug = slugify_latin(&c.name);
        if brand_slug.is_empty() || model_slug.is_empty() {
            continue;
        }
        model_slugs_by_brand
            .entry(brand_slug)
            .or_default()
            .insert(model_slug);
    }

    let allowed_set = allowed_suppliers_set(&allowed_key);
    let base = load_dt_products_cached(dt_repo).await;
    let templates = SeoTemplates::default();
    let mut seen_titles: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_slugs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_canonicals: std::collections::HashSet<String> = std::collections::HashSet::new();

    let mut items = Vec::new();
    let mut by_article: HashMap<String, usize> = HashMap::new();
    for p in base.iter() {
        if !product_allowed_for_site(p, &allowed_set) {
            continue;
        }
        let key = p.article.to_lowercase();
        if by_article.contains_key(&key) {
            continue;
        }
        let o = overrides_by_article.get(&key);
        let status = o
            .map(|x| x.status.clone())
            .unwrap_or_else(default_product_status);
        let is_hit = o.map(|x| x.is_hit).unwrap_or(false);
        let visibility = o
            .map(|x| x.visibility_on_site.clone())
            .unwrap_or_else(default_product_visibility);
        if matches!(status, crate::shop_product::ProductStatus::Draft) {
            continue;
        }

        let model_name = p.format_model().unwrap_or(p.model.0.clone());
        let title_ua = p
            .title_ua
            .clone()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let description_ua = p
            .description_ua
            .clone()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let title = o
            .and_then(|x| x.title.clone())
            .unwrap_or_else(|| title_ua.clone().unwrap_or_else(|| p.title.clone()));
        let description = o
            .and_then(|x| x.description.clone())
            .or_else(|| description_ua.clone())
            .or_else(|| p.description.clone());
        let price = o.and_then(|x| x.price).or(p.price);
        let mut available = o
            .and_then(|x| x.available.clone())
            .unwrap_or_else(|| p.available.clone());
        if matches!(
            site_publish::detect_supplier(&p).as_deref(),
            Some("maxton") | Some("jgd") | Some("skm")
        ) {
            available = rt_types::Availability::OnOrder;
        }
        let images = o
            .and_then(|x| x.images.clone())
            .unwrap_or_else(|| p.images.clone());
        let category_id = match o.and_then(|x| x.site_category_id) {
            Some(id) => Some(id),
            None => {
                let haystack = product_category_auto::build_haystack(
                    &title,
                    description.as_deref().unwrap_or_default(),
                );
                category_matcher.guess(&haystack)
            }
        };
        let category = category_id.and_then(|id| product_category_by_id.get(&id).cloned());

        let h1 = o
            .and_then(|x| x.h1.clone())
            .unwrap_or_else(|| title.clone());
        let mut seo_score = o.map(|x| x.seo_score).unwrap_or(0);
        let indexing = o
            .map(|x| x.indexing_status.clone())
            .unwrap_or_else(default_product_indexing);

        let raw_title = title.clone();
        let raw_description = description.clone();
        let raw_images = images.clone();
        let raw_slug = o
            .and_then(|x| x.slug.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let mut dto = ProductDto {
            article: p.article.clone(),
            title,
            model: model_name,
            brand: p.brand.clone(),
            price,
            available,
            url: p.url.0.clone(),
            images: normalize_images(&images),
            category,
            description,
            status: Some(status.as_str().to_string()),
            visibility_on_site: Some(visibility.as_str().to_string()),
            indexing_status: Some(indexing.as_str().to_string()),
            source_type: o
                .map(|x| x.source_type.as_str().to_string())
                .or(Some(default_product_source().as_str().to_string())),
            seo_score: Some(seo_score),
            h1: Some(h1),
            canonical: None,
            robots: None,
            meta_title: None,
            meta_description: None,
            seo_text: o.and_then(|x| x.seo_text.clone()),
            faq: o.and_then(|x| x.faq.clone()),
            og_title: o.and_then(|x| x.og_title.clone()),
            og_description: o.and_then(|x| x.og_description.clone()),
            og_image: o.and_then(|x| x.og_image.clone()),
            slug: None,
            path: String::new(),
            indexable: false,
        };
        apply_seo_templates(&mut dto, &templates);

        let slug_value = raw_slug
            .clone()
            .unwrap_or_else(|| build_product_slug(&dto.title, &dto.brand, &dto.model, &dto.article));
        let path = product_path_from_slug(&slug_value, &dto.article);
        let canonical_candidate = canonical_from_path(&path).unwrap_or_else(|| path.clone());

        let desc_plain = dto
            .description
            .as_deref()
            .map(plain_text)
            .unwrap_or_default();
        let raw_desc_plain = raw_description
            .as_deref()
            .map(plain_text)
            .unwrap_or_default();
        let has_title = !raw_title.trim().is_empty();
        let has_description = raw_desc_plain.chars().count() >= 50;
        let has_images = !raw_images.is_empty();
        let has_category = category_id.is_some();
        let slug_confirmed = !slug_value.trim().is_empty();
        let base_indexable = matches!(status, crate::shop_product::ProductStatus::SeoReady)
            && has_title
            && has_description
            && has_images
            && has_category
            && slug_confirmed;
        let mut indexable = base_indexable;

        if base_indexable {
            let guard = validate_seo_ready(
                &dto.title,
                dto.h1.as_deref().unwrap_or(""),
                &dto.description,
                &dto.images,
                &dto.category,
                &canonical_candidate,
                &slug_value,
                &mut seen_titles,
                &mut seen_slugs,
                &mut seen_canonicals,
            );
            if guard.ok {
                seo_score = 100;
            } else {
                indexable = false;
            }
        }

        let robots = if !indexable {
            Some("noindex,follow".to_string())
        } else {
            Some("index,follow".to_string())
        };

        let canonical = if indexable {
            canonical_from_path(&path)
        } else {
            None
        };
        let meta_title = if indexable {
            Some(trim_to(&plain_text(&dto.title), 60))
        } else {
            None
        };
        let meta_description = if indexable {
            Some(trim_to(&desc_plain, 155))
        } else {
            None
        };

        dto.seo_score = Some(seo_score);
        dto.slug = Some(slug_value);
        dto.path = path;
        dto.indexable = indexable;
        dto.robots = robots;
        dto.canonical = canonical;
        dto.meta_title = meta_title;
        dto.meta_description = meta_description;

        let mut display_brand = dto.brand.clone();
        let mut display_model = dto.model.clone();
        let mut brand_slug = slugify_latin(&display_brand);
        let mut model_slug = slugify_latin(&display_model);
        if !categories.is_empty() {
            let in_known_model = model_slugs_by_brand
                .get(&brand_slug)
                .map(|models| models.contains(&model_slug))
                .unwrap_or(false);
            if !in_known_model || brand_slug.is_empty() {
                if let Some((brand, model, category_id)) = category_auto::guess_brand_model(
                    &dto.title,
                    dto.description.as_deref(),
                    &categories,
                ) {
                    display_brand = brand;
                    brand_slug = slugify_latin(&display_brand);
                    if category_id.is_some() {
                        display_model = model;
                        model_slug = slugify_latin(&display_model);
                    } else {
                        model_slug.clear();
                    }
                }
            }
        }
        dto.brand = display_brand;
        dto.model = display_model;
        let category_slug = dto
            .category
            .as_deref()
            .map(slugify_latin)
            .unwrap_or_default();
        let search_blob = format!(
            "{} {} {} {} {} {}",
            dto.title,
            dto.brand,
            dto.model,
            dto.category.clone().unwrap_or_default(),
            desc_plain,
            dto.article
        )
        .to_lowercase();
        let article_lower = dto.article.to_lowercase();
        let lastmod = o.map(|x| x.updated_at).unwrap_or(p.last_visited);

        let idx = items.len();
        by_article.insert(article_lower.clone(), idx);
        items.push(CachedProduct {
            dto,
            brand_slug,
            model_slug,
            category_slug,
            search_blob,
            article_lower,
            category_id,
            lastmod,
            is_hit,
        });
    }

    let items = Arc::new(items);
    let by_article = Arc::new(by_article);
    
    // Build filtering indexes
    let mut by_brand_slug: HashMap<String, Vec<usize>> = HashMap::new();
    let mut by_model_slug: HashMap<String, Vec<usize>> = HashMap::new();
    let mut by_category_slug: HashMap<String, Vec<usize>> = HashMap::new();
    let mut hit_indices: HashSet<usize> = HashSet::new();
    
    for (idx, item) in items.iter().enumerate() {
        if !item.brand_slug.is_empty() {
            by_brand_slug.entry(item.brand_slug.clone()).or_default().push(idx);
        }
        if !item.model_slug.is_empty() {
            by_model_slug.entry(item.model_slug.clone()).or_default().push(idx);
        }
        if !item.category_slug.is_empty() {
            by_category_slug.entry(item.category_slug.clone()).or_default().push(idx);
        }
        if item.is_hit {
            hit_indices.insert(idx);
        }
    }
    
    let by_brand_slug = Arc::new(by_brand_slug);
    let by_model_slug = Arc::new(by_model_slug);
    let by_category_slug = Arc::new(by_category_slug);
    let hit_indices = Arc::new(hit_indices);
    
    let mut cache = SITE_PRODUCTS_CACHE.write().await;
    *cache = Some(SiteProductsCache {
        cached_at: Instant::now(),
        shop_id: shop.id.to_string(),
        allowed_suppliers: allowed_key,
        items: items.clone(),
        by_article: by_article.clone(),
        by_brand_slug: by_brand_slug.clone(),
        by_model_slug: by_model_slug.clone(),
        by_category_slug: by_category_slug.clone(),
        hit_indices: hit_indices.clone(),
    });
    (items, by_article)
}

fn allowed_suppliers_set(allowed_suppliers: &[String]) -> HashSet<String> {
    allowed_suppliers
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn product_allowed_for_site(
    product: &dt::product::Product,
    allowed: &HashSet<String>,
) -> bool {
    if allowed.is_empty() {
        return true;
    }
    site_publish::detect_supplier(product)
        .map(|s| allowed.contains(&s.to_lowercase()))
        .unwrap_or(false)
}

fn render_template(tpl: &str, ctx: &HashMap<&str, String>) -> String {
    let mut out = tpl.to_string();
    for (k, v) in ctx {
        out = out.replace(&format!("{{{k}}}"), v);
    }
    out.trim().to_string()
}

fn apply_seo_templates(dto: &mut ProductDto, templates: &SeoTemplates) {
    let ctx = {
        let mut m = HashMap::new();
        m.insert("title", dto.title.clone());
        m.insert("brand", dto.brand.clone());
        m.insert("model", dto.model.clone());
        m.insert("category", dto.category.clone().unwrap_or_default());
        m.insert("article", dto.article.clone());
        m
    };

    // manual -> product template -> category template -> global template
    if dto.title.trim().is_empty() {
        let candidate = render_template(&templates.product_title, &ctx);
        if !candidate.is_empty() {
            dto.title = candidate;
        } else if !dto.category.clone().unwrap_or_default().is_empty() {
            let candidate = render_template(&templates.category_title, &ctx);
            if !candidate.is_empty() {
                dto.title = candidate;
            }
        }
        if dto.title.trim().is_empty() {
            dto.title = templates.global_title.clone();
        }
    }

    if dto.h1.as_deref().unwrap_or("").trim().is_empty() {
        let candidate = render_template(&templates.product_h1, &ctx);
        if !candidate.is_empty() {
            dto.h1 = Some(candidate);
        } else if !dto.category.clone().unwrap_or_default().is_empty() {
            let candidate = render_template(&templates.category_h1, &ctx);
            if !candidate.is_empty() {
                dto.h1 = Some(candidate);
            }
        }
        if dto.h1.as_deref().unwrap_or("").trim().is_empty() {
            dto.h1 = Some(templates.global_h1.clone());
        }
    }

    if dto.description.as_deref().unwrap_or("").trim().is_empty() {
        let candidate = render_template(&templates.product_description, &ctx);
        if !candidate.is_empty() {
            dto.description = Some(candidate);
        } else if !dto.category.clone().unwrap_or_default().is_empty() {
            let candidate = render_template(&templates.category_description, &ctx);
            if !candidate.is_empty() {
                dto.description = Some(candidate);
            }
        }
        if dto.description.as_deref().unwrap_or("").trim().is_empty() {
            dto.description = Some(templates.global_description.clone());
        }
    }
}

#[derive(Deserialize)]
pub struct ProductsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub brand: Option<String>,
    pub model: Option<String>,
    pub category: Option<String>,
    pub q: Option<String>,
    pub include_total: Option<bool>,
    pub compact: Option<bool>,
    pub hit: Option<bool>,
}

#[derive(Deserialize)]
pub struct ModelCategoriesQuery {
    pub brand: Option<String>,
    pub model: Option<String>,
}

#[derive(Deserialize)]
pub struct ReviewsQuery {
    pub product: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
pub struct ReviewCreateRequest {
    pub product: Option<String>,
    pub name: String,
    pub text: String,
    pub rating: Option<i64>,
    pub photos: Option<Vec<String>>,
}

#[derive(Clone, Serialize)]
pub struct ProductDto {
    pub article: String,
    pub title: String,
    pub model: String,
    pub brand: String,
    pub price: Option<usize>,
    pub available: rt_types::Availability,
    pub url: String,
    pub images: Vec<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub status: Option<String>,
    pub visibility_on_site: Option<String>,
    pub indexing_status: Option<String>,
    pub source_type: Option<String>,
    pub seo_score: Option<i32>,
    pub h1: Option<String>,
    pub canonical: Option<String>,
    pub robots: Option<String>,
    pub meta_title: Option<String>,
    pub meta_description: Option<String>,
    pub seo_text: Option<String>,
    pub og_title: Option<String>,
    pub og_description: Option<String>,
    pub og_image: Option<String>,
    pub slug: Option<String>,
    pub path: String,
    pub indexable: bool,
    pub faq: Option<String>,
}

#[derive(Serialize)]
pub struct SeoPageDto {
    pub id: String,
    pub page_type: String,
    pub slug: String,
    pub path: String,
    pub title: String,
    pub h1: Option<String>,
    pub meta_title: Option<String>,
    pub meta_description: Option<String>,
    pub seo_text: Option<String>,
    pub faq: Option<String>,
    pub robots: Option<String>,
    pub canonical: Option<String>,
    pub indexable: bool,
    pub related_links: Vec<String>,
    pub payload: seo_page::SeoPagePayload,
    pub product_count: usize,
}

#[derive(Deserialize)]
pub struct SeoPagesQuery {
    pub page_type: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub status: Option<String>,
    pub indexable: Option<bool>,
}

#[derive(Serialize)]
pub struct SeoPageListItem {
    pub id: String,
    pub page_type: String,
    pub slug: String,
    pub path: String,
    pub title: String,
    pub h1: Option<String>,
    pub meta_description: Option<String>,
    pub seo_text: Option<String>,
    pub indexable: bool,
    pub product_count: usize,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize)]
pub struct ReviewDto {
    pub id: i64,
    pub name: String,
    pub text: String,
    pub rating: i64,
    pub photos: Vec<String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

#[derive(Serialize)]
pub struct SitemapEntry {
    pub loc: String,
    pub lastmod: String,
}

#[derive(Serialize)]
pub struct CategoryNode {
    pub id: String,
    pub name: String,
    pub children: Vec<CategoryNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seo_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seo_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seo_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexable: Option<bool>,
}

fn default_product_status() -> crate::shop_product::ProductStatus {
    crate::shop_product::ProductStatus::PublishedNoIndex
}

fn default_product_visibility() -> crate::shop_product::Visibility {
    crate::shop_product::Visibility::Visible
}

fn default_product_indexing() -> crate::shop_product::IndexingStatus {
    crate::shop_product::IndexingStatus::NoIndex
}

fn default_product_source() -> crate::shop_product::SourceType {
    crate::shop_product::SourceType::Parsing
}

fn normalize_images(images: &[String]) -> Vec<String> {
    let uploads_base = uploads_base();
    images
        .iter()
        .map(|i| {
            if i.starts_with("/static/uploads/") {
                if let Some(base) = &uploads_base {
                    return format!("{base}{i}");
                }
                return i.clone();
            }
            if i.starts_with('/') {
                // DT/Maxton зберігає відносні шляхи та mini_ прев'юшки
                format!("https://design-tuning.com{}", i.replace("mini_", ""))
            } else {
                i.clone()
            }
        })
        .collect()
}

fn ensure_api_key(req: &HttpRequest) -> Result<(), crate::control::ControllerError> {
    let expected = std::env::var("SITE_API_KEY")
        .map_err(|_| crate::control::ControllerError::Forbidden)?;
    if expected.trim().is_empty() {
        return Err(crate::control::ControllerError::Forbidden);
    }
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if Some(expected) != provided {
        return Err(crate::control::ControllerError::Forbidden);
    }
    Ok(())
}

fn api_key_valid(req: &HttpRequest) -> bool {
    let expected = match std::env::var("SITE_API_KEY") {
        Ok(value) => value,
        Err(_) => return false,
    };
    if expected.trim().is_empty() {
        return false;
    }
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Some(expected) == provided
}

fn slugify(input: &str) -> String {
    let lower = input.to_lowercase();
    let re = Regex::new(r"[^\p{L}\p{N}\s-]+").unwrap();
    let cleaned = re.replace_all(&lower, " ");
    let re_space = Regex::new(r"[\s_-]+").unwrap();
    let dashed = re_space.replace_all(cleaned.trim(), "-");
    dashed.trim_matches('-').to_string()
}

fn slugify_latin(input: &str) -> String {
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

fn normalize_filter(value: Option<&str>) -> Option<String> {
    value
        .map(slugify_latin)
        .filter(|v| !v.is_empty())
}

fn sanitize_review_photos(input: Option<Vec<String>>) -> Vec<String> {
    const MAX_PHOTOS: usize = 3;
    const MAX_LEN: usize = 1_000_000;
    let mut out = Vec::new();
    for src in input.unwrap_or_default() {
        let trimmed = src.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !trimmed.starts_with("data:image")
            && !trimmed.starts_with("http://")
            && !trimmed.starts_with("https://")
        {
            continue;
        }
        if trimmed.len() > MAX_LEN {
            continue;
        }
        out.push(trimmed.to_string());
        if out.len() >= MAX_PHOTOS {
            break;
        }
    }
    out
}

fn parse_page_types(input: Option<&str>) -> Vec<seo_page::SeoPageType> {
    let mut out = Vec::new();
    let raw = match input {
        Some(v) => v,
        None => return out,
    };
    for chunk in raw.split(',') {
        let token = chunk.trim();
        if token.is_empty() {
            continue;
        }
        let parsed = match token.to_lowercase().as_str() {
            "tuning" | "tuning_model" => Some(seo_page::SeoPageType::TuningModel),
            "accessories" | "accessories_car" => Some(seo_page::SeoPageType::AccessoriesCar),
            "guides" | "how_to_choose" => Some(seo_page::SeoPageType::HowToChoose),
            _ => None,
        };
        if let Some(parsed) = parsed {
            if !out.contains(&parsed) {
                out.push(parsed);
            }
        }
    }
    out
}

fn normalize_segment(input: &str) -> String {
    slugify_latin(input).trim_matches('-').to_string()
}

pub(crate) fn build_product_slug(title: &str, brand: &str, model: &str, article: &str) -> String {
    let base = format!("{title} {model} {brand}");
    let slug = normalize_segment(&base);
    if slug.is_empty() {
        normalize_segment(article)
    } else {
        slug
    }
}

pub(crate) fn product_path_from_slug(slug: &str, article: &str) -> String {
    let encoded_article: String = url::form_urlencoded::byte_serialize(article.as_bytes()).collect();
    if slug.is_empty() {
        return format!("/item/{encoded_article}");
    }
    if encoded_article.is_empty() {
        return format!("/item/{slug}");
    }
    format!("/item/{slug}-{encoded_article}")
}

fn product_path(title: &str, brand: &str, model: &str, article: &str) -> String {
    let slug = build_product_slug(title, brand, model, article);
    product_path_from_slug(&slug, article)
}

fn site_base() -> Option<String> {
    std::env::var("NEXT_PUBLIC_SITE_URL")
        .ok()
        .map(|s| s.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
}

fn uploads_base() -> Option<String> {
    std::env::var("UPLOADS_BASE_URL")
        .ok()
        .map(|s| s.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .or_else(site_base)
}

pub(crate) fn canonical_from_path(path: &str) -> Option<String> {
    site_base().map(|base| format!("{base}{path}"))
}

fn trim_to(input: &str, max: usize) -> String {
    if input.chars().count() <= max {
        return input.to_string();
    }
    input
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>()
        .trim_end()
        .to_string()
        + "…"
}

fn plain_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    for ch in input.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn format_lastmod(dt: OffsetDateTime) -> String {
    dt.date().to_string()
}

#[derive(Debug)]
struct SeoGuardResult {
    ok: bool,
    reasons: Vec<String>,
}

fn validate_seo_ready(
    title: &str,
    h1: &str,
    description: &Option<String>,
    images: &[String],
    category: &Option<String>,
    canonical: &str,
    slug: &str,
    seen_titles: &mut std::collections::HashSet<String>,
    seen_slugs: &mut std::collections::HashSet<String>,
    seen_canonicals: &mut std::collections::HashSet<String>,
) -> SeoGuardResult {
    let mut reasons = Vec::new();
    if title.trim().is_empty() {
        reasons.push("title_empty".to_string());
    }
    if h1.trim().is_empty() {
        reasons.push("h1_empty".to_string());
    }
    if description
        .as_ref()
        .map(|d| d.trim().is_empty())
        .unwrap_or(true)
    {
        reasons.push("description_empty".to_string());
    }
    if images.is_empty() {
        reasons.push("images_missing".to_string());
    }
    if category
        .as_ref()
        .map(|c| c.trim().is_empty())
        .unwrap_or(true)
    {
        reasons.push("category_missing".to_string());
    }
    let canonical_ok = canonical.starts_with("http://")
        || canonical.starts_with("https://")
        || canonical.starts_with('/');
    if !canonical_ok {
        reasons.push("canonical_invalid".to_string());
    }
    if slug.trim().is_empty() {
        reasons.push("slug_empty".to_string());
    }
    let key = title.trim().to_lowercase();
    if !key.is_empty() {
        if seen_titles.contains(&key) {
            reasons.push("title_duplicate".to_string());
        } else {
            seen_titles.insert(key);
        }
    }
    let slug_key = slug.trim().to_lowercase();
    if !slug_key.is_empty() {
        if seen_slugs.contains(&slug_key) {
            reasons.push("slug_duplicate".to_string());
        } else {
            seen_slugs.insert(slug_key);
        }
    }
    let canonical_key = canonical.trim().to_lowercase();
    if !canonical_key.is_empty() {
        if seen_canonicals.contains(&canonical_key) {
            reasons.push("canonical_duplicate".to_string());
        } else {
            seen_canonicals.insert(canonical_key);
        }
    }
    SeoGuardResult {
        ok: reasons.is_empty(),
        reasons,
    }
}

fn intersect_indices(indices_list: Vec<&[usize]>) -> Vec<usize> {
    if indices_list.is_empty() {
        return Vec::new();
    }
    if indices_list.len() == 1 {
        return indices_list[0].to_vec();
    }
    
    // Start with the smallest set for efficiency
    let mut sorted_list = indices_list.iter().collect::<Vec<_>>();
    sorted_list.sort_by_key(|indices| indices.len());
    
    let mut result: HashSet<usize> = sorted_list[0].iter().copied().collect();
    
    for indices in sorted_list.iter().skip(1) {
        let current: HashSet<usize> = indices.iter().copied().collect();
        result = result.intersection(&current).copied().collect();
        if result.is_empty() {
            break;
        }
    }
    
    let mut vec_result: Vec<usize> = result.into_iter().collect();
    vec_result.sort_unstable();
    vec_result
}

async fn get_primary_shop_cached(
    shop_service: &actix::Addr<rt_types::shop::service::ShopService>,
    shop_product_repo: &Arc<dyn shop_product::ShopProductRepository>,
    product_category_repo: &Arc<dyn product_category::ProductCategoryRepository>,
) -> Option<rt_types::shop::Shop> {
    {
        let cache = PRIMARY_SHOP_CACHE.read().await;
        if let Some((cached_at, shop)) = cache.as_ref() {
            if cached_at.elapsed() < *PRIMARY_SHOP_CACHE_TTL {
                return Some(shop.clone());
            }
        }
    }

    let shops = match shop_service.send(rt_types::shop::service::List).await {
        Ok(Ok(shops)) => shops,
        _ => return None,
    };

    let mut primary_shop = shops.get(0).cloned();
    
    for s in &shops {
        let shop_id = s.id;
        let product_categories_filter = product_category::ByShop(shop_id);
        let (cat_result, overrides_result) = tokio::join!(
            product_category_repo.select(&product_categories_filter),
            shop_product_repo.list_by_shop(shop_id)
        );
        
        let cat_len = cat_result.unwrap_or_default().len();
        let overrides_len = overrides_result.unwrap_or_default().len();
        
        if cat_len > 0 || overrides_len > 0 {
            primary_shop = Some(s.clone());
            break;
        }
    }

    if let Some(shop) = primary_shop.as_ref() {
        let mut cache = PRIMARY_SHOP_CACHE.write().await;
        *cache = Some((Instant::now(), shop.clone()));
    }

    primary_shop
}

#[get("/api/site/products")]
pub async fn list_products(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<ProductsQuery>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new())),
    };
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    // Get indexes from cache for efficient filtering
    let cache = SITE_PRODUCTS_CACHE.read().await;
    let indexes = cache.as_ref().map(|c| (
        c.by_brand_slug.clone(),
        c.by_model_slug.clone(),
        c.by_category_slug.clone(),
        c.hit_indices.clone(),
    ));
    drop(cache);

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(24).max(1).min(30);
    let brand_filter = normalize_filter(params.brand.as_deref());
    let model_filter = normalize_filter(params.model.as_deref());
    let category_filter = normalize_filter(params.category.as_deref());
    let query = params
        .q
        .as_ref()
        .map(|q| q.trim().to_lowercase())
        .filter(|q| !q.is_empty());
    let want_total = params.include_total.unwrap_or(false);
    let compact = params.compact.unwrap_or(false);
    let hit_only = params.hit.unwrap_or(false);

    // Build candidate indices using indexes
    let candidate_indices = if let Some((by_brand_slug, by_model_slug, by_category_slug, hit_indices)) = indexes {
        let mut index_sets: Vec<&[usize]> = Vec::new();
        let mut hit_vec: Option<Vec<usize>> = None;
        
        if let Some(ref brand_filter) = brand_filter {
            if let Some(indices) = by_brand_slug.get(brand_filter) {
                index_sets.push(indices.as_slice());
            } else {
                // No matches for brand, return empty result
                return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new()));
            }
        }
        
        if let Some(ref model_filter) = model_filter {
            if let Some(indices) = by_model_slug.get(model_filter) {
                index_sets.push(indices.as_slice());
            } else {
                // No matches for model, return empty result
                return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new()));
            }
        }
        
        if let Some(ref category_filter) = category_filter {
            if let Some(indices) = by_category_slug.get(category_filter) {
                index_sets.push(indices.as_slice());
            } else {
                // No matches for category, return empty result
                return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new()));
            }
        }
        
        if hit_only {
            let collected: Vec<usize> = hit_indices.iter().copied().collect();
            if collected.is_empty() {
                // No hits, return empty result
                return Ok(actix_web::HttpResponse::Ok().json(Vec::<ProductDto>::new()));
            }
            hit_vec = Some(collected);
        }

        if let Some(ref vec) = hit_vec {
            index_sets.push(vec.as_slice());
        }
        
        // Intersect all index sets
        let mut candidates = if !index_sets.is_empty() {
            intersect_indices(index_sets)
        } else {
            // No filters, use all indices
            (0..items.len()).collect()
        };
        
        // Apply query filter if present
        if let Some(ref query) = query {
            candidates.retain(|&idx| {
                let item = &items[idx];
                item.search_blob.contains(query) || item.article_lower.contains(query)
            });
        }
        
        candidates
    } else {
        // Fallback to linear scan if cache not available
        (0..items.len()).collect()
    };

    let total_matched = candidate_indices.len();
    let paginated_indices: Vec<usize> = candidate_indices
        .into_iter()
        .skip(offset)
        .take(if want_total { usize::MAX } else { limit })
        .collect();

    let mut slice = Vec::with_capacity(paginated_indices.len().min(limit));
    for idx in paginated_indices.iter().take(limit) {
        let item = &items[*idx];
        let mut dto = item.dto.clone();
        if compact {
            if dto.images.len() > 2 {
                dto.images.truncate(2);
            }
            dto.description = None;
            dto.h1 = None;
            dto.canonical = None;
            dto.robots = None;
            dto.meta_title = None;
            dto.meta_description = None;
            dto.seo_text = None;
            dto.og_title = None;
            dto.og_description = None;
            dto.og_image = None;
            dto.faq = None;
        }
        slice.push(dto);
    }

    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    if want_total {
        resp.insert_header(("X-Total-Count", total_matched.to_string()));
    }
    Ok(resp.json(slice))
}

#[get("/api/site/categories")]
pub async fn list_categories(
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<CategoryNode>::new()));
        }
    };
    
    let mut categories_for_shop = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    
    if categories_for_shop.is_empty() {
        // Якщо категорії відсутні, підтягуємо дефолтний набір і додаємо в БД
        let defs: Vec<(&str, &str)> = vec![
            (
                "Спліттери",
                r"(?i)(спліттер|сплиттер|splitter|lip|губа|передній дифузор|передный диффузор)",
            ),
            (
                "Дифузори",
                r"(?i)(дифузор|диффузор|diffuser|задній дифузор|задний диффузор)",
            ),
            ("Спойлери", r"(?i)(спойлер|spoiler)"),
            ("Пороги", r"(?i)(поріг|порог|side\s*skirt|skirt|пороги)"),
            (
                "Решітки радіатора",
                r"(?i)(решітка|решетка|решітки|решетки|grill|grille|гриль)",
            ),
            ("Бампери", r"(?i)(бампер|bumper|бампери)"),
            ("Диски", r"(?i)(диск|диски|wheels?|r\d{2}\s|r\d{2}\b)"),
            (
                "Плівка / захист",
                r"(?i)(плівка|пленка|захист|бронеплівка|бронепленка|paint\s*protection)",
            ),
            (
                "Комплекти обвісів",
                r"(?i)(обвіс|обвес|body\\s*kit|комплект\\s*обвісів|комплект\\s*обвесов)",
            ),
        ];
        let existing = product_category_repo
            .select(&product_category::ByShop(shop.id))
            .await
            .unwrap_or_default();
        let existing_names: std::collections::HashSet<String> =
            existing.iter().map(|c| c.name.to_lowercase()).collect();
        for (name, re) in defs {
            if existing_names.contains(&name.to_lowercase()) {
                continue;
            }
            if let Ok(regex) = Regex::new(re) {
                let _ = product_category_repo
                    .save(product_category::ProductCategory {
                        id: uuid::Uuid::new_v4(),
                        name: name.to_string(),
                        parent_id: None,
                        regex: Some(regex),
                        shop_id: shop.id,
                        status: product_category::CategoryStatus::PublishedNoIndex,
                        visibility_on_site: product_category::Visibility::Visible,
                        indexing_status: product_category::IndexingStatus::NoIndex,
                        seo_title: None,
                        seo_description: None,
                        seo_text: None,
                        image_url: None,
                    })
                    .await;
            }
        }
        categories_for_shop = product_category_repo
            .select(&product_category::ByShop(shop.id))
            .await
            .unwrap_or_default();
    }
    if categories_for_shop.is_empty() {
        categories_for_shop = product_category_repo
            .select(&product_category::ByShop(shop.id))
            .await
            .unwrap_or_default();
    }
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let mut used_categories: HashSet<uuid::Uuid> = HashSet::new();
    let mut category_counts: HashMap<uuid::Uuid, usize> = HashMap::new();
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;
    for item in items.iter() {
        if let Some(cid) = item.category_id {
            used_categories.insert(cid);
            *category_counts.entry(cid).or_insert(0) += 1;
        }
    }
    let mut by_parent: HashMap<Option<uuid::Uuid>, Vec<product_category::ProductCategory>> =
        HashMap::new();
    for c in categories_for_shop
        .into_iter()
        .filter(|c| matches!(c.visibility_on_site, product_category::Visibility::Visible))
        .filter(|c| !matches!(c.status, product_category::CategoryStatus::Draft))
    {
        by_parent.entry(c.parent_id).or_default().push(c);
    }

    fn build_tree(
        parent: Option<uuid::Uuid>,
        map: &HashMap<Option<uuid::Uuid>, Vec<product_category::ProductCategory>>,
        used: &HashSet<uuid::Uuid>,
        counts: &HashMap<uuid::Uuid, usize>,
    ) -> Vec<CategoryNode> {
        map.get(&parent)
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|c| {
                let children = build_tree(Some(c.id), map, used, counts);
                if used.contains(&c.id) || !children.is_empty() {
                    let slug = slugify_latin(&c.name);
                    let path = if slug.is_empty() {
                        None
                    } else {
                        Some(format!("/category/{slug}"))
                    };
                    let product_count = *counts.get(&c.id).unwrap_or(&0);
                    let raw_title = c.seo_title.clone().unwrap_or_else(|| c.name.clone());
                    let meta_title = trim_to(&plain_text(&raw_title), 60);
                    let raw_desc = c
                        .seo_description
                        .clone()
                        .or_else(|| c.seo_text.clone())
                        .unwrap_or_default();
                    let meta_desc = if raw_desc.trim().is_empty() {
                        None
                    } else {
                        Some(trim_to(&plain_text(&raw_desc), 155))
                    };
                    let indexable = matches!(c.status, product_category::CategoryStatus::SeoReady)
                        && matches!(c.indexing_status, product_category::IndexingStatus::Index)
                        && product_count > 0
                        && meta_desc.is_some()
                        && path.is_some();
                    let canonical = path
                        .as_deref()
                        .and_then(|p| if indexable { canonical_from_path(p) } else { None });
                    Some(CategoryNode {
                        id: c.id.to_string(),
                        name: c.name.clone(),
                        children,
                        slug: Some(slug),
                        path,
                        image_url: c.image_url.clone(),
                        canonical,
                        seo_title: Some(meta_title),
                        seo_description: meta_desc,
                        seo_text: c.seo_text.clone(),
                        product_count: Some(product_count),
                        indexable: Some(indexable),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    let tree = build_tree(None, &by_parent, &used_categories, &category_counts);
    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(tree))
}

#[get("/api/site/car_categories")]
pub async fn list_car_categories(
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<CategoryNode>::new()));
        }
    };
    
    let categories_for_shop = category_repo.select(&By(shop.id)).await.unwrap_or_default();

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;
    let mut brand_models: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut brand_names: HashMap<String, String> = HashMap::new();
    let mut brand_counts: HashMap<String, usize> = HashMap::new();
    let mut model_counts: HashMap<(String, String), usize> = HashMap::new();
    for item in items.iter() {
        let brand_name = item.dto.brand.clone();
        let brand_slug = item.brand_slug.clone();
        if brand_slug.is_empty() {
            continue;
        }
        brand_names.entry(brand_slug.clone()).or_insert(brand_name);
        *brand_counts.entry(brand_slug.clone()).or_insert(0) += 1;
        let model_name = item.dto.model.clone();
        let model_slug = item.model_slug.clone();
        if model_slug.is_empty() {
            continue;
        }
        *model_counts
            .entry((brand_slug.clone(), model_slug.clone()))
            .or_insert(0) += 1;
        brand_models
            .entry(brand_slug)
            .or_default()
            .entry(model_slug)
            .or_insert(model_name);
    }
    fn build_model_children(
        models: &HashMap<String, String>,
        brand_slug: &str,
        model_counts: &HashMap<(String, String), usize>,
    ) -> Vec<CategoryNode> {
        let mut model_names: Vec<String> = models.values().cloned().collect();
        model_names.sort_by_key(|name| name.to_lowercase());
        model_names
            .into_iter()
            .map(|name| {
                let slug = slugify_latin(&name);
                let product_count =
                    *model_counts.get(&(brand_slug.to_string(), slug.clone())).unwrap_or(&0);
                let path = if slug.is_empty() {
                    None
                } else {
                    Some(format!("/catalog/{brand_slug}/{slug}"))
                };
                CategoryNode {
                    id: slug.clone(),
                    name,
                    children: Vec::new(),
                    slug: Some(slug),
                    path,
                    image_url: None,
                    canonical: None,
                    seo_title: None,
                    seo_description: None,
                    seo_text: None,
                    product_count: Some(product_count),
                    indexable: Some(false),
                }
            })
            .collect()
    }

    if !categories_for_shop.is_empty() {
        let mut by_parent: HashMap<Option<uuid::Uuid>, Vec<Category>> = HashMap::new();
        for c in categories_for_shop {
            by_parent.entry(c.parent_id.clone()).or_default().push(c);
        }

        fn build_tree(
            parent: Option<uuid::Uuid>,
            map: &HashMap<Option<uuid::Uuid>, Vec<Category>>,
            brand_models: &HashMap<String, HashMap<String, String>>,
            brand_counts: &HashMap<String, usize>,
            model_counts: &HashMap<(String, String), usize>,
            parent_brand_slug: Option<String>,
        ) -> Vec<CategoryNode> {
            map.get(&parent)
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|c| {
                    let slug = slugify_latin(&c.name);
                    let current_brand_slug = if parent.is_none() {
                        Some(slug.clone())
                    } else {
                        parent_brand_slug.clone()
                    };
                    let mut children = build_tree(
                        Some(c.id),
                        map,
                        brand_models,
                        brand_counts,
                        model_counts,
                        current_brand_slug.clone(),
                    );
                    if parent.is_none() && children.is_empty() {
                        if let Some(models) = brand_models.get(&slug) {
                            children = build_model_children(models, &slug, model_counts);
                        }
                    }
                    let product_count = if parent.is_none() {
                        *brand_counts.get(&slug).unwrap_or(&0)
                    } else if let Some(brand_slug) = current_brand_slug.as_ref() {
                        *model_counts
                            .get(&(brand_slug.clone(), slug.clone()))
                            .unwrap_or(&0)
                    } else {
                        0
                    };
                    let include = if parent.is_none() {
                        !children.is_empty()
                    } else {
                        product_count > 0 || !children.is_empty()
                    };
                    if include {
                        let raw_title = c.seo_title.clone().unwrap_or_else(|| c.name.clone());
                        let meta_title = trim_to(&plain_text(&raw_title), 60);
                        let raw_desc = c
                            .seo_description
                            .clone()
                            .or_else(|| c.seo_text.clone())
                            .unwrap_or_default();
                        let meta_desc = if raw_desc.trim().is_empty() {
                            None
                        } else {
                            Some(trim_to(&plain_text(&raw_desc), 155))
                        };
                        let path = if parent.is_none() {
                            if slug.is_empty() {
                                None
                            } else {
                                Some(format!("/catalog/{slug}"))
                            }
                        } else if let Some(brand_slug) = current_brand_slug.as_ref() {
                            if slug.is_empty() {
                                None
                            } else {
                                Some(format!("/catalog/{brand_slug}/{slug}"))
                            }
                        } else {
                            None
                        };
                        let indexable =
                            product_count > 0 && meta_desc.is_some() && path.is_some();
                        let canonical = path
                            .as_deref()
                            .and_then(|p| if indexable { canonical_from_path(p) } else { None });
                        Some(CategoryNode {
                            id: c.id.to_string(),
                            name: c.name.clone(),
                            children,
                            slug: Some(slug),
                            path,
                            image_url: None,
                            canonical,
                            seo_title: Some(meta_title),
                            seo_description: meta_desc,
                            seo_text: c.seo_text.clone(),
                            product_count: Some(product_count),
                            indexable: Some(indexable),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        }

        let tree = build_tree(
            None,
            &by_parent,
            &brand_models,
            &brand_counts,
            &model_counts,
            None,
        );
        let mut resp = actix_web::HttpResponse::Ok();
        resp.insert_header(("Cache-Control", "public, max-age=300"));
        return Ok(resp.json(tree));
    }

    let mut brands: Vec<(String, String)> = brand_names.into_iter().collect();
    brands.sort_by_key(|(_, name)| name.to_lowercase());
    let tree: Vec<CategoryNode> = brands
        .into_iter()
        .filter_map(|(brand_slug, brand_name)| {
            let models = brand_models.get(&brand_slug)?;
            if models.is_empty() {
                return None;
            }
            let product_count = *brand_counts.get(&brand_slug).unwrap_or(&0);
            let path = if brand_slug.is_empty() {
                None
            } else {
                Some(format!("/catalog/{brand_slug}"))
            };
            Some(CategoryNode {
                id: brand_slug.clone(),
                name: brand_name,
                children: build_model_children(models, &brand_slug, &model_counts),
                slug: Some(brand_slug),
                path,
                image_url: None,
                canonical: None,
                seo_title: None,
                seo_description: None,
                seo_text: None,
                product_count: Some(product_count),
                indexable: Some(false),
            })
        })
        .collect();
    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(tree))
}

#[get("/api/site/model_categories")]
pub async fn list_model_categories(
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<ModelCategoriesQuery>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<String>::new()));
        }
    };
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    let brand_filter = normalize_filter(params.brand.as_deref());
    let model_filter = normalize_filter(params.model.as_deref());
    let mut categories: HashSet<String> = HashSet::new();
    for item in items.iter() {
        if let Some(ref brand_filter) = brand_filter {
            if item.brand_slug != *brand_filter {
                continue;
            }
        }
        if let Some(ref model_filter) = model_filter {
            if item.model_slug != *model_filter {
                continue;
            }
        }
        if let Some(ref category) = item.dto.category {
            let cleaned = category.trim();
            if !cleaned.is_empty() {
                categories.insert(cleaned.to_string());
            }
        }
    }

    let mut list: Vec<String> = categories.into_iter().collect();
    list.sort_by_key(|s| s.to_lowercase());
    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(list))
}

#[post("/api/site/quick_order")]
pub async fn create_quick_order(
    req: HttpRequest,
    payload: Json<QuickOrderRequest>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    quick_order_repo: Data<Arc<dyn quick_order::QuickOrderRepository>>,
) -> Response {
    ensure_api_key(&req)?;
    let phone = payload.phone.trim();
    let re = Regex::new(r"^\+380\d{9}$").unwrap();
    if !re.is_match(phone) {
        return Ok(actix_web::HttpResponse::BadRequest().json(serde_json::json!({
            "ok": false,
            "error": "invalid_phone"
        })));
    }
    let shops = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    let shop = match shops.get(0) {
        Some(s) => s.clone(),
        None => {
            return Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({
                "ok": false,
                "error": "no_shop"
            })))
        }
    };
    let created_at = OffsetDateTime::now_utc().unix_timestamp();
    let item = quick_order::NewQuickOrder {
        shop_id: shop.id,
        phone: phone.to_string(),
        article: payload.article.clone(),
        title: payload.title.clone(),
        created_at,
    };
    let _ = quick_order_repo.add(item).await?;
    Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({ "ok": true })))
}

#[post("/api/site/orders")]
pub async fn create_order(
    req: HttpRequest,
    payload: Json<OrderRequest>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    order_repo: Data<Arc<dyn order::OrderRepository>>,
) -> Response {
    ensure_api_key(&req)?;
    let phone = payload.phone.trim();
    if phone.is_empty() {
        return Ok(actix_web::HttpResponse::BadRequest().json(serde_json::json!({
            "ok": false,
            "error": "missing_phone"
        })));
    }
    if payload.items.is_empty() {
        return Ok(actix_web::HttpResponse::BadRequest().json(serde_json::json!({
            "ok": false,
            "error": "empty_items"
        })));
    }

    let shops = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    let shop = match shops.get(0) {
        Some(s) => s.clone(),
        None => {
            return Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({
                "ok": false,
                "error": "no_shop"
            })))
        }
    };

    let mut name_parts = vec![payload.last_name.trim(), payload.first_name.trim()];
    if let Some(ref middle) = payload.middle_name {
        if !middle.trim().is_empty() {
            name_parts.push(middle.trim());
        }
    }
    let customer_name = name_parts.join(" ").trim().to_string();

    let items = payload
        .items
        .iter()
        .map(|item| {
            let quantity = item.quantity.unwrap_or(1).max(1);
            order::OrderItem {
                article: item.article.clone(),
                title: item.title.clone(),
                price: item.price,
                quantity,
            }
        })
        .collect::<Vec<_>>();

    let total = items
        .iter()
        .map(|item| item.price.unwrap_or(0) as i64 * item.quantity as i64)
        .sum::<i64>();

    let items_json = serde_json::to_string(&items).map_err(|err| anyhow!(err))?;
    let created_at = OffsetDateTime::now_utc().unix_timestamp();
    let item = order::NewOrder {
        shop_id: shop.id,
        customer_name,
        phone: phone.to_string(),
        email: payload.email.clone().map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
        delivery: payload.delivery.trim().to_string(),
        city_name: payload
            .city_name
            .clone()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        branch_name: payload
            .branch_name
            .clone()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        payment: payload.payment.trim().to_string(),
        total,
        items_count: items.len(),
        items_json,
        comment: payload
            .comment
            .clone()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        created_at,
    };
    let order = order_repo.add(item).await?;
    Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({
        "ok": true,
        "id": order.id
    })))
}

#[get("/api/site/reviews")]
pub async fn list_reviews(
    review_repo: Data<Arc<dyn review::ReviewRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<ReviewsQuery>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shops = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    let shop = match shops.get(0) {
        Some(s) => s.clone(),
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<ReviewDto>::new()));
        }
    };

    let limit = params.limit.unwrap_or(6).min(100);
    let offset = params.offset.unwrap_or(0);
    let product_key = params
        .product
        .as_deref()
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("all") {
                None
            } else {
                review::normalize_product_key(Some(trimmed))
            }
        });

    let items = review_repo
        .list(
            shop.id,
            product_key,
            limit,
            offset,
            review::ReviewStatus::Published,
        )
        .await?;

    let dtos = items
        .into_iter()
        .map(|item| ReviewDto {
            id: item.id,
            name: item.name,
            text: item.text,
            rating: item.rating,
            photos: item.photos,
            created_at: item.created_at.saturating_mul(1000),
        })
        .collect::<Vec<_>>();

    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(dtos))
}

#[post("/api/site/reviews")]
pub async fn create_review(
    review_repo: Data<Arc<dyn review::ReviewRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    payload: Json<ReviewCreateRequest>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shops = shop_service
        .send(rt_types::shop::service::List)
        .await??
        .into_iter()
        .collect::<Vec<_>>();
    let shop = match shops.get(0) {
        Some(s) => s.clone(),
        None => {
            return Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({
                "ok": false,
                "error": "no_shop"
            })))
        }
    };

    let name = review::normalize_name(&payload.name);
    let text = review::normalize_text(&payload.text);
    if name.trim().is_empty() || text.trim().is_empty() {
        return Ok(actix_web::HttpResponse::BadRequest().json(serde_json::json!({
            "ok": false,
            "error": "invalid_input"
        })));
    }
    let rating = review::clamp_rating(payload.rating.unwrap_or(5));
    let product_key = review::normalize_product_key(payload.product.as_deref());
    let photos = sanitize_review_photos(payload.photos.clone());
    let created_at = OffsetDateTime::now_utc().unix_timestamp();

    let item = review_repo
        .add(review::NewReview {
            shop_id: shop.id,
            product_key,
            name: name.clone(),
            text: text.clone(),
            rating,
            photos: photos.clone(),
            status: review::ReviewStatus::Published,
            created_at,
        })
        .await?;

    let dto = ReviewDto {
        id: item.id,
        name,
        text,
        rating,
        photos,
        created_at: created_at.saturating_mul(1000),
    };

    Ok(actix_web::HttpResponse::Ok().json(serde_json::json!({
        "ok": true,
        "review": dto
    })))
}

#[get("/api/site/seo_pages")]
pub async fn list_seo_pages(
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    params: Query<SeoPagesQuery>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<SeoPageListItem>::new()));
        }
    };

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    let page_types = parse_page_types(params.page_type.as_deref());
    let status_filter = match params.status.as_deref() {
        Some(raw) if raw.trim().eq_ignore_ascii_case("all") => None,
        Some(raw) if raw.trim().is_empty() => Some(seo_page::SeoPageStatus::Published),
        Some(raw) => Some(seo_page::SeoPageStatus::from_str(raw)),
        None => Some(seo_page::SeoPageStatus::Published),
    };

    let mut pages = seo_page_repo
        .select(&seo_page::ByShop(shop.id))
        .await
        .unwrap_or_default();

    if !page_types.is_empty() {
        pages.retain(|p| page_types.contains(&p.page_type));
    }
    if let Some(filter) = status_filter {
        pages.retain(|p| p.status == filter);
    }

    let mut list = Vec::with_capacity(pages.len());
    for page in pages.into_iter() {
        let payload = seo_page::SeoPagePayload::from_json(page.source_payload.as_deref());
        let product_count = super::seo_page_product_count(&page.page_type, &payload, items.as_ref());
        let indexable = seo_page::seo_page_indexable(
            &page.page_type,
            &page.status,
            &page.meta_title,
            &page.meta_description,
            &page.seo_text,
            product_count,
        );
        if let Some(filter) = params.indexable {
            if filter != indexable {
                continue;
            }
        }
        list.push(SeoPageListItem {
            id: page.id.to_string(),
            page_type: page.page_type.as_str().to_string(),
            slug: page.slug.clone(),
            path: page.path(),
            title: page.title.clone(),
            h1: page.h1.clone(),
            meta_description: page.meta_description.clone(),
            seo_text: page.seo_text.clone(),
            indexable,
            product_count,
            created_at: page.created_at.unix_timestamp(),
            updated_at: page.updated_at.unix_timestamp(),
        });
    }

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20).min(200);
    let slice = list
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(slice))
}

#[get("/api/site/seo_pages/{page_type}/{slug}")]
pub async fn get_seo_page(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    path: Path<(String, String)>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }
    let (page_type_raw, slug_raw) = path.into_inner();
    let page_type = seo_page::SeoPageType::from_path_segment(&page_type_raw)
        .ok_or(crate::control::ControllerError::NotFound)?;
    let slug = slug_raw.trim().to_string();
    if slug.is_empty() {
        return Err(crate::control::ControllerError::NotFound);
    }

    let shop = get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    .ok_or(crate::control::ControllerError::NotFound)?;

    let mut page = seo_page_repo
        .get_by_slug(shop.id, &slug)
        .await?
        .filter(|p| p.page_type == page_type);
    if page.is_none() {
        if let Some(history) = seo_page_repo.get_slug_history(shop.id, &slug).await? {
            page = seo_page_repo
                .get_one(&history.page_id)
                .await?
                .filter(|p| p.page_type == page_type);
        }
    }
    let page = page.ok_or(crate::control::ControllerError::NotFound)?;

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;
    let payload = seo_page::SeoPagePayload::from_json(page.source_payload.as_deref());
    let product_count = super::seo_page_product_count(&page.page_type, &payload, items.as_ref());
    let indexable = seo_page::seo_page_indexable(
        &page.page_type,
        &page.status,
        &page.meta_title,
        &page.meta_description,
        &page.seo_text,
        product_count,
    );
    let robots = page
        .robots
        .clone()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            Some(if indexable {
                "index,follow".to_string()
            } else {
                "noindex,follow".to_string()
            })
        });
    let canonical = if indexable {
        canonical_from_path(&page.path())
    } else {
        None
    };
    let meta_title = if indexable {
        page.meta_title.clone()
    } else {
        None
    };
    let meta_description = if indexable {
        page.meta_description.clone()
    } else {
        None
    };

    let dto = SeoPageDto {
        id: page.id.to_string(),
        page_type: page.page_type.as_str().to_string(),
        slug: page.slug.clone(),
        path: page.path(),
        title: page.title.clone(),
        h1: page.h1.clone(),
        meta_title,
        meta_description,
        seo_text: page.seo_text.clone(),
        faq: page.faq.clone(),
        robots,
        canonical,
        indexable,
        related_links: page.related_links.clone(),
        payload,
        product_count,
    };
    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(dto))
}

#[get("/api/site/products/{article:.*}")]
pub async fn get_product(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    article: Path<String>,
    req: HttpRequest,
) -> Response {
    ensure_api_key(&req)?;
    if let Err(e) = check_api_rate_limit(&req).await {
        return Err(crate::control::ControllerError::TooManyRequests {
            retry_after: e.retry_after,
            message: e.message,
        });
    }
    let shop = get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    .ok_or(crate::control::ControllerError::NotFound)?;
    let article = article.into_inner();
    let article = article.trim().to_string();
    if article.is_empty() {
        return Err(crate::control::ControllerError::NotFound);
    }

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let (items, by_article) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;
    let key = article.to_lowercase();
    let idx = by_article
        .get(&key)
        .copied()
        .ok_or(crate::control::ControllerError::NotFound)?;
    let dto = items
        .get(idx)
        .map(|item| item.dto.clone())
        .ok_or(crate::control::ControllerError::NotFound)?;
    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(dto))
}

#[get("/api/site/sitemap")]
pub async fn sitemap(
    _user: Option<Record<rt_types::access::UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    shop_service: Data<actix::Addr<rt_types::shop::service::ShopService>>,
    req: HttpRequest,
) -> Response {
    if !api_key_valid(&req) {
        return Ok(actix_web::HttpResponse::Unauthorized().finish());
    }

    let shop = match get_primary_shop_cached(
        &shop_service,
        &shop_product_repo,
        &product_category_repo,
    )
    .await
    {
        Some(s) => s,
        None => {
            let mut resp = actix_web::HttpResponse::Ok();
            resp.insert_header(("Cache-Control", "public, max-age=300"));
            return Ok(resp.json(Vec::<SitemapEntry>::new()));
        }
    };

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let product_categories = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    let car_categories = category_repo.select(&By(shop.id)).await.unwrap_or_default();

    let (items, _) = load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    let site_base_url = site_base();
    let mut entries: Vec<SitemapEntry> = Vec::new();

    let mut category_counts: HashMap<uuid::Uuid, usize> = HashMap::new();
    let mut brand_counts: HashMap<String, usize> = HashMap::new();
    let mut model_counts: HashMap<(String, String), usize> = HashMap::new();
    for item in items.iter() {
        if let Some(cid) = item.category_id {
            *category_counts.entry(cid).or_insert(0) += 1;
        }
        if !item.brand_slug.is_empty() {
            *brand_counts.entry(item.brand_slug.clone()).or_insert(0) += 1;
        }
        if !item.brand_slug.is_empty() && !item.model_slug.is_empty() {
            *model_counts
                .entry((item.brand_slug.clone(), item.model_slug.clone()))
                .or_insert(0) += 1;
        }
    }

    // Категорії товарів (seo_ready + index + visible + seo_description + product_count)
    for c in product_categories.iter() {
        if !matches!(c.visibility_on_site, product_category::Visibility::Visible) {
            continue;
        }
        if !matches!(c.status, product_category::CategoryStatus::SeoReady) {
            continue;
        }
        if !matches!(c.indexing_status, product_category::IndexingStatus::Index) {
            continue;
        }
        let product_count = *category_counts.get(&c.id).unwrap_or(&0);
        if product_count == 0 {
            continue;
        }
        let raw_desc = c
            .seo_description
            .clone()
            .or_else(|| c.seo_text.clone())
            .unwrap_or_default();
        if raw_desc.trim().is_empty() {
            continue;
        }
        let slug = slugify_latin(&c.name);
        if slug.is_empty() {
            continue;
        }
        if let Some(base) = site_base_url.as_ref() {
            entries.push(SitemapEntry {
                loc: format!("{base}/category/{slug}"),
                lastmod: format_lastmod(OffsetDateTime::now_utc()),
            });
        }
    }

    // Каталог
    if let Some(base) = site_base_url.as_ref() {
        const CATALOG_SEO_DESCRIPTION: &str =
            "Підбір автотоварів за маркою, моделлю та категорією. Актуальні товари та фільтри.";
        if !items.is_empty() && !CATALOG_SEO_DESCRIPTION.trim().is_empty() {
            entries.push(SitemapEntry {
                loc: format!("{base}/catalog"),
                lastmod: format_lastmod(OffsetDateTime::now_utc()),
            });
        }
    }

    // Бренди та моделі (потрібен SEO опис і product_count)
    if let Some(base) = site_base_url.as_ref() {
        if !car_categories.is_empty() {
            let mut by_parent: HashMap<Option<uuid::Uuid>, Vec<Category>> = HashMap::new();
            for c in car_categories {
                by_parent.entry(c.parent_id.clone()).or_default().push(c);
            }

            fn push_catalog_entries(
                parent: Option<uuid::Uuid>,
                map: &HashMap<Option<uuid::Uuid>, Vec<Category>>,
                brand_counts: &HashMap<String, usize>,
                model_counts: &HashMap<(String, String), usize>,
                parent_brand_slug: Option<String>,
                base: &str,
                entries: &mut Vec<SitemapEntry>,
            ) {
                if let Some(list) = map.get(&parent) {
                    for c in list {
                        let slug = slugify_latin(&c.name);
                        if slug.is_empty() {
                            continue;
                        }
                        let current_brand_slug = if parent.is_none() {
                            Some(slug.clone())
                        } else {
                            parent_brand_slug.clone()
                        };
                        let product_count = if parent.is_none() {
                            *brand_counts.get(&slug).unwrap_or(&0)
                        } else if let Some(brand_slug) = current_brand_slug.as_ref() {
                            *model_counts
                                .get(&(brand_slug.clone(), slug.clone()))
                                .unwrap_or(&0)
                        } else {
                            0
                        };
                        let raw_desc = c
                            .seo_description
                            .clone()
                            .or_else(|| c.seo_text.clone())
                            .unwrap_or_default();
                        let has_desc = !raw_desc.trim().is_empty();
                        let path = if parent.is_none() {
                            format!("/catalog/{slug}")
                        } else if let Some(brand_slug) = current_brand_slug.as_ref() {
                            format!("/catalog/{brand_slug}/{slug}")
                        } else {
                            continue;
                        };
                        if product_count > 0 && has_desc {
                            entries.push(SitemapEntry {
                                loc: format!("{base}{path}"),
                                lastmod: format_lastmod(OffsetDateTime::now_utc()),
                            });
                        }
                        push_catalog_entries(
                            Some(c.id),
                            map,
                            brand_counts,
                            model_counts,
                            current_brand_slug,
                            base,
                            entries,
                        );
                    }
                }
            }

            push_catalog_entries(
                None,
                &by_parent,
                &brand_counts,
                &model_counts,
                None,
                base,
                &mut entries,
            );
        }
    }

    // SEO сторінки (published + meta + seo_text + product_count)
    if let Some(base) = site_base_url.as_ref() {
        let seo_pages = seo_page_repo
            .select(&seo_page::ByShop(shop.id))
            .await
            .unwrap_or_default();
        for page in seo_pages.into_iter() {
            if page.slug.trim().is_empty() {
                continue;
            }
            let payload = seo_page::SeoPagePayload::from_json(page.source_payload.as_deref());
            let product_count =
                super::seo_page_product_count(&page.page_type, &payload, items.as_ref());
            let indexable = seo_page::seo_page_indexable(
                &page.page_type,
                &page.status,
                &page.meta_title,
                &page.meta_description,
                &page.seo_text,
                product_count,
            );
            if !indexable {
                continue;
            }
            entries.push(SitemapEntry {
                loc: format!("{base}{}", page.path()),
                lastmod: format_lastmod(page.updated_at),
            });
        }
    }

    for item in items.iter() {
        if !item.dto.indexable {
            continue;
        }
        let loc = if let Some(canonical) = item.dto.canonical.as_ref() {
            canonical.clone()
        } else if let Some(base) = site_base_url.as_ref() {
            format!("{base}{}", item.dto.path)
        } else {
            continue;
        };
        entries.push(SitemapEntry {
            loc,
            lastmod: format_lastmod(item.lastmod),
        });
    }

    let mut resp = actix_web::HttpResponse::Ok();
    resp.insert_header(("Cache-Control", "public, max-age=300"));
    Ok(resp.json(entries))
}
