use actix_web::{get, post, web::Query, HttpResponse};
use serde::Deserialize;

use crate::control::ShopAccess;
use crate::category_auto;
use crate::dt;
use crate::dt::product::Product;
use crate::restal;
use crate::site_publish;
use crate::{Model, Url as DtUrl};
use rt_types::category::{By, CategoryRepository};
use rt_types::Availability;
use time::OffsetDateTime;
use typesafe_repository::IdentityOf;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CategoriesQuery {
    #[serde(default)]
    pub parents: Option<String>,
}

fn ensure_restal_enabled(shop_id: &IdentityOf<rt_types::shop::Shop>) -> actix_web::Result<()> {
    let allowed = site_publish::load_site_publish_suppliers(shop_id);
    if allowed.iter().any(|s| s == "restal") {
        return Ok(());
    }
    Err(actix_web::error::ErrorForbidden("RESTAL API disabled"))
}

#[get("/api/site_publish/restal/categories")]
pub async fn restal_categories(
    ShopAccess { shop, .. }: ShopAccess,
    query: Query<CategoriesQuery>,
) -> actix_web::Result<HttpResponse> {
    ensure_restal_enabled(&shop.id)?;
    let data = restal::fetch_categories(query.parents.as_deref())
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(data))
}

#[derive(Deserialize)]
pub struct ProductsByCategory {
    pub category_id: String,
}

#[post("/api/site_publish/restal/products_by_category")]
pub async fn restal_products_by_category(
    ShopAccess { shop, .. }: ShopAccess,
    form: actix_web::web::Form<ProductsByCategory>,
) -> actix_web::Result<HttpResponse> {
    ensure_restal_enabled(&shop.id)?;
    let data = restal::fetch_products_by_category(&form.category_id)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(data))
}

#[derive(Deserialize)]
pub struct ProductsQuery {
    #[serde(default = "default_start")]
    pub start: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_start() -> usize {
    0
}
fn default_limit() -> usize {
    1000
}

#[get("/api/site_publish/restal/products")]
pub async fn restal_products(
    ShopAccess { shop, .. }: ShopAccess,
    query: Query<ProductsQuery>,
) -> actix_web::Result<HttpResponse> {
    ensure_restal_enabled(&shop.id)?;
    let data = restal::fetch_products(query.start, query.limit)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(data))
}

#[get("/api/site_publish/restal/stock")]
pub async fn restal_stock(ShopAccess { shop, .. }: ShopAccess) -> actix_web::Result<HttpResponse> {
    ensure_restal_enabled(&shop.id)?;
    let data = restal::fetch_stock()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(data))
}

#[post("/shop/{shop_id}/api/site_publish/restal/import")]
pub async fn restal_import(
    ShopAccess { shop, .. }: ShopAccess,
    category_repo: actix_web::web::Data<std::sync::Arc<dyn CategoryRepository>>,
    dt_repo: actix_web::web::Data<std::sync::Arc<dyn dt::product::ProductRepository + Send>>,
) -> actix_web::Result<HttpResponse> {
    ensure_restal_enabled(&shop.id)?;
    let category_map = category_repo.select(&By(shop.id)).await.unwrap_or_default();
    let mut start = 0usize;
    let mut imported = 0usize;
    let limit = 500usize;
    loop {
        let batch = restal::fetch_products(start, limit)
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?;
        if batch.is_empty() {
            break;
        }
        for p in &batch {
            if let Some(mapped) = map_restal_product(&p, &category_map) {
                dt_repo
                    .save(mapped)
                    .await
                    .map_err(actix_web::error::ErrorInternalServerError)?;
                imported += 1;
            }
        }
        if batch.len() < limit {
            break;
        }
        start += limit;
    }
    Ok(HttpResponse::Ok().json(serde_json::json!({ "imported": imported })))
}

fn map_restal_product(
    src: &restal::RestalProduct,
    categories: &[rt_types::category::Category],
) -> Option<Product> {
    let article = src
        .sku
        .clone()
        .or_else(|| src.product_id.clone())
        .or_else(|| src.model.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let title = src.name.clone().unwrap_or_else(|| article.clone());
    let price = src
        .price
        .as_ref()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|p| p.round() as usize);
    let available = match src.quantity.as_ref().and_then(|s| s.parse::<i64>().ok()) {
        Some(q) if q > 0 => Availability::Available,
        Some(_) => Availability::NotAvailable,
        None => Availability::OnOrder,
    };
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
    let url = DtUrl(format!("/restal/{}.html", article));
    let mut images = src.images.clone();
    if images.is_empty() {
        if let Some(i) = src.image.as_ref() {
            images.push(i.clone());
        }
    }
    Some(Product {
        title,
        description: src.description.clone(),
        title_ua: None,
        description_ua: None,
        price,
        source_price: price,
        article,
        brand,
        model,
        category,
        attributes: None,
        available,
        quantity: src
            .quantity
            .as_ref()
            .and_then(|s| s.parse::<i64>().ok())
            .map(|q| q.max(0) as usize),
        url,
        supplier: Some("restal".to_string()),
        discount_percent: None,
        last_visited: OffsetDateTime::now_utc(),
        images,
        upsell: None,
    })
}

fn categorize_to_brand_model(
    categories: &[rt_types::category::Category],
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
