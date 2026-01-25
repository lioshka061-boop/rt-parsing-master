use crate::control::{render_template, Record, Response};
use crate::dt;
use crate::site_publish;
use actix_web::get;
use actix_web::web::{Data, Path};
use askama::Template;
use rt_types::access::UserCredentials;
use rt_types::shop::{self, service::ShopService};
use std::sync::Arc;

#[derive(Template)]
#[template(path = "product.html")]
pub struct ProductPage {
    user: Option<UserCredentials>,
    product: dt::product::Product,
}

#[get("/product/{slug:.*}")]
pub async fn view(
    slug: Path<String>,
    user: Option<Record<UserCredentials>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_service: Data<actix::Addr<ShopService>>,
) -> Response {
    let slug = slug.into_inner();
    let shop = shop_service
        .send(shop::service::List)
        .await??
        .into_iter()
        .next();
    let allowed_suppliers = shop
        .as_ref()
        .map(|s| site_publish::load_site_publish_suppliers(&s.id))
        .unwrap_or_default();
    if allowed_suppliers.is_empty() {
        return Err(crate::control::ControllerError::NotFound.into());
    }

    let products = site_publish::filter_products_for_site(
        dt_repo
            .select(&dt::product::AvailableSelector)
            .await
            .unwrap_or_default(),
        &allowed_suppliers,
    );

    let slug_lower = slug.to_lowercase();
    let product = products
        .into_iter()
        .find(|p| {
            let p_slug = p.slug().to_lowercase();
            let url = p.url.0.to_lowercase();
            p_slug == slug_lower
                || p.article.to_lowercase() == slug_lower
                || url.trim_matches('/').ends_with(&slug_lower)
        })
        .ok_or(crate::control::ControllerError::NotFound)?;

    render_template(ProductPage {
        user: user.map(|u| u.t),
        product,
    })
}
