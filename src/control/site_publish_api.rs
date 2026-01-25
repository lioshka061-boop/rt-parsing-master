use actix_web::{
    get, post,
    web::{Json, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::control::ShopAccess;
use crate::site_publish::{
    load_site_publish_configs, update_supplier_status, upsert_site_supplier, ExportConfig,
    SupplierStatus,
};

#[derive(Deserialize)]
pub struct CreateSupplierPayload {
    pub xml_url: String,
    #[serde(default)]
    pub config: ExportConfig,
}

#[post("/shop/{shop_id}/api/site_publish/supplier")]
pub async fn add_supplier(
    ShopAccess { shop, .. }: ShopAccess,
    Json(payload): Json<CreateSupplierPayload>,
) -> actix_web::Result<HttpResponse> {
    let supplier = upsert_site_supplier(&shop.id, payload.xml_url, payload.config)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    Ok(HttpResponse::Ok().json(supplier))
}

#[get("/shop/{shop_id}/api/site_publish/suppliers")]
pub async fn list_suppliers(
    ShopAccess { shop, .. }: ShopAccess,
) -> actix_web::Result<HttpResponse> {
    let suppliers = load_site_publish_configs(&shop.id)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    Ok(HttpResponse::Ok().json(suppliers))
}

#[post("/shop/{shop_id}/api/site_publish/{supplier_id}/parse")]
pub async fn parse_supplier(
    ShopAccess { shop, .. }: ShopAccess,
    path: Path<(Uuid, Uuid)>,
) -> actix_web::Result<HttpResponse> {
    let (_shop_path, supplier_id) = path.into_inner();
    let updated = update_supplier_status(
        &shop.id,
        supplier_id,
        SupplierStatus::Parsed,
        None,
        Some(100),
        Some("Parse finished".to_string()),
    )
    .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    Ok(HttpResponse::Ok().json(updated))
}

#[post("/shop/{shop_id}/api/site_publish/{supplier_id}/publish")]
pub async fn publish_supplier(
    ShopAccess { shop, .. }: ShopAccess,
    path: Path<(Uuid, Uuid)>,
) -> actix_web::Result<HttpResponse> {
    let (_shop_path, supplier_id) = path.into_inner();
    let updated = update_supplier_status(
        &shop.id,
        supplier_id,
        SupplierStatus::Published,
        None,
        Some(100),
        Some("Publish finished".to_string()),
    )
    .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    Ok(HttpResponse::Ok().json(updated))
}

#[get("/shop/{shop_id}/api/site_publish/{supplier_id}/logs")]
pub async fn supplier_logs(
    ShopAccess { shop, .. }: ShopAccess,
    path: Path<(Uuid, Uuid)>,
) -> actix_web::Result<HttpResponse> {
    let (_shop_path, supplier_id) = path.into_inner();
    // For now just return placeholder structure; extend with real logs store if needed.
    let payload = json!({
        "supplier_id": supplier_id,
        "shop_id": shop.id,
        "entries": []
    });
    Ok(HttpResponse::Ok().json(payload))
}
