use crate::empty_string_as_none_parse;
use crate::export::{
    self, AddExportPermission, Export, ExportService, ExportStatus, UpdateExportEntryPermission,
};
use crate::category_auto;
use crate::product_category;
use crate::product_category_auto;
use crate::quick_order;
use crate::seo_page;
use crate::order;
use crate::shop_product;
use crate::site_import;
use crate::site_publish;
use crate::ddaudio;
use crate::ddaudio_import;
use crate::watermark::WatermarkOptionsDto;
use crate::{dt, tt};
use actix::fut::{ready, Ready};
use actix::prelude::*;
use actix_files::NamedFile;
use actix_multipart::form::{tempfile::TempFile, text::Text, MultipartForm};
use actix_session::Session;
use actix_web::{
    dev::{forward_ready, Payload, Service, ServiceRequest, ServiceResponse, Transform},
    get,
    http::header::ContentType,
    post,
    web::{Bytes, Data, Form, Json, Path, Query},
    Either, FromRequest, HttpMessage, HttpRequest, HttpResponse,
};
use anyhow::{anyhow, Context};
use askama::Template;
use derive_more::{Display, Error};
use futures::future::LocalBoxFuture;
use log_error::LogError;
use mime::IMAGE;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Method;
use rt_types::access::service::UserCredentialsService;
use rt_types::access::{self, Login, RegistrationToken, UserCredentials};
use rt_types::category::{
    parse_categories, By, ByParentId, Category, CategoryRepository, TopLevel,
};
use rt_types::shop::{self, service::ShopService, SiteImportEntry};
use rt_types::shop::{
    Discount, DtParsingOptions, ExportEntry, ExportEntryLink, ExportOptions, FileFormat,
    ParsingCategoriesAction, Shop, TtParsingOptions,
};
use rt_types::subscription::{self, service::SubscriptionService, Subscription};
use rt_types::watermark::{WatermarkGroup, WatermarkGroupRepository};
use rt_types::Availability;
use rt_types::{DescriptionOptions, Pause, Resume};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{
    de::{self, Error as DeError},
    Deserialize, Serialize,
};
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::future::Future;
use std::io::BufReader;
use std::os::unix::ffi::OsStrExt;
use std::ops::Deref;
use std::os::unix::fs::PermissionsExt;
use std::path::Path as StdPath;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use time::format_description::well_known::{iso8601, Rfc3339};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use typesafe_repository::GetIdentity;
use typesafe_repository::IdentityOf;
use url::form_urlencoded;
use uuid::Uuid;

pub mod catalog;
pub mod landing;
pub mod product;
pub mod restal_api;
pub mod site_api;
pub mod site_publish_api;

pub type Response = Result<HttpResponse, ControllerError>;
pub type ShopResponse = Result<HttpResponse, ShopControllerError>;
pub type InputData<T> = Either<Form<T>, Json<T>>;

pub const MAX_DESCRIPTION_SIZE: usize = 512 * 1024;
const USER_CACHE_TTL: Duration = Duration::from_secs(30);

struct UserCacheEntry {
    user: UserCredentials,
    cached_at: Instant,
}

static USER_CACHE: Lazy<RwLock<HashMap<String, UserCacheEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

async fn load_user_cached(
    login: &Login,
    service: &Addr<UserCredentialsService>,
) -> Result<UserCredentials, ControllerError> {
    let key = login.to_string();
    {
        let cache = USER_CACHE.read().await;
        if let Some(entry) = cache.get(&key) {
            if entry.cached_at.elapsed() < USER_CACHE_TTL {
                return Ok(entry.user.clone());
            }
        }
    }
    let user = service
        .send(access::service::Get(login.clone()))
        .await
        .map_err(ControllerError::from)?
        .map_err(ControllerError::from)?
        .ok_or(ControllerError::Unauthorized)
        .map_err(ControllerError::from)?;
    {
        let mut cache = USER_CACHE.write().await;
        cache.insert(
            key,
            UserCacheEntry {
                user: user.clone(),
                cached_at: Instant::now(),
            },
        );
    }
    Ok(user)
}

#[derive(Debug, Display, Error)]
pub enum ControllerError {
    NotFound,
    Unauthorized,
    Forbidden,
    #[display("Rate limit exceeded: {message}")]
    TooManyRequests {
        retry_after: u64,
        message: String,
    },
    #[error(ignore)]
    InternalServerError(anyhow::Error),
    #[error(ignore)]
    CorruptedData(String),
    #[error(ignore)]
    #[display("Invalid field {field}")]
    InvalidInput {
        field: String,
        msg: String,
    },
}

#[derive(Debug, Display, Error)]
#[display("{error}")]
pub struct ShopControllerError {
    #[error(ignore)]
    shop: Option<Shop>,
    #[error(ignore)]
    user: Option<UserCredentials>,
    error: ControllerError,
}

impl ShopControllerError {
    pub fn with_user<'a, E: Into<ControllerError>, U: Into<Option<&'a UserCredentials>>>(
        user: U,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: None,
            user: user.into().cloned(),
            error: error.into(),
        }
    }
    pub fn with_shop<'a, E: Into<ControllerError>, S: Into<Option<&'a Shop>>>(
        shop: S,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: shop.into().cloned(),
            user: None,
            error: error.into(),
        }
    }
    pub fn with<
        'a,
        E: Into<ControllerError>,
        U: Into<Option<&'a UserCredentials>>,
        S: Into<Option<&'a Shop>>,
    >(
        user: U,
        shop: S,
    ) -> impl FnOnce(E) -> Self {
        move |error| ShopControllerError {
            shop: shop.into().cloned(),
            user: user.into().cloned(),
            error: error.into(),
        }
    }
    pub fn or<'a, U: Into<Option<&'a UserCredentials>>, S: Into<Option<&'a Shop>>>(
        u: U,
        s: S,
    ) -> impl FnOnce(Self) -> Self {
        move |ShopControllerError { shop, user, error }| ShopControllerError {
            shop: shop.or_else(|| s.into().cloned()),
            user: user.or_else(|| u.into().cloned()),
            error,
        }
    }
}

impl From<ControllerError> for ShopControllerError {
    fn from(error: ControllerError) -> ShopControllerError {
        Self {
            error,
            user: None,
            shop: None,
        }
    }
}

impl From<ShopControllerError> for ControllerError {
    fn from(error: ShopControllerError) -> ControllerError {
        error.error
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ControllerError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        err.downcast::<ControllerError>()
            .map(|e| *e)
            .unwrap_or_else(|e| ControllerError::InternalServerError(anyhow!(e)))
    }
}

impl From<anyhow::Error> for ControllerError {
    fn from(err: anyhow::Error) -> Self {
        Self::InternalServerError(err)
    }
}

impl From<actix::MailboxError> for ControllerError {
    fn from(err: actix::MailboxError) -> Self {
        Self::InternalServerError(err.into())
    }
}

impl actix_web::error::ResponseError for ControllerError {
    fn error_response(&self) -> HttpResponse {
        log::warn!("{self:?}\n");
        use ControllerError::*;
        match self {
            NotFound => NotFoundPage { user: None }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::NotFound()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::NotFound().body(())),
            Unauthorized => HttpResponse::SeeOther()
                .insert_header(("Location", "/login"))
                .body(()),
            Forbidden => ForbiddenPage { user: None }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::Forbidden()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::Forbidden().body(())),
            TooManyRequests { retry_after, message } => {
                HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", retry_after.to_string()))
                    .insert_header(("X-RateLimit-Limit", "100"))
                    .insert_header(("X-RateLimit-Remaining", "0"))
                    .json(serde_json::json!({
                        "error": "Rate limit exceeded",
                        "message": message,
                        "retry_after": retry_after
                    }))
            }
            InternalServerError(err) => InternalServerErrorPage {
                error: format!("{err:?}"),
                user: None,
            }
            .render()
            .log_error("Unable to render error template")
            .map(|res| {
                HttpResponse::InternalServerError()
                    .content_type(ContentType::html())
                    .body(res)
            })
            .unwrap_or_else(|| HttpResponse::InternalServerError().body(err.to_string())),
            CorruptedData(err) => HttpResponse::InternalServerError().body(err.to_string()),
            InvalidInput { field, msg } => {
                HttpResponse::BadRequest().body(format!("{field}\n{msg}"))
            }
        }
    }
}

impl actix_web::error::ResponseError for ShopControllerError {
    fn error_response(&self) -> HttpResponse {
        log::warn!("{:?}", self.error);
        match (&self.error, self.shop.clone(), self.user.clone()) {
            (ControllerError::NotFound, shop, user) => {
                let template = match shop {
                    Some(shop) => ShopNotFoundPage { user, shop }.render(),
                    None => NotFoundPage { user }.render(),
                };
                template
                    .log_error("Unable to render error template")
                    .map(|res| {
                        HttpResponse::NotFound()
                            .content_type(ContentType::html())
                            .body(res)
                    })
                    .unwrap_or_else(|| HttpResponse::NotFound().body(()))
            }
            (ControllerError::Unauthorized, ..) => HttpResponse::SeeOther()
                .insert_header(("Location", "/login"))
                .body(()),
            (ControllerError::Forbidden, _, user) => ForbiddenPage { user }
                .render()
                .log_error("Unable to render error template")
                .map(|res| {
                    HttpResponse::Forbidden()
                        .content_type(ContentType::html())
                        .body(res)
                })
                .unwrap_or_else(|| HttpResponse::Forbidden().body(())),
            (ControllerError::TooManyRequests { retry_after, message }, ..) => {
                HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", retry_after.to_string()))
                    .insert_header(("X-RateLimit-Limit", "100"))
                    .insert_header(("X-RateLimit-Remaining", "0"))
                    .json(serde_json::json!({
                        "error": "Rate limit exceeded",
                        "message": message,
                        "retry_after": retry_after
                    }))
            }
            (ControllerError::InternalServerError(err), shop, user) => {
                log::warn!("{}", err.backtrace());
                let res = match shop {
                    Some(shop) => ShopInternalServerErrorPage {
                        error: err.to_string(),
                        user,
                        shop,
                    }
                    .render(),
                    None => InternalServerErrorPage {
                        error: err.to_string(),
                        user,
                    }
                    .render(),
                };
                res.log_error("Unable to render error template")
                    .map(|res| {
                        HttpResponse::InternalServerError()
                            .content_type(ContentType::html())
                            .body(res)
                    })
                    .unwrap_or_else(|| HttpResponse::NotFound().body(()))
            }
            (ControllerError::CorruptedData(err), ..) => {
                HttpResponse::InternalServerError().body(err.to_string())
            }
            (ControllerError::InvalidInput { field, msg }, ..) => {
                HttpResponse::BadRequest().body(format!("{field}\n{msg}"))
            }
        }
    }
}

#[derive(Template)]
#[template(path = "500.html")]
pub struct InternalServerErrorPage {
    error: String,
    user: Option<UserCredentials>,
}

#[derive(Template)]
#[template(path = "shop/500.html")]
pub struct ShopInternalServerErrorPage {
    error: String,
    user: Option<UserCredentials>,
    shop: Shop,
}

#[derive(Template)]
#[template(path = "404.html")]
pub struct NotFoundPage {
    user: Option<UserCredentials>,
}

pub async fn not_found(user: Option<Record<UserCredentials>>) -> Response {
    render_template(NotFoundPage {
        user: user.map(|u| u.t),
    })
}

#[derive(Template)]
#[template(path = "403.html")]
pub struct ForbiddenPage {
    user: Option<UserCredentials>,
}

#[derive(Template)]
#[template(path = "shop/404.html")]
pub struct ShopNotFoundPage {
    user: Option<UserCredentials>,
    shop: Shop,
}

#[derive(Clone)]
pub struct Identity {
    pub login: String,
}

impl FromRequest for Identity {
    type Error = ControllerError;
    type Future = Ready<Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(
            req.extensions()
                .get::<Identity>()
                .cloned()
                .ok_or_else(|| ControllerError::Unauthorized),
        )
    }
}

impl FromRequest for Record<UserCredentials> {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let login = Login(
                Identity::extract(&req)
                    .await
                    .map_err(|_err| ControllerError::Unauthorized)?
                    .login,
            );
            let user_credentials_service = Data::<Addr<UserCredentialsService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract UserCredentialsService from request")
                })
                .map_err(ControllerError::from)?;
            let user = load_user_cached(&login, &user_credentials_service)
                .await
                .map_err(ControllerError::from)?;
            let s = user_credentials_service.clone();
            Ok(Self {
                t: user,
                g: RecordGuard {
                    f: Box::new(move |u| {
                        let s = s.clone();
                        Box::pin(async move { Ok(s.send(access::service::Update(u)).await??) })
                    }),
                },
            })
        })
    }
}

impl RecordResponse for RecordGuard<UserCredentials> {
    type Response = Result<(), anyhow::Error>;
}

pub struct ShopAccess {
    pub shop: Shop,
    pub user: UserCredentials,
}

impl FromRequest for ShopAccess {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let user = Record::<UserCredentials>::extract(&req)
                .await
                .map_err(ControllerError::from)?
                .t;
            let path = req.match_info().get("shop_id");
            let path = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!("Unable to extract shop id from request"))
                        .map_err(ShopControllerError::with(&user, None))?
                        .1
                }
            };
            let shop_id: IdentityOf<Shop> = Uuid::parse_str(path)
                .context("Unable to extract shop id from request")
                .map_err(ShopControllerError::with(&user, None))?;
            let shop_service = Data::<Addr<ShopService>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ShopService from request"))
                .map_err(ShopControllerError::with(&user, None))?;
            let shop = shop_service
                .send(shop::service::Get(shop_id))
                .await
                .map_err(ShopControllerError::with(&user, None))?
                .map_err(ShopControllerError::with(&user, None))?
                .ok_or(ControllerError::NotFound)
                .map_err(ShopControllerError::with(&user, None))?;
            let owned_shops = shop_service
                .send(shop::service::ListBy(user.id()))
                .await
                .map_err(ShopControllerError::with(&user, None))?
                .map_err(ShopControllerError::with(&user, None))?
                .into_inner();
            if (user.has_access_to(&shop.id) || owned_shops.contains(&shop))
                && !(req.method() == Method::POST && shop.is_suspended)
            {
                Ok(ShopAccess { shop, user })
            } else {
                Err(ControllerError::Forbidden).map_err(ShopControllerError::with(&user, &shop))
            }
        })
    }
}

pub struct ShopActionAccess {
    pub shop: Shop,
    pub user: UserCredentials,
}

impl FromRequest for ShopActionAccess {
    type Error = ShopControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let ShopAccess { shop, user } = ShopAccess::extract(&req).await?;
            if shop.is_suspended {
                Err(ShopControllerError::with(&user, &shop)(
                    ControllerError::Forbidden,
                ))
            } else {
                Ok(Self { shop, user })
            }
        })
    }
}

pub struct ControlPanelAccess {
    pub user: UserCredentials,
}

impl FromRequest for ControlPanelAccess {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let user = Record::<UserCredentials>::extract(&req).await?.t;
            if user.has_access_to_control_panel() {
                Ok(Self { user })
            } else {
                Err(ControllerError::Forbidden)
            }
        })
    }
}

pub struct SessionMiddlewareFactory {}

impl<S, B: 'static> Transform<S, ServiceRequest> for SessionMiddlewareFactory
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Transform = SessionMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(SessionMiddleware {
            service: Arc::new(service),
        }))
    }
}

pub struct SessionMiddleware<S> {
    service: Arc<S>,
}

impl<S, B> Service<ServiceRequest> for SessionMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        let service = self.service.clone();
        Box::pin(async move {
            let session = req.extract::<Session>().await?;
            match session.get::<String>("login") {
                Ok(Some(l)) => {
                    let identity = Identity { login: l };
                    req.extensions_mut().insert(identity);
                }
                Err(err) => {
                    log::error!("Unable to extract login from session:\n{err:?}");
                    req.extensions_mut().insert(None::<Identity>);
                }
                _ => (),
            }
            let res = service.call(req).await?;
            Ok(res)
        })
    }
}

#[derive(Deserialize)]
pub struct LoginDto {
    pub login: String,
    pub password: String,
}

pub fn see_other(location: &str) -> HttpResponse {
    HttpResponse::SeeOther()
        .insert_header(("Location", location))
        .json(())
}

pub fn render_template(t: impl Template) -> Result<HttpResponse, ControllerError> {
    let result = t
        .render()
        .map_err(|x| ControllerError::InternalServerError(anyhow!(x)))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(result))
}

fn parse_vendor<T: AsRef<str> + ToString>(s: T) -> String {
    crate::parse_vendor_from_link(s.as_ref()).unwrap_or(s.to_string())
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexPage {
    pub export_status: Vec<(String, Export, Vec<(FileFormat, FileInfo)>)>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[derive(Serialize)]
struct ExportStatusJson {
    hash: String,
    status: export::ExportStatus,
    ready: bool,
    file_name: String,
    progress: Option<export::ProgressInfo>,
}

#[get("/shop/{shop_id}/status")]
async fn export_status_json(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let export_status = export_service
        .send(export::GetAllStatus(shop_id))
        .await
        .context("Unable to send message to ExportService")?;
    let mut res = Vec::with_capacity(export_status.len());
    for (hash, export) in export_status {
        let file_name = export.entry.file_name(None);
        let ready = file_info(format!("export/{shop_id}/{file_name}"))
            .log_error("Unable to get file info")
            .flatten()
            .map(|info| info.last_modified >= export.entry.edited_time)
            .unwrap_or(false);
        res.push(ExportStatusJson {
            hash,
            status: export.status().clone(),
            ready,
            file_name,
            progress: export.progress.clone(),
        });
    }
    Ok(HttpResponse::Ok().json(res))
}

#[get("/shop/{shop_id}")]
async fn index(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let export_status = export_service
        .send(export::GetAllStatus(shop.id))
        .await
        .context("Unable to send message to ExportService")?;
    let mut export_status = export_status.into_iter().collect::<Vec<_>>();
    export_status.sort_by_key(|(_, e)| e.entry.file_name(None));
    let shop_id = shop.id;
    let export_status = export_status
        .into_iter()
        .map(|(h, e)| {
            let mut f = vec![];
            let formats = [
                FileFormat::Csv,
                FileFormat::Xml,
                FileFormat::Xlsx,
                FileFormat::HoroshopCsv,
                FileFormat::HoroshopCategories,
            ];
            for format in formats {
                let info = file_info(format!(
                    "export/{shop_id}/{}",
                    e.entry.file_name(format.clone())
                ))
                .log_error("Unable to get file info")
                .flatten();
                if let Some(info) = info {
                    f.push((format, info));
                }
            }
            (h, e, f)
        })
        .collect();
    render_template(IndexPage {
        shop,
        export_status,
        user,
    })
}

#[derive(Template)]
#[template(path = "parsing.html")]
pub struct ParsingPage {
    dt_progress: Result<dt::parser::ParsingProgress, anyhow::Error>,
    tt_progress: Result<tt::parser::ParsingProgress, anyhow::Error>,
    user: UserCredentials,
}

#[derive(Template)]
#[template(path = "control_panel/dt_products.html")]
pub struct DtProductsPage {
    products: Vec<DtProductRow>,
    stale_days: Option<i64>,
    page: usize,
    per_page: usize,
    total: usize,
    prev_page: Option<String>,
    next_page: Option<String>,
    user: UserCredentials,
    sort: String,
}

#[derive(Clone)]
pub struct DtProductRow {
    article: String,
    title: String,
    price: Option<usize>,
    available: String,
    last_visited: String,
    url: String,
}

#[derive(Deserialize)]
pub struct DtProductsQuery {
    stale_days: Option<i64>,
    page: Option<usize>,
    per_page: Option<usize>,
    sort: Option<String>,
}

#[get("/control_panel/parsing")]
async fn parsing(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    tt_parser: Option<Data<Arc<Addr<tt::parser::ParserService>>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
) -> Response {
    let dt_progress = dt_parser
        .send(dt::parser::GetProgress)
        .await
        .context("Unable to send message to dt::ParserService")?;
    let tt_progress = match tt_parser {
        Some(parser) => parser
            .send(tt::parser::GetProgress)
            .await
            .context("Unable to send message to tt::ParserService")?,
        None => Err(anyhow!("TT parsing disabled")),
    };
    render_template(ParsingPage {
        dt_progress,
        tt_progress,
        user,
    })
}

#[get("/control_panel/dt/products")]
async fn control_panel_dt_products(
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    query: Query<DtProductsQuery>,
) -> Response {
    let params = query.into_inner();
    let per_page = params.per_page.unwrap_or(50).clamp(10, 500);
    let page = params.page.unwrap_or(1).max(1);
    let stale_days = params.stale_days.filter(|d| *d > 0);
    let sort = params.sort.clone().unwrap_or_else(|| "oldest".to_string());
    let now = OffsetDateTime::now_utc();

    let mut products = dt_repo.list().await?;
    if let Some(days) = stale_days {
        let cutoff = now - time::Duration::days(days);
        products.retain(|p| p.last_visited <= cutoff);
    }
    match sort.as_str() {
        "newest" => products.sort_by_key(|p| std::cmp::Reverse(p.last_visited)),
        "price_asc" => products.sort_by(|a, b| {
            let a_price = a.price.unwrap_or(usize::MAX);
            let b_price = b.price.unwrap_or(usize::MAX);
            a_price
                .cmp(&b_price)
                .then_with(|| a.last_visited.cmp(&b.last_visited))
        }),
        "price_desc" => products.sort_by(|a, b| {
            let a_price = a.price.unwrap_or(0);
            let b_price = b.price.unwrap_or(0);
            b_price
                .cmp(&a_price)
                .then_with(|| a.last_visited.cmp(&b.last_visited))
        }),
        _ => products.sort_by_key(|p| p.last_visited),
    }

    let total = products.len();
    let start = (page - 1) * per_page;
    let rows = products
        .into_iter()
        .skip(start)
        .take(per_page)
        .map(|p| DtProductRow {
            article: p.article,
            title: p.title,
            price: p.price,
            available: p.available.to_string(),
            last_visited: format_dt_last_visited(p.last_visited, now),
            url: p.url.0,
        })
        .collect::<Vec<_>>();

    let total_pages = (total + per_page - 1) / per_page;
    let build_link = |page: usize| {
        let mut params = Vec::new();
        if let Some(days) = stale_days {
            params.push(format!("stale_days={days}"));
        }
        if sort != "oldest" {
            params.push(format!("sort={sort}"));
        }
        params.push(format!("per_page={per_page}"));
        params.push(format!("page={page}"));
        format!("/control_panel/dt/products?{}", params.join("&"))
    };
    let prev_page = if page > 1 {
        Some(build_link(page - 1))
    } else {
        None
    };
    let next_page = if page < total_pages {
        Some(build_link(page + 1))
    } else {
        None
    };

    render_template(DtProductsPage {
        products: rows,
        stale_days,
        page,
        per_page,
        total,
        prev_page,
        next_page,
        user,
        sort,
    })
}

#[derive(Template)]
#[template(path = "shop/products.html")]
pub struct ShopProductsPage {
    shop: Shop,
    user: UserCredentials,
    products: Vec<ShopProductListItem>,
    query: String,
    supplier_filter: String,
    missing_filter: String,
    review_filter: String,
    page: usize,
    per_page: usize,
    total_items: usize,
    page_links: Vec<PageLink>,
    bulk_suppliers: Vec<SelectOption>,
    bulk_categories: Vec<SelectOption>,
}

#[derive(Template)]
#[template(path = "shop/seo_pages.html")]
pub struct ShopSeoPagesPage {
    shop: Shop,
    user: UserCredentials,
    pages: Vec<ShopSeoPageListItem>,
}

pub struct ShopSeoPageListItem {
    pub id: String,
    pub title: String,
    pub path: String,
    pub page_type: String,
    pub status: String,
    pub seo_text_auto: bool,
    pub updated_at: String,
    pub indexable: bool,
    pub product_count: usize,
}

#[derive(Template)]
#[template(path = "shop/seo_page_edit.html")]
pub struct ShopSeoPageEditPage {
    shop: Shop,
    user: UserCredentials,
    seo_page: seo_page::SeoPage,
    page_type: String,
    status: String,
    payload: seo_page::SeoPagePayload,
    related_links: String,
    path: String,
    product_count: usize,
    indexable: bool,
    is_new: bool,
}

pub struct ShopProductListItem {
    pub article: String,
    pub title: String,
    pub brand: String,
    pub model: String,
    pub description: String,
    pub price: Option<usize>,
    pub source_price: Option<usize>,
    pub available: Availability,
    pub image: Option<String>,
    pub updated_at: OffsetDateTime,
    pub status: shop_product::ProductStatus,
    pub visibility_on_site: shop_product::Visibility,
    pub indexing_status: shop_product::IndexingStatus,
    pub configured: bool,
    pub is_hit: bool,
}

#[derive(Template)]
#[template(path = "shop/crm.html")]
struct ShopCrmPage {
    shop: Shop,
    user: UserCredentials,
}

struct QuickOrderView {
    id: i64,
    phone: String,
    article: String,
    title: String,
    created_at: String,
}

struct OrderItemView {
    article: String,
    title: String,
    price: Option<usize>,
    quantity: usize,
}

struct OrderView {
    id: i64,
    customer_name: String,
    phone: String,
    email: Option<String>,
    delivery: String,
    city_name: Option<String>,
    branch_name: Option<String>,
    payment: String,
    total: i64,
    items_count: usize,
    items: Vec<OrderItemView>,
    comment: Option<String>,
    created_at: String,
}

#[derive(Template)]
#[template(path = "shop/quick_orders.html")]
struct ShopQuickOrdersPage {
    shop: Shop,
    user: UserCredentials,
    items: Vec<QuickOrderView>,
}

#[derive(Template)]
#[template(path = "shop/orders.html")]
struct ShopOrdersPage {
    shop: Shop,
    user: UserCredentials,
    items: Vec<OrderView>,
}

#[derive(Template)]
#[template(path = "shop/users.html")]
struct ShopUsersPage {
    shop: Shop,
    user: UserCredentials,
}

pub struct PageLink {
    pub label: String,
    pub url: Option<String>,
    pub current: bool,
}

enum PaginationItem {
    Page(usize),
    Gap,
}

fn build_pagination_items(current: usize, total: usize) -> Vec<PaginationItem> {
    if total <= 7 {
        return (1..=total).map(PaginationItem::Page).collect();
    }
    if current <= 3 {
        let mut items = (1..=4).map(PaginationItem::Page).collect::<Vec<_>>();
        items.push(PaginationItem::Gap);
        items.push(PaginationItem::Page(total));
        return items;
    }
    if current >= total.saturating_sub(2) {
        let mut items = vec![PaginationItem::Page(1), PaginationItem::Gap];
        for p in (total.saturating_sub(3))..=total {
            items.push(PaginationItem::Page(p));
        }
        return items;
    }
    vec![
        PaginationItem::Page(1),
        PaginationItem::Gap,
        PaginationItem::Page(current.saturating_sub(1)),
        PaginationItem::Page(current),
        PaginationItem::Page(current + 1),
        PaginationItem::Gap,
        PaginationItem::Page(total),
    ]
}

pub struct SelectOption {
    pub value: String,
    pub label: String,
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

fn snippet(input: &str, max: usize) -> String {
    let s = plain_text(input);
    if s.chars().count() <= max {
        return s;
    }
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i >= max {
            break;
        }
        out.push(ch);
    }
    format!("{out}…")
}

fn normalize_string(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn related_links_to_text(list: &[String]) -> String {
    list.join("\n")
}

fn parse_related_links(input: Option<String>) -> Vec<String> {
    input
        .unwrap_or_default()
        .split(|ch| ch == '\n' || ch == ',')
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .collect()
}

fn seo_page_type_label(page_type: &seo_page::SeoPageType) -> &'static str {
    match page_type {
        seo_page::SeoPageType::TuningModel => "Тюнінг моделі",
        seo_page::SeoPageType::AccessoriesCar => "Аксесуари для авто",
        seo_page::SeoPageType::HowToChoose => "Гайд",
    }
}

fn seo_page_product_count(
    page_type: &seo_page::SeoPageType,
    payload: &seo_page::SeoPagePayload,
    items: &[crate::control::site_api::CachedProduct],
) -> usize {
    if matches!(page_type, seo_page::SeoPageType::HowToChoose) {
        return 0;
    }
    let brand_slug = payload.brand_slug.as_deref().unwrap_or("");
    let model_slug = payload.model_slug.as_deref().unwrap_or("");
    let category_slug = payload.category_slug.as_deref().unwrap_or("");
    let mut query = payload
        .car
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if query.is_none() {
        let mut parts = Vec::new();
        if let Some(brand) = payload.brand.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty())
        {
            parts.push(brand);
        }
        if let Some(model) = payload.model.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty())
        {
            parts.push(model);
        }
        if !parts.is_empty() {
            query = Some(parts.join(" "));
        }
    }
    let query = query.map(|v| v.to_lowercase()).filter(|v| !v.is_empty());

    let mut count = items
        .iter()
        .filter(|item| {
            if !brand_slug.is_empty() && item.brand_slug != brand_slug {
                return false;
            }
            if !model_slug.is_empty() && item.model_slug != model_slug {
                return false;
            }
            if !category_slug.is_empty() && item.category_slug != category_slug {
                return false;
            }
            true
        })
        .count();
    if count == 0 {
        if let Some(query) = query {
            count = items
                .iter()
                .filter(|item| {
                    item.search_blob.contains(&query) || item.article_lower.contains(&query)
                })
                .count();
        }
    }
    count
}

fn format_unix_timestamp(ts: i64) -> String {
    let format_description = iso8601::Iso8601::<
        {
            iso8601::Config::DEFAULT
                .set_time_precision(iso8601::TimePrecision::Minute { decimal_digits: None })
                .set_formatted_components(iso8601::FormattedComponents::DateTime)
                .encode()
        },
    >;
    let dt = OffsetDateTime::from_unix_timestamp(ts).unwrap_or_else(|_| OffsetDateTime::now_utc());
    dt.format(&format_description).unwrap_or_else(|_| ts.to_string())
}

fn format_delivery(value: &str) -> String {
    match value {
        "nova-poshta-branch" => "Нова пошта (на відділення)".to_string(),
        "nova-poshta-courier" => "Нова пошта (курʼєр)".to_string(),
        "pickup" => "Самовивіз".to_string(),
        _ => value.to_string(),
    }
}

fn format_payment(value: &str) -> String {
    match value {
        "cod" => "Накладений платіж".to_string(),
        "wayforpay" => "Оплата онлайн Wayforpay".to_string(),
        "installments" => "Оплата частинами".to_string(),
        "invoice" => "Оплата на рахунок".to_string(),
        _ => value.to_string(),
    }
}

fn parse_usize_param(value: Option<&str>) -> Option<usize> {
    value
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok())
}

#[derive(Template)]
#[template(path = "shop/product_new.html")]
pub struct ShopProductNewPage {
    shop: Shop,
    user: UserCredentials,
    category_options: Vec<CategoryOption>,
    model_options: Vec<String>,
}

#[derive(Deserialize)]
pub struct ShopProductsQuery {
    pub q: Option<String>,
    pub supplier: Option<String>,
    pub missing: Option<String>,
    pub review: Option<String>,
    pub page: Option<String>,
    pub per_page: Option<String>,
}

#[derive(Deserialize)]
pub struct ShopProductsBulkForm {
    pub action: String,
    pub scope: String,
    pub supplier: Option<String>,
    pub category_id: Option<String>,
    #[serde(default)]
    pub articles: Vec<String>,
    pub q: Option<String>,
    pub missing: Option<String>,
    pub review: Option<String>,
    pub page: Option<String>,
    pub per_page: Option<String>,
}

#[derive(Clone, Copy)]
struct AutoCategoryStatus {
    brand_ok: bool,
    model_ok: bool,
}

#[get("/shop/{shop_id}/products")]
async fn shop_products(
    ShopAccess { shop, user }: ShopAccess,
    params: Query<ShopProductsQuery>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let per_page = parse_usize_param(params.per_page.as_deref())
        .filter(|v| matches!(*v, 25 | 50 | 75 | 100))
        .unwrap_or(25);
    let mut page = parse_usize_param(params.page.as_deref()).unwrap_or(1).max(1);
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let mut products = dt_repo.list().await.unwrap_or_default();

    if let Some(q) = params.q.as_ref().filter(|s| !s.trim().is_empty()) {
        let q = q.to_lowercase();
        products = products
            .into_iter()
            .filter(|p| {
                p.title.to_lowercase().contains(&q)
                    || p.brand.to_lowercase().contains(&q)
                    || p.model.0.to_lowercase().contains(&q)
                    || p.article.to_lowercase().contains(&q)
            })
            .collect();
    }

    if let Some(supplier) = params
        .supplier
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
    {
        products = products
            .into_iter()
            .filter(|p| {
                site_publish::detect_supplier(p)
                    .map(|s| s == supplier)
                    .unwrap_or(false)
            })
            .collect();
    }

    let missing_filter = params
        .missing
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_default();

    let review_filter = params
        .review
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_default();
    let need_auto = missing_filter == "auto_category"
        || matches!(
            review_filter.as_str(),
            "needs_review" | "brand_missing" | "model_missing"
        );
    let need_product_category = missing_filter == "product_category"
        || matches!(
            review_filter.as_str(),
            "needs_review" | "product_category_missing"
        );

    let categories_all = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    let category_matcher = if need_product_category && !categories_all.is_empty() {
        Some(product_category_auto::CategoryMatcher::new(&categories_all))
    } else {
        None
    };
    let categories = if need_auto {
        category_repo
            .select(&By(shop.id))
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Поточні налаштування товарів по магазину
    let overrides = shop_product_repo
        .list_by_shop(shop.id)
        .await
        .unwrap_or_default();
    let mut settings_by_article =
        std::collections::HashMap::<String, shop_product::ShopProduct>::new();
    for s in overrides.into_iter() {
        settings_by_article.insert(s.article.to_lowercase(), s);
    }

    let mut auto_status_by_article = std::collections::HashMap::<String, AutoCategoryStatus>::new();
    let mut product_category_missing_by_article = std::collections::HashMap::<String, bool>::new();
    if need_auto || need_product_category {
        for p in &products {
            let key = p.article.to_lowercase();
            let title = settings_by_article
                .get(&key)
                .and_then(|x| x.title.as_deref())
                .unwrap_or(&p.title);
            let description = settings_by_article
                .get(&key)
                .and_then(|x| x.description.as_deref())
                .or(p.description.as_deref());

            if need_auto {
                let mut brand_ok = false;
                let mut model_ok = false;
                if let Some((b, _m, id)) =
                    category_auto::guess_brand_model(title, description, &categories)
                {
                    brand_ok = !b.trim().is_empty();
                    model_ok = id.is_some();
                }
                auto_status_by_article.insert(
                    key.clone(),
                    AutoCategoryStatus {
                        brand_ok,
                        model_ok,
                    },
                );
            }

            if need_product_category {
                let missing_product_category = {
                    let current = settings_by_article.get(&key).and_then(|x| x.site_category_id);
                    if current.is_some() {
                        false
                    } else if let Some(matcher) = category_matcher.as_ref() {
                        let description = description.unwrap_or("");
                        let haystack = product_category_auto::build_haystack(title, description);
                        matcher.guess(&haystack).is_none()
                    } else {
                        true
                    }
                };
                product_category_missing_by_article.insert(key, missing_product_category);
            }
        }
    }

    if missing_filter == "auto_category" {
        products = products
            .into_iter()
            .filter(|p| {
                auto_status_by_article
                    .get(&p.article.to_lowercase())
                    .map(|s| !s.model_ok)
                    .unwrap_or(true)
            })
            .collect();
    } else if missing_filter == "product_category" {
        products = products
            .into_iter()
            .filter(|p| {
                let key = p.article.to_lowercase();
                product_category_missing_by_article
                    .get(&key)
                    .copied()
                    .unwrap_or(true)
            })
            .collect();
    }

    if !review_filter.is_empty() {
        products = products
            .into_iter()
            .filter(|p| {
                let key = p.article.to_lowercase();
                let auto_status = auto_status_by_article
                    .get(&key)
                    .copied()
                    .unwrap_or(AutoCategoryStatus {
                        brand_ok: false,
                        model_ok: false,
                    });
                let product_category_missing = product_category_missing_by_article
                    .get(&key)
                    .copied()
                    .unwrap_or(true);
                let needs_review =
                    !auto_status.brand_ok || !auto_status.model_ok || product_category_missing;
                match review_filter.as_str() {
                    "needs_review" => needs_review,
                    "brand_missing" => !auto_status.brand_ok,
                    "model_missing" => auto_status.brand_ok && !auto_status.model_ok,
                    "product_category_missing" => product_category_missing,
                    _ => true,
                }
            })
            .collect();
    }

    products.sort_by_key(|p| p.last_visited);
    products.reverse();
    let total_items = products.len();
    let total_pages = if total_items == 0 {
        1
    } else {
        (total_items + per_page - 1) / per_page
    };
    page = page.min(total_pages);
    let start = (page.saturating_sub(1)) * per_page;

    let mut bulk_suppliers = collect_supplier_labels(&shop, &allowed_suppliers)
        .into_iter()
        .map(|(key, label)| SelectOption { value: key, label })
        .collect::<Vec<_>>();
    bulk_suppliers.sort_by_key(|o| o.label.to_lowercase());

    let mut bulk_categories = categories_all
        .iter()
        .map(|c| SelectOption {
            value: c.id.to_string(),
            label: c.name.clone(),
        })
        .collect::<Vec<_>>();
    bulk_categories.sort_by_key(|o| o.label.to_lowercase());

    let mut page_links = Vec::new();
    if total_pages > 1 {
        let build_url = |target: usize| {
            let mut serializer = form_urlencoded::Serializer::new(String::new());
            if let Some(q) = params.q.as_ref().filter(|s| !s.trim().is_empty()) {
                serializer.append_pair("q", q);
            }
            if let Some(s) = params
                .supplier
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                serializer.append_pair("supplier", s);
            }
            if let Some(m) = params
                .missing
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                serializer.append_pair("missing", m);
            }
            if let Some(r) = params
                .review
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                serializer.append_pair("review", r);
            }
            serializer.append_pair("page", &target.to_string());
            serializer.append_pair("per_page", &per_page.to_string());
            let qs = serializer.finish();
            if qs.is_empty() {
                format!("/shop/{}/products", shop.id)
            } else {
                format!("/shop/{}/products?{}", shop.id, qs)
            }
        };

        if page > 1 {
            page_links.push(PageLink {
                label: "«".to_string(),
                url: Some(build_url(page - 1)),
                current: false,
            });
        }
        for item in build_pagination_items(page, total_pages) {
            match item {
                PaginationItem::Page(p) => page_links.push(PageLink {
                    label: p.to_string(),
                    url: Some(build_url(p)),
                    current: p == page,
                }),
                PaginationItem::Gap => page_links.push(PageLink {
                    label: "...".to_string(),
                    url: None,
                    current: false,
                }),
            }
        }
        if page < total_pages {
            page_links.push(PageLink {
                label: "»".to_string(),
                url: Some(build_url(page + 1)),
                current: false,
            });
        }
    }

    let items = products
        .into_iter()
        .skip(start)
        .take(per_page)
        .map(|p| {
            let description = snippet(&p.description_text(), 160);
            let model = p.format_model().unwrap_or_else(|| p.model.0.clone());
            let image = p.images.first().cloned();
            let key = p.article.to_lowercase();
            let status = settings_by_article
                .get(&key)
                .map(|x| x.status.clone())
                .unwrap_or(crate::shop_product::ProductStatus::PublishedNoIndex);
            let visibility_on_site = settings_by_article
                .get(&key)
                .map(|x| x.visibility_on_site.clone())
                .unwrap_or(crate::shop_product::Visibility::Visible);
            let indexing_status = settings_by_article
                .get(&key)
                .map(|x| x.indexing_status.clone())
                .unwrap_or(crate::shop_product::IndexingStatus::NoIndex);
            let is_hit = settings_by_article
                .get(&key)
                .map(|x| x.is_hit)
                .unwrap_or(false);
            let configured = matches!(status, shop_product::ProductStatus::SeoReady)
                && matches!(visibility_on_site, shop_product::Visibility::Visible)
                && matches!(indexing_status, shop_product::IndexingStatus::Index);
            ShopProductListItem {
                article: p.article,
                title: p.title,
                brand: p.brand,
                model,
                description,
                price: p.price,
                source_price: p.source_price,
                available: p.available,
                image,
                updated_at: p.last_visited,
                status,
                visibility_on_site,
                indexing_status,
                configured,
                is_hit,
            }
        })
        .collect::<Vec<_>>();

    render_template(ShopProductsPage {
        shop,
        user,
        products: items,
        query: params.q.clone().unwrap_or_default(),
        supplier_filter: params.supplier.clone().unwrap_or_default(),
        missing_filter: missing_filter.clone(),
        review_filter: review_filter.clone(),
        page,
        per_page,
        total_items,
        page_links,
        bulk_suppliers,
        bulk_categories,
    })
}

#[get("/shop/{shop_id}/crm")]
async fn shop_crm_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(ShopCrmPage { shop, user })
}

#[get("/shop/{shop_id}/crm/quick_orders")]
async fn shop_quick_orders_page(
    ShopAccess { shop, user }: ShopAccess,
    quick_order_repo: Data<Arc<dyn quick_order::QuickOrderRepository>>,
) -> Response {
    let items = quick_order_repo
        .list_by_shop(shop.id)
        .await?
        .into_iter()
        .map(|item| QuickOrderView {
            id: item.id,
            phone: item.phone,
            article: item.article.unwrap_or_else(|| "—".to_string()),
            title: item.title.unwrap_or_else(|| "Без назви".to_string()),
            created_at: format_unix_timestamp(item.created_at),
        })
        .collect();
    render_template(ShopQuickOrdersPage { shop, user, items })
}

#[post("/shop/{shop_id}/crm/quick_orders/{id}/delete")]
async fn shop_quick_order_delete(
    ShopAccess { shop, .. }: ShopAccess,
    path: Path<(Uuid, i64)>,
    quick_order_repo: Data<Arc<dyn quick_order::QuickOrderRepository>>,
) -> Response {
    let (_, id) = path.into_inner();
    quick_order_repo.remove(shop.id, id).await?;
    Ok(see_other(&format!("/shop/{}/crm/quick_orders", shop.id)))
}

#[get("/shop/{shop_id}/crm/orders")]
async fn shop_orders_page(
    ShopAccess { shop, user }: ShopAccess,
    order_repo: Data<Arc<dyn order::OrderRepository>>,
) -> Response {
    let items = order_repo
        .list_by_shop(shop.id)
        .await?
        .into_iter()
        .map(|item| {
            let parsed_items = match serde_json::from_str::<Vec<order::OrderItem>>(&item.items_json)
            {
                Ok(items) => items,
                Err(err) => {
                    log::warn!("Unable to parse order items for {}: {}", item.id, err);
                    Vec::new()
                }
            };
            let items = parsed_items
                .into_iter()
                .map(|order_item| OrderItemView {
                    article: order_item.article,
                    title: order_item.title,
                    price: order_item.price,
                    quantity: order_item.quantity,
                })
                .collect::<Vec<_>>();
            OrderView {
                id: item.id,
                customer_name: item.customer_name,
                phone: item.phone,
                email: item.email,
                delivery: format_delivery(&item.delivery),
                city_name: item.city_name,
                branch_name: item.branch_name,
                payment: format_payment(&item.payment),
                total: item.total,
                items_count: item.items_count,
                items,
                comment: item.comment,
                created_at: format_unix_timestamp(item.created_at),
            }
        })
        .collect();
    render_template(ShopOrdersPage { shop, user, items })
}

#[post("/shop/{shop_id}/crm/orders/{id}/delete")]
async fn shop_order_delete(
    ShopAccess { shop, .. }: ShopAccess,
    path: Path<(Uuid, i64)>,
    order_repo: Data<Arc<dyn order::OrderRepository>>,
) -> Response {
    let (_, id) = path.into_inner();
    order_repo.remove(shop.id, id).await?;
    Ok(see_other(&format!("/shop/{}/crm/orders", shop.id)))
}

#[get("/shop/{shop_id}/crm/users")]
async fn shop_users_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(ShopUsersPage { shop, user })
}

async fn perform_bulk_visibility_update(
    shop_id: Uuid,
    action: String,
    scope: String,
    supplier: Option<String>,
    category_id: Option<String>,
    articles: Vec<String>,
    dt_repo: Arc<dyn dt::product::ProductRepository + Send>,
    shop_product_repo: Arc<dyn shop_product::ShopProductRepository>,
    product_category_repo: Arc<dyn product_category::ProductCategoryRepository>,
) -> anyhow::Result<()> {
    let action = action.trim().to_lowercase();
    let scope = scope.trim().to_lowercase();
    if action.is_empty() || scope.is_empty() {
        return Ok(());
    }

    let mut target_articles = std::collections::HashSet::<String>::new();
    match scope.as_str() {
        "selected" => {
            for article in articles.into_iter() {
                let trimmed = article.trim();
                if !trimmed.is_empty() {
                    target_articles.insert(trimmed.to_string());
                }
            }
        }
        "supplier" => {
            let supplier = supplier.unwrap_or_default().trim().to_lowercase();
            if !supplier.is_empty() {
                let products = dt_repo.list().await.unwrap_or_default();
                for p in products.iter() {
                    if site_publish::detect_supplier(p)
                        .as_deref()
                        .map(|s| s == supplier)
                        .unwrap_or(false)
                    {
                        target_articles.insert(p.article.clone());
                    }
                }
            }
        }
        "category" => {
            let category_id = category_id
                .as_deref()
                .and_then(|id| uuid::Uuid::parse_str(id).ok());
            if let Some(category_id) = category_id {
                let overrides = shop_product_repo
                    .list_by_shop(shop_id)
                    .await
                    .unwrap_or_default();
                let mut overrides_by_article =
                    std::collections::HashMap::<String, shop_product::ShopProduct>::new();
                for s in overrides.into_iter() {
                    overrides_by_article.insert(s.article.to_lowercase(), s);
                }
                let categories = product_category_repo
                    .select(&product_category::ByShop(shop_id))
                    .await
                    .unwrap_or_default();
                if !categories.is_empty() {
                    let category_matcher =
                        product_category_auto::CategoryMatcher::new(&categories);
                    let products = dt_repo.list().await.unwrap_or_default();
                    for p in products.iter() {
                        let key = p.article.to_lowercase();
                        let cur = overrides_by_article.get(&key);
                        let cat_id = match cur.and_then(|x| x.site_category_id) {
                            Some(id) => Some(id),
                            None => {
                                let title = cur
                                    .and_then(|x| x.title.as_deref())
                                    .unwrap_or(&p.title);
                                let description = cur
                                    .and_then(|x| x.description.as_deref())
                                    .or(p.description.as_deref())
                                    .unwrap_or("");
                                let haystack =
                                    product_category_auto::build_haystack(title, description);
                                category_matcher.guess(&haystack)
                            }
                        };
                        if cat_id == Some(category_id) {
                            target_articles.insert(p.article.clone());
                        }
                    }
                }
            }
        }
        _ => {}
    }

    if target_articles.is_empty() {
        return Ok(());
    }

    if matches!(action.as_str(), "set_hit" | "unset_hit") {
        let is_hit = action == "set_hit";
        let ensure_missing = is_hit;
        let articles = target_articles.into_iter().collect::<Vec<_>>();
        shop_product_repo
            .bulk_set_hit(shop_id, &articles, is_hit, ensure_missing)
            .await?;
        return Ok(());
    }

    let (visibility, indexing_status, status, ensure_missing) = match action.as_str() {
        "show_noindex" => (
            shop_product::Visibility::Visible,
            shop_product::IndexingStatus::NoIndex,
            shop_product::ProductStatus::PublishedNoIndex,
            false,
        ),
        "hide" => (
            shop_product::Visibility::Hidden,
            shop_product::IndexingStatus::NoIndex,
            shop_product::ProductStatus::Draft,
            true,
        ),
        _ => return Ok(()),
    };
    let articles = target_articles.into_iter().collect::<Vec<_>>();
    shop_product_repo
        .bulk_set_visibility(
            shop_id,
            &articles,
            visibility,
            indexing_status,
            status,
            Some("noindex,follow".to_string()),
            shop_product::SourceType::Manual,
            ensure_missing,
        )
        .await?;

    Ok(())
}

#[post("/shop/{shop_id}/products/bulk")]
async fn shop_products_bulk(
    ShopAccess { shop, .. }: ShopAccess,
    Form(form): Form<ShopProductsBulkForm>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let ShopProductsBulkForm {
        action,
        scope,
        supplier,
        category_id,
        articles,
        q,
        missing,
        review,
        page,
        per_page,
    } = form;

    let mut redirect_qs = form_urlencoded::Serializer::new(String::new());
    if let Some(q) = q.as_ref().filter(|s| !s.trim().is_empty()) {
        redirect_qs.append_pair("q", q);
    }
    if let Some(missing) = missing.as_ref().filter(|s| !s.trim().is_empty()) {
        redirect_qs.append_pair("missing", missing);
    }
    if let Some(review) = review.as_ref().filter(|s| !s.trim().is_empty()) {
        redirect_qs.append_pair("review", review);
    }
    if let Some(page) = parse_usize_param(page.as_deref()) {
        redirect_qs.append_pair("page", &page.to_string());
    }
    if let Some(per_page) = parse_usize_param(per_page.as_deref()) {
        redirect_qs.append_pair("per_page", &per_page.to_string());
    }
    let redirect = {
        let qs = redirect_qs.finish();
        if qs.is_empty() {
            format!("/shop/{}/products", shop.id)
        } else {
            format!("/shop/{}/products?{}", shop.id, qs)
        }
    };

    let action = action.trim().to_lowercase();
    let scope = scope.trim().to_lowercase();
    if action.is_empty() || scope.is_empty() {
        return Ok(see_other(&redirect));
    }

    let run_async = scope.as_str() != "selected";
    let dt_repo = dt_repo.get_ref().clone();
    let product_category_repo = product_category_repo.get_ref().clone();
    let shop_id = shop.id;
    if run_async {
        actix_web::rt::spawn(async move {
            let conn = match tokio_rusqlite::Connection::open("storage/shop_products.db").await {
                Ok(conn) => conn,
                Err(err) => {
                    log::error!("Bulk update failed: unable to open shop_products.db: {err}");
                    return;
                }
            };
            let shop_product_repo = match shop_product::SqliteShopProductRepository::init(conn).await
            {
                Ok(repo) => Arc::new(repo) as Arc<dyn shop_product::ShopProductRepository>,
                Err(err) => {
                    log::error!("Bulk update failed: {err}");
                    return;
                }
            };
            if let Err(err) = perform_bulk_visibility_update(
                shop_id,
                action,
                scope,
                supplier,
                category_id,
                articles,
                dt_repo,
                shop_product_repo,
                product_category_repo,
            )
            .await
            {
                log::error!("Bulk update failed: {err}");
            }
        });
        return Ok(see_other(&redirect));
    }

    let shop_product_repo = shop_product_repo.get_ref().clone();
    let _ = perform_bulk_visibility_update(
        shop_id,
        action,
        scope,
        supplier,
        category_id,
        articles,
        dt_repo,
        shop_product_repo,
        product_category_repo,
    )
    .await;

    Ok(see_other(&redirect))
}

#[get("/shop/{shop_id}/products/new")]
async fn shop_product_new_page(
    ShopAccess { shop, user }: ShopAccess,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
) -> Response {
    let category_options = build_category_options(product_category_repo.get_ref(), &shop).await;
    let mut model_set = std::collections::HashSet::new();
    if let Ok(list) = dt_repo.list().await {
        for item in list {
            let model = item.model.0.trim().to_string();
            if model.is_empty() {
                continue;
            }
            model_set.insert(model);
            if model_set.len() >= 500 {
                break;
            }
        }
    }
    let mut model_options: Vec<String> = model_set.into_iter().collect();
    model_options.sort();
    render_template(ShopProductNewPage {
        shop,
        user,
        category_options,
        model_options,
    })
}

#[derive(Deserialize)]
pub struct ProductUpdateForm {
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(deserialize_with = "empty_string_as_none_parse")]
    pub price: Option<usize>,
    pub images: Option<String>,
    pub upsell: Option<String>,
}

#[derive(Template)]
#[template(path = "shop/product_edit.html")]
pub struct ShopProductEditPage {
    shop: Shop,
    user: UserCredentials,
    product: dt::product::Product,
    category_options: Vec<CategoryOption>,
    settings: shop_product::ShopProduct,
    images_csv: String,
    recommended_csv: String,
    selected_category_id: String,
    available_value: String,
    preview_image: Option<String>,
    is_manual: bool,
}

pub struct CategoryOption {
    pub id: String,
    pub label: String,
}

fn flatten_category_options(
    parent: Option<uuid::Uuid>,
    indent: usize,
    map: &std::collections::HashMap<Option<uuid::Uuid>, Vec<product_category::ProductCategory>>,
    out: &mut Vec<CategoryOption>,
) {
    let Some(list) = map.get(&parent) else { return };
    for c in list.iter() {
        out.push(CategoryOption {
            id: c.id.to_string(),
            label: format!("{}{}", "— ".repeat(indent), c.name),
        });
        flatten_category_options(Some(c.id), indent + 1, map, out);
    }
}

async fn build_category_options(
    product_category_repo: &Arc<dyn product_category::ProductCategoryRepository>,
    shop: &Shop,
) -> Vec<CategoryOption> {
    let mut categories = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    categories.sort_by_key(|c| (c.parent_id.is_some(), c.name.clone()));

    let mut by_parent: std::collections::HashMap<
        Option<uuid::Uuid>,
        Vec<product_category::ProductCategory>,
    > = std::collections::HashMap::new();
    for c in categories {
        by_parent.entry(c.parent_id).or_default().push(c);
    }

    let mut category_options = Vec::new();
    flatten_category_options(None, 0, &by_parent, &mut category_options);
    category_options
}

fn availability_to_value(a: Availability) -> &'static str {
    match a {
        Availability::Available => "available",
        Availability::OnOrder => "on_order",
        Availability::NotAvailable => "not_available",
    }
}

fn is_manual_product(product: &dt::product::Product) -> bool {
    product.url.0.to_lowercase().contains("/manual/")
}

#[get("/shop/{shop_id}/products/{article}/edit")]
async fn shop_product_edit_page(
    ShopAccess { shop, user }: ShopAccess,
    article: Path<(String, String)>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let (_shop_param, article) = article.into_inner();
    let all = dt_repo.list().await.unwrap_or_default();
    let product = all
        .into_iter()
        .find(|p| p.article.eq_ignore_ascii_case(&article))
        .ok_or(ControllerError::NotFound)?;

    shop_product_repo
        .ensure_exists(shop.id, &product.article)
        .await?;
    let settings = shop_product_repo
        .get(shop.id, &product.article)
        .await?
        .ok_or_else(|| {
            ControllerError::InternalServerError(anyhow!("Unable to load shop product settings"))
        })?;

    let category_options = build_category_options(product_category_repo.get_ref(), &shop).await;

    let effective_images = settings
        .images
        .clone()
        .unwrap_or_else(|| product.images.clone());
    let images_csv = effective_images.join(", ");
    let preview_image = effective_images.first().cloned();
    let recommended_csv = settings.recommended_articles.join(", ");
    let selected_category_id = settings
        .site_category_id
        .map(|u| u.to_string())
        .unwrap_or_default();
    let available_value = availability_to_value(
        settings
            .available
            .clone()
            .unwrap_or_else(|| product.available.clone()),
    )
    .to_string();
    let is_manual = is_manual_product(&product);

    render_template(ShopProductEditPage {
        shop,
        user,
        product,
        category_options,
        settings,
        images_csv,
        recommended_csv,
        selected_category_id,
        available_value,
        preview_image,
        is_manual,
    })
}

#[derive(Deserialize)]
pub struct ShopProductEditForm {
    pub article: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(deserialize_with = "empty_string_as_none_parse")]
    pub price: Option<usize>,
    pub images: Option<String>,
    pub available: Option<String>,
    pub site_category_id: Option<String>,
    pub recommend_mode: Option<String>,
    pub recommended_articles: Option<String>,
    pub is_hit: Option<String>,
    pub h1: Option<String>,
    pub seo_text: Option<String>,
    pub canonical: Option<String>,
    pub robots: Option<String>,
    pub og_title: Option<String>,
    pub og_description: Option<String>,
    pub og_image: Option<String>,
    pub slug: Option<String>,
    pub faq: Option<String>,
    pub visibility_on_site: Option<String>,
    pub indexing_status: Option<String>,
    pub status: Option<String>,
}

#[post("/shop/{shop_id}/products/{article}/edit")]
async fn shop_product_edit_save(
    ShopAccess { shop, .. }: ShopAccess,
    article: Path<(String, String)>,
    Form(form): Form<ShopProductEditForm>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
) -> Response {
    let (_shop_param, article) = article.into_inner();
    let all = dt_repo.list().await.unwrap_or_default();
    let base = all
        .iter()
        .find(|p| p.article.eq_ignore_ascii_case(&article))
        .cloned()
        .ok_or(ControllerError::NotFound)?;

    let now = OffsetDateTime::now_utc();
    let mut current = shop_product_repo
        .get(shop.id, &base.article)
        .await?
        .unwrap_or(shop_product::ShopProduct {
            shop_id: shop.id,
            article: base.article.clone(),
            internal_product_id: uuid::Uuid::new_v4(),
            title: None,
            description: None,
            price: None,
            images: None,
            available: None,
            site_category_id: None,
            recommend_mode: shop_product::RecommendMode::Auto,
            recommended_articles: Vec::new(),
            is_hit: false,
            source_type: shop_product::SourceType::Parsing,
            visibility_on_site: shop_product::Visibility::Hidden,
            indexing_status: shop_product::IndexingStatus::NoIndex,
            status: shop_product::ProductStatus::Draft,
            seo_score: 0,
            h1: None,
            seo_text: None,
            canonical: None,
            robots: None,
            og_title: None,
            og_description: None,
            og_image: None,
            slug: None,
            faq: None,
            created_at: now,
            updated_at: now,
        });

    let mut redirect_article = base.article.clone();
    let mut remove_article: Option<String> = None;
    if is_manual_product(&base) {
        let requested_article = form
            .article
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(requested_article) = requested_article {
            if !requested_article.eq_ignore_ascii_case(&base.article) {
                if all
                    .iter()
                    .any(|p| p.article.eq_ignore_ascii_case(requested_article))
                {
                    return Err(ControllerError::InvalidInput {
                        field: "article".to_string(),
                        msg: "Артикул вже використовується іншим товаром".to_string(),
                    });
                }

                let mut updated = base.clone();
                updated.article = requested_article.to_string();
                dt_repo.save(updated).await?;
                let _ = dt_repo.delete_articles(&[base.article.clone()]).await;

                current.article = requested_article.to_string();
                redirect_article = requested_article.to_string();
                remove_article = Some(base.article.clone());
            }
        }
    }

    current.title = form
        .title
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.description = form
        .description
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.price = form.price;
    current.images = form
        .images
        .map(|images| {
            images
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());
    current.available = match form.available.as_deref() {
        Some("on_order") => Some(Availability::OnOrder),
        Some("not_available") => Some(Availability::NotAvailable),
        Some("available") => Some(Availability::Available),
        _ => None,
    };
    current.site_category_id = form
        .site_category_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    current.recommend_mode = match form.recommend_mode.as_deref() {
        Some("manual") => shop_product::RecommendMode::Manual,
        _ => shop_product::RecommendMode::Auto,
    };
    current.recommended_articles = form
        .recommended_articles
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    current.is_hit = form.is_hit.is_some();
    current.h1 = form
        .h1
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.seo_text = form
        .seo_text
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.canonical = form
        .canonical
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.robots = form
        .robots
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.og_title = form
        .og_title
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.og_description = form
        .og_description
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.og_image = form
        .og_image
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.slug = form
        .slug
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.faq = form
        .faq
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    current.visibility_on_site = match form.visibility_on_site.as_deref() {
        Some("visible") => shop_product::Visibility::Visible,
        _ => shop_product::Visibility::Hidden,
    };
    current.indexing_status = match form.indexing_status.as_deref() {
        Some("index") => shop_product::IndexingStatus::Index,
        _ => shop_product::IndexingStatus::NoIndex,
    };
    current.source_type = shop_product::SourceType::Manual;
    let requested_status = match form.status.as_deref() {
        Some("published_noindex") => shop_product::ProductStatus::PublishedNoIndex,
        Some("seo_ready") => shop_product::ProductStatus::SeoReady,
        _ => shop_product::ProductStatus::Draft,
    };

    if matches!(requested_status, shop_product::ProductStatus::SeoReady) {
        let title = current.title.clone().unwrap_or_else(|| base.title.clone());
        let h1 = current.h1.clone().unwrap_or_else(|| title.clone());
        let description = current
            .description
            .clone()
            .or_else(|| base.description.clone())
            .unwrap_or_default();
        let images = current
            .images
            .clone()
            .unwrap_or_else(|| base.images.clone());
        let category_set = current.site_category_id.is_some();
        let canonical = current
            .canonical
            .clone()
            .unwrap_or_else(|| base.url.0.clone());
        let canonical_ok = canonical.starts_with("http://")
            || canonical.starts_with("https://")
            || canonical.starts_with('/');
        let mut reasons = Vec::new();
        if title.trim().is_empty() {
            reasons.push("title");
        }
        if h1.trim().is_empty() {
            reasons.push("h1");
        }
        if description.trim().is_empty() {
            reasons.push("description");
        }
        if images.is_empty() {
            reasons.push("images");
        }
        if !category_set {
            reasons.push("category");
        }
        if !canonical_ok {
            reasons.push("canonical");
        }
        if !reasons.is_empty() {
            return Err(ControllerError::InvalidInput {
                field: "seo_ready_guard".to_string(),
                msg: format!("Заповніть обов'язкові поля: {}", reasons.join(", ")),
            });
        }
        // Унікальність title/slug/canonical в межах магазину
        let existing = shop_product_repo
            .list_by_shop(shop.id)
            .await
            .unwrap_or_default();
        let desired_slug = current.slug.clone().unwrap_or_else(|| {
            base.url
                .0
                .trim_matches('/')
                .split('/')
                .last()
                .unwrap_or(&base.article)
                .to_string()
        });
        for p in existing {
            if p.article.eq_ignore_ascii_case(&base.article) {
                continue;
            }
            if let Some(s) = p.slug.as_ref() {
                if s.eq_ignore_ascii_case(&desired_slug) {
                    return Err(ControllerError::InvalidInput {
                        field: "slug_unique".to_string(),
                        msg: "Slug вже використовується іншим товаром".to_string(),
                    });
                }
            }
            if let Some(c) = p.canonical.as_ref() {
                if c.eq_ignore_ascii_case(&canonical) {
                    return Err(ControllerError::InvalidInput {
                        field: "canonical_unique".to_string(),
                        msg: "Canonical вже використовується іншим товаром".to_string(),
                    });
                }
            }
            if let Some(t) = p.title.as_ref() {
                if t.trim().eq_ignore_ascii_case(&title.trim()) {
                    return Err(ControllerError::InvalidInput {
                        field: "title_unique".to_string(),
                        msg: "Title вже використовується іншим товаром".to_string(),
                    });
                }
            }
        }
        // Якщо guardrails пройдені, переводимо в seo_ready + index+visible.
        current.indexing_status = shop_product::IndexingStatus::Index;
        current.visibility_on_site = shop_product::Visibility::Visible;
        current.status = shop_product::ProductStatus::SeoReady;
    } else {
        current.status = requested_status;
    }
    // robots за замовчуванням узгоджуємо з indexing_status
    if current.robots.is_none() {
        current.robots = Some(match current.indexing_status {
            shop_product::IndexingStatus::Index => "index,follow".to_string(),
            shop_product::IndexingStatus::NoIndex => "noindex,follow".to_string(),
        });
    }

    current.updated_at = now;

    shop_product_repo.upsert(current).await?;
    if let Some(old_article) = remove_article {
        let _ = shop_product_repo.remove(shop.id, &old_article).await;
    }
    Ok(see_other(&format!(
        "/shop/{}/products/{}/edit",
        shop.id, redirect_article
    )))
}

#[post("/shop/{shop_id}/products/{article}/remove")]
async fn shop_product_remove(
    ShopAccess { shop, .. }: ShopAccess,
    article: Path<(String, String)>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
) -> Response {
    let (_shop_param, article) = article.into_inner();
    if let Ok(Some(product)) = dt_repo.get_one(&article).await {
        if product.url.0.to_lowercase().contains("/manual/") {
            let _ = dt_repo.delete_articles(&[article.clone()]).await;
        }
    }
    shop_product_repo.remove(shop.id, &article).await?;
    Ok(see_other(&format!("/shop/{}/products", shop.id)))
}

#[derive(MultipartForm, Debug)]
pub struct ProductCreateMultipartForm {
    title: Text<String>,
    description: Option<Text<String>>,
    title_ua: Text<String>,
    description_ua: Option<Text<String>>,
    price: Option<Text<String>>,
    article: Option<Text<String>>,
    brand: Text<String>,
    model: Option<Text<String>>,
    category: Option<Text<String>>,
    site_category_id: Option<Text<String>>,
    available: Option<Text<String>>,
    delivery_days: Option<Text<String>>,
    images: Option<Text<String>>,
    upsell: Option<Text<String>>,
    images_files: Vec<TempFile>,
}

struct ProductCreateInput {
    title: String,
    description: Option<String>,
    title_ua: String,
    description_ua: Option<String>,
    price: Option<usize>,
    article: Option<String>,
    brand: String,
    model: String,
    category: Option<String>,
    site_category_id: Option<String>,
    available: Option<String>,
    delivery_days: Option<usize>,
    images: Option<String>,
    upsell: Option<String>,
    images_files: Vec<TempFile>,
}

fn normalize_text_value(value: Option<Text<String>>) -> Option<String> {
    value
        .map(|v| v.0.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_images_csv(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(|c| c == ',' || c == '\n' || c == '\r')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>()
}

fn sanitize_upload_segment(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn save_product_uploads(
    files: &[TempFile],
    article: &str,
) -> Result<Vec<String>, ControllerError> {
    if files.is_empty() {
        return Ok(Vec::new());
    }

    let article_dir = sanitize_upload_segment(article);
    let root = format!("./static/uploads/products/{article_dir}");
    std::fs::create_dir_all(&root).map_err(anyhow::Error::new)?;

    let mut urls = Vec::with_capacity(files.len());
    for (idx, file) in files.iter().enumerate() {
        if file.size == 0 {
            continue;
        }
        if let Some(content_type) = &file.content_type {
            if content_type.type_() != IMAGE {
                return Err(anyhow!("Only image uploads are supported").into());
            }
        }

        let ext = file
            .file_name
            .as_deref()
            .and_then(|name| std::path::Path::new(name).extension())
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.trim().to_ascii_lowercase())
            .filter(|ext| !ext.is_empty())
            .unwrap_or_else(|| "jpg".to_string());
        let filename = format!("{}_{}.{}", Uuid::new_v4(), idx, ext);
        let dest = format!("{root}/{filename}");
        std::fs::copy(file.file.path(), &dest).map_err(anyhow::Error::new)?;
        urls.push(format!("/static/uploads/products/{article_dir}/{filename}"));
    }

    Ok(urls)
}

impl ProductCreateInput {
    fn from_multipart(form: ProductCreateMultipartForm) -> Self {
        let price = normalize_text_value(form.price)
            .and_then(|v| v.parse::<usize>().ok());

        ProductCreateInput {
            title: form.title.0.trim().to_string(),
            description: normalize_text_value(form.description),
            title_ua: form.title_ua.0.trim().to_string(),
            description_ua: normalize_text_value(form.description_ua),
            price,
            article: normalize_text_value(form.article),
            brand: form.brand.0.trim().to_string(),
            model: normalize_text_value(form.model)
                .unwrap_or_else(|| "Універсальна".to_string()),
            category: normalize_text_value(form.category),
            site_category_id: normalize_text_value(form.site_category_id),
            available: normalize_text_value(form.available),
            delivery_days: normalize_text_value(form.delivery_days)
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0),
            images: normalize_text_value(form.images),
            upsell: normalize_text_value(form.upsell),
            images_files: form.images_files,
        }
    }
}

#[post("/shop/{shop_id}/products/{article}")]
async fn shop_product_update(
    ShopAccess { shop, .. }: ShopAccess,
    article: Path<(String, String)>,
    form: Form<ProductUpdateForm>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
) -> Response {
    let (_shop_param, article) = article.into_inner();
    let form = form.into_inner();

    let products = dt_repo.list().await?;
    let mut product = products
        .into_iter()
        .find(|p| p.article.eq_ignore_ascii_case(&article))
        .ok_or(ControllerError::NotFound)?;

    if let Some(title) = form.title {
        if !title.trim().is_empty() {
            product.title = title.trim().to_string();
        }
    }
    if let Some(desc) = form.description {
        product.description = Some(desc);
    }
    if let Some(price) = form.price {
        product.price = Some(price);
    }
    if let Some(images) = form.images {
        let imgs = parse_images_csv(Some(images));
        if !imgs.is_empty() {
            product.images = imgs;
        }
    }
    if let Some(upsell) = form.upsell {
        let upsell = upsell.trim();
        product.upsell = if upsell.is_empty() {
            None
        } else {
            Some(upsell.to_string())
        };
    }

    dt_repo.save(product).await?;
    Ok(see_other(&format!("/shop/{}/products", shop.id)))
}

#[post("/shop/{shop_id}/products/new")]
async fn shop_product_create(
    ShopAccess { shop, .. }: ShopAccess,
    form: MultipartForm<ProductCreateMultipartForm>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    use crate::dt::product::Product;
    use crate::Model;
    use crate::Url as DtUrl;
    let input = ProductCreateInput::from_multipart(form.into_inner());
    let article = input
        .article
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let article_for_shop = article.clone();
    let mut slug = input.title.clone();
    slug.make_ascii_lowercase();
    let re = Regex::new(r"[^a-z0-9]+").unwrap();
    slug = re.replace_all(&slug, "-").trim_matches('-').to_string();
    if slug.is_empty() {
        slug = article.clone();
    }
    let url = DtUrl(format!("/manual/op-tuning/{}.html", slug));
    // Завантажені файли мають пріоритет над URL з поля images
    let uploaded_images = save_product_uploads(&input.images_files, &article)?;
    let mut images = if !uploaded_images.is_empty() {
        // Якщо є завантажені файли, використовуємо їх як основні
        uploaded_images.clone()
    } else {
        // Інакше використовуємо URL з поля images
        parse_images_csv(input.images.clone())
    };
    // Додаємо додаткові URL з поля images, якщо вони не є завантаженими файлами
    if !uploaded_images.is_empty() {
        let additional_urls = parse_images_csv(input.images.clone())
            .into_iter()
            .filter(|url| !uploaded_images.contains(url))
            .collect::<Vec<_>>();
        images.extend(additional_urls);
    }
    let available = match input.available.as_deref() {
        Some("on_order") => Availability::OnOrder,
        Some("not_available") => Availability::NotAvailable,
        _ => Availability::Available,
    };
    let mut attributes = None;
    if matches!(available, Availability::OnOrder) {
        if let Some(days) = input.delivery_days {
            let mut attrs = HashMap::new();
            attrs.insert("delivery_days".to_string(), days.to_string());
            attributes = Some(attrs);
        }
    }
    let mut site_category_id = input
        .site_category_id
        .as_deref()
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    let mut category = input.category;
    if site_category_id.is_none() {
        if let Some(name) = category.as_ref().map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
            let existing = product_category_repo
                .select(&product_category::ByShop(shop.id))
                .await
                .unwrap_or_default()
                .into_iter()
                .find(|c| c.name.eq_ignore_ascii_case(&name));
            if let Some(found) = existing {
                site_category_id = Some(found.id);
                category = Some(found.name);
            } else {
                let new_category = product_category::ProductCategory {
                    id: uuid::Uuid::new_v4(),
                    parent_id: None,
                    name: name.clone(),
                    regex: None,
                    shop_id: shop.id,
                    status: product_category::CategoryStatus::PublishedNoIndex,
                    visibility_on_site: product_category::Visibility::Visible,
                    indexing_status: product_category::IndexingStatus::NoIndex,
                    seo_title: None,
                    seo_description: None,
                    seo_text: None,
                    image_url: None,
                };
                if product_category_repo.save(new_category.clone()).await.is_ok() {
                    site_category_id = Some(new_category.id);
                    category = Some(new_category.name);
                }
            }
        }
    }
    if let Some(id) = site_category_id {
        let categories = product_category_repo
            .select(&product_category::ByShop(shop.id))
            .await
            .unwrap_or_default();
        category = categories.into_iter().find(|c| c.id == id).map(|c| c.name);
    }
    let title = input.title;
    let title_ua = input.title_ua;
    let description = input.description;
    let description_ua = input.description_ua;
    let product = Product {
        title,
        description,
        title_ua: Some(title_ua),
        description_ua,
        price: input.price,
        source_price: input.price,
        article,
        brand: input.brand,
        model: Model(input.model),
        category,
        attributes,
        available,
        quantity: None,
        url,
        supplier: Some("op_tuning".to_string()),
        discount_percent: None,
        last_visited: OffsetDateTime::now_utc(),
        images,
        upsell: input.upsell,
    };
    // Зберігаємо дані для використання після збереження
    let product_title = product.title.clone();
    let product_brand = product.brand.clone();
    let product_model = product.model.0.clone();
    let product_article = product.article.clone();
    
    dt_repo.save(product).await?;
    shop_product_repo
        .ensure_exists(shop.id, &article_for_shop)
        .await?;
    if let Ok(Some(mut settings)) = shop_product_repo.get(shop.id, &article_for_shop).await {
        // Генеруємо правильний slug та path для фронтенду (використовуємо ту саму логіку, що і на фронтенді)
        use crate::control::site_api::{build_product_slug, product_path_from_slug, canonical_from_path};
        let slug = build_product_slug(&product_title, &product_brand, &product_model, &product_article);
        let path = product_path_from_slug(&slug, &product_article);
        let canonical = canonical_from_path(&path).unwrap_or_else(|| path.clone());
        
        settings.source_type = shop_product::SourceType::Manual;
        settings.visibility_on_site = shop_product::Visibility::Visible;
        settings.indexing_status = shop_product::IndexingStatus::NoIndex;
        settings.status = shop_product::ProductStatus::PublishedNoIndex;
        settings.slug = Some(slug);
        settings.canonical = Some(canonical);
        settings.updated_at = OffsetDateTime::now_utc();
        shop_product_repo.upsert(settings).await?;
    }
    if site_category_id.is_some() {
        let _ = shop_product_repo
            .set_site_category(shop.id, &article_for_shop, site_category_id)
            .await;
    }
    Ok(see_other(&format!("/shop/{}/products", shop.id)))
}

#[derive(Template)]
#[template(path = "shop/files.html")]
pub struct ShopFilesPage {
    shop: Shop,
    user: UserCredentials,
    files: Vec<UploadedFileInfo>,
    directories: Vec<String>,
    current_dir: String,
    storage: StorageStats,
}

#[get("/shop/{shop_id}/files")]
async fn shop_files(
    ShopAccess { shop, user }: ShopAccess,
    query: Option<Query<std::collections::HashMap<String, String>>>,
) -> Response {
    let uploads_dir = "./static/uploads/products";
    let storage = StorageStats::from_path(StdPath::new(uploads_dir));
    let current_dir = query
        .as_ref()
        .and_then(|q| q.get("dir"))
        .cloned()
        .unwrap_or_else(|| "".to_string());
    
    let dir_path = if current_dir.is_empty() {
        std::path::PathBuf::from(uploads_dir)
    } else {
        std::path::PathBuf::from(uploads_dir).join(&current_dir)
    };

    let mut files = Vec::new();
    let mut directories = Vec::new();

    if dir_path.exists() && dir_path.is_dir() {
        match std::fs::read_dir(&dir_path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(metadata) = entry.metadata() {
                        if path.is_dir() {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                directories.push(name.to_string());
                            }
                        } else {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                let relative_path = if current_dir.is_empty() {
                                    name.to_string()
                                } else {
                                    format!("{}/{}", current_dir, name)
                                };
                                files.push(UploadedFileInfo {
                                    name: name.to_string(),
                                    path: relative_path,
                                    size: metadata.len(),
                                    last_modified: OffsetDateTime::from(metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH)),
                                });
                            }
                        }
                    }
                }
            }
            Err(err) => {
                log::error!("Unable to read directory {}: {}", dir_path.display(), err);
            }
        }
    }

    files.sort_by(|a, b| a.name.cmp(&b.name));
    directories.sort();

    render_template(ShopFilesPage {
        shop,
        user,
        files,
        directories,
        current_dir,
        storage,
    })
}

#[post("/shop/{shop_id}/files/delete")]
async fn shop_files_delete(
    ShopAccess { shop, .. }: ShopAccess,
    form: Form<std::collections::HashMap<String, String>>,
) -> Response {
    let file_path = form
        .get("path")
        .ok_or_else(|| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Path is required".to_string(),
        })?;

    let full_path = std::path::PathBuf::from("./static/uploads/products").join(file_path);
    
    // Перевірка безпеки - переконаємося, що шлях не виходить за межі дозволеної директорії
    let canonical_path = full_path.canonicalize()
        .map_err(|_| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Invalid path".to_string(),
        })?;
    
    let uploads_dir = std::path::PathBuf::from("./static/uploads/products")
        .canonicalize()
        .map_err(|_| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Invalid uploads directory".to_string(),
        })?;

    if !canonical_path.starts_with(&uploads_dir) {
        return Err(ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Path outside allowed directory".to_string(),
        });
    }

    std::fs::remove_file(&canonical_path)
        .map_err(|err| ControllerError::InternalServerError(anyhow::anyhow!(
            "Unable to delete file: {}", err
        )))?;

    Ok(see_other(&format!("/shop/{}/files", shop.id)))
}

#[derive(Deserialize)]
pub struct SeoPageForm {
    pub page_type: Option<String>,
    pub status: Option<String>,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub h1: Option<String>,
    pub meta_title: Option<String>,
    pub meta_description: Option<String>,
    pub seo_text: Option<String>,
    pub seo_text_auto: Option<String>,
    pub faq: Option<String>,
    pub robots: Option<String>,
    pub brand: Option<String>,
    pub model: Option<String>,
    pub car: Option<String>,
    pub category: Option<String>,
    pub topic: Option<String>,
    pub related_links: Option<String>,
}

fn build_seo_payload(form: &SeoPageForm) -> seo_page::SeoPagePayload {
    let brand = normalize_string(form.brand.clone());
    let model = normalize_string(form.model.clone());
    let car = normalize_string(form.car.clone());
    let category = normalize_string(form.category.clone());
    let topic = normalize_string(form.topic.clone());
    let brand_slug = brand
        .as_ref()
        .map(|v| seo_page::slugify_latin(v))
        .filter(|v| !v.is_empty());
    let model_slug = model
        .as_ref()
        .map(|v| seo_page::slugify_latin(v))
        .filter(|v| !v.is_empty());
    let category_slug = category
        .as_ref()
        .map(|v| seo_page::slugify_latin(v))
        .filter(|v| !v.is_empty());
    seo_page::SeoPagePayload {
        brand,
        model,
        car,
        category,
        topic,
        brand_slug,
        model_slug,
        category_slug,
    }
}

#[get("/shop/{shop_id}/seo_pages")]
async fn shop_seo_pages(
    ShopAccess { shop, user }: ShopAccess,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let dt_repo = dt_repo.get_ref().clone();
    let shop_product_repo = shop_product_repo.get_ref().clone();
    let category_repo = category_repo.get_ref().clone();
    let product_category_repo = product_category_repo.get_ref().clone();
    let (items, _) = crate::control::site_api::load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    let pages = seo_page_repo
        .select(&seo_page::ByShop(shop.id))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|page| {
            let payload = seo_page::SeoPagePayload::from_json(page.source_payload.as_deref());
            let product_count = seo_page_product_count(&page.page_type, &payload, items.as_ref());
            let indexable = seo_page::seo_page_indexable(
                &page.page_type,
                &page.status,
                &page.meta_title,
                &page.meta_description,
                &page.seo_text,
                product_count,
            );
            ShopSeoPageListItem {
                id: page.id.to_string(),
                title: page.title.clone(),
                path: page.path(),
                page_type: seo_page_type_label(&page.page_type).to_string(),
                status: page.status.as_str().to_string(),
                seo_text_auto: page.seo_text_auto,
                updated_at: format_unix_timestamp(page.updated_at.unix_timestamp()),
                indexable,
                product_count,
            }
        })
        .collect::<Vec<_>>();

    render_template(ShopSeoPagesPage { shop, user, pages })
}

#[get("/shop/{shop_id}/seo_pages/new")]
async fn shop_seo_page_new_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    let now = OffsetDateTime::now_utc();
    let page = seo_page::SeoPage {
        id: Uuid::new_v4(),
        shop_id: shop.id,
        page_type: seo_page::SeoPageType::TuningModel,
        slug: String::new(),
        title: String::new(),
        h1: None,
        meta_title: None,
        meta_description: None,
        seo_text: None,
        seo_text_auto: true,
        faq: None,
        robots: None,
        status: seo_page::SeoPageStatus::Draft,
        source_payload: None,
        related_links: Vec::new(),
        created_at: now,
        updated_at: now,
    };
    render_template(ShopSeoPageEditPage {
        shop,
        user,
        page_type: page.page_type.as_str().to_string(),
        status: page.status.as_str().to_string(),
        payload: seo_page::SeoPagePayload::default(),
        related_links: String::new(),
        path: String::new(),
        product_count: 0,
        indexable: false,
        seo_page: page,
        is_new: true,
    })
}

#[post("/shop/{shop_id}/seo_pages/new")]
async fn shop_seo_page_create(
    ShopAccess { shop, .. }: ShopAccess,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    form: Form<SeoPageForm>,
) -> Response {
    let form = form.into_inner();
    let page_type = seo_page::SeoPageType::from_str(
        form.page_type.as_deref().unwrap_or("tuning_model"),
    );
    let status = seo_page::SeoPageStatus::from_str(form.status.as_deref().unwrap_or("draft"));
    let seo_text_auto = form.seo_text_auto.is_some();
    let payload = build_seo_payload(&form);
    let generated = seo_page::generate_from_template(&page_type, &payload);

    let mut slug = normalize_string(form.slug)
        .map(|s| seo_page::slugify_latin(&s))
        .unwrap_or_default();
    if slug.is_empty() {
        slug = seo_page::build_auto_slug(&page_type, &payload);
    }
    if slug.is_empty() {
        return Err(ControllerError::InvalidInput {
            field: "slug".to_string(),
            msg: "Slug не може бути порожнім".to_string(),
        });
    }
    if seo_page_repo.slug_exists(shop.id, &slug, None).await? {
        return Err(ControllerError::InvalidInput {
            field: "slug".to_string(),
            msg: "Slug вже використовується іншою сторінкою".to_string(),
        });
    }

    let title = normalize_string(form.title).unwrap_or(generated.title);
    let h1 = normalize_string(form.h1).or_else(|| Some(generated.h1));
    let meta_title = normalize_string(form.meta_title).or_else(|| Some(generated.meta_title));
    let meta_description =
        normalize_string(form.meta_description).or_else(|| Some(generated.meta_description));
    let seo_text = if seo_text_auto {
        normalize_string(form.seo_text).or_else(|| Some(generated.seo_text))
    } else {
        normalize_string(form.seo_text)
    };
    let faq = normalize_string(form.faq);
    let robots = normalize_string(form.robots);
    let related_links = parse_related_links(form.related_links);
    let source_payload = serde_json::to_string(&payload).ok();
    let now = OffsetDateTime::now_utc();
    let page = seo_page::SeoPage {
        id: Uuid::new_v4(),
        shop_id: shop.id,
        page_type,
        slug,
        title,
        h1,
        meta_title,
        meta_description,
        seo_text,
        seo_text_auto,
        faq,
        robots,
        status,
        source_payload,
        related_links,
        created_at: now,
        updated_at: now,
    };
    seo_page_repo.save(page.clone()).await?;
    Ok(see_other(&format!(
        "/shop/{}/seo_pages/{}",
        shop.id, page.id
    )))
}

#[get("/shop/{shop_id}/seo_pages/{page_id}")]
async fn shop_seo_page_edit_page(
    ShopAccess { shop, user }: ShopAccess,
    page_id: Path<(String, String)>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let (_shop_param, page_id) = page_id.into_inner();
    let page_id = Uuid::parse_str(&page_id).map_err(|_| ControllerError::NotFound)?;
    let page = seo_page_repo
        .get_one(&page_id)
        .await?
        .filter(|p| p.shop_id == shop.id)
        .ok_or(ControllerError::NotFound)?;
    let payload = seo_page::SeoPagePayload::from_json(page.source_payload.as_deref());

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let dt_repo = dt_repo.get_ref().clone();
    let shop_product_repo = shop_product_repo.get_ref().clone();
    let category_repo = category_repo.get_ref().clone();
    let product_category_repo = product_category_repo.get_ref().clone();
    let (items, _) = crate::control::site_api::load_site_products_cached(
        &shop,
        &allowed_suppliers,
        &dt_repo,
        &shop_product_repo,
        &category_repo,
        &product_category_repo,
    )
    .await;

    let product_count = seo_page_product_count(&page.page_type, &payload, items.as_ref());
    let indexable = seo_page::seo_page_indexable(
        &page.page_type,
        &page.status,
        &page.meta_title,
        &page.meta_description,
        &page.seo_text,
        product_count,
    );
    let related_links = related_links_to_text(&page.related_links);
    let path = if page.slug.trim().is_empty() {
        String::new()
    } else {
        page.path()
    };

    render_template(ShopSeoPageEditPage {
        shop,
        user,
        page_type: page.page_type.as_str().to_string(),
        status: page.status.as_str().to_string(),
        payload,
        related_links,
        path,
        product_count,
        indexable,
        seo_page: page,
        is_new: false,
    })
}

#[post("/shop/{shop_id}/seo_pages/{page_id}")]
async fn shop_seo_page_update(
    ShopAccess { shop, .. }: ShopAccess,
    page_id: Path<(String, String)>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
    form: Form<SeoPageForm>,
) -> Response {
    let (_shop_param, page_id) = page_id.into_inner();
    let page_id = Uuid::parse_str(&page_id).map_err(|_| ControllerError::NotFound)?;
    let current = seo_page_repo
        .get_one(&page_id)
        .await?
        .filter(|p| p.shop_id == shop.id)
        .ok_or(ControllerError::NotFound)?;
    let form = form.into_inner();
    let page_type = seo_page::SeoPageType::from_str(
        form.page_type.as_deref().unwrap_or("tuning_model"),
    );
    let status = seo_page::SeoPageStatus::from_str(form.status.as_deref().unwrap_or("draft"));
    let seo_text_auto = form.seo_text_auto.is_some();
    let payload = build_seo_payload(&form);
    let generated = seo_page::generate_from_template(&page_type, &payload);

    let mut slug = normalize_string(form.slug)
        .map(|s| seo_page::slugify_latin(&s))
        .unwrap_or_default();
    if slug.is_empty() {
        slug = seo_page::build_auto_slug(&page_type, &payload);
    }
    if slug.is_empty() {
        return Err(ControllerError::InvalidInput {
            field: "slug".to_string(),
            msg: "Slug не може бути порожнім".to_string(),
        });
    }
    if seo_page_repo
        .slug_exists(shop.id, &slug, Some(current.id))
        .await?
    {
        return Err(ControllerError::InvalidInput {
            field: "slug".to_string(),
            msg: "Slug вже використовується іншою сторінкою".to_string(),
        });
    }

    let title = normalize_string(form.title).unwrap_or(generated.title);
    let h1 = normalize_string(form.h1).or_else(|| Some(generated.h1));
    let meta_title = normalize_string(form.meta_title).or_else(|| Some(generated.meta_title));
    let meta_description =
        normalize_string(form.meta_description).or_else(|| Some(generated.meta_description));
    let seo_text = if seo_text_auto {
        normalize_string(form.seo_text).or_else(|| Some(generated.seo_text))
    } else {
        normalize_string(form.seo_text)
    };
    let faq = normalize_string(form.faq);
    let robots = normalize_string(form.robots);
    let related_links = parse_related_links(form.related_links);
    let source_payload = serde_json::to_string(&payload).ok();
    let now = OffsetDateTime::now_utc();

    if !current.slug.eq_ignore_ascii_case(&slug) {
        let entry = seo_page::SeoPageSlug {
            id: Uuid::new_v4(),
            page_id: current.id,
            shop_id: shop.id,
            slug: current.slug.clone(),
            created_at: now,
        };
        seo_page_repo.insert_slug_history(entry).await?;
    }

    let page = seo_page::SeoPage {
        id: current.id,
        shop_id: shop.id,
        page_type,
        slug,
        title,
        h1,
        meta_title,
        meta_description,
        seo_text,
        seo_text_auto,
        faq,
        robots,
        status,
        source_payload,
        related_links,
        created_at: current.created_at,
        updated_at: now,
    };
    seo_page_repo.save(page).await?;
    Ok(see_other(&format!(
        "/shop/{}/seo_pages/{}",
        shop.id, page_id
    )))
}

#[post("/shop/{shop_id}/seo_pages/{page_id}/remove")]
async fn shop_seo_page_remove(
    ShopAccess { shop, .. }: ShopAccess,
    page_id: Path<(String, String)>,
    seo_page_repo: Data<Arc<dyn seo_page::SeoPageRepository>>,
) -> Response {
    let (_shop_param, page_id) = page_id.into_inner();
    let page_id = Uuid::parse_str(&page_id).map_err(|_| ControllerError::NotFound)?;
    let page = seo_page_repo
        .get_one(&page_id)
        .await?
        .filter(|p| p.shop_id == shop.id)
        .ok_or(ControllerError::NotFound)?;
    seo_page_repo.remove(&page.id).await?;
    Ok(see_other(&format!("/shop/{}/seo_pages", shop.id)))
}

#[derive(Template)]
#[template(path = "parsing_product_info.html")]
pub struct ProductInfoPage {
    product: dt::product::Product,
    user: UserCredentials,
}

#[derive(Deserialize)]
pub struct DtParseQuery {
    pub link: String,
}

#[post("/control_panel/dt/parse")]
async fn dt_parse(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtParseQuery>,
) -> Response {
    let form = form.into_inner();
    let product = dt_parser
        .send(dt::parser::Parse(form.link))
        .await?
        .map_err(anyhow::Error::from)?;
    render_template(ProductInfoPage { product, user })
}

#[derive(Template)]
#[template(path = "dt_parsing_page_info.html")]
pub struct DtPageInfoPage {
    links: Vec<String>,
    url: String,
    user: UserCredentials,
}

#[post("/control_panel/dt/parse_page")]
async fn dt_parse_page(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtParseQuery>,
) -> Response {
    let form = form.into_inner();
    let url = "design-tuning.com".to_string();
    let links = dt_parser
        .send(dt::parser::ParsePage(form.link))
        .await?
        .context("Unable to parse page")?;
    render_template(DtPageInfoPage { links, url, user })
}

#[derive(Deserialize)]
pub struct DtProductInfoQuery {
    pub article: String,
}

#[post("/control_panel/dt/product_info")]
async fn dt_product_info(
    dt_parser: Data<Arc<Addr<dt::parser::ParserService>>>,
    ControlPanelAccess { user }: ControlPanelAccess,
    form: Form<DtProductInfoQuery>,
) -> Response {
    let form = form.into_inner();
    let product = dt_parser
        .send(dt::parser::ProductInfo(form.article))
        .await?
        .map_err(anyhow::Error::from)?;
    if let Some(product) = product {
        render_template(ProductInfoPage { product, user })
    } else {
        Err(anyhow::anyhow!("Товар не найден").into())
    }
}

#[derive(Deserialize)]
struct ExportInfoQuery {
    with_new_link: Option<String>,
}

#[derive(Template)]
#[template(path = "export_info.html")]
struct ExportInfoPage {
    export: ExportViewDto,
    hash: String,
    with_new_link: bool,
    descriptions: Vec<String>,
    shop: ShopDto,
    user: UserCredentials,
    watermarks: Vec<String>,
    groups: Vec<WatermarkGroupDto>,
    ddaudio_api_categories: Vec<DDAudioCategoryView>,
    ddaudio_api_warehouses: Vec<String>,
    ddaudio_api_error: Option<String>,
    ddaudio_api_langs: HashSet<String>,
    ddaudio_api_selected_warehouses: HashSet<String>,
    ddaudio_api_warehouse_statuses: HashMap<String, String>,
    ddaudio_api_warehouse_views: Vec<DDAudioWarehouseView>,
}

#[derive(Serialize, Clone)]
struct ExportViewDto {
    status: String,
    entry: ExportEntry,
}

impl From<export::Export> for ExportViewDto {
    fn from(e: export::Export) -> Self {
        Self {
            status: e.status().to_string(),
            entry: e.entry().clone(),
        }
    }
}

#[derive(Serialize, Clone)]
struct ShopDto {
    id: String,
    name: String,
    is_suspended: bool,
}

impl From<Shop> for ShopDto {
    fn from(s: Shop) -> Self {
        Self {
            id: s.id.to_string(),
            name: s.name,
            is_suspended: s.is_suspended,
        }
    }
}

#[derive(Serialize, Clone)]
struct WatermarkGroupDto {
    id: String,
    name: String,
}

impl From<WatermarkGroup> for WatermarkGroupDto {
    fn from(w: WatermarkGroup) -> Self {
        Self {
            id: w.id().1.to_string(),
            name: w.name,
        }
    }
}

impl WatermarkGroupDto {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[get("/shop/{shop_id}/export_info/{hash}")]
async fn export_info(
    path: Path<(IdentityOf<Shop>, String)>,
    q: Query<ExportInfoQuery>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let export = export_service
        .send(export::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to ExportService")?;
    let export = match export {
        Some(export) => Some(ExportViewDto::from(export)),
        None => None,
    };
    let groups = watermark_group_repository.list_by(&shop_id).await?;
    let path = format!("./description/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path).context("Unable to create dir")?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm).context("Unable to set dir permissions")?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let descriptions = res
        .context("Unable to read descriptions")?
        .map(|d| {
            d?.file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)?;
    let path = format!("./watermark/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path)
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm)
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let watermarks: Vec<_> = res
        .map_err(ShopControllerError::with(&user, &shop))?
        .map(|e| {
            let e = e?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(file_name)
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;
    match export {
        Some(export) => {
            let mut ddaudio_api_categories = Vec::new();
            let mut ddaudio_api_warehouses = Vec::new();
            let mut ddaudio_api_error = None;
            let mut ddaudio_api_langs = HashSet::new();
            let mut ddaudio_api_selected_warehouses = HashSet::new();
            let mut ddaudio_api_warehouse_statuses = HashMap::new();
            let mut ddaudio_api_warehouse_views = Vec::new();
            if let Some(ddaudio_api) = export.entry.ddaudio_api.as_ref() {
                let has_cat_selection = !ddaudio_api.selected_categories.is_empty();
                let has_sub_selection = !ddaudio_api.selected_subcategories.is_empty();
                let default_selected = !has_cat_selection && !has_sub_selection;
                let rule_to_view = |rule: &rt_types::shop::DDAudioCategoryRule| DDAudioRuleView {
                    price_type: match rule.price_type {
                        rt_types::shop::DDAudioPriceType::Wholesale => "wholesale".to_string(),
                        rt_types::shop::DDAudioPriceType::Retail => "retail".to_string(),
                    },
                    markup_percent: rule.markup_percent.and_then(|v| v.to_f64()),
                    discount_percent: rule.discount_percent,
                    discount_hours: rule.discount_hours,
                    round_to_9: rule.round_to_9,
                    zero_stock_policy: match rule.zero_stock_policy {
                        rt_types::shop::ZeroStockPolicy::OnOrder => "on_order".to_string(),
                        rt_types::shop::ZeroStockPolicy::NotAvailable => "not_available".to_string(),
                        rt_types::shop::ZeroStockPolicy::Inherit => "inherit".to_string(),
                    },
                };
                ddaudio_api_langs = ddaudio_api
                    .languages
                    .iter()
                    .map(|l| l.trim().to_lowercase())
                    .filter(|l| !l.is_empty())
                    .collect();
                ddaudio_api_selected_warehouses = ddaudio_api
                    .selected_warehouses
                    .iter()
                    .cloned()
                    .collect();
                ddaudio_api_warehouse_statuses = ddaudio_api
                    .warehouse_statuses
                    .iter()
                    .map(|(key, policy)| {
                        let value = match policy {
                            rt_types::shop::ZeroStockPolicy::OnOrder => "on_order",
                            rt_types::shop::ZeroStockPolicy::NotAvailable => "not_available",
                            rt_types::shop::ZeroStockPolicy::Inherit => "inherit",
                        };
                        (key.clone(), value.to_string())
                    })
                    .collect::<HashMap<_, _>>();
                if !ddaudio_api.token.trim().is_empty() {
                    let lang = ddaudio_api.languages.first().map(|l| l.as_str());
                    match ddaudio::fetch_categories(ddaudio_api.token.trim(), lang).await {
                        Ok(data) => {
                            for (id, node) in data.data {
                                let mut children = Vec::new();
                                let parent_selected = default_selected
                                    || ddaudio_api.selected_categories.iter().any(|v| v == &id);
                                for (child_id, child_name) in node.children {
                                    let child_checked = default_selected
                                        || (!has_sub_selection && parent_selected)
                                        || ddaudio_api
                                            .selected_subcategories
                                            .iter()
                                            .any(|v| v == &child_id);
                                    let child_rule = ddaudio_api
                                        .subcategory_rules
                                        .get(&child_id)
                                        .unwrap_or(&ddaudio_api.default_rule);
                                    children.push(DDAudioCategoryView {
                                        id: child_id,
                                        name: child_name,
                                        checked: child_checked,
                                        rule: rule_to_view(child_rule),
                                        children: Vec::new(),
                                    });
                                }
                                let checked = parent_selected || children.iter().any(|c| c.checked);
                                let rule = ddaudio_api
                                    .category_rules
                                    .get(&id)
                                    .unwrap_or(&ddaudio_api.default_rule);
                                ddaudio_api_categories.push(DDAudioCategoryView {
                                    id,
                                    name: node.title,
                                    checked,
                                    rule: rule_to_view(rule),
                                    children,
                                });
                            }
                            ddaudio_api_categories.sort_by_key(|c| c.name.to_lowercase());
                        }
                        Err(err) => {
                            ddaudio_api_error = Some(format!("DD Audio categories: {err}"));
                        }
                    }
                    match ddaudio::fetch_warehouses(ddaudio_api.token.trim()).await {
                        Ok(data) => {
                            ddaudio_api_warehouses = data.data;
                            ddaudio_api_warehouses.sort();
                        }
                        Err(err) => {
                            ddaudio_api_error = Some(format!(
                                "{}{}DD Audio warehouses: {err}",
                                ddaudio_api_error.clone().unwrap_or_default(),
                                if ddaudio_api_error.is_some() { "; " } else { "" }
                            ));
                        }
                    }
                }
                if ddaudio_api_warehouses.is_empty() && !ddaudio_api.known_warehouses.is_empty() {
                    ddaudio_api_warehouses = ddaudio_api.known_warehouses.clone();
                    ddaudio_api_warehouses.sort();
                }
                if ddaudio_api_warehouses.is_empty() && !ddaudio_api.selected_warehouses.is_empty() {
                    ddaudio_api_warehouses = ddaudio_api.selected_warehouses.clone();
                    ddaudio_api_warehouses.sort();
                }
                if ddaudio_api_warehouses.is_empty() {
                    let site_ddaudio = site_publish::load_ddaudio_config(&shop.id);
                    if !site_ddaudio.known_warehouses.is_empty() {
                        ddaudio_api_warehouses = site_ddaudio.known_warehouses;
                        ddaudio_api_warehouses.sort();
                    } else if !site_ddaudio.selected_warehouses.is_empty() {
                        ddaudio_api_warehouses = site_ddaudio.selected_warehouses;
                        ddaudio_api_warehouses.sort();
                    }
                }
                ddaudio_api_warehouse_views = ddaudio_api_warehouses
                    .iter()
                    .map(|name| DDAudioWarehouseView {
                        name: name.clone(),
                        checked: ddaudio_api_selected_warehouses.contains(name.as_str()),
                        policy: ddaudio_api_warehouse_statuses
                            .get(name)
                            .cloned()
                            .unwrap_or_else(|| "inherit".to_string()),
                    })
                    .collect::<Vec<_>>();
            }
            render_template(ExportInfoPage {
                export: export.into(),
                hash,
                with_new_link: q.with_new_link.is_some(),
                descriptions,
                shop: shop.into(),
                user: user.into(),
                watermarks,
                groups: groups.into_iter().map(Into::into).collect(),
                ddaudio_api_categories,
                ddaudio_api_warehouses,
                ddaudio_api_error,
                ddaudio_api_langs,
                ddaudio_api_selected_warehouses,
                ddaudio_api_warehouse_statuses,
                ddaudio_api_warehouse_views,
            })
        }
        None => Ok(see_other(&format!("/shop/{shop_id}"))),
    }
}

#[post("/shop/{shop_id}/export_info/{hash}/remove")]
async fn remove_export(
    hash: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = hash.into_inner();
    export_service
        .send(export::Remove(hash.clone()))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/export_info")]
async fn add_export(
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let shop_id = shop.id;
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let permission = AddExportPermission::acquire(&user, &shop, &subscription)
        .ok_or(anyhow::anyhow!("This shop cannot contain any more exports"))?;
    export_service
        .send(export::Add(permission, ExportEntry::default()))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryDto {
    pub file_name: Option<String>,
    #[serde(deserialize_with = "deserialize_parse_form::<_, u64>")]
    pub update_rate: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryLinkDto {
    pub vendor_name: Option<String>,
    pub link: Option<String>,
    #[serde(default, deserialize_with = "deserialize_publish_form")]
    pub publish: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_prefix: bool,
    pub title_prefix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_suffix: bool,
    pub title_suffix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_prefix_ua: bool,
    pub title_prefix_ua: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_title_suffix_ua: bool,
    pub title_suffix_ua: Option<String>,
    pub title_replacements: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub format_years: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub only_available: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub discount: bool,
    #[serde(deserialize_with = "deserialize_parse_form::<_, u64>")]
    pub discount_duration: Option<u64>,
    #[serde(deserialize_with = "deserialize_parse_form::<_, usize>")]
    pub discount_percent: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub add_vendor: bool,
    pub description_action: Option<String>,
    pub description_path: Option<String>,
    pub description_action_ua: Option<String>,
    pub description_path_ua: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub delivery_time: bool,
    #[serde(deserialize_with = "deserialize_parse_form::<_, usize>")]
    pub delivery_time_duration: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub adjust_price: bool,
    #[serde(deserialize_with = "deserialize_decimal_form")]
    pub adjust_price_by: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub categories: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub convert_to_uah: bool,
    pub watermark: Option<WatermarkType>,
    pub watermark_name: Option<String>,
    #[serde(flatten)]
    pub watermark_options: Option<WatermarkOptionsDto>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub set_availability_enabled: bool,
    pub set_availability: Option<Availability>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkType {
    Watermark,
    Group,
}

impl TryInto<ExportEntryLink> for ExportEntryLinkDto {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ExportEntryLink, Self::Error> {
        let publish = self.publish_value();
        Ok(ExportEntryLink {
            vendor_name: self.vendor_name.as_ref().filter(|v| !v.is_empty()).cloned(),
            link: self
                .link
                .clone()
                .ok_or_else(|| anyhow!("Export entry must have a link"))?,
            publish,
            options: Some(self.into()),
        })
    }
}

impl Into<ExportOptions> for ExportEntryLinkDto {
    fn into(self) -> ExportOptions {
        let publish = self.publish_value();
        let parse_replacements = |raw: Option<String>| -> Option<Vec<(String, String)>> {
            let raw = raw?;
            let pairs: Vec<_> = raw
                .lines()
                .filter_map(|line| {
                    let mut parts = line.splitn(2, '=');
                    let from = parts.next().map(str::trim).unwrap_or_default();
                    let to = parts.next().map(str::trim).unwrap_or_default();
                    if !from.is_empty() && !to.is_empty() {
                        Some((from.to_string(), to.to_string()))
                    } else {
                        None
                    }
                })
                .collect();
            if pairs.is_empty() {
                None
            } else {
                Some(pairs)
            }
        };
        ExportOptions {
            title_prefix: self
                .title_prefix
                .filter(|_| self.add_title_prefix)
                .filter(|s| !s.trim().is_empty()),
            title_prefix_ua: self
                .title_prefix_ua
                .filter(|_| self.add_title_prefix_ua)
                .filter(|s| !s.trim().is_empty()),
            title_suffix: self
                .title_suffix
                .filter(|_| self.add_title_suffix)
                .filter(|s| !s.trim().is_empty()),
            title_suffix_ua: self
                .title_suffix_ua
                .filter(|_| self.add_title_suffix_ua)
                .filter(|s| !s.trim().is_empty()),
            title_replacements: parse_replacements(self.title_replacements),
            only_available: self.only_available,
            publish,
            discount: self
                .discount_duration
                .zip(self.discount_percent)
                .filter(|_| self.discount)
                .map(|(duration, percent)| Discount {
                    duration: Duration::from_secs(duration * 60 * 60),
                    percent,
                }),
            format_years: self.format_years,
            add_vendor: self.add_vendor,
            description: self
                .description_path
                .zip(self.description_action)
                .and_then(|(path, action)| DescriptionOptions::try_from(action, path)),
            description_ua: self
                .description_path_ua
                .zip(self.description_action_ua)
                .and_then(|(path, action)| DescriptionOptions::try_from(action, path)),
            adjust_price: self.adjust_price_by.filter(|_| self.adjust_price),
            delivery_time: self.delivery_time_duration.filter(|_| self.delivery_time),
            categories: self.categories,
            convert_to_uah: self.convert_to_uah,
            custom_options: None,
            watermarks: self
                .watermark_name
                .filter(|_| self.watermark.is_some())
                .zip(Some(
                    self.watermark_options
                        .filter(|_| matches!(self.watermark, Some(WatermarkType::Watermark)))
                        .map(Into::into),
                )),
            set_availability: self
                .set_availability
                .filter(|_| self.set_availability_enabled),
        }
    }
}

impl ExportEntryLinkDto {
    fn publish_value(&self) -> bool {
        self.publish
            .iter()
            .filter_map(|value| match value.as_str() {
                "on" | "true" => Some(true),
                "off" | "false" => Some(false),
                _ => None,
            })
            .last()
            .unwrap_or(true)
    }
}

#[derive(Debug, Deserialize)]
pub struct SiteImportEntryDto {
    pub name: Option<String>,
    #[serde(deserialize_with = "deserialize_parse_form::<_, u64>")]
    pub update_rate: Option<u64>,
    pub source_kind: Option<String>,
    pub parsing_supplier: Option<String>,
    pub xml_link: Option<String>,
    pub xml_vendor_name: Option<String>,
    pub missing_policy: Option<String>,
    pub image_strategy: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub append_images: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub round_to_9: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_title_ru: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_title_ua: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_description_ru: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_description_ua: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_sku: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_price: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_images: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_availability: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_quantity: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_attributes: bool,
    #[serde(default, deserialize_with = "deserialize_bool_form")]
    pub update_discounts: bool,
    #[serde(flatten)]
    pub transform: ExportEntryLinkDto,
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryDtDto {
    #[serde(flatten)]
    options: ExportEntryLinkDto,
}

impl Into<DtParsingOptions> for ExportEntryDtDto {
    fn into(self) -> DtParsingOptions {
        DtParsingOptions {
            options: self.options.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParsingCategoriesActionDto {
    BeforeTitle,
    AfterTitle,
}

#[derive(Debug, Deserialize)]
pub struct ExportEntryTtDto {
    #[serde(flatten)]
    options: ExportEntryLinkDto,
    append_categories: Option<ParsingCategoriesActionDto>,
    categories_separator: Option<String>,
}

impl Into<TtParsingOptions> for ExportEntryTtDto {
    fn into(self) -> TtParsingOptions {
        let separator = self
            .categories_separator
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .unwrap_or_else(|| "+".to_string());
        let append_categories = match self.append_categories {
            Some(ParsingCategoriesActionDto::BeforeTitle) => {
                Some(ParsingCategoriesAction::BeforeTitle { separator })
            }
            Some(ParsingCategoriesActionDto::AfterTitle) => {
                Some(ParsingCategoriesAction::AfterTitle { separator })
            }
            None => None,
        };
        TtParsingOptions {
            options: self.options.into(),
            append_categories,
        }
    }
}

pub fn deserialize_bool_form<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;

    match s {
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        _ => Err(de::Error::unknown_variant(
            s,
            &["on", "off", "true", "false"],
        )),
    }
}

fn deserialize_publish_form<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct PublishVisitor;

    impl<'de> de::Visitor<'de> for PublishVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string or a list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_string()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                values.push(item);
            }
            Ok(values)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(PublishVisitor)
}

fn bool_true() -> bool {
    true
}

pub fn deserialize_option_bool_form<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;

    match s {
        Some("on" | "true") => Ok(Some(true)),
        Some("off" | "false") => Ok(Some(false)),
        Some(s) => Err(de::Error::unknown_variant(
            s,
            &["on", "off", "true", "false"],
        )),
        None => Ok(None),
    }
}

pub fn deserialize_decimal_form<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;
    s.filter(|s| !s.trim().is_empty())
        .map(Decimal::from_str_exact)
        .transpose()
        .map_err(D::Error::custom)
}

fn deserialize_parse_form<'de, D, RT>(deserializer: D) -> Result<Option<RT>, D::Error>
where
    D: de::Deserializer<'de>,
    RT: FromStr,
    <RT as FromStr>::Err: std::fmt::Display,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer)?;
    s.filter(|s| !s.trim().is_empty())
        .map(FromStr::from_str)
        .transpose()
        .map_err(D::Error::custom)
}

#[post("/shop/{shop_id}/export_info/{export_hash}")]
async fn update_export(
    form: Form<ExportEntryDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let dto = form.into_inner();

    let hash = export_entry
        .map(|export_entry| {
            export_entry.file_name = dto.file_name.clone();
            if let Some(rate) = dto.update_rate {
                export_entry.update_rate = Duration::from_secs(rate * 60 * 60);
            }
        })
        .await?;

    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/copy_export/{export_hash}")]
async fn copy_export(
    path: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { shop, user }: ShopAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let mut export_entry = export_service
        .send(export::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to ExportService")?
        .ok_or(anyhow::anyhow!("Export entry does not exist"))?
        .entry;
    export_entry.created_time = OffsetDateTime::now_utc();
    export_entry.edited_time = OffsetDateTime::now_utc();
    export_entry.file_name = Some(format!("{} (Копия)", export_entry.file_name(None)));
    let subscription = subscription_service
        .send(subscription::service::GetBy(user.clone()))
        .await??;
    let permission = AddExportPermission::acquire(&user, &shop, &subscription)
        .ok_or(anyhow::anyhow!("This shop cannot contain any more exports"))?;
    export_service
        .send(export::Add(permission, export_entry))
        .await
        .context("Unable to send message to ExportService")??;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/{link_hash}")]
async fn update_export_link(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _, link_hash) = path.into_inner();
    let new_link = form.into_inner();
    let (mut export_entry, g) = export_entry.into_inner();
    let link = export_entry
        .entry
        .get_link_by_hash_mut(link_hash)
        .ok_or(anyhow::anyhow!("Export entry link does not exist"))?;
    let description = new_link
        .description_path
        .clone()
        .zip(new_link.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = new_link
        .description_path_ua
        .clone()
        .zip(new_link.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    *link = new_link.try_into()?;
    let hash = g.save(export_entry).await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/new")]
async fn add_export_link(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    export_entry: Record<Export>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let new_link = form.into_inner().try_into()?;

    let hash = export_entry
        .map(|export_entry| {
            if let Some(links) = &mut export_entry.entry.links {
                links.push(new_link);
            } else {
                export_entry.entry.links = Some(vec![new_link]);
            }
        })
        .await?;

    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_dt")]
async fn add_export_dt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.dt_parsing.is_none() {
                export_entry.entry.dt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_op_tuning")]
async fn add_export_op_tuning(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.op_tuning_parsing.is_none() {
                export_entry.entry.op_tuning_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_maxton")]
async fn add_export_maxton(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.maxton_parsing.is_none() {
                export_entry.entry.maxton_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_pl")]
async fn add_export_pl(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.pl_parsing.is_none() {
                export_entry.entry.pl_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_skm")]
async fn add_export_skm(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.skm_parsing.is_none() {
                export_entry.entry.skm_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_dt_tt")]
async fn add_export_dt_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.dt_tt_parsing.is_none() {
                export_entry.entry.dt_tt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_jgd")]
async fn add_export_jgd(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.jgd_parsing.is_none() {
                export_entry.entry.jgd_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_tt")]
async fn add_export_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.tt_parsing.is_none() {
                export_entry.entry.tt_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_davi")]
async fn add_export_davi(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.davi_parsing.is_none() {
                export_entry.entry.davi_parsing = Some(Default::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/add_ddaudio_api")]
async fn add_export_ddaudio_api(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            if export_entry.entry.ddaudio_api.is_none() {
                export_entry.entry.ddaudio_api =
                    Some(rt_types::shop::DDAudioExportOptions::default());
            }
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_dt")]
async fn remove_export_dt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.dt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_op_tuning")]
async fn remove_export_op_tuning(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.op_tuning_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_maxton")]
async fn remove_export_maxton(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.maxton_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_pl")]
async fn remove_export_pl(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.pl_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_skm")]
async fn remove_export_skm(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.skm_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_dt_tt")]
async fn remove_export_dt_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.dt_tt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_jgd")]
async fn remove_export_jgd(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.jgd_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_tt")]
async fn remove_export_tt(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.tt_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_davi")]
async fn remove_export_davi(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.davi_parsing = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/remove_ddaudio_api")]
async fn remove_export_ddaudio_api(
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<Export>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let hash = export_entry
        .map(|export_entry| {
            export_entry.entry.ddaudio_api = None;
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/{link_hash}/remove")]
async fn remove_export_link(
    path: Path<(IdentityOf<Shop>, String, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let (shop_id, hash, link_hash) = path.into_inner();
    let hash = export_entry
        .filter_map(move |entry| {
            let link = entry.remove_link_by_hash(link_hash);
            link.is_some()
        })
        .await
        .transpose()?
        .unwrap_or(hash);
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/dt")]
async fn update_export_dt(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.dt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/op_tuning")]
async fn update_export_op_tuning(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.op_tuning_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/maxton")]
async fn update_export_maxton(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.maxton_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/pl")]
async fn update_export_pl(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.pl_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/skm")]
async fn update_export_skm(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.skm_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/dt_tt")]
async fn update_export_dt_tt(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.dt_tt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/jgd")]
async fn update_export_jgd(
    form: Form<ExportEntryDtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.jgd_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/tt")]
async fn update_export_tt(
    form: Form<ExportEntryTtDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .options
        .description_path
        .clone()
        .zip(opts.options.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .options
        .description_path_ua
        .clone()
        .zip(opts.options.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.tt_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/davi")]
async fn update_export_davi(
    form: Form<ExportEntryLinkDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let opts = form.into_inner();
    let (shop_id, _) = path.into_inner();

    let description = opts
        .description_path
        .clone()
        .zip(opts.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .description_path_ua
        .clone()
        .zip(opts.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let hash = export_entry
        .map(|entry| {
            entry.davi_parsing = Some(opts.into());
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/shop/{shop_id}/export_info/{export_hash}/ddaudio_api")]
async fn update_export_ddaudio_api(
    body: Bytes,
    path: Path<(IdentityOf<Shop>, String)>,
    ShopAccess { .. }: ShopAccess,
    export_entry: Record<ExportEntry>,
) -> Response {
    let (shop_id, _) = path.into_inner();
    let export_entry = export_entry;
    let existing_known_warehouses = export_entry
        .t
        .ddaudio_api
        .as_ref()
        .map(|c| c.known_warehouses.clone())
        .unwrap_or_default();
    let mut params = HashMap::<String, Vec<String>>::new();
    for (key, value) in form_urlencoded::parse(&body) {
        params.entry(key.into_owned()).or_default().push(value.into_owned());
    }
    let opts: ExportEntryLinkDto =
        serde_urlencoded::from_bytes(&body).map_err(anyhow::Error::new)?;

    let description = opts
        .description_path
        .clone()
        .zip(opts.description_action.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description {
        check_description(shop_id, path.value()).await?;
    }
    let description_ua = opts
        .description_path_ua
        .clone()
        .zip(opts.description_action_ua.as_ref())
        .and_then(|(path, action)| DescriptionOptions::try_from(action, path));
    if let Some(path) = &description_ua {
        check_description(shop_id, path.value()).await?;
    }

    let mut config = rt_types::shop::DDAudioExportOptions::default();
    config.options = opts.into();
    config.token = parse_ddaudio_value(&params, "ddaudio_token").unwrap_or_default();
    config.append_attributes = parse_ddaudio_bool(&params, "ddaudio_append_attributes");
    config.known_warehouses = existing_known_warehouses;

    let langs = params.get("ddaudio_lang").cloned().unwrap_or_default();
    if !langs.is_empty() {
        config.languages = langs;
    }

    config.selected_categories = params
        .get("ddaudio_category")
        .cloned()
        .unwrap_or_default();
    config.selected_subcategories = params
        .get("ddaudio_subcategory")
        .cloned()
        .unwrap_or_default();
    config.selected_warehouses = params
        .get("ddaudio_warehouse")
        .cloned()
        .unwrap_or_default();
    config.warehouse_statuses = parse_ddaudio_export_warehouse_statuses(&params);

    config.title_replacements_ru =
        parse_ddaudio_replacements(parse_ddaudio_value(&params, "ddaudio_replacements_ru"));
    config.title_replacements_ua =
        parse_ddaudio_replacements(parse_ddaudio_value(&params, "ddaudio_replacements_ua"));

    let default_rule = {
        let price_type =
            parse_ddaudio_export_price_type(parse_ddaudio_value(&params, "ddaudio_default_price_type"));
        let markup_percent =
            parse_ddaudio_decimal(parse_ddaudio_value(&params, "ddaudio_default_markup"));
        let discount_percent = parse_ddaudio_value(&params, "ddaudio_default_discount")
            .and_then(|v| v.parse::<usize>().ok());
        let discount_hours = parse_ddaudio_value(&params, "ddaudio_default_discount_hours")
            .and_then(|v| v.parse::<u32>().ok());
        let round_to_9 = parse_ddaudio_bool(&params, "ddaudio_default_round_to_9");
        let zero_stock_policy = parse_ddaudio_export_zero_stock_policy(parse_ddaudio_value(
            &params,
            "ddaudio_default_zero_stock",
        ));
        rt_types::shop::DDAudioCategoryRule {
            price_type,
            markup_percent,
            discount_percent,
            discount_hours,
            round_to_9,
            zero_stock_policy,
        }
    };
    config.default_rule = default_rule.clone();

    let mut category_rules = HashMap::new();
    for key in params.keys().filter(|k| k.starts_with("cat_price_type_")) {
        if let Some(id) = key.strip_prefix("cat_price_type_") {
            let rule = parse_ddaudio_export_rule(&params, "cat_", id, &default_rule);
            if !ddaudio_export_rule_matches_default(&rule, &default_rule) {
                category_rules.insert(id.to_string(), rule);
            }
        }
    }
    config.category_rules = category_rules;

    let mut subcategory_rules = HashMap::new();
    for key in params.keys().filter(|k| k.starts_with("sub_price_type_")) {
        if let Some(id) = key.strip_prefix("sub_price_type_") {
            let rule = parse_ddaudio_export_rule(&params, "sub_", id, &default_rule);
            if !ddaudio_export_rule_matches_default(&rule, &default_rule) {
                subcategory_rules.insert(id.to_string(), rule);
            }
        }
    }
    config.subcategory_rules = subcategory_rules;

    if !config.token.trim().is_empty() {
        match ddaudio::fetch_warehouses(config.token.trim()).await {
            Ok(data) => {
                config.known_warehouses = data.data;
                config.known_warehouses.sort();
            }
            Err(err) => {
                log::warn!("Unable to refresh DD Audio warehouses: {err}");
            }
        }
    }

    let hash = export_entry
        .map(|entry| {
            entry.ddaudio_api = Some(config);
        })
        .await?;
    Ok(see_other(&format!("/shop/{shop_id}/export_info/{hash}")))
}

#[post("/stop_dt")]
async fn stop_dt(
    _identity: Identity,
    addr: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    log::info!("Pause");
    addr.send(Pause)
        .await
        .context("Unable to send message to ParserService")?;
    Ok(see_other("/shops"))
}

#[post("/resume_dt")]
async fn resume_dt(
    _identity: Identity,
    addr: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    log::info!("Resume");
    addr.send(Resume)
        .await
        .context("Unable to send message to ParserService")?;
    Ok(see_other("/shops"))
}

#[post("/shop/{shop_id}/start_export/{hash}")]
async fn start_export(
    path: Path<(IdentityOf<Shop>, String)>,
    addr: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    addr.send(export::Start(hash))
        .await
        .context("Unable to send message to ExportService")?;
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[post("/shop/{shop_id}/start_export_all")]
async fn start_export_all(
    path: Path<IdentityOf<Shop>>,
    addr: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    addr.send(export::StartAll)
        .await
        .context("Unable to send message to ExportService")?;
    let shop_id = path.into_inner();
    Ok(see_other(&format!("/shop/{shop_id}")))
}

#[derive(MultipartForm, Debug)]
pub struct DescriptionQuery {
    file: TempFile,
}

#[post("/shop/{shop_id}/upload_description")]
async fn upload_description_file(
    q: MultipartForm<DescriptionQuery>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let name = match q.file.file_name {
        Some(name) => name,
        None => return Ok(see_other("/description?err=empty_filename")),
    };
    let description_count = std::fs::read_dir(format!("./description/{}", shop.id))
        .context("Unable to read description dir")?
        .count();
    if shop
        .limits
        .as_ref()
        .and_then(|l| l.descriptions)
        .is_some_and(|d| description_count >= d.get() as usize)
    {
        return Err(anyhow::anyhow!("Description limit exceeded").into());
    }
    let shop_id = shop.id;
    std::fs::copy(
        q.file.file.path(),
        format!("./description/{shop_id}/{name}"),
    )
    .map_err(anyhow::Error::new)?;
    Ok(see_other(&format!("/shop/{shop_id}/description")))
}

#[post("/shop/{shop_id}/remove_description{path:/.+}")]
async fn remove_description_file(
    path: Path<(IdentityOf<Shop>, String)>,
    export_service: Data<Arc<Addr<export::ExportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, path) = path.into_inner();
    let status = export_service
        .send(export::GetAllStatus(shop_id))
        .await
        .context("Unable to send message to ExportService")?;
    let file_used = status.values().any(|e| {
        e.entry
            .tt_parsing
            .as_ref()
            .and_then(|p| p.options.description.as_ref())
            .is_some_and(|d| d.value() == &path)
            || e.entry
                .dt_parsing
                .as_ref()
                .and_then(|p| p.options.description.as_ref())
                .is_some_and(|d| d.value() == &path)
            || e.entry.links.as_ref().is_some_and(|l| {
                l.iter().any(|l| {
                    l.options
                        .as_ref()
                        .and_then(|o| o.description.as_ref())
                        .is_some_and(|d| d.value() == &path)
                })
            })
    });
    if file_used {
        return Ok(see_other("/shop/{shop_id}/description?err=file_used"));
    }
    let path = match path.ends_with("/shops") {
        true => &path[..path.len() - 1],
        false => &path,
    };
    let path = format!("./description/{shop_id}{}", path);
    std::fs::remove_file(path).context("Unable to remove file")?;
    Ok(see_other(&format!("/shop/{shop_id}/description")))
}

#[derive(Template)]
#[template(path = "categories.html")]
pub struct CategoriesPage {
    pub categories: Vec<Category>,
    pub total_categories: usize,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[derive(Template)]
#[template(path = "site_publish.html")]
pub struct SitePublishPage {
    pub shop: Shop,
    pub user: UserCredentials,
    pub site_imports: Vec<SiteImportView>,
    pub suppliers: Vec<crate::site_publish::XmlSupplier>,
    pub categories: Vec<Category>,
    pub restal_key: Option<String>,
    pub xml_error: Option<String>,
    pub supplier_options: Vec<SupplierOption>,
    pub allow_restal: bool,
    pub ddaudio_config: crate::site_publish::DDAudioConfig,
    pub ddaudio_categories: Vec<DDAudioCategoryView>,
    pub ddaudio_warehouses: Vec<String>,
    pub ddaudio_status: ddaudio_import::ImportState,
    pub ddaudio_error: Option<String>,
    pub ddaudio_site_lang: String,
    pub ddaudio_selected_warehouses: HashSet<String>,
    pub ddaudio_warehouse_statuses: HashMap<String, String>,
    pub ddaudio_warehouse_views: Vec<DDAudioWarehouseView>,
}

#[derive(Clone)]
pub struct DDAudioCategoryView {
    pub id: String,
    pub name: String,
    pub checked: bool,
    pub rule: DDAudioRuleView,
    pub children: Vec<DDAudioCategoryView>,
}

#[derive(Clone)]
pub struct DDAudioWarehouseView {
    pub name: String,
    pub checked: bool,
    pub policy: String,
}

#[derive(Clone)]
pub struct DDAudioRuleView {
    pub price_type: String,
    pub markup_percent: Option<f64>,
    pub discount_percent: Option<usize>,
    pub discount_hours: Option<u32>,
    pub round_to_9: bool,
    pub zero_stock_policy: String,
}

#[derive(Clone)]
pub struct SiteImportView {
    pub hash: String,
    pub name: String,
    pub source: String,
    pub status: String,
    pub progress_percent: Option<u8>,
    pub progress_label: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct SupplierOption {
    pub key: String,
    pub label: String,
    pub checked: bool,
}

#[derive(Template)]
#[template(path = "site_import_info.html")]
struct SiteImportInfoPage {
    shop: Shop,
    user: UserCredentials,
    entry: SiteImportEntry,
    hash: String,
    descriptions: Vec<String>,
    watermarks: Vec<String>,
    groups: Vec<WatermarkGroupDto>,
    supplier_options: Vec<SelectOption>,
    source_kind: String,
    parsing_supplier: String,
    xml_link: String,
    xml_vendor_name: String,
    missing_policy: String,
}

pub struct SupplierFlag {
    pub key: String,
    pub checked: bool,
}

fn parse_ddaudio_replacements(raw: Option<String>) -> Vec<(String, String)> {
    let Some(raw) = raw else { return Vec::new(); };
    raw.lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, '=');
            let from = parts.next().map(str::trim).unwrap_or_default();
            let to = parts.next().map(str::trim).unwrap_or_default();
            if from.is_empty() || to.is_empty() {
                None
            } else {
                Some((from.to_string(), to.to_string()))
            }
        })
        .collect()
}

fn parse_ddaudio_bool(params: &HashMap<String, Vec<String>>, key: &str) -> bool {
    params.get(key).is_some()
}

fn parse_ddaudio_value(params: &HashMap<String, Vec<String>>, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.first()).cloned()
}

fn parse_ddaudio_update_fields(
    params: &HashMap<String, Vec<String>>,
    prefix: &str,
) -> rt_types::shop::SiteImportUpdateFields {
    let has = |suffix: &str| parse_ddaudio_bool(params, &format!("{prefix}{suffix}"));
    rt_types::shop::SiteImportUpdateFields {
        title_ru: has("title_ru"),
        title_ua: has("title_ua"),
        description_ru: has("description_ru"),
        description_ua: has("description_ua"),
        sku: has("sku"),
        price: has("price"),
        images: has("images"),
        availability: has("availability"),
        quantity: has("quantity"),
        attributes: has("attributes"),
        discounts: has("discounts"),
    }
}

fn parse_ddaudio_price_type(raw: Option<String>) -> crate::site_publish::DDAudioPriceType {
    match raw.as_deref() {
        Some("wholesale") => crate::site_publish::DDAudioPriceType::Wholesale,
        _ => crate::site_publish::DDAudioPriceType::Retail,
    }
}

fn parse_zero_stock_policy(
    raw: Option<String>,
) -> crate::site_publish::ZeroStockPolicy {
    match raw.as_deref() {
        Some("on_order") => crate::site_publish::ZeroStockPolicy::OnOrder,
        Some("not_available") => crate::site_publish::ZeroStockPolicy::NotAvailable,
        _ => crate::site_publish::ZeroStockPolicy::Inherit,
    }
}

fn parse_ddaudio_export_price_type(
    raw: Option<String>,
) -> rt_types::shop::DDAudioPriceType {
    match raw.as_deref() {
        Some("wholesale") => rt_types::shop::DDAudioPriceType::Wholesale,
        _ => rt_types::shop::DDAudioPriceType::Retail,
    }
}

fn parse_ddaudio_export_zero_stock_policy(
    raw: Option<String>,
) -> rt_types::shop::ZeroStockPolicy {
    match raw.as_deref() {
        Some("on_order") => rt_types::shop::ZeroStockPolicy::OnOrder,
        Some("not_available") => rt_types::shop::ZeroStockPolicy::NotAvailable,
        _ => rt_types::shop::ZeroStockPolicy::Inherit,
    }
}

fn parse_ddaudio_decimal(raw: Option<String>) -> Option<Decimal> {
    raw.and_then(|v| Decimal::from_str(&v.replace(',', ".")).ok())
}

fn parse_ddaudio_export_rule(
    params: &HashMap<String, Vec<String>>,
    prefix: &str,
    id: &str,
    default_rule: &rt_types::shop::DDAudioCategoryRule,
) -> rt_types::shop::DDAudioCategoryRule {
    let price_type = parse_ddaudio_export_price_type(parse_ddaudio_value(
        params,
        &format!("{prefix}price_type_{id}"),
    ));
    let markup_percent = parse_ddaudio_decimal(parse_ddaudio_value(
        params,
        &format!("{prefix}markup_{id}"),
    ));
    let discount_percent = parse_ddaudio_value(params, &format!("{prefix}discount_{id}"))
        .and_then(|v| v.parse::<usize>().ok());
    let discount_hours = parse_ddaudio_value(params, &format!("{prefix}discount_hours_{id}"))
        .and_then(|v| v.parse::<u32>().ok());
    let round_to_9 = parse_ddaudio_bool(params, &format!("{prefix}round_to_9_{id}"))
        || default_rule.round_to_9;
    let zero_stock_policy = parse_ddaudio_export_zero_stock_policy(parse_ddaudio_value(
        params,
        &format!("{prefix}zero_stock_{id}"),
    ));
    rt_types::shop::DDAudioCategoryRule {
        price_type,
        markup_percent,
        discount_percent,
        discount_hours,
        round_to_9,
        zero_stock_policy,
    }
}

fn ddaudio_decimal_matches_default(value: Option<Decimal>, default_value: Option<Decimal>) -> bool {
    match (value, default_value) {
        (None, None) => true,
        (Some(v), Some(d)) => v == d,
        (None, Some(d)) => d == Decimal::ZERO,
        (Some(v), None) => v == Decimal::ZERO,
    }
}

fn ddaudio_usize_matches_default(value: Option<usize>, default_value: Option<usize>) -> bool {
    match (value, default_value) {
        (None, None) => true,
        (Some(v), Some(d)) => v == d,
        (None, Some(0)) => true,
        (Some(0), None) => true,
        _ => false,
    }
}

fn ddaudio_export_rule_matches_default(
    rule: &rt_types::shop::DDAudioCategoryRule,
    default_rule: &rt_types::shop::DDAudioCategoryRule,
) -> bool {
    rule.price_type == default_rule.price_type
        && ddaudio_decimal_matches_default(rule.markup_percent, default_rule.markup_percent)
        && ddaudio_usize_matches_default(rule.discount_percent, default_rule.discount_percent)
        && rule.discount_hours == default_rule.discount_hours
        && rule.round_to_9 == default_rule.round_to_9
        && rule.zero_stock_policy == default_rule.zero_stock_policy
}

fn parse_missing_policy(
    raw: Option<String>,
) -> rt_types::shop::MissingProductPolicy {
    match raw.as_deref() {
        Some("not_available") => rt_types::shop::MissingProductPolicy::NotAvailable,
        Some("hidden") => rt_types::shop::MissingProductPolicy::Hidden,
        Some("deleted") => rt_types::shop::MissingProductPolicy::Deleted,
        _ => rt_types::shop::MissingProductPolicy::Keep,
    }
}

fn parse_ddaudio_rule(
    params: &HashMap<String, Vec<String>>,
    prefix: &str,
    id: &str,
    default_rule: &crate::site_publish::DDAudioCategoryRule,
) -> crate::site_publish::DDAudioCategoryRule {
    let price_type =
        parse_ddaudio_price_type(parse_ddaudio_value(params, &format!("{prefix}price_type_{id}")));
    let markup_percent = parse_ddaudio_value(params, &format!("{prefix}markup_{id}"))
        .and_then(|v| v.replace(',', ".").parse::<f64>().ok());
    let discount_percent = parse_ddaudio_value(params, &format!("{prefix}discount_{id}"))
        .and_then(|v| v.parse::<usize>().ok());
    let discount_hours = parse_ddaudio_value(params, &format!("{prefix}discount_hours_{id}"))
        .and_then(|v| v.parse::<u32>().ok());
    let round_to_9 = parse_ddaudio_bool(params, &format!("{prefix}round_to_9_{id}"))
        || default_rule.round_to_9;
    let zero_stock_policy = parse_zero_stock_policy(parse_ddaudio_value(
        params,
        &format!("{prefix}zero_stock_{id}"),
    ));
    crate::site_publish::DDAudioCategoryRule {
        price_type,
        markup_percent,
        discount_percent,
        discount_hours,
        round_to_9,
        zero_stock_policy,
    }
}

fn ddaudio_f64_matches_default(value: Option<f64>, default_value: Option<f64>) -> bool {
    match (value, default_value) {
        (None, None) => true,
        (Some(v), Some(d)) => (v - d).abs() < f64::EPSILON,
        (None, Some(d)) => d.abs() < f64::EPSILON,
        (Some(v), None) => v.abs() < f64::EPSILON,
    }
}

fn ddaudio_site_rule_matches_default(
    rule: &crate::site_publish::DDAudioCategoryRule,
    default_rule: &crate::site_publish::DDAudioCategoryRule,
) -> bool {
    rule.price_type == default_rule.price_type
        && ddaudio_f64_matches_default(rule.markup_percent, default_rule.markup_percent)
        && ddaudio_usize_matches_default(rule.discount_percent, default_rule.discount_percent)
        && rule.discount_hours == default_rule.discount_hours
        && rule.round_to_9 == default_rule.round_to_9
        && rule.zero_stock_policy == default_rule.zero_stock_policy
}

fn parse_ddaudio_warehouse_statuses(
    params: &HashMap<String, Vec<String>>,
) -> HashMap<String, site_publish::ZeroStockPolicy> {
    let mut out = HashMap::new();
    let names = params.get("ddaudio_warehouse_name").cloned().unwrap_or_default();
    let policies = params
        .get("ddaudio_warehouse_policy")
        .cloned()
        .unwrap_or_default();
    for (idx, name) in names.iter().enumerate() {
        let policy_raw = policies
            .get(idx)
            .cloned()
            .or_else(|| policies.first().cloned())
            .unwrap_or_else(|| "inherit".to_string());
        out.insert(name.clone(), parse_zero_stock_policy(Some(policy_raw)));
    }
    out
}

fn parse_ddaudio_export_warehouse_statuses(
    params: &HashMap<String, Vec<String>>,
) -> HashMap<String, rt_types::shop::ZeroStockPolicy> {
    let mut out = HashMap::new();
    let names = params.get("ddaudio_warehouse_name").cloned().unwrap_or_default();
    let policies = params
        .get("ddaudio_warehouse_policy")
        .cloned()
        .unwrap_or_default();
    for (idx, name) in names.iter().enumerate() {
        let policy_raw = policies
            .get(idx)
            .cloned()
            .or_else(|| policies.first().cloned())
            .unwrap_or_else(|| "inherit".to_string());
        out.insert(name.clone(), parse_ddaudio_export_zero_stock_policy(Some(policy_raw)));
    }
    out
}

#[derive(Deserialize, Debug)]
pub struct SitePublishForm {
    #[serde(default)]
    pub suppliers: Vec<String>,
    #[serde(default)]
    pub xml_link: Option<String>,
    #[serde(default)]
    pub restal_key: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PurgeSupplierForm {
    pub supplier: String,
}

#[get("/shop/{shop_id}/categories")]
async fn categories_page(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let categories = category_repo.select(&TopLevel(By(shop.id))).await?;
    let total_categories = category_repo.count_by(&By(shop.id)).await?;
    let categories = categories
        .into_iter()
        .filter(|c| c.parent_id.is_none())
        .collect();
    render_template(CategoriesPage {
        categories,
        total_categories,
        shop,
        user,
    })
}

#[get("/shop/{shop_id}/site_publish")]
async fn site_publish_page(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let suppliers = site_publish::load_site_publish_configs(&shop.id).unwrap_or_default();
    let restal_key = site_publish::load_restal_key(&shop.id);
    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let allow_restal = allowed_suppliers.contains(&"restal".to_string());
    let ddaudio_config = site_publish::load_ddaudio_config(&shop.id);
    let categories = category_repo
        .select(&TopLevel(By(shop.id)))
        .await?
        .into_iter()
        .filter(|c| c.parent_id.is_none())
        .collect();
    let site_imports = site_import_service
        .send(site_import::GetAllStatus(shop.id))
        .await
        .context("Unable to get site import status")?
        .into_iter()
        .map(|(hash, import)| {
            let (status, error) = match &import.status {
                site_import::SiteImportStatus::Failure(msg) => {
                    ("Помилка".to_string(), Some(msg.clone()))
                }
                other => (other.to_string(), None),
            };
            let (progress_percent, progress_label) = import
                .progress
                .as_ref()
                .and_then(|p| {
                    if p.total == 0 {
                        return None;
                    }
                    let percent = (p.done.saturating_mul(100) / p.total).min(100) as u8;
                    Some((percent, format!("{} ({} / {})", p.stage, p.done, p.total)))
                })
                .unwrap_or((0, String::new()));
            let progress_percent = if progress_label.is_empty() {
                None
            } else {
                Some(progress_percent)
            };
            let source = match &import.entry.source {
                rt_types::shop::SiteImportSource::Parsing { supplier } => {
                    format!("Парсинг: {}", supplier)
                }
                rt_types::shop::SiteImportSource::Xml { link, vendor_name } => {
                    vendor_name
                        .as_ref()
                        .filter(|v| !v.trim().is_empty())
                        .map(|v| format!("XML: {v}"))
                        .unwrap_or_else(|| format!("XML: {}", link))
                }
                rt_types::shop::SiteImportSource::RestalApi => "RESTAL API".to_string(),
            };
            let name = import
                .entry
                .name
                .clone()
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| source.clone());
            SiteImportView {
                hash,
                name,
                source,
                status,
                progress_percent,
                progress_label: if progress_label.is_empty() {
                    None
                } else {
                    Some(progress_label)
                },
                error,
            }
        })
        .collect::<Vec<_>>();

    let allowed_set = allowed_suppliers
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect::<HashSet<_>>();
    let mut supplier_options = collect_supplier_labels(&shop, &allowed_suppliers)
        .into_iter()
        .map(|(key, label)| SupplierOption {
            checked: allowed_set.contains(&key),
            key,
            label,
        })
        .collect::<Vec<_>>();
    supplier_options.sort_by_key(|o| o.label.to_lowercase());

    let mut ddaudio_categories = Vec::new();
    let mut ddaudio_warehouses = Vec::new();
    let mut ddaudio_error = None;
    let rule_to_view = |rule: &site_publish::DDAudioCategoryRule| DDAudioRuleView {
        price_type: match rule.price_type {
            site_publish::DDAudioPriceType::Wholesale => "wholesale".to_string(),
            site_publish::DDAudioPriceType::Retail => "retail".to_string(),
        },
        markup_percent: rule.markup_percent,
        discount_percent: rule.discount_percent,
        discount_hours: rule.discount_hours,
        round_to_9: rule.round_to_9,
        zero_stock_policy: match rule.zero_stock_policy {
            site_publish::ZeroStockPolicy::OnOrder => "on_order".to_string(),
            site_publish::ZeroStockPolicy::NotAvailable => "not_available".to_string(),
            site_publish::ZeroStockPolicy::Inherit => "inherit".to_string(),
        },
    };
    if !ddaudio_config.token.trim().is_empty() {
        let has_cat_selection = !ddaudio_config.selected_categories.is_empty();
        let has_sub_selection = !ddaudio_config.selected_subcategories.is_empty();
        let default_selected = !has_cat_selection && !has_sub_selection;
        match ddaudio::fetch_categories(
            ddaudio_config.token.trim(),
            ddaudio_config
                .site
                .languages
                .first()
                .map(|s| s.as_str()),
        )
        .await
        {
            Ok(data) => {
                for (id, node) in data.data {
                    let mut children = Vec::new();
                    let parent_selected = default_selected
                        || ddaudio_config.selected_categories.iter().any(|v| v == &id);
                    for (child_id, child_name) in node.children {
                        let child_checked = default_selected
                            || (!has_sub_selection && parent_selected)
                            || ddaudio_config.selected_subcategories.iter().any(|v| v == &child_id);
                        let child_rule = ddaudio_config
                            .subcategory_rules
                            .get(&child_id)
                            .unwrap_or(&ddaudio_config.default_rule);
                        children.push(DDAudioCategoryView {
                            id: child_id,
                            name: child_name,
                            checked: child_checked,
                            rule: rule_to_view(child_rule),
                            children: Vec::new(),
                        });
                    }
                    let checked = parent_selected || children.iter().any(|c| c.checked);
                    let rule = ddaudio_config
                        .category_rules
                        .get(&id)
                        .unwrap_or(&ddaudio_config.default_rule);
                    ddaudio_categories.push(DDAudioCategoryView {
                        id,
                        name: node.title,
                        checked,
                        rule: rule_to_view(rule),
                        children,
                    });
                }
                ddaudio_categories.sort_by_key(|c| c.name.to_lowercase());
            }
            Err(err) => {
                ddaudio_error = Some(format!("DD Audio categories: {err}"));
            }
        }
        match ddaudio::fetch_warehouses(ddaudio_config.token.trim()).await {
            Ok(data) => {
                ddaudio_warehouses = data.data;
                ddaudio_warehouses.sort();
            }
            Err(err) => {
                ddaudio_error = Some(format!(
                    "{}{}DD Audio warehouses: {err}",
                    ddaudio_error.clone().unwrap_or_default(),
                    if ddaudio_error.is_some() { "; " } else { "" }
                ));
            }
        }
        if ddaudio_warehouses.is_empty() && !ddaudio_config.known_warehouses.is_empty() {
            ddaudio_warehouses = ddaudio_config.known_warehouses.clone();
            ddaudio_warehouses.sort();
        }
    }
    let ddaudio_status = ddaudio_import::get_status(shop.id).await;
    let ddaudio_site_lang = ddaudio_config
        .site
        .languages
        .first()
        .cloned()
        .unwrap_or_else(|| "ua".to_string());
    let ddaudio_selected_warehouses = ddaudio_config
        .selected_warehouses
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let ddaudio_warehouse_statuses = ddaudio_config
        .warehouse_statuses
        .iter()
        .map(|(key, policy)| {
            let value = match policy {
                site_publish::ZeroStockPolicy::OnOrder => "on_order",
                site_publish::ZeroStockPolicy::NotAvailable => "not_available",
                site_publish::ZeroStockPolicy::Inherit => "inherit",
            };
            (key.clone(), value.to_string())
        })
        .collect::<HashMap<_, _>>();
    let ddaudio_warehouse_views = ddaudio_warehouses
        .iter()
        .map(|name| DDAudioWarehouseView {
            name: name.clone(),
            checked: ddaudio_selected_warehouses.contains(name.as_str()),
            policy: ddaudio_warehouse_statuses
                .get(name)
                .cloned()
                .unwrap_or_else(|| "inherit".to_string()),
        })
        .collect::<Vec<_>>();

    render_template(SitePublishPage {
        shop,
        user,
        site_imports,
        suppliers,
        categories,
        restal_key,
        xml_error: None,
        supplier_options,
        allow_restal,
        ddaudio_config,
        ddaudio_categories,
        ddaudio_warehouses,
        ddaudio_status,
        ddaudio_error,
        ddaudio_site_lang,
        ddaudio_selected_warehouses,
        ddaudio_warehouse_statuses,
        ddaudio_warehouse_views,
    })
}

#[post("/shop/{shop_id}/site_publish")]
async fn site_publish_save(
    ShopAccess { shop, .. }: ShopAccess,
    Form(form): Form<SitePublishForm>,
) -> Response {
    if !form.suppliers.is_empty() {
        site_publish::save_site_publish_suppliers(&shop.id, form.suppliers.clone())
            .map_err(ControllerError::InternalServerError)?;
    }
    if let Some(link) = form
        .xml_link
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        site_publish::upsert_site_supplier(
            &shop.id,
            link.to_string(),
            site_publish::ExportConfig::default(),
        )
        .map_err(ControllerError::InternalServerError)?;
    }
    if let Some(key) = form
        .restal_key
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        site_publish::save_restal_key(&shop.id, key)
            .map_err(ControllerError::InternalServerError)?;
    }
    Ok(see_other(&format!("/shop/{}/site_publish", shop.id)))
}

#[post("/shop/{shop_id}/site_publish/ddaudio")]
async fn site_publish_ddaudio_save(
    ShopAccess { shop, .. }: ShopAccess,
    body: Bytes,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let mut params = HashMap::<String, Vec<String>>::new();
    for (key, value) in form_urlencoded::parse(&body) {
        params.entry(key.into_owned()).or_default().push(value.into_owned());
    }

    let mut config = site_publish::load_ddaudio_config(&shop.id);
    config.token = parse_ddaudio_value(&params, "ddaudio_token").unwrap_or_default();
    config.append_images = parse_ddaudio_bool(&params, "ddaudio_append_images");
    config.append_attributes = parse_ddaudio_bool(&params, "ddaudio_append_attributes");
    config.auto_update = parse_ddaudio_bool(&params, "ddaudio_auto_update");
    config.auto_target = site_publish::DDAudioTarget::Site;
    if let Some(hours) = parse_ddaudio_value(&params, "ddaudio_update_rate_hours")
        .and_then(|v| v.parse::<u64>().ok())
    {
        config.update_rate = std::time::Duration::from_secs(hours.max(1) * 60 * 60);
    }
    let site_lang = parse_ddaudio_value(&params, "ddaudio_site_lang").unwrap_or_else(|| "ua".to_string());
    config.site.languages = vec![site_lang];
    config.site.update_fields = parse_ddaudio_update_fields(&params, "site_update_");
    config.site.missing_policy =
        parse_missing_policy(parse_ddaudio_value(&params, "site_missing_policy"));

    config.selected_categories = params
        .get("ddaudio_category")
        .cloned()
        .unwrap_or_default();
    config.selected_subcategories = params
        .get("ddaudio_subcategory")
        .cloned()
        .unwrap_or_default();
    config.selected_warehouses = params
        .get("ddaudio_warehouse")
        .cloned()
        .unwrap_or_default();
    config.warehouse_statuses = parse_ddaudio_warehouse_statuses(&params);

    config.title_replacements_ru =
        parse_ddaudio_replacements(parse_ddaudio_value(&params, "ddaudio_replacements_ru"));
    config.title_replacements_ua =
        parse_ddaudio_replacements(parse_ddaudio_value(&params, "ddaudio_replacements_ua"));

    let default_rule = {
        let price_type = parse_ddaudio_price_type(parse_ddaudio_value(&params, "ddaudio_default_price_type"));
        let markup_percent = parse_ddaudio_value(&params, "ddaudio_default_markup")
            .and_then(|v| v.replace(',', ".").parse::<f64>().ok());
        let discount_percent = parse_ddaudio_value(&params, "ddaudio_default_discount")
            .and_then(|v| v.parse::<usize>().ok());
        let discount_hours = parse_ddaudio_value(&params, "ddaudio_default_discount_hours")
            .and_then(|v| v.parse::<u32>().ok());
        let round_to_9 = parse_ddaudio_bool(&params, "ddaudio_default_round_to_9");
        let zero_stock_policy = parse_zero_stock_policy(parse_ddaudio_value(&params, "ddaudio_default_zero_stock"));
        site_publish::DDAudioCategoryRule {
            price_type,
            markup_percent,
            discount_percent,
            discount_hours,
            round_to_9,
            zero_stock_policy,
        }
    };
    config.default_rule = default_rule.clone();

    let mut category_rules = HashMap::new();
    for key in params.keys().filter(|k| k.starts_with("cat_price_type_")) {
        if let Some(id) = key.strip_prefix("cat_price_type_") {
            let rule = parse_ddaudio_rule(&params, "cat_", id, &default_rule);
            if !ddaudio_site_rule_matches_default(&rule, &default_rule) {
                category_rules.insert(id.to_string(), rule);
            }
        }
    }
    config.category_rules = category_rules;

    let mut subcategory_rules = HashMap::new();
    for key in params.keys().filter(|k| k.starts_with("sub_price_type_")) {
        if let Some(id) = key.strip_prefix("sub_price_type_") {
            let rule = parse_ddaudio_rule(&params, "sub_", id, &default_rule);
            if !ddaudio_site_rule_matches_default(&rule, &default_rule) {
                subcategory_rules.insert(id.to_string(), rule);
            }
        }
    }
    config.subcategory_rules = subcategory_rules;

    site_publish::save_ddaudio_config(&shop.id, &config)
        .map_err(ControllerError::InternalServerError)?;
    ddaudio_import::sync_scheduler(
        shop.id,
        dt_repo.get_ref().clone(),
        shop_product_repo.get_ref().clone(),
        category_repo.get_ref().clone(),
        product_category_repo.get_ref().clone(),
    )
    .await;
    Ok(see_other(&format!("/shop/{}/site_publish", shop.id)))
}

#[derive(Deserialize)]
pub struct DDAudioImportParams {
    pub target: Option<String>,
}

#[post("/shop/{shop_id}/api/site_publish/ddaudio/import")]
async fn ddaudio_import_start(
    ShopAccess { shop, .. }: ShopAccess,
    params: Query<DDAudioImportParams>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    category_repo: Data<Arc<dyn CategoryRepository>>,
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
) -> Response {
    let _ = params;
    let target = site_publish::DDAudioTarget::Site;
    ddaudio_import::trigger_import(
        shop.id,
        target,
        dt_repo.get_ref().clone(),
        shop_product_repo.get_ref().clone(),
        category_repo.get_ref().clone(),
        product_category_repo.get_ref().clone(),
        true,
    )
    .await
    .map_err(ControllerError::InternalServerError)?;
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "DD Audio імпорт запущено",
        "target": format!("{target:?}").to_lowercase(),
    })))
}

#[get("/shop/{shop_id}/api/site_publish/ddaudio/status")]
async fn ddaudio_import_status(ShopAccess { shop, .. }: ShopAccess) -> Response {
    let status = ddaudio_import::get_status(shop.id).await;
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": status.status.to_string(),
        "progress": status.progress,
        "last_error": status.last_error,
        "last_log": status.last_log,
        "last_started": status.last_started,
        "last_finished": status.last_finished,
        "target": status.target.map(|t| format!("{t:?}").to_lowercase()),
    })))
}

fn default_shop_product_record(shop: &Shop, article: &str, now: OffsetDateTime) -> shop_product::ShopProduct {
    shop_product::ShopProduct {
        shop_id: shop.id,
        article: article.to_string(),
        internal_product_id: uuid::Uuid::new_v4(),
        title: None,
        description: None,
        price: None,
        images: None,
        available: None,
        site_category_id: None,
        recommend_mode: shop_product::RecommendMode::Auto,
        recommended_articles: Vec::new(),
        is_hit: false,
        source_type: shop_product::SourceType::Manual,
        visibility_on_site: shop_product::Visibility::Hidden,
        indexing_status: shop_product::IndexingStatus::NoIndex,
        status: shop_product::ProductStatus::Draft,
        seo_score: 0,
        h1: None,
        seo_text: None,
        canonical: None,
        robots: None,
        og_title: None,
        og_description: None,
        og_image: None,
        slug: None,
        faq: None,
        created_at: now,
        updated_at: now,
    }
}

#[post("/shop/{shop_id}/site_publish/allowed")]
async fn site_publish_allowed(
    ShopAccess { shop, .. }: ShopAccess,
    body: Bytes,
) -> Response {
    let suppliers = form_urlencoded::parse(&body)
        .filter(|(key, _)| key == "suppliers")
        .map(|(_, value)| value.into_owned())
        .collect::<Vec<_>>();
    site_publish::save_site_publish_suppliers(&shop.id, suppliers)
        .map_err(ControllerError::InternalServerError)?;
    Ok(see_other(&format!("/shop/{}/site_publish", shop.id)))
}

#[post("/shop/{shop_id}/site_publish/purge_supplier")]
async fn site_publish_purge_supplier(
    ShopAccess { shop, .. }: ShopAccess,
    Form(form): Form<PurgeSupplierForm>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
) -> Response {
    let target = form.supplier.trim().to_lowercase();
    if target.is_empty() {
        return Ok(see_other(&format!("/shop/{}/site_publish", shop.id)));
    }
    let products = dt_repo.list().await.unwrap_or_default();
    let mut purged = 0usize;
    let mut articles: Vec<String> = Vec::new();
    let now = OffsetDateTime::now_utc();
    for p in products.into_iter() {
        let supplier = site_publish::detect_supplier(&p).unwrap_or_default();
        if supplier != target {
            continue;
        }
        articles.push(p.article.clone());
        let existing = shop_product_repo
            .get(shop.id, &p.article)
            .await
            .unwrap_or(None);
        let mut record = existing.unwrap_or_else(|| default_shop_product_record(&shop, &p.article, now));
        record.visibility_on_site = shop_product::Visibility::Hidden;
        record.status = shop_product::ProductStatus::Draft;
        record.indexing_status = shop_product::IndexingStatus::NoIndex;
        record.updated_at = now;
        record.source_type = shop_product::SourceType::Manual;
        let _ = shop_product_repo.upsert(record).await;
        purged += 1;
    }
    if !articles.is_empty() {
        let _ = dt_repo.delete_articles(&articles).await;
        let _ = shop_product_repo.remove_many(shop.id, &articles).await;
    }
    Ok(see_other(&format!(
        "/shop/{}/site_publish?purged={}",
        shop.id, purged
    )))
}

fn supplier_label(key: &str) -> String {
    match key {
        "dt" => "DT / Design-tuning",
        "maxton" => "Maxton",
        "jgd" => "JGD",
        "skm" => "SKM",
        "tt" => "TT",
        "restal" => "RESTAL",
        "restal_xml" => "RESTAL XML",
        "op_tuning" => "O&P Tuning",
        other => other,
    }
    .to_string()
}

fn build_supplier_options() -> Vec<SelectOption> {
    let mut options = site_publish::list_suppliers()
        .into_iter()
        .map(|key| SelectOption {
            value: key.clone(),
            label: supplier_label(&key),
        })
        .collect::<Vec<_>>();
    options.sort_by_key(|o| o.label.to_lowercase());
    options
}

fn supplier_label_from_entry(entry: &SiteImportEntry) -> Option<String> {
    if let Some(name) = entry
        .name
        .as_ref()
        .map(|n| n.trim())
        .filter(|n| !n.is_empty())
    {
        return Some(name.to_string());
    }
    match &entry.source {
        rt_types::shop::SiteImportSource::Parsing { supplier } => Some(supplier_label(supplier)),
        rt_types::shop::SiteImportSource::Xml { vendor_name, link } => vendor_name
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .or_else(|| crate::parse_vendor_from_link(link)),
        rt_types::shop::SiteImportSource::RestalApi => Some("RESTAL".to_string()),
    }
}

fn collect_supplier_labels(
    shop: &Shop,
    allowed_suppliers: &[String],
) -> HashMap<String, String> {
    let mut labels = HashMap::<String, String>::new();
    for key in site_publish::list_suppliers() {
        labels.insert(key.clone(), supplier_label(&key));
    }
    for supplier in site_publish::load_known_suppliers(&shop.id) {
        labels.insert(supplier.key, supplier.label);
    }
    for entry in &shop.site_import_entries {
        if let Some(key) = entry.supplier_key() {
            let label = supplier_label_from_entry(entry).unwrap_or_else(|| key.clone());
            labels.entry(key).or_insert(label);
        }
    }
    for key in allowed_suppliers {
        let key = key.trim().to_lowercase();
        if key.is_empty() {
            continue;
        }
        labels.entry(key.clone()).or_insert_with(|| key.clone());
    }
    labels
}

#[post("/shop/{shop_id}/site_publish/import")]
async fn site_import_add(
    ShopAccess { shop, .. }: ShopAccess,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
) -> Response {
    let hash = site_import_service
        .send(site_import::Add(shop.id, SiteImportEntry::default()))
        .await
        .context("Unable to send message to SiteImportService")??;
    Ok(see_other(&format!(
        "/shop/{}/site_publish/import/{}",
        shop.id, hash
    )))
}

#[get("/shop/{shop_id}/site_publish/import/{hash}")]
async fn site_import_info(
    path: Path<(IdentityOf<Shop>, String)>,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
    watermark_group_repository: Data<Arc<dyn WatermarkGroupRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let import = site_import_service
        .send(site_import::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to SiteImportService")?
        .ok_or(ControllerError::NotFound)?;
    if import.shop != shop.id {
        return Ok(see_other(&format!("/shop/{shop_id}/site_publish")));
    }

    let groups = watermark_group_repository.list_by(&shop_id).await?;
    let path = format!("./description/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path).context("Unable to create dir")?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm).context("Unable to set dir permissions")?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let descriptions = res
        .context("Unable to read descriptions")?
        .map(|d| {
            d?.file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)?;
    let path = format!("./watermark/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path)
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm)
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let watermarks: Vec<_> = res
        .map_err(ShopControllerError::with(&user, &shop))?
        .map(|e| {
            let e = e?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(file_name)
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;

    let entry = import.entry;
    let (source_kind, parsing_supplier, xml_link, xml_vendor_name) = match &entry.source {
        rt_types::shop::SiteImportSource::Parsing { supplier } => (
            "parsing".to_string(),
            supplier.clone(),
            String::new(),
            String::new(),
        ),
        rt_types::shop::SiteImportSource::Xml { link, vendor_name } => (
            "xml".to_string(),
            String::new(),
            link.clone(),
            vendor_name.clone().unwrap_or_default(),
        ),
        rt_types::shop::SiteImportSource::RestalApi => (
            "restal".to_string(),
            String::new(),
            String::new(),
            String::new(),
        ),
    };
    let missing_policy = match entry.options.missing_policy {
        rt_types::shop::MissingProductPolicy::Keep => "keep",
        rt_types::shop::MissingProductPolicy::NotAvailable => "not_available",
        rt_types::shop::MissingProductPolicy::Hidden => "hidden",
        rt_types::shop::MissingProductPolicy::Deleted => "deleted",
    }
    .to_string();

    render_template(SiteImportInfoPage {
        shop,
        user,
        entry,
        hash,
        descriptions,
        watermarks,
        groups: groups.into_iter().map(Into::into).collect(),
        supplier_options: build_supplier_options(),
        source_kind,
        parsing_supplier,
        xml_link,
        xml_vendor_name,
        missing_policy,
    })
}

#[post("/shop/{shop_id}/site_publish/import/{hash}")]
async fn site_import_update(
    form: Form<SiteImportEntryDto>,
    path: Path<(IdentityOf<Shop>, String)>,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    let import = site_import_service
        .send(site_import::GetStatus(hash.clone()))
        .await
        .context("Unable to send message to SiteImportService")?
        .ok_or(ControllerError::NotFound)?;
    if import.shop != shop.id {
        return Ok(see_other(&format!("/shop/{shop_id}/site_publish")));
    }
    let SiteImportEntryDto {
        name,
        update_rate,
        source_kind,
        parsing_supplier,
        xml_link,
        xml_vendor_name,
        missing_policy,
        image_strategy,
        append_images,
        round_to_9,
        update_title_ru,
        update_title_ua,
        update_description_ru,
        update_description_ua,
        update_sku,
        update_price,
        update_images,
        update_availability,
        update_quantity,
        update_attributes,
        update_discounts,
        transform,
    } = form.into_inner();

    let mut entry = import.entry;
    entry.name = name.filter(|s| !s.trim().is_empty());
    if let Some(rate) = update_rate {
        entry.update_rate = Duration::from_secs(rate * 60 * 60);
    }
    let source_kind = source_kind.unwrap_or_else(|| "xml".to_string());
    entry.source = match source_kind.as_str() {
        "parsing" => {
            let supplier = parsing_supplier
                .unwrap_or_default()
                .trim()
                .to_string();
            if supplier.is_empty() {
                return Err(anyhow!("Потрібно вибрати постачальника для парсингу").into());
            }
            rt_types::shop::SiteImportSource::Parsing { supplier }
        }
        "restal" => rt_types::shop::SiteImportSource::RestalApi,
        _ => {
            let link = xml_link.unwrap_or_default().trim().to_string();
            if link.is_empty() {
                return Err(anyhow!("Потрібно вказати XML посилання").into());
            }
            let vendor_name = xml_vendor_name
                .filter(|v| !v.trim().is_empty())
                .map(|v| v.trim().to_string());
            rt_types::shop::SiteImportSource::Xml { link, vendor_name }
        }
    };

    let transform: ExportOptions = transform.into();
    if let Some(desc) = transform.description.as_ref() {
        check_description(shop_id, desc.value()).await?;
    }
    if let Some(desc) = transform.description_ua.as_ref() {
        check_description(shop_id, desc.value()).await?;
    }
    let update_fields = rt_types::shop::SiteImportUpdateFields {
        title_ru: update_title_ru,
        title_ua: update_title_ua,
        description_ru: update_description_ru,
        description_ua: update_description_ua,
        sku: update_sku,
        price: update_price,
        images: update_images,
        availability: update_availability,
        quantity: update_quantity,
        attributes: update_attributes,
        discounts: update_discounts,
    };
    let missing_policy = match missing_policy.as_deref().unwrap_or("keep") {
        "not_available" => rt_types::shop::MissingProductPolicy::NotAvailable,
        "hidden" => rt_types::shop::MissingProductPolicy::Hidden,
        "deleted" => rt_types::shop::MissingProductPolicy::Deleted,
        _ => rt_types::shop::MissingProductPolicy::Keep,
    };
    let append_images = match image_strategy.as_deref() {
        Some("append") => true,
        Some("replace") => false,
        _ => append_images,
    };

    entry.options = rt_types::shop::SiteImportOptions {
        update_fields,
        missing_policy,
        append_images,
        round_to_9,
        transform,
    };

    if let Some(key) = entry.supplier_key() {
        let label = supplier_label_from_entry(&entry).unwrap_or_else(|| key.clone());
        site_publish::upsert_known_supplier(&shop.id, &key, &label)
            .map_err(ControllerError::InternalServerError)?;
    }

    let new_hash = site_import_service
        .send(site_import::Update(shop.id, hash, entry))
        .await
        .context("Unable to send message to SiteImportService")??;
    Ok(see_other(&format!(
        "/shop/{}/site_publish/import/{}",
        shop.id, new_hash
    )))
}

#[post("/shop/{shop_id}/site_publish/import/{hash}/start")]
async fn site_import_start(
    path: Path<(IdentityOf<Shop>, String)>,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    site_import_service
        .send(site_import::Start(hash))
        .await
        .context("Unable to send message to SiteImportService")?;
    Ok(see_other(&format!("/shop/{shop_id}/site_publish")))
}

#[post("/shop/{shop_id}/site_publish/import/{hash}/remove")]
async fn site_import_remove(
    path: Path<(IdentityOf<Shop>, String)>,
    site_import_service: Data<Arc<Addr<site_import::SiteImportService>>>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, hash) = path.into_inner();
    site_import_service
        .send(site_import::Remove(hash))
        .await
        .context("Unable to send message to SiteImportService")??;
    Ok(see_other(&format!("/shop/{shop_id}/site_publish")))
}

#[derive(Template)]
#[template(path = "category.html")]
pub struct CategoryPage {
    pub category: Category,
    pub subcategories: Vec<Category>,
    pub categories: Vec<Category>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/categories/{id}")]
async fn category_page(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (_, category_id) = path.into_inner();
    let category = category_repo
        .get_one(&category_id)
        .await?
        .ok_or(ControllerError::NotFound)?;
    let subcategories = category_repo.select(&ByParentId(category_id)).await?;
    let categories = category_repo.select(&By(shop.id)).await?;
    render_template(CategoryPage {
        category,
        subcategories,
        categories,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "import_categories.html")]
pub struct ImportCategoriesPage {
    shop: Shop,
    user: UserCredentials,
}

#[get("/shop/{shop_id}/categories/import")]
async fn import_categories_page(ShopAccess { shop, user }: ShopAccess) -> Response {
    render_template(ImportCategoriesPage { shop, user })
}

#[derive(MultipartForm, Debug)]
pub struct ImportCategoriesQuery {
    file: TempFile,
}

#[post("/shop/{shop_id}/categories/import")]
async fn import_categories(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: MultipartForm<ImportCategoriesQuery>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let file = q.file.file.as_file();
    let categories = parse_categories(BufReader::new(file), shop.id)?;
    for c in categories {
        category_repo.save(c).await?;
    }
    Ok(see_other(&format!("/shop/{}/categories", shop.id)))
}

#[post("/shop/{shop_id}/categories/clear")]
async fn clear_categories(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let categories = category_repo.select(&By(shop.id)).await?;
    for c in categories {
        category_repo.remove(&c.id).await?;
    }
    Ok(see_other(&format!("/shop/{}/categories", shop.id)))
}

#[derive(Deserialize, Debug)]
pub struct CategoryDto {
    pub name: String,
    pub regex: String,
    #[serde(default)]
    pub parent_id: Option<String>,
}

#[post("/shop/{shop_id}/categories/update/{id}")]
async fn update_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: Form<CategoryDto>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let (shop_id, id) = path.into_inner();
    let regex = Regex::new(&q.regex).context("Unable to compile regex")?;
    let mut category = category_repo.get_one(&id).await?;
    if let Some(category) = &mut category {
        category.regex = Some(regex);
        category.name = q.name;
        category.parent_id = q
            .parent_id
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?;
        category_repo.save(category.clone()).await?;
    }
    if let Some(id) = category.and_then(|c| c.parent_id) {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    }
}

#[post("/shop/{shop_id}/categories/add")]
async fn add_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    q: Form<CategoryDto>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let CategoryDto {
        name,
        regex,
        parent_id,
    } = q.into_inner();
    let id = Uuid::new_v4();
    let regex = Regex::new(&regex).context("Unable to compile regex")?;
    let category = Category {
        id: id,
        name,
        parent_id: parent_id
            .clone()
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?,
        regex: Some(regex),
        shop_id,
        seo_title: None,
        seo_description: None,
        seo_text: None,
    };
    category_repo.save(category).await?;
    if let Some(id) = parent_id {
        Ok(see_other(&format!("/shop/{shop_id}/categories/{id}")))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories")))
    }
}

#[post("/shop/{shop_id}/category/delete/{id}")]
async fn delete_category(
    category_repo: Data<Arc<dyn CategoryRepository>>,
    path: Path<(IdentityOf<Shop>, IdentityOf<Category>)>,
    req: HttpRequest,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, id) = path.into_inner();
    category_repo.remove(&id).await?;
    if let Some(re) = req.headers().get(actix_web::http::header::REFERER) {
        Ok(see_other(
            re.to_str().context("Unable to parse referer header")?,
        ))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/categories")))
    }
}

#[derive(Template)]
#[template(path = "product_categories.html")]
pub struct ProductCategoriesPage {
    pub categories: Vec<product_category::ProductCategory>,
    pub total_categories: usize,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/product_categories")]
async fn product_categories_page(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let categories = product_category_repo
        .select(&product_category::TopLevel(shop.id))
        .await
        .unwrap_or_default();
    let total_categories = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default()
        .len();
    render_template(ProductCategoriesPage {
        categories,
        total_categories,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "product_category.html")]
pub struct ProductCategoryPage {
    pub category: product_category::ProductCategory,
    pub subcategories: Vec<product_category::ProductCategory>,
    pub categories: Vec<product_category::ProductCategory>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/product_categories/{id}")]
async fn product_category_page(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    path: Path<(IdentityOf<Shop>, uuid::Uuid)>,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let (_, category_id) = path.into_inner();
    let category = product_category_repo
        .get_one(&category_id)
        .await?
        .ok_or(ControllerError::NotFound)?;
    let subcategories = product_category_repo
        .select(&product_category::ByParentId(category_id))
        .await
        .unwrap_or_default();
    let categories = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    render_template(ProductCategoryPage {
        category,
        subcategories,
        categories,
        shop,
        user,
    })
}

#[derive(Deserialize, Debug)]
pub struct ProductCategoryDto {
    pub name: String,
    pub regex: String,
    #[serde(default)]
    pub image_url: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub visibility_on_site: Option<String>,
    #[serde(default)]
    pub indexing_status: Option<String>,
}

#[post("/shop/{shop_id}/product_categories/add")]
async fn add_product_category(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    q: Form<ProductCategoryDto>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let shop_id = shop.id;
    let ProductCategoryDto {
        name,
        regex,
        image_url,
        parent_id,
        status,
        visibility_on_site: _,
        indexing_status: _,
    } = q.into_inner();
    let id = uuid::Uuid::new_v4();
    let regex = Regex::new(&regex).context("Unable to compile regex")?;
    let status_val = product_category::CategoryStatus::from_str(status.unwrap_or_default().as_str());
    let (visibility, indexing) = match status_val {
        product_category::CategoryStatus::Draft => (
            product_category::Visibility::Hidden,
            product_category::IndexingStatus::NoIndex,
        ),
        product_category::CategoryStatus::PublishedNoIndex => (
            product_category::Visibility::Visible,
            product_category::IndexingStatus::NoIndex,
        ),
        product_category::CategoryStatus::SeoReady => {
            if name.trim().is_empty() {
                return Err(ControllerError::InvalidInput {
                    field: "status".to_string(),
                    msg: "Для seo_ready потрібна назва".to_string(),
                });
            }
            (
                product_category::Visibility::Visible,
                product_category::IndexingStatus::Index,
            )
        }
    };
    let category = product_category::ProductCategory {
        id,
        name,
        parent_id: parent_id
            .clone()
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(uuid::Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?,
        regex: Some(regex),
        shop_id,
        status: status_val,
        visibility_on_site: visibility,
        indexing_status: indexing,
        seo_title: None,
        seo_description: None,
        seo_text: None,
        image_url: image_url.and_then(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }),
    };
    product_category_repo.save(category).await?;
    if let Some(id) = parent_id {
        Ok(see_other(&format!(
            "/shop/{shop_id}/product_categories/{id}"
        )))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/product_categories")))
    }
}

#[post("/shop/{shop_id}/product_categories/update/{id}")]
async fn update_product_category(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    q: Form<ProductCategoryDto>,
    path: Path<(IdentityOf<Shop>, uuid::Uuid)>,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let q = q.into_inner();
    let (shop_id, id) = path.into_inner();
    let regex = Regex::new(&q.regex).context("Unable to compile regex")?;
    let mut category = product_category_repo.get_one(&id).await?;
    if let Some(category) = &mut category {
        category.regex = Some(regex);
        category.name = q.name;
        category.image_url = q.image_url.and_then(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });
        category.parent_id = q
            .parent_id
            .filter(|s| !s.is_empty())
            .as_deref()
            .map(uuid::Uuid::from_str)
            .transpose()
            .context("Unable to parse parent_id")?;
        category.status =
            product_category::CategoryStatus::from_str(q.status.unwrap_or_default().as_str());
        category.visibility_on_site = product_category::Visibility::from_str(
            q.visibility_on_site
                .unwrap_or_else(|| "hidden".to_string())
                .as_str(),
        );
        category.indexing_status = product_category::IndexingStatus::from_str(
            q.indexing_status
                .unwrap_or_else(|| "noindex".to_string())
                .as_str(),
        );
        match category.status {
            product_category::CategoryStatus::Draft => {
                category.visibility_on_site = product_category::Visibility::Hidden;
                category.indexing_status = product_category::IndexingStatus::NoIndex;
            }
            product_category::CategoryStatus::PublishedNoIndex => {
                category.visibility_on_site = product_category::Visibility::Visible;
                category.indexing_status = product_category::IndexingStatus::NoIndex;
            }
            product_category::CategoryStatus::SeoReady => {
                category.visibility_on_site = product_category::Visibility::Visible;
                category.indexing_status = product_category::IndexingStatus::Index;
                if category.name.trim().is_empty() {
                    return Err(ControllerError::InvalidInput {
                        field: "status".to_string(),
                        msg: "Для seo_ready потрібна назва".to_string(),
                    });
                }
            }
        }
        product_category_repo.save(category.clone()).await?;
    }
    if let Some(id) = category.and_then(|c| c.parent_id) {
        Ok(see_other(&format!(
            "/shop/{shop_id}/product_categories/{id}"
        )))
    } else {
        Ok(see_other(&format!(
            "/shop/{shop_id}/product_categories/{id}"
        )))
    }
}

#[post("/shop/{shop_id}/product_category/delete/{id}")]
async fn delete_product_category(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    path: Path<(IdentityOf<Shop>, uuid::Uuid)>,
    req: HttpRequest,
    ShopAccess { .. }: ShopAccess,
) -> Response {
    let (shop_id, id) = path.into_inner();
    product_category_repo.remove(&id).await?;
    if let Some(re) = req.headers().get(actix_web::http::header::REFERER) {
        Ok(see_other(
            re.to_str().context("Unable to parse referer header")?,
        ))
    } else {
        Ok(see_other(&format!("/shop/{shop_id}/product_categories")))
    }
}

#[post("/shop/{shop_id}/product_categories/clear")]
async fn clear_product_categories(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    product_category_repo.clear(shop.id).await?;
    Ok(see_other(&format!("/shop/{}/product_categories", shop.id)))
}

#[post("/shop/{shop_id}/product_categories/seed")]
async fn seed_product_categories(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    // Only seed when empty to avoid surprising overwrites.
    let existing = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    if !existing.is_empty() {
        return Ok(see_other(&format!("/shop/{}/product_categories", shop.id)));
    }

    let shop_id = shop.id;
    let defs: Vec<(&str, &str)> = vec![
        (
            "Спліттери",
            r"(?i)(спліттер|сплиттер|splitter|lip|губа|передній дифузор|передний диффузор)",
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
        ("Диски", r"(?i)(диск|диски|wheels?|r\\d{2}\\s|r\\d{2}\\b)"),
    ];

    for (name, re) in defs {
        let regex = Regex::new(re).context("Unable to compile regex")?;
        product_category_repo
            .save(product_category::ProductCategory {
                id: uuid::Uuid::new_v4(),
                name: name.to_string(),
                parent_id: None,
                regex: Some(regex),
                shop_id,
                status: product_category::CategoryStatus::Draft,
                visibility_on_site: product_category::Visibility::Hidden,
                indexing_status: product_category::IndexingStatus::NoIndex,
                seo_title: None,
                seo_description: None,
                seo_text: None,
                image_url: None,
            })
            .await?;
    }

    Ok(see_other(&format!("/shop/{}/product_categories", shop.id)))
}

#[post("/shop/{shop_id}/product_categories/auto_assign")]
async fn auto_assign_product_categories(
    product_category_repo: Data<Arc<dyn product_category::ProductCategoryRepository>>,
    shop_product_repo: Data<Arc<dyn shop_product::ShopProductRepository>>,
    dt_repo: Data<Arc<dyn dt::product::ProductRepository + Send>>,
    ShopAccess { shop, .. }: ShopAccess,
) -> Response {
    let slugify = |s: &str| {
        s.to_lowercase()
            .replace(|c: char| !c.is_alphanumeric() && !c.is_whitespace(), " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("-")
    };
    let mut categories = product_category_repo
        .select(&product_category::ByShop(shop.id))
        .await
        .unwrap_or_default();
    if categories.is_empty() {
        return Ok(see_other(&format!("/shop/{}/product_categories", shop.id)));
    }

    // Оновлюємо regex для базових категорій, якщо вони задані за назвою
    let default_regex: Vec<(&str, &str)> = vec![
        (
            "спліттери",
            r"(?i)(спліттер|сплиттер|splitter|lip|губа|передній дифузор|передний диффузор)",
        ),
        (
            "дифузори",
            r"(?i)(дифузор|диффузор|diffuser|задній дифузор|задний диффузор)",
        ),
        ("спойлери", r"(?i)(спойлер|spoiler)"),
        ("пороги", r"(?i)(поріг|порог|side\\s*skirt|skirt|пороги)"),
        (
            "решітки радіатора",
            r"(?i)(решітка|решетка|решітки|решетки|grill|grille|гриль)",
        ),
        ("бампери", r"(?i)(бампер|bumper|бампери)"),
    ];
    let defaults: std::collections::HashMap<String, &str> = default_regex
        .into_iter()
        .map(|(n, re)| (slugify(n), re))
        .collect();
    for c in categories.iter_mut() {
        let slug = slugify(&c.name);
        if let Some(re) = defaults.get(&slug) {
            if c.regex.as_ref().map(|r| r.as_str()) != Some(*re) {
                let compiled = Regex::new(re).context("Unable to compile default regex")?;
                c.regex = Some(compiled);
                product_category_repo.save(c.clone()).await?;
            }
        }
    }

    let allowed_suppliers = site_publish::load_site_publish_suppliers(&shop.id);
    let mut products = dt_repo
        .select(&dt::product::AvailableSelector)
        .await
        .unwrap_or_default();
    // Якщо постачальники не обрані — працюємо з усіма товарами, інакше фільтруємо по опублікованих.
    if !allowed_suppliers.is_empty() {
        products = site_publish::filter_products_for_site(products, &allowed_suppliers);
    }

    for p in products {
        let haystack = product_category_auto::build_haystack(
            &p.title,
            p.description.as_deref().unwrap_or_default(),
        );
        if let Some(cat_id) =
            product_category_auto::guess_product_category_id(&haystack, &categories)
        {
            shop_product_repo
                .set_site_category(shop.id, &p.article, Some(cat_id))
                .await?;
        }
    }

    Ok(see_other(&format!("/shop/{}/product_categories", shop.id)))
}

pub struct FileInfo {
    pub name: String,
    pub last_modified: OffsetDateTime,
    pub size: u64,
}

impl FileInfo {
    pub fn format_size(&self) -> String {
        match self.size {
            x if x >= (1024 * 1024 * 1024) => {
                format!("{:.2} GB", (x as f32 / 1024. / 1024. / 1024.))
            }
            x if x >= (1024 * 1024) => format!("{:.2} MB", (x as f32 / 1024. / 1024.)),
            x if x >= 1024 => format!("{:.2} KB", (x as f32 / 1024.)),
            x => format!("{} Bytes", x),
        }
    }
    pub fn format_modified_ago(&self) -> String {
        let now = OffsetDateTime::now_utc();
        let duration = now - self.last_modified;
        crate::format_duration_short(&duration.unsigned_abs())
    }
    pub fn format_modified_at(&self) -> Option<String> {
        let format_description = iso8601::Iso8601::<
            {
                iso8601::Config::DEFAULT
                    .set_time_precision(iso8601::TimePrecision::Minute {
                        decimal_digits: None,
                    })
                    .set_formatted_components(iso8601::FormattedComponents::DateTime)
                    .encode()
            },
        >;
        self.last_modified
            .format(&format_description)
            .log_error("Unable to format time")
    }
}

fn format_dt_last_visited(value: OffsetDateTime, now: OffsetDateTime) -> String {
    let duration = now - value;
    let ago = crate::format_duration_short(&duration.unsigned_abs());
    let format_description = iso8601::Iso8601::<
        {
            iso8601::Config::DEFAULT
                .set_time_precision(iso8601::TimePrecision::Minute { decimal_digits: None })
                .set_formatted_components(iso8601::FormattedComponents::DateTime)
                .encode()
        },
    >;
    let formatted = value
        .format(&format_description)
        .unwrap_or_else(|_| value.to_string());
    format!("{formatted} ({ago} тому)")
}

#[derive(Deserialize)]
pub struct DescriptionsPageQuery {
    err: Option<DescriptionQueryError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptionQueryError {
    EmptyFilename,
    FileUsed,
    #[serde(untagged)]
    Other(String),
}

#[derive(Template)]
#[template(path = "descriptions.html")]
struct DescriptionsTemplate<'a> {
    pub directory: &'a str,
    pub files: Vec<FileInfo>,
    pub err: Option<String>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/description{path:(/.*)?}")]
pub async fn descriptions_page(
    path: Path<(IdentityOf<Shop>, Option<String>)>,
    req: HttpRequest,
) -> ShopResponse {
    let (shop_id, path) = path.into_inner();
    let q: Option<Query<DescriptionsPageQuery>> = match Query::from_query(req.query_string()) {
        Ok(q) => Some(q),
        Err(err) => {
            log::error!("Unable to parse query: {err}");
            None
        }
    };
    let path = format!(
        "./description/{shop_id}/{}",
        path.map(|p| p.replace(&format!("/{shop_id}/description/"), ""))
            .unwrap_or_default(),
    );
    let meta = std::fs::metadata(&path)
        .context("Unable to read file metadata")
        .map_err(ControllerError::InternalServerError)?;
    if meta.is_file() {
        return Ok(NamedFile::open(path)
            .context("Unable to open file")
            .map_err(ControllerError::InternalServerError)?
            .into_response(&req));
    }
    let ShopAccess { shop, user } = ShopAccess::extract(&req).await?;
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path)
                .context("Unable to create dir")
                .map_err(ShopControllerError::with(&user, &shop))?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm)
                .context("Unable to set dir permissions")
                .map_err(ShopControllerError::with(&user, &shop))?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let mut files: Vec<_> = res
        .map_err(ShopControllerError::with(&user, &shop))?
        .map(|e| {
            let e = e?;
            let meta = e.metadata()?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(FileInfo {
                name: file_name,
                last_modified: OffsetDateTime::from(meta.modified()?),
                size: meta.len(),
            })
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")
        .map_err(ShopControllerError::with(&user, &shop))?;
    let err = match q.and_then(|q| q.into_inner().err) {
        Some(DescriptionQueryError::EmptyFilename) => Some("Имя файла не указано".to_string()),
        Some(DescriptionQueryError::FileUsed) => {
            Some("Этот файл используется, и не может быть удален".to_string())
        }
        Some(DescriptionQueryError::Other(s)) => Some(s),
        None => None,
    };
    files.sort_by(|a, b| a.name.cmp(&b.name));
    render_template(DescriptionsTemplate {
        directory: &path,
        files,
        err,
        shop: shop.clone(),
        user: user.clone(),
    })
    .map_err(ShopControllerError::with(&user, &shop))
}

#[derive(Template)]
#[template(path = "export.html")]
struct ExportTemplate<'a> {
    pub directory: &'a str,
    pub files: Vec<FileInfo>,
    pub shop: Shop,
    pub user: UserCredentials,
}

#[get("/shop/{shop_id}/export")]
async fn export_page(
    path: Path<IdentityOf<Shop>>,
    req: HttpRequest,
    ShopAccess { shop, user }: ShopAccess,
) -> Response {
    let shop_id = path.into_inner();
    let path = req
        .path()
        .replace(&format!("/shop/{shop_id}"), "")
        .replacen("/shops", "", 1);
    let path = format!("{path}/{shop_id}");
    let res = match std::fs::read_dir(&path) {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&path).context("Unable to create dir")?;
            let meta = std::fs::metadata(&path)
                .context("Unable to read file metadata")
                .map_err(ControllerError::InternalServerError)?;
            let mut perm = meta.permissions();
            perm.set_mode(0o777);
            std::fs::set_permissions(&path, perm).context("Unable to set dir permissions")?;
            std::fs::read_dir(&path).context("Unable to read dir")
        }
        Err(err) => Err(err).context("Unable to read dir"),
    };
    let mut files: Vec<_> = res
        .context("Unable to read directory")?
        .map(|e| {
            let e = e?;
            let meta = e.metadata()?;
            let file_name = e
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))
                .map_err(std::io::Error::other)?;
            Ok(FileInfo {
                name: file_name,
                last_modified: OffsetDateTime::from(meta.modified()?),
                size: meta.len(),
            })
        })
        .collect::<Result<_, std::io::Error>>()
        .context("Unable to read directory")?;
    files.sort_by(|a, b| a.name.cmp(&b.name));
    render_template(ExportTemplate {
        directory: &path,
        files,
        shop,
        user,
    })
}

#[derive(Template)]
#[template(path = "control_panel/index.html")]
pub struct ControlPanelPage {
    user: UserCredentials,
    status: Vec<(String, ServiceStatus)>,
}

#[derive(Display)]
pub enum ServiceStatus {
    #[display("Работает")]
    Up,
    #[display("Не работает")]
    Down,
}

impl From<bool> for ServiceStatus {
    fn from(v: bool) -> Self {
        match v {
            true => Self::Up,
            false => Self::Down,
        }
    }
}

#[derive(Serialize)]
struct LoadAvg {
    one: f64,
    five: f64,
    fifteen: f64,
}

#[derive(Serialize)]
struct MemoryInfo {
    total_mb: u64,
    available_mb: u64,
    used_mb: u64,
    used_percent: u8,
}

#[derive(Serialize)]
struct ImportStats {
    site_in_progress: usize,
    site_enqueued: usize,
    site_suspended: usize,
    site_failed: usize,
    ddaudio_in_progress: usize,
    ddaudio_failed: usize,
    exports_in_progress: usize,
    dt_stage: Option<String>,
    dt_ready: Option<u64>,
    dt_total: Option<u64>,
}

#[derive(Serialize)]
struct SystemStats {
    load: LoadAvg,
    memory: MemoryInfo,
    imports: ImportStats,
    errors: Vec<String>,
    updated_at: String,
}

fn read_loadavg() -> LoadAvg {
    let raw = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let mut parts = raw.split_whitespace();
    let one = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let five = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let fifteen = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    LoadAvg { one, five, fifteen }
}

fn read_meminfo() -> MemoryInfo {
    let raw = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let mut total_kb: Option<u64> = None;
    let mut available_kb: Option<u64> = None;
    for line in raw.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok());
        } else if line.starts_with("MemAvailable:") {
            available_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok());
        }
    }
    let total = total_kb.unwrap_or(0);
    let available = available_kb.unwrap_or(total);
    let used = total.saturating_sub(available);
    let used_percent = if total == 0 {
        0
    } else {
        ((used as f64 / total as f64) * 100.0).round() as u8
    };
    MemoryInfo {
        total_mb: total / 1024,
        available_mb: available / 1024,
        used_mb: used / 1024,
        used_percent,
    }
}

fn push_error(errors: &mut Vec<String>, message: &str) {
    let msg = message.trim();
    if msg.is_empty() {
        return;
    }
    if errors.iter().any(|e| e == msg) {
        return;
    }
    if errors.len() < 5 {
        errors.push(msg.to_string());
    }
}

#[get("/control_panel")]
async fn control_panel(
    ControlPanelAccess { user }: ControlPanelAccess,
    shop_service: Data<Addr<ShopService>>,
    export_service: Data<Arc<Addr<ExportService>>>,
    tt_service: Option<Data<Arc<Addr<tt::parser::ParserService>>>>,
    dt_service: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    let tt_connected = tt_service
        .as_ref()
        .map(|svc| svc.get_ref().connected())
        .unwrap_or(false);
    render_template(ControlPanelPage {
        status: vec![
            ("shop_service".to_string(), shop_service.connected().into()),
            (
                "export_service".to_string(),
                export_service.connected().into(),
            ),
            ("dt_service".to_string(), dt_service.connected().into()),
            ("tt_service".to_string(), tt_connected.into()),
        ],
        user,
    })
}

#[get("/control_panel/system_stats")]
async fn system_stats(
    ControlPanelAccess { user: _ }: ControlPanelAccess,
    shop_service: Data<Addr<ShopService>>,
    site_import_service: Data<Addr<site_import::SiteImportService>>,
    export_service: Data<Addr<ExportService>>,
    dt_service: Data<Arc<Addr<dt::parser::ParserService>>>,
) -> Response {
    let shops = shop_service.send(rt_types::shop::service::List).await??;
    let mut site_in_progress = 0usize;
    let mut site_enqueued = 0usize;
    let mut site_suspended = 0usize;
    let mut site_failed = 0usize;
    let mut ddaudio_in_progress = 0usize;
    let mut ddaudio_failed = 0usize;
    let mut exports_in_progress = 0usize;
    let mut errors = Vec::new();

    for shop in &shops {
        let imports = site_import_service
            .send(site_import::GetAllStatus(shop.id))
            .await
            .map_err(ControllerError::from)?;
        for (_, import) in imports {
            match &import.status {
                site_import::SiteImportStatus::InProgress => site_in_progress += 1,
                site_import::SiteImportStatus::Enqueued => site_enqueued += 1,
                site_import::SiteImportStatus::Suspended => site_suspended += 1,
                site_import::SiteImportStatus::Failure(msg) => {
                    site_failed += 1;
                    push_error(&mut errors, msg);
                }
                site_import::SiteImportStatus::Success => {}
            }
        }

        let exports = export_service
            .send(export::GetAllStatus(shop.id))
            .await
            .map_err(ControllerError::from)?;
        for (_, export) in exports {
            match &export.status {
                ExportStatus::InProgress => exports_in_progress += 1,
                ExportStatus::Failure(msg) => {
                    push_error(&mut errors, msg);
                }
                _ => {}
            }
        }

        let dd_state = ddaudio_import::get_status(shop.id).await;
        match &dd_state.status {
            ddaudio_import::ImportStatus::InProgress => ddaudio_in_progress += 1,
            ddaudio_import::ImportStatus::Failure(msg) => {
                ddaudio_failed += 1;
                push_error(&mut errors, msg);
            }
            _ => {}
        }
        if let Some(msg) = dd_state.last_error.as_deref() {
            push_error(&mut errors, msg);
        }
    }

    let dt_progress = dt_service
        .get_ref()
        .send(dt::parser::GetProgress)
        .await
        .ok()
        .and_then(|res| res.ok());
    let dt_stage = dt_progress
        .as_ref()
        .map(|p| p.stage.to_string());
    let dt_ready = dt_progress.as_ref().map(|p| p.ready);
    let dt_total = dt_progress.as_ref().map(|p| p.total);

    let stats = SystemStats {
        load: read_loadavg(),
        memory: read_meminfo(),
        imports: ImportStats {
            site_in_progress,
            site_enqueued,
            site_suspended,
            site_failed,
            ddaudio_in_progress,
            ddaudio_failed,
            exports_in_progress,
            dt_stage,
            dt_ready,
            dt_total,
        },
        errors,
        updated_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string()),
    };

    Ok(HttpResponse::Ok().json(stats))
}

#[derive(Template)]
#[template(path = "control_panel/users.html")]
pub struct ControlPanelUsersPage {
    user: UserCredentials,
    users: Vec<UserCredentials>,
    tokens: BTreeSet<RegistrationToken>,
}

#[get("/control_panel/users")]
async fn control_panel_users(
    ControlPanelAccess { user }: ControlPanelAccess,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let users = user_credentials_service
        .send(access::service::List)
        .await??;
    let tokens = user_credentials_service
        .send(access::service::ListTokens)
        .await?;
    render_template(ControlPanelUsersPage {
        user,
        users,
        tokens,
    })
}

#[derive(Template)]
#[template(path = "control_panel/edit_user.html")]
pub struct ControlPanelEditUserPage {
    user: UserCredentials,
    edited_user: UserCredentials,
    subscription: Option<Subscription>,
    subscriptions: Vec<Subscription>,
}

#[get("/control_panel/users/{user_id}/edit")]
async fn control_panel_edit_user_page(
    ControlPanelAccess { user }: ControlPanelAccess,
    subscription_service: Data<Addr<SubscriptionService>>,
    user_id: Path<IdentityOf<UserCredentials>>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
) -> Response {
    let edited_user = user_credentials_service
        .send(access::service::Get(user_id.into_inner()))
        .await??
        .ok_or(ControllerError::NotFound)?;
    let subscriptions = subscription_service
        .send(subscription::service::List)
        .await??;
    let subscription = match edited_user.subscription {
        Some((id, ver)) => Some(
            subscriptions
                .iter()
                .find(|s| id == s.id && ver == s.version)
                .cloned()
                .ok_or(ControllerError::NotFound)?,
        ),
        None => None,
    };
    render_template(ControlPanelEditUserPage {
        user,
        edited_user,
        subscription,
        subscriptions,
    })
}

#[derive(Deserialize)]
pub struct EditUserCredentialsDto {
    #[serde(deserialize_with = "empty_string_as_none_parse")]
    subscription: Option<IdentityOf<Subscription>>,
}

#[post("/control_panel/users/{user_id}/edit")]
async fn control_panel_edit_user(
    ControlPanelAccess { .. }: ControlPanelAccess,
    dto: Form<EditUserCredentialsDto>,
    user_credentials_service: Data<Addr<UserCredentialsService>>,
    subscription_service: Data<Addr<SubscriptionService>>,
    user_id: Path<IdentityOf<UserCredentials>>,
) -> Response {
    let user_id = user_id.into_inner();
    let mut user = user_credentials_service
        .send(access::service::Get(user_id.clone()))
        .await??
        .ok_or(ControllerError::NotFound)?;
    let dto = dto.into_inner();
    let subscription = match dto.subscription {
        Some(id) => Some(
            subscription_service
                .send(subscription::service::Get(id))
                .await??
                .ok_or(ControllerError::NotFound)?,
        ),
        None => None,
    };
    user.subscription = subscription.map(|s| (s.id, s.version));
    user_credentials_service
        .send(access::service::Update(user))
        .await??;
    Ok(see_other(&format!("/control_panel/users/{user_id}/edit")))
}

#[derive(Template)]
#[template(path = "control_panel/shops.html")]
pub struct ControlPanelShopsPage {
    user: UserCredentials,
    shops: Vec<Shop>,
}

#[get("/control_panel/shops")]
async fn control_panel_shops(
    ControlPanelAccess { user }: ControlPanelAccess,
    shop_service: Data<Addr<ShopService>>,
) -> Response {
    let shops = shop_service.send(shop::service::List).await??;
    render_template(ControlPanelShopsPage { user, shops })
}

#[derive(Template)]
#[template(path = "control_panel/settings.html")]
pub struct ControlPanelSettingsPage {
    user: UserCredentials,
}

#[get("/control_panel/settings")]
async fn control_panel_settings(ControlPanelAccess { user }: ControlPanelAccess) -> Response {
    render_template(ControlPanelSettingsPage { user })
}

#[derive(Template)]
#[template(path = "control_panel/files.html")]
pub struct ControlPanelFilesPage {
    user: UserCredentials,
    files: Vec<UploadedFileInfo>,
    directories: Vec<String>,
    current_dir: String,
    storage: StorageStats,
}

#[derive(Clone)]
pub struct UploadedFileInfo {
    pub name: String,
    pub path: String,
    pub size: u64,
    pub last_modified: OffsetDateTime,
}

impl UploadedFileInfo {
    pub fn format_size(&self) -> String {
        format_bytes(self.size)
    }
    
    pub fn format_date(&self) -> String {
        // Простий спосіб форматування дати
        let year = self.last_modified.year();
        let month = self.last_modified.month() as u8;
        let day = self.last_modified.day();
        let hour = self.last_modified.hour();
        let minute = self.last_modified.minute();
        format!("{:04}-{:02}-{:02} {:02}:{:02}", year, month, day, hour, minute)
    }
}

#[derive(Clone)]
pub struct StorageStats {
    uploads_bytes: u64,
    disk_total: u64,
    disk_free: u64,
}

impl StorageStats {
    pub fn from_path(path: &StdPath) -> Self {
        let uploads_bytes = dir_size(path);
        let (disk_total, disk_free) = disk_usage(path).unwrap_or((0, 0));
        StorageStats {
            uploads_bytes,
            disk_total,
            disk_free,
        }
    }

    pub fn format_uploads(&self) -> String {
        format_bytes(self.uploads_bytes)
    }

    pub fn format_total(&self) -> String {
        format_bytes(self.disk_total)
    }

    pub fn format_free(&self) -> String {
        format_bytes(self.disk_free)
    }

    pub fn format_used(&self) -> String {
        format_bytes(self.disk_total.saturating_sub(self.disk_free))
    }
}

fn format_bytes(size: u64) -> String {
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.2} KB", size as f64 / 1024.0)
    } else if size < 1024 * 1024 * 1024 {
        format!("{:.2} MB", size as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", size as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn dir_size(path: &StdPath) -> u64 {
    let mut total: u64 = 0;
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.is_dir() {
            total = total.saturating_add(dir_size(&path));
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn disk_usage(path: &StdPath) -> Option<(u64, u64)> {
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if res != 0 {
        return None;
    }
    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
    let free = stat.f_bavail as u64 * stat.f_frsize as u64;
    Some((total, free))
}

#[get("/control_panel/files")]
async fn control_panel_files(
    ControlPanelAccess { user }: ControlPanelAccess,
    query: Option<Query<std::collections::HashMap<String, String>>>,
) -> Response {
    let uploads_dir = "./static/uploads/products";
    let storage = StorageStats::from_path(StdPath::new(uploads_dir));
    let current_dir = query
        .as_ref()
        .and_then(|q| q.get("dir"))
        .cloned()
        .unwrap_or_else(|| "".to_string());
    
    let dir_path = if current_dir.is_empty() {
        std::path::PathBuf::from(uploads_dir)
    } else {
        std::path::PathBuf::from(uploads_dir).join(&current_dir)
    };

    let mut files = Vec::new();
    let mut directories = Vec::new();

    if dir_path.exists() && dir_path.is_dir() {
        match std::fs::read_dir(&dir_path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(metadata) = entry.metadata() {
                        if path.is_dir() {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                directories.push(name.to_string());
                            }
                        } else {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                let relative_path = if current_dir.is_empty() {
                                    name.to_string()
                                } else {
                                    format!("{}/{}", current_dir, name)
                                };
                                files.push(UploadedFileInfo {
                                    name: name.to_string(),
                                    path: relative_path,
                                    size: metadata.len(),
                                    last_modified: OffsetDateTime::from(metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH)),
                                });
                            }
                        }
                    }
                }
            }
            Err(err) => {
                log::error!("Unable to read directory {}: {}", dir_path.display(), err);
            }
        }
    }

    files.sort_by(|a, b| a.name.cmp(&b.name));
    directories.sort();

    render_template(ControlPanelFilesPage {
        user,
        files,
        directories,
        current_dir,
        storage,
    })
}

#[post("/control_panel/files/delete")]
async fn control_panel_files_delete(
    ControlPanelAccess { .. }: ControlPanelAccess,
    form: Form<std::collections::HashMap<String, String>>,
) -> Response {
    let file_path = form
        .get("path")
        .ok_or_else(|| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Path is required".to_string(),
        })?;

    let full_path = std::path::PathBuf::from("./static/uploads/products").join(file_path);
    
    // Перевірка безпеки - переконаємося, що шлях не виходить за межі дозволеної директорії
    let canonical_path = full_path.canonicalize()
        .map_err(|_| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Invalid path".to_string(),
        })?;
    
    let uploads_dir = std::path::PathBuf::from("./static/uploads/products")
        .canonicalize()
        .map_err(|_| ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Invalid uploads directory".to_string(),
        })?;

    if !canonical_path.starts_with(&uploads_dir) {
        return Err(ControllerError::InvalidInput {
            field: "path".to_string(),
            msg: "Path outside allowed directory".to_string(),
        });
    }

    std::fs::remove_file(&canonical_path)
        .map_err(|err| ControllerError::InternalServerError(anyhow::anyhow!(
            "Unable to delete file: {}", err
        )))?;

    Ok(see_other("/control_panel/files"))
}

pub fn into_user_option<'a, T>(t: T) -> Option<&'a UserCredentials>
where
    T: Into<Option<&'a UserCredentials>>,
{
    t.into()
}

pub struct Record<T>
where
    T: Clone + 'static,
    RecordGuard<T>: RecordResponse,
{
    pub g: RecordGuard<T>,
    pub t: T,
}

impl<T: Clone + 'static> Record<T>
where
    RecordGuard<T>: RecordResponse,
{
    #[must_use]
    pub fn into_inner(self) -> (T, RecordGuard<T>) {
        (self.t, self.g)
    }

    pub async fn map(
        mut self,
        f: impl FnOnce(&mut T),
    ) -> <RecordGuard<T> as RecordResponse>::Response {
        f(&mut self.t);
        self.g.save(self.t).await
    }

    pub async fn try_map<E>(
        mut self,
        f: impl FnOnce(&mut T) -> Result<(), E>,
    ) -> Result<<RecordGuard<T> as RecordResponse>::Response, E> {
        f(&mut self.t)?;
        Ok(self.g.save(self.t).await)
    }

    pub async fn filter_map(
        mut self,
        f: impl FnOnce(&mut T) -> bool,
    ) -> Option<<RecordGuard<T> as RecordResponse>::Response> {
        let res = f(&mut self.t);
        if res {
            Some(self.g.save(self.t).await)
        } else {
            None
        }
    }
}

#[must_use]
pub struct RecordGuard<T>
where
    Self: RecordResponse,
{
    pub f: Box<dyn Fn(T) -> Pin<Box<dyn Future<Output = <Self as RecordResponse>::Response>>>>,
}

pub trait RecordResponse {
    type Response;
}

impl RecordResponse for RecordGuard<ExportEntry> {
    type Response = Result<String, anyhow::Error>;
}

impl RecordResponse for RecordGuard<Export> {
    type Response = Result<String, anyhow::Error>;
}

impl<T: Clone + 'static> RecordGuard<T>
where
    Self: RecordResponse,
{
    #[must_use]
    pub async fn save(self, t: T) -> <Self as RecordResponse>::Response {
        let f = self.f;
        f(t).await
    }
}

impl FromRequest for Record<Export> {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let service = Data::<Arc<Addr<ExportService>>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ExportService from request"))?;
            let subscription_service = Data::<Addr<SubscriptionService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract SubscriptionService from request")
                })?;
            let path = req.match_info().get("export_hash");
            let hash = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!(
                            "Unable to extract export entry hash from request"
                        ))?
                        .1
                }
            };
            let hash = hash.to_string();
            let export = service
                .send(export::GetStatus(hash.clone()))
                .await?
                .ok_or(ControllerError::NotFound)?;
            let shop_id = export.shop;
            let s = service.clone();
            let ShopAccess { user, .. } = ShopAccess::extract(&req).await?;
            Ok(Self {
                t: export,
                g: RecordGuard {
                    f: Box::new(move |mut e| {
                        let s = s.clone();
                        let hash = hash.clone();
                        let user = user.clone();
                        let subscription_service = subscription_service.clone();
                        Box::pin(async move {
                            let subscription = subscription_service
                                .send(subscription::service::GetBy(user))
                                .await??;
                            e.entry.edited_time = OffsetDateTime::now_utc();
                            let permission =
                                UpdateExportEntryPermission::acquire(e.entry, hash, &subscription)
                                    .ok_or(anyhow::anyhow!("Permission denied"))?;
                            Ok(s.send(export::Update(shop_id, permission)).await??)
                        })
                    }),
                },
            })
        })
    }
}

impl FromRequest for Record<ExportEntry> {
    type Error = ControllerError;
    type Future = futures_util::future::LocalBoxFuture<'static, Result<Self, Self::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let req = req.clone();
        Box::pin(async move {
            let service = Data::<Arc<Addr<ExportService>>>::extract(&req)
                .await
                .map_err(|_err| anyhow::anyhow!("Unable to extract ExportService from request"))?;
            let subscription_service = Data::<Addr<SubscriptionService>>::extract(&req)
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Unable to extract SubscriptionService from request")
                })?;
            let path = req.match_info().get("export_hash");
            let hash = match path {
                Some(p) => p,
                None => {
                    req.match_info()
                        .iter()
                        .nth(0)
                        .ok_or(anyhow::anyhow!(
                            "Unable to extract export entry hash from request"
                        ))?
                        .1
                }
            };
            let hash = hash.to_string();
            let export = service
                .send(export::GetStatus(hash.clone()))
                .await?
                .ok_or(anyhow::anyhow!("Unable to find export entry by hash"))?;
            let shop_id = export.shop;
            let s = service.clone();
            let e = export.entry;
            let ShopAccess { user, .. } = ShopAccess::extract(&req).await?;
            Ok(Self {
                t: e,
                g: RecordGuard {
                    f: Box::new(move |mut e| {
                        let s = s.clone();
                        let hash = hash.clone();
                        let user = user.clone();
                        let subscription_service = subscription_service.clone();
                        Box::pin(async move {
                            let subscription = subscription_service
                                .send(subscription::service::GetBy(user))
                                .await??;
                            e.edited_time = OffsetDateTime::now_utc();
                            let permission =
                                UpdateExportEntryPermission::acquire(e, hash, &subscription)
                                    .ok_or(anyhow::anyhow!("Permission denied"))?;
                            Ok(s.send(export::Update(shop_id, permission)).await??)
                        })
                    }),
                },
            })
        })
    }
}

pub fn file_info<P: AsRef<std::path::Path>>(p: P) -> Result<Option<FileInfo>, anyhow::Error> {
    let name = p
        .as_ref()
        .file_name()
        .ok_or_else(|| anyhow!("File without filename"))?
        .to_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("Unable to convert file name"))
        .map_err(std::io::Error::other)?;
    let file = match std::fs::File::open(p) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let meta = file.metadata()?;
    Ok(Some(FileInfo {
        name,
        last_modified: OffsetDateTime::from(meta.modified()?),
        size: meta.len(),
    }))
}

pub async fn check_description(shop_id: IdentityOf<Shop>, name: &str) -> Result<(), anyhow::Error> {
    match tokio::fs::read_to_string(format!("./description/{shop_id}/{name}",)).await {
        Ok(d) if d.len() > MAX_DESCRIPTION_SIZE => Err(anyhow::anyhow!(
            "Description size must be < {}kB",
            MAX_DESCRIPTION_SIZE / 1024
        )
        .into()),
        Ok(_) => Ok(()),
        Err(err) => Err(anyhow::anyhow!("Unable to open description file: {err}").into()),
    }
}
