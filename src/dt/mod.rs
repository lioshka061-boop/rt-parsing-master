pub mod parser;
pub mod product;
pub mod manual_queue;

pub mod selectors {
    #![allow(clippy::unwrap_used)]

    use once_cell::sync::Lazy;
    use scraper::Selector;

    pub static BRANDS: Lazy<Selector> =
        Lazy::new(|| Selector::parse("ul.brands-wrap:first-of-type > li > a ").unwrap());
    pub static CATEGORIES: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".lp-cat-ul > li > a").unwrap());
    pub static TITLE: Lazy<Selector> = Lazy::new(|| Selector::parse(".item-title").unwrap());
    pub static DESCRIPTION: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".item-description-full").unwrap());
    pub static ARTICLE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".item-title-article").unwrap());
    pub static CATEGORY: Lazy<Selector> = Lazy::new(|| {
        Selector::parse(".cat-breadcrumbs-text span[typeof=\"v:Breadcrumb\"]:nth-child(odd) a")
            .unwrap()
    });
    pub static AVAILABLE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".available-wrap").unwrap());
    pub static PRICE: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".product__price-block_text1").unwrap());
    pub static LOGO: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".item-logo > a > img").unwrap());
    pub static GALLERY_IMAGES: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".item-images-wrap > a > img.item-gallery-image").unwrap());
    pub static SUBCATEGORY: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".cat-item-wrap a").unwrap());
    pub static MODEL: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".cat-item-wrap > .cat-item-title > a").unwrap());
    pub static PRODUCT_ITEM: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".cat-item-list-wrap > .cat-item-list-title > a").unwrap());
    pub static AVAILABLE_ON_ORDER: Lazy<Selector> =
        Lazy::new(|| Selector::parse(".item-info-block > .cat-item-list-prices-avail").unwrap());
    pub static PAGINATION_NEXT: Lazy<Selector> = Lazy::new(|| {
        Selector::parse(
            "a[rel=\"next\"], a[aria-label*=\"Next\"], a[aria-label*=\"next\"], \
             a[aria-label*=\"След\"], a[aria-label*=\"Далее\"], a.next, a.pagination-next",
        )
        .unwrap()
    });
    pub static LINKS: Lazy<Selector> = Lazy::new(|| Selector::parse("a").unwrap());
    pub static JSON_LD: Lazy<Selector> =
        Lazy::new(|| Selector::parse("script[type=\"application/ld+json\"]").unwrap());
}
