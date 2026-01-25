import type { Metadata } from 'next';
import { loadCarCategories, loadProductCategories } from '../../lib/categories';
import { loadProducts, loadProductsWithMeta, plainText } from '../../lib/products';
import { slugify } from '../../lib/slug';
import { ProductInfiniteGrid } from '../../components/ProductInfiniteGrid';
import { MobileFilterSheet } from '../../components/MobileFilterSheet';

const SEO_TITLE = 'Каталог тюнінгу та автотоварів';

export async function generateMetadata({
  params,
  searchParams,
}: {
  params: { brand: string };
  searchParams?: { pcat?: string; page?: string; perPage?: string };
}): Promise<Metadata> {
  const carCategories = await loadCarCategories({ revalidate: 900 });
  const brandSlug = params.brand.toLowerCase();
  const brandNode = carCategories.find((c) => (c.slug || slugify(c.name)) === brandSlug);
  const brandName = plainText(brandNode?.name || brandSlug);
  const canonical = brandNode?.canonical || undefined;
  const page = Number.parseInt(searchParams?.page || '1', 10) || 1;
  const perPage = Number.parseInt(searchParams?.perPage || '24', 10) || 24;
  const hasFilters = Boolean(searchParams?.pcat) || page > 1 || perPage !== 24;
  const indexable = brandNode?.indexable === true;
  const title = brandNode?.seo_title || brandName;
  const description = brandNode?.seo_description || '';
  return {
    title,
    description,
    alternates: !hasFilters && indexable && canonical ? { canonical } : undefined,
    robots: !hasFilters && indexable ? { index: true, follow: true } : { index: false, follow: true },
  };
}

export default async function BrandPage({
  params,
  searchParams,
}: {
  params: { brand: string };
  searchParams?: { pcat?: string; page?: string; perPage?: string };
}) {
  const brandSlug = params.brand.toLowerCase();
  const page = Math.max(Number.parseInt(searchParams?.page || '1', 10) || 1, 1);
  const perPage = (() => {
    const v = Number.parseInt(searchParams?.perPage || '24', 10);
    if ([12, 24, 30].includes(v)) return v;
    return 24;
  })();
  const offset = (page - 1) * perPage;
  const activeCategorySlug = (searchParams?.pcat || '').toLowerCase();

  const [carCategories, productCategories, { items, total }] = await Promise.all([
    loadCarCategories({ revalidate: 900 }),
    loadProductCategories({ revalidate: 900 }),
    loadProductsWithMeta(
      {
        brand: brandSlug,
        category: activeCategorySlug || undefined,
        limit: perPage,
        offset,
        compact: true,
      },
      { revalidate: 300 },
    ),
  ]);

  const brandNode = carCategories.find((c) => (c.slug || slugify(c.name)) === brandSlug);
  let brandName = brandNode?.name || brandSlug;
  const modelsFromTree = brandNode?.children || [];
  let models = modelsFromTree.map((m) => m.name);

  const brandProducts = await loadProducts(
    { brand: brandSlug, limit: 30, offset: 0, compact: true },
    { revalidate: 300 },
  );
  if (!brandNode) {
    brandName = brandProducts[0]?.brand || brandName;
  }
  const modelMap = new Map<string, string>();
  modelsFromTree.forEach((m) => {
    const name = plainText(m.name);
    const slug = m.slug || slugify(name);
    if (slug) modelMap.set(slug, name);
  });
  brandProducts.forEach((p) => {
    const name = plainText(p.model);
    const slug = slugify(name);
    if (slug && !modelMap.has(slug)) modelMap.set(slug, name);
  });
  models = Array.from(modelMap.values()).sort((a, b) => a.localeCompare(b));

  const categories = productCategories
    .filter((c) => Boolean(c.name))
    .map((c) => ({
      name: plainText(c.name),
      slug: c.slug || slugify(c.name),
    }));

  const activeCategoryName =
    categories.find((c) => c.slug === activeCategorySlug)?.name || '';

  const paramsNext = new URLSearchParams();
  if (activeCategorySlug) paramsNext.set('pcat', activeCategorySlug);
  paramsNext.set('perPage', perPage.toString());
  paramsNext.set('page', (page + 1).toString());
  const nextHref = items.length + offset < total ? `/catalog/${brandSlug}?${paramsNext.toString()}` : null;

  const resetKey = JSON.stringify({ brandSlug, pcat: activeCategorySlug, perPage, page });

  return (
    <main className="page catalog-page">
      <section className="catalog-layout">
        <aside className="catalog-sidebar">
          <div className="sidebar-head">
            <p className="eyebrow">Каталог</p>
            <h2>{SEO_TITLE}</h2>
            <p className="muted">Обери модель або категорію для бренду {plainText(brandName)}.</p>
          </div>

          <details className="filter-block filter-accordion" open>
            <summary className="filter-summary">
              <span className="filter-summary-main">
                <span className="filter-title">Марки та моделі</span>
                <a className="filter-back" href="/catalog">
                  ← До марок
                </a>
              </span>
              <span className="filter-chevron" aria-hidden="true"></span>
            </summary>
            <div className="filter-list">
              <span className="filter-link active">{plainText(brandName)}</span>
              {models.map((model) => (
                <a
                  key={model}
                  className="filter-link"
                  href={`/catalog/${brandSlug}/${slugify(model)}`}
                >
                  {plainText(model)}
                </a>
              ))}
              {models.length === 0 && <span className="muted">Моделі ще не додані.</span>}
            </div>
          </details>

          <details className="filter-block filter-accordion" open>
            <summary className="filter-summary">
              <span className="filter-title">Категорії</span>
              <span className="filter-chevron" aria-hidden="true"></span>
            </summary>
            <div className="filter-list">
              {categories.map((cat) => (
                <a
                  key={cat.slug}
                  className={`filter-link ${cat.slug === activeCategorySlug ? 'active' : ''}`}
                  href={`/catalog/${brandSlug}?pcat=${encodeURIComponent(cat.slug)}`}
                >
                  {cat.name}
                </a>
              ))}
              {categories.length === 0 && <span className="muted">Категорії ще не додані.</span>}
            </div>
          </details>
        </aside>

        <div className="catalog-content">
          <nav className="breadcrumbs" aria-label="Breadcrumbs">
            <a href="/">Головна</a>
            <span>/</span>
            <a href="/catalog">Каталог</a>
            <span>/</span>
            <span>{plainText(brandName)}</span>
          </nav>

          <div className="catalog-summary">
            <div>
              <h1>{plainText(brandName)}</h1>
              <p className="muted">Знайдено товарів: {total}</p>
            </div>
            <div className="filter-chips">
              <a className="filter-chip" href="/catalog">
                {plainText(brandName)}
              </a>
              {activeCategoryName && (
                <a className="filter-chip" href={`/catalog/${brandSlug}`}>
                  {activeCategoryName}
                </a>
              )}
              {(activeCategoryName || brandName) && (
                <a className="chip-clear" href={`/catalog/${brandSlug}`}>
                  Скинути фільтри
                </a>
              )}
            </div>
          </div>
          <a
            className="filter-trigger filter-trigger--side mobile-only"
            href="#mobile-filters-brand"
            aria-label="Фільтр"
          >
            Фільтр
          </a>
          <MobileFilterSheet
            id="mobile-filters-brand"
            title="Категорії"
            basePath={`/catalog/${brandSlug}`}
            activeValue={activeCategorySlug}
            options={categories.map((cat) => ({
              label: cat.name,
              value: cat.slug,
            }))}
          />

          <ProductInfiniteGrid
            initialItems={items}
            total={total}
            perPage={perPage}
            initialOffset={offset}
            query={{
              brand: brandSlug,
              category: activeCategorySlug || undefined,
              compact: true,
            }}
            resetKey={resetKey}
            nextHref={nextHref}
            emptyTitle="Порожньо"
            emptyText="Для цього бренду поки немає товарів за вибраними фільтрами."
          />
        </div>
      </section>
    </main>
  );
}
