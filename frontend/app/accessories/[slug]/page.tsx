import '../../globals.css';
import type { Metadata } from 'next';
import { notFound, permanentRedirect } from 'next/navigation';
import { ProductInfiniteGrid } from '../../components/ProductInfiniteGrid';
import { loadProductsWithMeta, plainText } from '../../lib/products';
import { filtersFromPayload, loadSeoPage } from '../../lib/seo-pages';

const PAGE_TYPE = 'accessories';
const PAGE_LABEL = 'Аксесуари';
const PER_PAGE = 24;

export async function generateMetadata({ params }: { params: { slug: string } }): Promise<Metadata> {
  const page = await loadSeoPage(PAGE_TYPE, params.slug, { revalidate: 3600 });
  if (!page) {
    return {
      title: 'Сторінка не знайдена',
      description: 'Сторінка більше не доступна',
    };
  }
  const title = page.meta_title || page.title || 'SEO сторінка';
  const description = page.meta_description || plainText(page.seo_text || '');
  const canonical = page.indexable ? page.canonical || undefined : undefined;
  const robotsValue = page.robots || (page.indexable ? 'index,follow' : 'noindex,follow');

  return {
    title,
    description,
    alternates: canonical ? { canonical } : undefined,
    robots: robotsValue,
    openGraph: {
      title,
      description,
      url: canonical,
      type: 'website',
    },
  };
}

export default async function AccessoriesSeoPage({ params }: { params: { slug: string } }) {
  const page = await loadSeoPage(PAGE_TYPE, params.slug, { revalidate: 3600 });
  if (!page) notFound();

  const currentPath = `/${PAGE_TYPE}/${params.slug}`;
  if (page.path && page.path !== currentPath) {
    permanentRedirect(page.path);
  }

  const payload = page.payload || {};
  const { brand, model, category, query } = filtersFromPayload(payload);
  const hasPrimaryFilters = Boolean(brand || model || category);
  const hasFilters = hasPrimaryFilters || Boolean(query);
  let items = [] as Awaited<ReturnType<typeof loadProductsWithMeta>>['items'];
  let total = 0;
  let usingQuery = false;
  if (hasPrimaryFilters) {
    const primary = await loadProductsWithMeta(
      {
        limit: PER_PAGE,
        offset: 0,
        brand: brand || undefined,
        model: model || undefined,
        category: category || undefined,
        compact: true,
      },
      { revalidate: 300 },
    );
    items = primary.items;
    total = primary.total;
    if (total === 0 && query) {
      const fallback = await loadProductsWithMeta(
        {
          limit: PER_PAGE,
          offset: 0,
          query,
          compact: true,
        },
        { revalidate: 300 },
      );
      items = fallback.items;
      total = fallback.total;
      usingQuery = true;
    }
  } else if (query) {
    const fallback = await loadProductsWithMeta(
      {
        limit: PER_PAGE,
        offset: 0,
        query,
        compact: true,
      },
      { revalidate: 300 },
    );
    items = fallback.items;
    total = fallback.total;
    usingQuery = true;
  }

  const resetKey = JSON.stringify({
    slug: page.slug,
    brand: usingQuery ? '' : brand || '',
    model: usingQuery ? '' : model || '',
    category: usingQuery ? '' : category || '',
    query: usingQuery ? query || '' : '',
    type: PAGE_TYPE,
  });
  const relatedLinks = (page.related_links || []).filter(Boolean);
  const chips = [payload.brand, payload.model, payload.category]
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value));
  if (chips.length === 0 && payload.car?.trim()) {
    chips.push(payload.car.trim());
  }
  const showPanel = chips.length > 0 || relatedLinks.length > 0 || hasFilters;
  const productQuery = usingQuery
    ? { query: query || undefined, compact: true }
    : {
      brand: brand || undefined,
      model: model || undefined,
      category: category || undefined,
      compact: true,
    };

  return (
    <main className="page seo-page">
      <section className="seo-hero">
        <div className="seo-hero-copy">
          <p className="eyebrow">{PAGE_LABEL}</p>
          <h1>{page.h1 || page.title}</h1>
          {page.seo_text && (
            <div className="desc-block" dangerouslySetInnerHTML={{ __html: page.seo_text }} />
          )}
        </div>
        {showPanel && (
          <aside className="seo-hero-panel">
            {chips.length > 0 && (
              <div className="seo-panel-group">
                <p className="seo-panel-label">Параметри підбору</p>
                <div className="filter-chips">
                  {chips.map((chip) => (
                    <span key={chip} className="filter-chip">{chip}</span>
                  ))}
                </div>
              </div>
            )}
            {hasFilters && (
              <div className="seo-panel-group">
                <p className="seo-panel-label">Підбір товарів</p>
                <div className="seo-panel-stat">Знайдено товарів: {total}</div>
              </div>
            )}
            {relatedLinks.length > 0 && (
              <div className="seo-panel-group">
                <p className="seo-panel-label">Пов’язані сторінки</p>
                <ul className="seo-links">
                  {relatedLinks.map((link) => (
                    <li key={link}>
                      <a href={link}>{link}</a>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </aside>
        )}
      </section>

      {hasFilters && (
        <section>
          <div className="catalog-summary">
            <div>
              <h2>Підібрані товари</h2>
              <p className="muted">Знайдено товарів: {total}</p>
            </div>
          </div>
          <ProductInfiniteGrid
            initialItems={items}
            total={total}
            perPage={PER_PAGE}
            query={productQuery}
            resetKey={resetKey}
            emptyTitle="Порожньо"
            emptyText="Для цієї сторінки поки немає товарів."
          />
        </section>
      )}

      {page.faq && (
        <section className="seo-copy">
          <h2>Питання та відповіді</h2>
          <div className="desc-block" dangerouslySetInnerHTML={{ __html: page.faq }} />
        </section>
      )}
    </main>
  );
}
