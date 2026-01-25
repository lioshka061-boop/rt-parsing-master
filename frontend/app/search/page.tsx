import type { Metadata } from 'next';
import { loadProductsWithMeta } from '../lib/products';
import { loadCarCategories, loadProductCategories } from '../lib/categories';
import { SearchFilter } from '../components/SearchFilter';
import { ProductInfiniteGrid } from '../components/ProductInfiniteGrid';

export function generateMetadata(): Metadata {
  return {
    title: 'Пошук товарів',
    description: 'Каталог товарів з фільтрами по бренду, моделі та категорії. Параметричні сторінки не індексуємо.',
    robots: { index: false, follow: true },
  };
}

export default async function SearchPage({
  searchParams,
}: {
  searchParams?: { q?: string; brand?: string; model?: string; pcat?: string; page?: string; perPage?: string };
}) {
  const query = (searchParams?.q || '').trim();
  const brand = (searchParams?.brand || '').toLowerCase();
  const model = (searchParams?.model || '').toLowerCase();
  const productCategory = (searchParams?.pcat || '').toLowerCase();
  const page = Math.max(parseInt(searchParams?.page || '1', 10) || 1, 1);
  const perPage = (() => {
    const v = parseInt(searchParams?.perPage || '15', 10);
    if ([15, 30].includes(v)) return v;
    return 15;
  })();

  const offset = (page - 1) * perPage;
  const [{ items, total }, carCategories, productCategories] = await Promise.all([
    loadProductsWithMeta(
      {
        limit: perPage,
        offset,
        brand: brand || undefined,
        model: model || undefined,
        category: productCategory || undefined,
        query: query || undefined,
        compact: true,
      },
      { revalidate: 60 },
    ),
    loadCarCategories({ revalidate: 900 }),
    loadProductCategories({ revalidate: 900 }),
  ]);
  const resetKey = JSON.stringify({
    q: searchParams?.q || '',
    brand: searchParams?.brand || '',
    model: searchParams?.model || '',
    pcat: searchParams?.pcat || '',
    perPage,
    page,
  });

  return (
    <main className="page">
      <section className="categories">
        <div className="section-head">
          <div>
            <p className="eyebrow">Наша оферта</p>
            <h2>Каталог товарів</h2>
          </div>
          <SearchFilter
            carCategories={carCategories}
            productCategories={productCategories}
            initialQuery={searchParams?.q || ''}
            initialBrand={searchParams?.brand || ''}
            initialModel={searchParams?.model || ''}
            initialProductCategory={searchParams?.pcat || ''}
          />
        </div>
        <ProductInfiniteGrid
          initialItems={items}
          total={total}
          perPage={perPage}
          initialOffset={offset}
          query={{
            brand: brand || undefined,
            model: model || undefined,
            category: productCategory || undefined,
            query: query || undefined,
            compact: true,
          }}
          resetKey={resetKey}
          emptyTitle="Нічого не знайдено"
          emptyText="Спробуйте інший запит або перевірте постачальників."
        />
      </section>
    </main>
  );
}
