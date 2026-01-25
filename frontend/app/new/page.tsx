import type { Metadata } from 'next';
import { loadProductsWithMeta } from '../lib/products';
import { siteBase } from '../lib/seo';
import { ProductInfiniteGrid } from '../components/ProductInfiniteGrid';

export function generateMetadata(): Metadata {
  const base = siteBase();
  const canonicalPath = '/new';
  const canonical = `${base}${canonicalPath}`;
  return {
    title: 'Новинки автотоварів',
    description: 'Нові позиції каталогу тюнінгу та автотоварів з актуальними оновленнями.',
    alternates: { canonical },
    robots: { index: true, follow: true },
  };
}

export default async function NewProductsPage() {
  const perPage = 48;
  const { items: products, total } = await loadProductsWithMeta(
    { limit: perPage, offset: 0, compact: true },
    { revalidate: 300 },
  );
  const resetKey = 'new-products';

  return (
    <main className="page">
      <section className="products">
        <div className="section-head">
          <div>
            <p className="eyebrow">Новинки</p>
            <h2>Щойно додані позиції</h2>
            <p style={{ color: 'var(--muted)' }}>Оновлення після кожного парсингу.</p>
          </div>
        </div>
        <ProductInfiniteGrid
          initialItems={products}
          total={total}
          perPage={perPage}
          query={{ compact: true }}
          resetKey={resetKey}
          emptyTitle="Порожньо"
          emptyText="Нових товарів поки немає. Додайте постачальників або зачекайте оновлення."
        />
      </section>
    </main>
  );
}
