import '../globals.css';
import { loadSeoPages } from '../lib/seo-pages';
import { plainText } from '../lib/products';

const PAGE_SIZE = 60;

function excerpt(value: string, max = 140) {
  const normalized = plainText(value);
  if (normalized.length <= max) return normalized;
  return `${normalized.slice(0, max).trim()}…`;
}

export default async function SeoPagesIndex() {
  const items = await loadSeoPages(
    { pageType: 'tuning,accessories', limit: PAGE_SIZE, status: 'published' },
    { revalidate: 3600 },
  );

  const tuning = items.filter((item) => item.page_type === 'tuning_model');
  const accessories = items.filter((item) => item.page_type === 'accessories_car');

  return (
    <main className="page">
      <section className="articles">
        <div className="section-head">
          <div>
            <p className="eyebrow">SEO</p>
            <h1>Сторінки для просування</h1>
          </div>
          <a className="ghost" href="/catalog">
            До каталогу
          </a>
        </div>

        {items.length === 0 ? (
          <div className="reviews-empty">Поки що немає SEO сторінок.</div>
        ) : (
          <>
            {tuning.length > 0 && (
              <>
                <h2>Тюнінг</h2>
                <div className="article-grid">
                  {tuning.map((item) => (
                    <article key={item.id}>
                      <p className="eyebrow">Тюнінг</p>
                      <h3>
                        <a href={item.path}>{item.title}</a>
                      </h3>
                      <p>{excerpt(item.meta_description || item.seo_text || '') || 'Опис готується.'}</p>
                    </article>
                  ))}
                </div>
              </>
            )}

            {accessories.length > 0 && (
              <>
                <h2>Аксесуари</h2>
                <div className="article-grid">
                  {accessories.map((item) => (
                    <article key={item.id}>
                      <p className="eyebrow">Аксесуари</p>
                      <h3>
                        <a href={item.path}>{item.title}</a>
                      </h3>
                      <p>{excerpt(item.meta_description || item.seo_text || '') || 'Опис готується.'}</p>
                    </article>
                  ))}
                </div>
              </>
            )}
          </>
        )}
      </section>
    </main>
  );
}
