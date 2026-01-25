import '../globals.css';
import { loadSeoPages } from '../lib/seo-pages';
import { plainText } from '../lib/products';

const PAGE_SIZE = 24;

function excerpt(value: string, max = 140) {
  const normalized = plainText(value);
  if (normalized.length <= max) return normalized;
  return `${normalized.slice(0, max).trim()}…`;
}

export default async function BlogPage() {
  const items = await loadSeoPages(
    { pageType: 'guides', limit: PAGE_SIZE, status: 'published' },
    { revalidate: 3600 },
  );

  return (
    <main className="page">
      <section className="articles">
        <div className="section-head">
          <div>
            <p className="eyebrow">Блог</p>
            <h1>Останні матеріали</h1>
          </div>
          <a className="ghost" href="/catalog">
            До каталогу
          </a>
        </div>

        {items.length === 0 ? (
          <div className="reviews-empty">Поки що немає матеріалів.</div>
        ) : (
          <div className="article-grid">
            {items.map((item) => (
              <article key={item.id}>
                <p className="eyebrow">Гайд</p>
                <h3>
                  <a href={item.path}>{item.title}</a>
                </h3>
                <p>{excerpt(item.meta_description || item.seo_text || '') || 'Опис готується.'}</p>
              </article>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
