import { loadCarCategories, loadProductCategories } from './lib/categories';
import {
  loadProducts,
  pickPrimaryImage,
  plainText,
  Product,
} from './lib/products';
import { slugify } from './lib/slug';
import { NewArrivalsSlider } from './components/NewArrivalsSlider';
import { CategorySlider } from './components/CategorySlider';

const EMPTY_THUMB =
  'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="400" height="300" viewBox="0 0 400 300" fill="none"><rect width="400" height="300" fill="%23f2f5fa"/><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="%235b6474" font-family="Arial" font-size="16">Фото оновлюється</text></svg>';

function matchCategoryImage(products: Product[], name: string) {
  const tokens = plainText(name)
    .toLowerCase()
    .split(/[\s/\\-]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 2);
  if (!tokens.length) return EMPTY_THUMB;

  const match = products.find((product) => {
    const category = plainText(product.category || '').toLowerCase();
    const title = plainText(product.title).toLowerCase();
    return tokens.some((token) => category.includes(token) || title.includes(token));
  });
  return pickPrimaryImage(match?.images) || EMPTY_THUMB;
}


export default async function Home() {
  const [products, hitsProducts, productCategories, carCategories] = await Promise.all([
    loadProducts({ limit: 30, compact: true }, { revalidate: 300 }),
    loadProducts({ limit: 24, compact: true, hit: true }, { revalidate: 300 }),
    loadProductCategories({ revalidate: 900 }),
    loadCarCategories({ revalidate: 900 }),
  ]);

  const categoryCards = productCategories.map((category) => {
    const image = category.image_url?.trim()
      ? category.image_url
      : matchCategoryImage(products, category.name);
    return {
      label: category.name,
      slug: category.slug || slugify(category.name),
      path: category.path,
      image,
    };
  });

  const hits = hitsProducts;
  const newArrivals = products.slice(0, 24);
  const faqSchema = {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: [
      {
        '@type': 'Question',
        name: 'Чи підійде запчастина на моє авто?',
        acceptedAnswer: {
          '@type': 'Answer',
          text: 'Перевіряємо сумісність за маркою, моделлю, роком і комплектацією перед підтвердженням.',
        },
      },
      {
        '@type': 'Question',
        name: 'Скільки триває доставка?',
        acceptedAnswer: {
          '@type': 'Answer',
          text: 'Середній термін 1–2 дні, точні строки залежать від виробника та складу.',
        },
      },
      {
        '@type': 'Question',
        name: 'Чи є гарантія на товар?',
        acceptedAnswer: {
          '@type': 'Answer',
          text: 'Так, на всі позиції діє гарантія виробника та наш сервісний супровід.',
        },
      },
      {
        '@type': 'Question',
        name: 'Чи можна оплатити частинами?',
        acceptedAnswer: {
          '@type': 'Answer',
          text: 'Доступна оплата частинами для більшості товарів — уточнюйте під час замовлення.',
        },
      },
      {
        '@type': 'Question',
        name: 'Чи потрібна установка у сервісі?',
        acceptedAnswer: {
          '@type': 'Answer',
          text: 'Для складних елементів рекомендуємо сервісну установку, щоб зберегти гарантію.',
        },
      },
    ],
  };

  return (
    <main className="page">
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(faqSchema) }}
      />
      <section className="home-hero">
        <div className="home-hero-media" aria-hidden="true">
          <video
            className="home-hero-video"
            autoPlay
            muted
            loop
            playsInline
          >
            <source src="/installments/bmw_g10_site.mp4" type="video/mp4" />
          </video>
        </div>
        <div className="home-hero-content">
          <h1>Тюнінг запчастини для автомобілів — обвіси, фари, оптика, аксесуари</h1>
          <p>
            Підбір під авто, чесні ціни та швидка доставка по Україні — допоможемо знайти
            ідеальну деталь під вашу комплектацію.
          </p>
          <a className="home-hero-cta" href="/catalog">
            Перейти до каталогу
          </a>
        </div>
      </section>

      <section className="home-categories">
        <div className="section-head">
          <div>
            <h2>Основні розділи каталогу</h2>
          </div>
        </div>
        <CategorySlider items={categoryCards} />
        <div className="home-categories-cta">
          <a className="home-categories-link" href="/catalog">
            Весь каталог
          </a>
        </div>
      </section>

      <section id="new" className="new-arrivals">
        <div className="section-head">
          <div>
            <h2>Новинки</h2>
          </div>
        </div>
        <NewArrivalsSlider
          products={newArrivals}
          fallbackQuery={{ limit: 16, compact: true }}
        />
      </section>


      {hits.length > 0 && (
        <section className="home-hits">
          <div className="section-head">
            <div>
              <p className="eyebrow">Хіти продажу</p>
              <h2>Топ-товари, які беруть найчастіше</h2>
            </div>
          </div>
          <NewArrivalsSlider
            products={hits}
            title="Хіти продажу"
            badgeLabel="хіт"
            fallbackQuery={{ limit: 16, compact: true, hit: true }}
          />
        </section>
      )}

      <section className="home-benefits">
        <div className="section-head">
          <div>
            <p className="eyebrow">Переваги</p>
            <h2>Чому нас обирають</h2>
          </div>
        </div>
        <div className="benefits-grid">
          <div className="benefit-card">
            <i className="ri-shield-check-line" aria-hidden="true"></i>
            <div>
              <h3>Оригінальні запчастини</h3>
              <p>Працюємо з офіційними постачальниками та перевіреними брендами.</p>
            </div>
          </div>
          <div className="benefit-card">
            <i className="ri-roadster-line" aria-hidden="true"></i>
            <div>
              <h3>Підбір під авто</h3>
              <p>Підказуємо сумісність за кузовом, роком і комплектацією.</p>
            </div>
          </div>
          <div className="benefit-card">
            <i className="ri-truck-line" aria-hidden="true"></i>
            <div>
              <h3>Доставка 1–2 дні</h3>
              <p>Контролюємо логістику й попереджаємо про строки завчасно.</p>
            </div>
          </div>
          <div className="benefit-card">
            <i className="ri-bank-card-line" aria-hidden="true"></i>
            <div>
              <h3>Оплата частинами</h3>
              <p>Гнучкі варіанти розрахунку для великих замовлень.</p>
            </div>
          </div>
          <div className="benefit-card">
            <i className="ri-customer-service-2-line" aria-hidden="true"></i>
            <div>
              <h3>Підтримка перед покупкою</h3>
              <p>Перевіряємо комплектацію та радимо оптимальний варіант.</p>
            </div>
          </div>
          <div className="benefit-card">
            <i className="ri-award-line" aria-hidden="true"></i>
            <div>
              <h3>Гарантія</h3>
              <p>Офіційні умови повернення та гарантійний супровід.</p>
            </div>
          </div>
        </div>
      </section>

      <section className="home-seo-text">
        <p className="eyebrow">Підбір за моделями</p>
        <h2>Тюнінг для популярних авто</h2>
        <p>
          У нас можна купити тюнінг для{' '}
          <a href="/catalog/bmw/e46">BMW E46</a>,{' '}
          <a href="/catalog/bmw/f10">BMW F10</a>,{' '}
          <a href="/catalog/bmw/g30">BMW G30</a>,{' '}
          <a href="/catalog/audi/a4">Audi A4</a>,{' '}
          <a href="/catalog/audi/a6">Audi A6</a>,{' '}
          <a href="/catalog/mercedes/w204">Mercedes W204</a>,{' '}
          <a href="/catalog/mercedes/w213">Mercedes W213</a>,{' '}
          <a href="/catalog/mercedes/w447">Mercedes W447</a> та багатьох інших моделей.
          Для кожної категорії доступні обвіси, бампери, оптика, решітки та аксесуари з доставкою по Україні.
        </p>
      </section>

      <section className="home-faq">
        <div className="section-head">
          <div>
            <p className="eyebrow">FAQ</p>
            <h2>Питання, які ставлять найчастіше</h2>
          </div>
        </div>
        <div className="faq-list">
          <details open>
            <summary>Чи підійде запчастина на моє авто?</summary>
            <p>Перевіряємо сумісність за маркою, моделлю, роком і комплектацією перед підтвердженням.</p>
          </details>
          <details>
            <summary>Скільки триває доставка?</summary>
            <p>Середній термін 1–2 дні, точні строки залежать від виробника та складу.</p>
          </details>
          <details>
            <summary>Чи є гарантія на товар?</summary>
            <p>Так, на всі позиції діє гарантія виробника та наш сервісний супровід.</p>
          </details>
          <details>
            <summary>Чи можна оплатити частинами?</summary>
            <p>Доступна оплата частинами для більшості товарів — уточнюйте під час замовлення.</p>
          </details>
          <details>
            <summary>Чи потрібна установка у сервісі?</summary>
            <p>Для складних елементів рекомендуємо сервісну установку, щоб зберегти гарантію.</p>
          </details>
        </div>
      </section>

    </main>
  );
}
