import { ProductPurchaseActions } from './ProductPurchaseActions';
import { ReviewsSection } from './ReviewsSection';
import { ProductGallery } from './ProductGallery';
import { ItemTabs } from '../item/[slug]/Tabs';
import { formatPrice as formatCatalogPrice, plainText } from '../lib/products';
import { slugify } from '../lib/slug';
import styles from './ProductPageLayout.module.css';
import { ProductRecommendations, type Recommendation } from './ProductRecommendations';

type Availability = {
  text: string;
  className: string;
  note?: string;
};

type ProductView = {
  title?: string;
  h1?: string;
  article: string;
  brand?: string;
  model?: string;
  category?: string;
  images?: string[];
  price?: number;
  description?: string;
  faq?: string;
  years?: string | null;
  availability: Availability;
  path?: string;
};

type Props = {
  product: ProductView;
  recommendations: Recommendation[];
};

function installmentPlan(price?: number) {
  if (!price) return null;
  const total = price * 1.15;
  const monthly = total / 10;
  return { total, monthly };
}

export function ProductPageLayout({ product, recommendations }: Props) {
  const title = plainText(product.h1 || product.title || '');
  const isMaxton =
    /maxton/i.test(product.title || '') ||
    /maxton/i.test(product.article || '') ||
    /maxton/i.test(product.description || '');
  const installment = installmentPlan(product.price);
  const brandSlug = product.brand ? slugify(product.brand) : '';
  const modelSlug = product.model ? slugify(product.model) : '';
  const isAvailable = product.availability.className === 'available';

  const tabs = [
    {
      id: 'desc',
      label: 'Опис',
      content: product.description ? (
        <div dangerouslySetInnerHTML={{ __html: product.description }} />
      ) : (
        <div className="desc-block">Опис буде додано найближчим часом.</div>
      ),
    },
    {
      id: 'reviews',
      label: 'Відгуки',
      content: <ReviewsSection productKey={product.article} isMaxton={isMaxton} />,
    },
    {
      id: 'faq',
      label: 'Питання та відповіді',
      content: product.faq ? (
        <div dangerouslySetInnerHTML={{ __html: product.faq }} />
      ) : (
        <div className="desc-block">Питання та відповіді готуються.</div>
      ),
    },
  ];

  return (
    <main className={styles.shell}>
      <div className={styles.breadcrumbs}>
        <a href="/">Головна</a> / <a href="/catalog">Каталог</a>
        {product.brand && (
          <>
            {' '}
            / <a href={`/catalog/${brandSlug}`}>{product.brand}</a>
          </>
        )}
        {product.model && (
          <>
            {' '}
            / <a href={`/catalog/${brandSlug}/${modelSlug}`}>{product.model}</a>
          </>
        )}
        {title && (
          <>
            {' '}
            / <span>{title}</span>
          </>
        )}
      </div>

      <section className={styles.layout}>
        <div className={styles.left}>
          <div className={styles.media}>
            <ProductGallery images={product.images || []} title={title} />
          </div>
          <div className={styles.tabs}>
            <ItemTabs tabs={tabs} />
          </div>
        </div>

        <aside className={styles.right}>
          <div className={`${styles.infoBlock} info card`}>
            <h1>{title}</h1>
            {product.article && <p className="meta meta-article">Артикул: {product.article}</p>}
            {isMaxton && (
              <div className="maxton-fit">
                <i className="ri-thumb-up-line" aria-hidden="true"></i>
                <span>3D-сканування авто - точна геометрія без зазорів</span>
              </div>
            )}
            {isAvailable && (
              <div className="badges">
                <span className="pill available">Відправимо за 24 години</span>
              </div>
            )}
            <div className="price-line">
              <div className={`price ${product.price ? '' : 'muted'}`}>{formatCatalogPrice(product.price)}</div>
              <ProductPurchaseActions
                article={product.article}
                title={product.title || product.article}
                price={product.price}
                image={product.images?.[0]}
                path={product.path}
              />
            </div>
          </div>
          <div className={`${styles.badgeBlock} info-badges`}>
            <div className="info-badge">
              <div className="info-badge-title">
                <i className="ri-shield-check-line" aria-hidden="true"></i>
                Гарантія на товар
              </div>
              <div className="info-badge-sub">Офіційна гарантія та підтримка</div>
            </div>
            <div className="info-badge">
              <div className="info-badge-title">
                <i className="ri-lock-2-line" aria-hidden="true"></i>
                Безпечна оплата
              </div>
              <div className="info-badge-sub">Захищені транзакції та перевірені сервіси</div>
            </div>
          </div>
          <div className={`${styles.installmentBlock} installment-box card`}>
            <div className="installment-title">Оплата частинами</div>
            <div className="installment-body">
              <div className="installment-highlight">72% клієнтів обрали саме цей платіж</div>
              <div className="installment-logos" aria-hidden="true">
                <img src="/installments/a-bank.jpg" alt="" width={32} height={32} />
                <img src="/installments/pumb.svg" alt="" width={32} height={32} />
                <img src="/installments/mono-lapka(1).png" alt="" width={32} height={32} />
              </div>
              {installment ? (
                <>
                  <div className="installment-monthly">
                    {formatCatalogPrice(installment.monthly)} <span>/ місяць</span>
                  </div>
                  <div className="installment-sub">10 платежів</div>
                  <div className="installment-total">
                    Підсумкова сума: {formatCatalogPrice(installment.total)}
                  </div>
                </>
              ) : (
                <div className="installment-sub">Уточнюйте ціну для розрахунку платежів.</div>
              )}
            </div>
          </div>

          <div className={`${styles.recommendBlock} recommend-box card`}>
            <h3>Рекомендовані товари</h3>
            <ProductRecommendations items={recommendations} />
          </div>
        </aside>
      </section>
    </main>
  );
}
