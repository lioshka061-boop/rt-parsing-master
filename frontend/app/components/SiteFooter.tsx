import Link from 'next/link';
import { loadCarCategories, loadProductCategories } from '../lib/categories';
import { slugify } from '../lib/slug';
import {
  CONTACT_EMAIL,
  CONTACT_INSTAGRAM,
  CONTACT_PHONE,
  CONTACT_PHONE_DISPLAY,
  CONTACT_TELEGRAM,
  CONTACT_VIBER,
  CONTACT_WHATSAPP,
} from '../lib/site';

const fallbackCategories = [
  { label: 'Обвіси', slug: 'obvisy' },
  { label: 'Бампери', slug: 'bampery' },
  { label: 'Фари / Оптика', slug: 'fary-optyka' },
  { label: 'Дифузори', slug: 'dyfuzory' },
  { label: 'Спойлери', slug: 'spoilery' },
  { label: 'Тюнінг салону', slug: 'tyuning-salonu' },
];

const infoLinks = [
  { label: 'Про нас', href: '/about' },
  { label: 'Доставка та оплата', href: '/delivery' },
  { label: 'Гарантія та повернення', href: '/warranty' },
  { label: 'FAQ', href: '/faq' },
  { label: 'Контакти', href: '/contacts' },
  { label: 'Постачальникам', href: '/suppliers' },
];

const defaultBrands = ['BMW', 'Audi', 'Mercedes', 'Volkswagen'];

export async function SiteFooter() {
  const productCategories = await loadProductCategories({ revalidate: 900 });
  const carCategories = await loadCarCategories({ revalidate: 900 });
  const categoryCandidates: { label: string; slug: string; count: number }[] = [];
  const collectCategories = (nodes: typeof productCategories) => {
    nodes.forEach((cat) => {
      const slug = cat.slug || slugify(cat.name);
      if (slug) {
        categoryCandidates.push({
          label: cat.name,
          slug,
          count: cat.product_count || 0,
        });
      }
      if (cat.children && cat.children.length > 0) {
        collectCategories(cat.children);
      }
    });
  };
  collectCategories(productCategories);
  const uniqueCategories = Array.from(
    new Map(categoryCandidates.map((cat) => [cat.slug, cat])).values(),
  );
  const categoriesSorted = [...uniqueCategories]
    .filter((cat) => cat.slug)
    .sort((a, b) => (b.count || 0) - (a.count || 0) || a.label.localeCompare(b.label));
  const categoryLinks = categoriesSorted.length ? categoriesSorted.slice(0, 5) : fallbackCategories;
  const brandCandidates = carCategories.map((brand) => ({
    label: brand.name,
    slug: brand.slug || slugify(brand.name),
  }));
  const brandMap = new Map<string, { label: string; slug: string }>();
  for (const brand of brandCandidates) {
    if (!brand.slug) continue;
    if (!brandMap.has(brand.slug)) {
      brandMap.set(brand.slug, brand);
    }
  }
  for (const name of defaultBrands) {
    const slug = slugify(name);
    if (!brandMap.has(slug)) {
      brandMap.set(slug, { label: name, slug });
    }
  }
  const brandLinks = Array.from(brandMap.values()).slice(0, 5);
  const year = new Date().getFullYear();

  return (
    <footer className="site-footer">
      <section className="footer-trust" aria-label="Блок довіри">
        <div className="trust-item">
          <i className="ri-shield-check-line" aria-hidden="true"></i>
          <div>
            <strong>Захищена оплата</strong>
            <span>Платежі з TLS і перевіркою</span>
          </div>
        </div>
        <div className="trust-item">
          <i className="ri-truck-line" aria-hidden="true"></i>
          <div>
            <strong>Швидка доставка</strong>
            <span>Відправка по Україні</span>
          </div>
        </div>
        <div className="trust-item">
          <i className="ri-award-line" aria-hidden="true"></i>
          <div>
            <strong>Оригінальна продукція</strong>
            <span>Гарантія та контроль якості</span>
          </div>
        </div>
        <div className="trust-item">
          <i className="ri-customer-service-2-line" aria-hidden="true"></i>
          <div>
            <strong>Підтримка клієнтів</strong>
            <span>Підбір і консультації</span>
          </div>
        </div>
      </section>

      <section className="footer-main">
        <div className="footer-col">
          <h3>Категорії</h3>
          <nav aria-label="Категорії">
            <ul>
              {categoryLinks.map((cat) => (
                <li key={cat.slug}>
                  <Link href={`/catalog?pcat=${encodeURIComponent(cat.slug)}`}>
                    {cat.label}
                  </Link>
                </li>
              ))}
              <li>
                <Link href="/catalog">Інші категорії</Link>
              </li>
            </ul>
          </nav>
        </div>

        <div className="footer-col">
          <h3>Інформація</h3>
          <nav aria-label="Інформація">
            <ul>
              {infoLinks.map((item) => (
                <li key={item.href}>
                  <Link href={item.href}>{item.label}</Link>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        <div className="footer-col">
          <h3>Автомобілі</h3>
          <nav aria-label="Автомобілі та бренди">
            <ul>
              {brandLinks.map((brand) => (
                <li key={brand.slug}>
                  <Link href={`/catalog/${encodeURIComponent(brand.slug)}`}>{brand.label}</Link>
                </li>
              ))}
              <li>
                <Link href="/catalog">Інші бренди</Link>
              </li>
            </ul>
          </nav>
        </div>

        <div className="footer-col">
          <h3>Контакти</h3>
          <address>
            <div>
              <span>Телефон:</span>
              <a href={`tel:${CONTACT_PHONE}`}>{CONTACT_PHONE_DISPLAY}</a>
            </div>
            <div>
              <span>Email:</span>
              <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>
            </div>
            <div>
              <span>Графік:</span>
              <span>Пн–Сб: 9:00–19:00</span>
            </div>
          </address>
          <div className="footer-messengers" aria-label="Месенджери">
            <a
              className="footer-messenger"
              href={CONTACT_TELEGRAM}
              aria-label="Telegram"
              target="_blank"
              rel="noopener noreferrer"
            >
              <i className="ri-telegram-line" aria-hidden="true"></i>
            </a>
            <a className="footer-messenger" href={CONTACT_VIBER} aria-label="Viber">
              <i className="ri-phone-line" aria-hidden="true"></i>
            </a>
            <a
              className="footer-messenger"
              href={CONTACT_WHATSAPP}
              aria-label="WhatsApp"
              target="_blank"
              rel="noopener noreferrer"
            >
              <i className="ri-whatsapp-line" aria-hidden="true"></i>
            </a>
            <a
              className="footer-messenger"
              href={CONTACT_INSTAGRAM}
              aria-label="Instagram"
              target="_blank"
              rel="noopener noreferrer"
            >
              <i className="ri-instagram-line" aria-hidden="true"></i>
            </a>
          </div>
        </div>
      </section>

      <div className="footer-bottom">
        <span>© {year} O&amp;P Tuning</span>
        <nav aria-label="Документи">
          <Link href="/privacy">Політика конфіденційності</Link>
          <Link href="/terms">Умови користування</Link>
        </nav>
      </div>
    </footer>
  );
}
