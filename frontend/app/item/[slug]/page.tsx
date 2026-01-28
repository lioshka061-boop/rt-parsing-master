import '../../globals.css';
import type { Metadata } from 'next';
import { cache } from 'react';
import { notFound, permanentRedirect } from 'next/navigation';
import { ProductPageLayout } from '../../components/ProductPageLayout';
import {
  loadProduct,
  loadProducts,
  plainText,
  Product as CatalogProduct,
  resolveAvailability,
} from '../../lib/products';
import { productYears } from '../../lib/seo';
import { slugify } from '../../lib/slug';

type Product = CatalogProduct & {
  description?: string;
  h1?: string;
  canonical?: string;
  robots?: string;
  meta_title?: string;
  meta_description?: string;
  seo_text?: string;
  og_title?: string;
  og_description?: string;
  og_image?: string;
  slug?: string;
  indexing_status?: string;
  path?: string;
  indexable?: boolean;
  faq?: string;
};

type RecommendedItem = {
  id: string;
  article: string;
  title: string;
  price?: number;
  image?: string;
  carModel?: string;
  category?: string;
  brand?: string;
  path?: string;
};

function extractArticleFromSlug(slug: string): string | null {
  const encodedSuffix = slug.match(/-([^/]+)$/);
  if (encodedSuffix?.[1] && /%[0-9A-Fa-f]{2}/.test(encodedSuffix[1])) {
    const decodedSuffix = decodeURIComponent(encodedSuffix[1]);
    if (decodedSuffix.trim()) return decodedSuffix;
  }
  const decoded = decodeURIComponent(slug);
  const upperSuffix = decoded.match(/([A-Z0-9]+(?:-[A-Z0-9]+)+)$/);
  if (upperSuffix?.[1]) return upperSuffix[1];
  if (decoded.includes('--')) {
    const candidate = decoded.split('--').pop() || '';
    return candidate.trim() ? candidate.trim() : null;
  }
  const last = decoded.split('-').pop() || '';
  if (!last) return null;
  if (/^[a-z0-9+]+$/i.test(last) && /[A-Z]/.test(last)) return last;
  if (/[+]/.test(last) && /^[a-z0-9+]+$/i.test(last)) return last;
  if (!/[0-9]/.test(last)) return null;
  if (!/^[a-z0-9]+$/i.test(last)) return null;
  return last;
}

function buildArticleCandidates(slug: string): string[] {
  const decoded = decodeURIComponent(slug);
  const tokens = decoded.split('-').filter(Boolean);
  if (tokens.length < 2) return [];
  const candidates: string[] = [];
  let suffix = '';
  for (let i = tokens.length - 1; i >= 0; i -= 1) {
    suffix = suffix ? `${tokens[i]}-${suffix}` : tokens[i];
    if (suffix.length >= 4 && /[0-9]/.test(suffix)) {
      candidates.push(suffix);
    }
    if (candidates.length >= 8) break;
  }
  return candidates.reverse();
}

const resolveProduct = cache(async (paramsSlug: string) => {
  const decoded = decodeURIComponent(paramsSlug);
  const articleFromSlug = extractArticleFromSlug(paramsSlug);
  const loadRecommendations = async (product: Product) => {
    const brand = product.brand || undefined;
    const model = product.model || undefined;
    const pool = await loadProducts(
      {
        limit: 30,
        brand,
        model,
        compact: true,
      },
      { revalidate: 300 },
    );
    if (pool.length > 0) return pool;
    return loadProducts({ limit: 30, compact: true }, { revalidate: 300 });
  };

  if (articleFromSlug) {
    const direct = await loadProduct(articleFromSlug, { revalidate: 300 });
    if (direct) {
      const catalog = await loadRecommendations(direct as Product);
      return { product: direct as Product, catalog };
    }
  } else {
    const direct = await loadProduct(decoded, { revalidate: 300 });
    if (direct) {
      const catalog = await loadRecommendations(direct as Product);
      return { product: direct as Product, catalog };
    }
  }

  const articleCandidates = buildArticleCandidates(paramsSlug).filter(
    (candidate) => !articleFromSlug || candidate.toLowerCase() !== articleFromSlug.toLowerCase(),
  );
  for (const candidate of articleCandidates) {
    const direct = await loadProduct(candidate, { revalidate: 300 });
    if (direct) {
      const catalog = await loadRecommendations(direct as Product);
      return { product: direct as Product, catalog };
    }
  }

  let match: Product | undefined;
  const decodedLower = decoded.toLowerCase();
  const searchQuery = articleFromSlug || decoded;
  const searchPool = await loadProducts(
    { limit: 30, query: searchQuery, compact: true },
    { revalidate: 300 },
  );
  if (articleFromSlug) {
    match = searchPool.find((p) => p.article.toLowerCase() === articleFromSlug.toLowerCase());
  }
  if (!match) {
    match = searchPool.find((p) => p.article.toLowerCase() === decodedLower);
  }
  if (!match) {
    match = searchPool.find((p) => decodedLower.endsWith(`-${p.article.toLowerCase()}`));
  }
  if (!match) {
    match = searchPool.find((p: Product) => p.slug && p.slug.toLowerCase() === decodedLower);
  }
  if (!match) return null;
  const full = await loadProduct(match.article, { revalidate: 300 });
  const product: Product = full ? { ...match, ...full } : match;
  const catalog = await loadRecommendations(product);
  return { product, catalog };
});

function getRecommendedProducts(current: Product, all: CatalogProduct[]): RecommendedItem[] {
  const sameBrand = (p: CatalogProduct) =>
    slugify(p.brand) === slugify(current.brand) || p.brand.toLowerCase() === current.brand.toLowerCase();
  const sameModel = (p: CatalogProduct) =>
    slugify(p.model) === slugify(current.model) || p.model.toLowerCase() === current.model.toLowerCase();
  const sameCategory = (p: CatalogProduct) => {
    const currentCat = current.category || '';
    const nextCat = p.category || '';
    if (!currentCat || !nextCat) return false;
    return slugify(currentCat) === slugify(nextCat);
  };

  const base = all.filter((p) => p.article !== current.article);
  const sameModelItems = base.filter((p) => sameBrand(p) && sameModel(p));
  const sameModelCategory = sameModelItems.filter((p) => sameCategory(p));
  const filtered =
    sameModelCategory.length > 0 ? sameModelCategory : sameModelItems.length > 0 ? sameModelItems : [];

  return filtered
    .sort((a, b) => a.title.localeCompare(b.title))
    .slice(0, 6)
    .map((p) => ({
    id: p.path || p.article,
    article: p.article,
    title: p.title,
    price: p.price,
    image: p.images?.[0],
    carModel: p.model,
    category: p.category,
    brand: p.brand,
    path: p.path,
    }));
}

function faqSchemaFromHtml(html: string, canonical: string) {
  const qaRegex = /qa-q[^>]*>([^<]+)<.*?qa-a[^>]*>([^<]+)</gis;
  const items = [];
  let match;
  while ((match = qaRegex.exec(html))) {
    const question = plainText(match[1] || '');
    const answer = plainText(match[2] || '');
    if (question && answer) {
      items.push({
        '@type': 'Question',
        name: question,
        acceptedAnswer: { '@type': 'Answer', text: answer },
      });
    }
  }
  if (!items.length) return null;
  return {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: items,
    url: canonical,
  };
}

function productSchema(product: Product, canonical: string, availability: string) {
  const price = product.price ? product.price.toFixed(0) : undefined;
  const images = product.images?.filter(Boolean) || [];
  return {
    '@context': 'https://schema.org',
    '@type': 'Product',
    name: plainText(product.title),
    image: images,
    description: plainText(product.description) || plainText(product.title),
    sku: product.article,
    brand: { '@type': 'Brand', name: product.brand },
    offers: {
      '@type': 'Offer',
      url: canonical,
      priceCurrency: 'UAH',
      price,
      priceValidUntil: new Date(Date.now() + 1000 * 60 * 60 * 24 * 90).toISOString().slice(0, 10),
      itemCondition: 'https://schema.org/NewCondition',
      availability,
    },
  };
}

function breadcrumbSchema(canonical: string, brandSlug: string, modelSlug: string, productTitle: string) {
  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Головна', item: '/' },
      { '@type': 'ListItem', position: 2, name: 'Каталог', item: '/catalog' },
      { '@type': 'ListItem', position: 3, name: 'Марка', item: `/catalog/${brandSlug}` },
      { '@type': 'ListItem', position: 4, name: 'Модель', item: `/catalog/${brandSlug}/${modelSlug}` },
      { '@type': 'ListItem', position: 5, name: productTitle, item: canonical },
    ],
  };
}

export async function generateMetadata({ params }: { params: { slug: string } }): Promise<Metadata> {
  const data = await resolveProduct(params.slug);
  if (!data) {
    return {
      title: 'Товар не знайдено',
      description: 'Сторінка більше не доступна',
    };
  }

  const { product } = data;
  const title = product.meta_title || product.og_title || product.title || 'Товар';
  const description = product.meta_description
    || product.og_description
    || (product.description ? plainText(product.description) : '');
  const indexable = product.indexable === true;
  const canonical = indexable ? product.canonical || undefined : undefined;
  const images = (product.og_image ? [product.og_image] : product.images?.filter(Boolean) || []);
  const robotsValue = product.robots || (indexable ? 'index,follow' : 'noindex,follow');

  return {
    title,
    description,
    alternates: canonical ? { canonical } : undefined,
    robots: robotsValue,
    openGraph: {
      title: product.og_title || title,
      description: product.og_description || description,
      url: canonical,
      type: 'website',
      images: images.length ? images : undefined,
    },
  };
}

export default async function ItemPage({ params }: { params: { slug: string } }) {
  const data = await resolveProduct(params.slug);
  if (!data) notFound();
  const { product, catalog } = data;
  if (product.path && product.path.startsWith('/item/')) {
    const currentPath = `/item/${params.slug}`;
    if (product.path !== currentPath) {
      permanentRedirect(product.path);
    }
  }

  const availability = resolveAvailability(product);
  const canonical = product.canonical || '';
  const indexable = product.indexable === true;
  const brandSlug = slugify(product.brand);
  const modelSlug = slugify(product.model);
  const recommendations = getRecommendedProducts(product, catalog);
  const years = productYears(product)?.replace('_', '–');
  const faqSchema = indexable && product.faq ? faqSchemaFromHtml(product.faq, canonical) : null;
  const schema = indexable
    ? [
        productSchema(product, canonical, availability.schema),
        breadcrumbSchema(canonical, brandSlug, modelSlug, plainText(product.title)),
        ...(faqSchema ? [faqSchema] : []),
      ]
    : [];
  return (
    <>
      <ProductPageLayout
        product={{
          title: product.title,
          h1: product.h1,
          article: product.article,
          brand: product.brand,
          model: product.model,
          category: product.category,
          images: product.images || [],
          price: product.price,
          description: product.description,
          faq: product.faq,
          years,
          availability,
          path: product.path,
        }}
        recommendations={recommendations.map((item) => ({
          article: item.article,
          title: item.title,
          price: item.price,
          image: item.image,
          model: item.carModel,
          brand: item.brand,
          category: item.category,
          path: item.path,
        }))}
      />
      {schema.length > 0 && (
        <section className="schema-block" aria-hidden="true">
          <script
            type="application/ld+json"
            dangerouslySetInnerHTML={{
              __html: JSON.stringify(schema),
            }}
          />
        </section>
      )}
    </>
  );
}
