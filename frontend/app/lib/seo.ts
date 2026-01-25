import { plainText, Product } from './products';
import { slugify } from './slug';

type MinimalProduct = Pick<Product, 'title' | 'brand' | 'model' | 'category' | 'article' | 'description' | 'images' | 'price' | 'available'>;

function trimTo(input: string, max: number): string {
  if (input.length <= max) return input;
  return `${input.slice(0, max - 3).trimEnd()}...`;
}

function normalizeSegment(input?: string | null): string {
  if (!input) return '';
  return slugify(plainText(input))
    .replace(/-/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

function detectProductType(product: MinimalProduct): string {
  const source = `${plainText(product.category)} ${plainText(product.title)}`.toLowerCase();
  if (source.match(/фара|fara|headlight|head lamp|headlamp/)) return 'led_fari';
  if (source.match(/фонар|stop|tail/)) return 'zadnie_fonari';
  if (source.match(/решетк/)) return 'reshetka_radiatora';
  if (source.match(/бампер|bumper/)) return 'bamperset';
  return 'avto_aksessuary';
}

function detectYears(text: string): string {
  const numeric = text.match(/(20\d{2})/g);
  if (numeric && numeric.length >= 2) {
    const first = numeric[0];
    const last = numeric[numeric.length - 1];
    if (first !== last) return `${first}_${last}`;
  }
  const range = text.match(/(20\d{2})\D+(20\d{2})/);
  if (range) return `${range[1]}_${range[2]}`;
  return '';
}

function detectGeneration(text: string): string {
  const match = text.match(/(t\d{1,2}|mk\d{1,2}|px\d{1,2})/i);
  return match ? normalizeSegment(match[1]) : '';
}

function detectFeatures(text: string): string[] {
  const features: string[] = [];
  const lower = text.toLowerCase();
  if (lower.includes('welcome')) features.push('welcome_lights');
  if (lower.includes('matrix')) features.push('matrix_led');
  if (lower.includes('dynamic') || lower.includes('динамич')) features.push('dinamicheskie_povoroty');
  if (lower.includes('black') || lower.includes('черн')) features.push('chernie');
  if (lower.includes('smoke') || lower.includes('smok')) features.push('smoked_lens');
  if (lower.includes('led')) features.push('led');
  return Array.from(new Set(features));
}

export function productYears(product: MinimalProduct): string {
  const combined = `${product.title} ${product.model}`;
  return detectYears(combined);
}

export function buildProductSlug(product: MinimalProduct): string {
  const title = plainText(product.title);
  const combined = `${title} ${product.model} ${product.brand}`;
  const productType = normalizeSegment(detectProductType(product));
  const brand = normalizeSegment(product.brand);
  const model = normalizeSegment(product.model);
  const generation = detectGeneration(combined);
  const years = detectYears(combined);
  const features = detectFeatures(combined).join('_');
  const fallback = normalizeSegment(title) || normalizeSegment(product.article);

  return [productType, brand, model, generation, years, features]
    .filter(Boolean)
    .join('_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '') || fallback;
}

export function buildProductPath(product: MinimalProduct): string {
  const slug = buildProductSlug(product);
  const suffix = product.article ? `-${encodeURIComponent(product.article)}` : '';
  return `/item/${slug}${suffix}`;
}

export function buildProductPathFromMeta(meta: { title: string; article: string; brand?: string; model?: string; category?: string }): string {
  const slug = buildProductSlug({
    title: meta.title,
    article: meta.article,
    brand: meta.brand || '',
    model: meta.model || '',
    category: meta.category,
    images: [],
    price: undefined,
    available: '',
    description: '',
  });
  const suffix = meta.article ? `-${encodeURIComponent(meta.article)}` : '';
  return `/item/${slug}${suffix}`;
}

export function siteBase(): string {
  const base = process.env.NEXT_PUBLIC_SITE_URL?.replace(/\/$/, '');
  if (!base) {
    throw new Error('NEXT_PUBLIC_SITE_URL is required');
  }
  return base;
}

export function canonicalUrl(product: MinimalProduct): string {
  const path = buildProductPath(product);
  const base = siteBase();
  return base ? `${base}${path}` : path;
}

export function buildSeoTitle(product: MinimalProduct): string {
  const title = plainText(product.title);
  const years = detectYears(`${product.title} ${product.model}`);
  const base = title.length > 0 ? title : `${product.brand} ${product.model}`;
  const composed = years ? `${base} ${years.replace('_', '–')}` : base;
  return trimTo(composed, 60);
}

export function buildSeoDescription(product: MinimalProduct): string {
  const brandModel = `${product.brand} ${product.model}`.trim();
  const cleanDesc = plainText(product.description) || plainText(product.title);
  const prefix = cleanDesc.slice(0, 90);
  const price = product.price ? `Ціна від ${product.price} ₴.` : 'Швидка відправка по ЄС.';
  const availability = product.available === 'Available' ? 'В наявності на складі.' : 'Швидке постачання під замовлення.';
  return trimTo(`${prefix}. LED фари для ${brandModel}. ${availability} ${price}`, 158);
}
