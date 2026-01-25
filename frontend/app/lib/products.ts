export type Product = {
  article: string;
  title: string;
  model: string;
  brand: string;
  price?: number;
  available: string;
  url: string;
  images: string[];
  category?: string;
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
  availability?: string;
  stock_status?: string;
  availability_type?: string;
};

export type LoadProductsParams = {
  limit?: number;
  offset?: number;
  brand?: string;
  model?: string;
  category?: string;
  query?: string;
  includeTotal?: boolean;
  compact?: boolean;
  hit?: boolean;
};

const apiBase =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.NEXT_PUBLIC_SITE_API_KEY || process.env.SITE_API_KEY;
const apiHeaders: HeadersInit = siteApiKey ? { 'x-api-key': siteApiKey } : {};
const PRICE_FORMATTER = new Intl.NumberFormat('uk-UA', { maximumFractionDigits: 0 });
const REQUEST_TIMEOUT_MS = 15000;

async function fetchWithTimeout(
  input: RequestInfo | URL,
  init: RequestInit = {},
  timeoutMs = REQUEST_TIMEOUT_MS,
) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

type FetchMode = {
  revalidate?: number;
  cache?: RequestCache;
};

function buildFetchInit(headers: HeadersInit, mode?: FetchMode): RequestInit {
  const revalidate = mode?.revalidate ?? 0;
  const init: RequestInit = { headers };
  if (revalidate > 0) {
    init.next = { revalidate };
  } else {
    init.cache = mode?.cache ?? 'no-store';
  }
  return init;
}

function buildProductsUrl(params: LoadProductsParams): string {
  const query = new URLSearchParams();
  if (typeof params.limit === 'number') query.set('limit', params.limit.toString());
  if (typeof params.offset === 'number') query.set('offset', params.offset.toString());
  if (params.brand) query.set('brand', params.brand);
  if (params.model) query.set('model', params.model);
  if (params.category) query.set('category', params.category);
  if (params.query) query.set('q', params.query);
  if (params.includeTotal) query.set('include_total', 'true');
  if (params.compact) query.set('compact', 'true');
  if (params.hit) query.set('hit', 'true');
  const qs = query.toString();
  return `${apiBase}/api/site/products${qs ? `?${qs}` : ''}`;
}

export async function loadProduct(article: string, mode?: FetchMode): Promise<Product | null> {
  try {
    const articleSegment = encodeURIComponent(article);
    const res = await fetchWithTimeout(
      `${apiBase}/api/site/products/${articleSegment}`,
      buildFetchInit(apiHeaders, mode),
    );
    if (!res.ok) return null;
    return (await res.json()) as Product;
  } catch {
    return null;
  }
}

export async function loadProducts(
  input: number | LoadProductsParams = 1000,
  mode?: FetchMode,
): Promise<Product[]> {
  const params = typeof input === 'number' ? { limit: input } : input;
  try {
    const res = await fetchWithTimeout(
      buildProductsUrl(params),
      buildFetchInit(apiHeaders, mode),
    );
    if (!res.ok) throw new Error('Failed');
    return (await res.json()) as Product[];
  } catch {
    return [];
  }
}

export async function loadProductsWithMeta(
  params: LoadProductsParams = {},
  mode?: FetchMode,
): Promise<{ items: Product[]; total: number }> {
  try {
    const res = await fetchWithTimeout(
      buildProductsUrl({ ...params, includeTotal: true }),
      buildFetchInit(apiHeaders, mode),
    );
    if (!res.ok) throw new Error('Failed');
    const items = (await res.json()) as Product[];
    const totalHeader = res.headers.get('x-total-count');
    const total = totalHeader ? Number.parseInt(totalHeader, 10) : items.length;
    return {
      items,
      total: Number.isFinite(total) ? total : items.length,
    };
  } catch {
    return { items: [], total: 0 };
  }
}

export function formatPrice(price?: number) {
  if (!price) return 'Ціну уточнюйте';
  const value = PRICE_FORMATTER.format(price);
  // Фіксуємо однакове відображення на SSR/CSR для символу валюти.
  return `${value} ₴`;
}

export function formatMonthlyInstallment(price?: number): string | null {
  if (!price) return null;
  const monthly = Math.ceil((price * 1.15) / 10);
  return `від ${PRICE_FORMATTER.format(monthly)} ₴/міс.`;
}

export function unique<T>(items: T[]): T[] {
  return Array.from(new Set(items));
}

export function plainText(input?: string | null): string {
  if (!input) return '';
  return input.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

export function productLink(product: Pick<Product, 'article' | 'path'>): string {
  if (product.path && product.path.startsWith('/item/')) return product.path;
  return `/item/${encodeURIComponent(product.article)}`;
}

export type AvailabilityState = {
  status: 'in_stock' | 'on_order' | 'out_of_stock';
  text: string;
  className: string;
  schema: string;
  buttonLabel: string;
  note?: string;
  disabled: boolean;
};

type AvailabilitySource = {
  available?: string;
  availability?: string;
  stock_status?: string;
  availability_type?: string;
  brand?: string;
};

function normalizeAvailability(value?: string | null): string {
  if (!value) return '';
  return value.trim().toLowerCase();
}

function detectAvailabilityStatus(raw: string): AvailabilityState['status'] | null {
  if (!raw) return null;
  if (/(немає|нет|відсут|out of stock|out_of_stock|outofstock|sold out|no stock)/i.test(raw)) {
    return 'out_of_stock';
  }
  if (/(під замовлення|под заказ|preorder|pre-order|on order|backorder|back order|made to order)/i.test(raw)) {
    return 'on_order';
  }
  if (/(в наявності|в наличии|in stock|instock|available)/i.test(raw)) {
    return 'in_stock';
  }
  return null;
}

export function resolveAvailability(product: AvailabilitySource): AvailabilityState {
  const raw =
    product.availability ||
    product.stock_status ||
    product.availability_type ||
    product.available ||
    '';
  const normalized = normalizeAvailability(String(raw));
  const brand = normalizeAvailability(product.brand);
  const isOnOrderBrand = /\b(maxton|jgd|skm)\b/.test(brand);

  let status: AvailabilityState['status'] = 'in_stock';
  const detected = detectAvailabilityStatus(normalized);
  if (detected) {
    status = detected;
  } else if (['disabled', 'out_of_stock', 'outofstock', 'not_available', 'notavailable'].includes(normalized)) {
    status = 'out_of_stock';
  } else if (['on_order', 'onorder', 'preorder'].includes(normalized)) {
    status = 'on_order';
  } else if (['in_stock', 'instock', 'available'].includes(normalized)) {
    status = 'in_stock';
  }

  if (status !== 'out_of_stock' && (status === 'on_order' || isOnOrderBrand)) {
    status = 'on_order';
  }

  if (status === 'on_order') {
    return {
      status,
      text: 'Під замовлення',
      className: 'on-order',
      schema: 'https://schema.org/PreOrder',
      buttonLabel: 'Під замовлення',
      note: 'Виготовлення 1–2 дні',
      disabled: false,
    };
  }
  if (status === 'out_of_stock') {
    return {
      status,
      text: 'Немає в наявності',
      className: 'not-available',
      schema: 'https://schema.org/OutOfStock',
      buttonLabel: 'Немає в наявності',
      disabled: true,
    };
  }
  return {
    status: 'in_stock',
    text: 'В наявності',
    className: 'available',
    schema: 'https://schema.org/InStock',
    buttonLabel: 'Купити',
    note: 'Відправка сьогодні / 24 години',
    disabled: false,
  };
}

export function pickPrimaryImage(images?: string[]): string | undefined {
  if (!images || images.length === 0) return undefined;
  return images.find(Boolean);
}
