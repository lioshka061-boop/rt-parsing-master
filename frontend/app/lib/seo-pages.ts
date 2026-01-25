import { slugify } from './slug';

export type SeoPagePayload = {
  brand?: string;
  model?: string;
  car?: string;
  category?: string;
  topic?: string;
  brand_slug?: string;
  model_slug?: string;
  category_slug?: string;
};

export type SeoPage = {
  id: string;
  page_type: string;
  slug: string;
  path: string;
  title: string;
  h1?: string;
  meta_title?: string;
  meta_description?: string;
  seo_text?: string;
  faq?: string;
  robots?: string;
  canonical?: string;
  indexable: boolean;
  related_links: string[];
  payload: SeoPagePayload;
  product_count: number;
  created_at?: number;
  updated_at?: number;
};

const apiBase =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.NEXT_PUBLIC_SITE_API_KEY || process.env.SITE_API_KEY;
const apiHeaders: HeadersInit = siteApiKey ? { 'x-api-key': siteApiKey } : {};
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

export type SeoPagesQuery = {
  pageType?: string;
  limit?: number;
  offset?: number;
  status?: string;
  indexable?: boolean;
};

function buildFetchInit(mode?: FetchMode): RequestInit {
  const revalidate = mode?.revalidate ?? 0;
  const init: RequestInit = { headers: apiHeaders };
  if (revalidate > 0) {
    init.next = { revalidate };
  } else {
    init.cache = mode?.cache ?? 'no-store';
  }
  return init;
}

function buildSeoPagesUrl(params: SeoPagesQuery): string {
  const query = new URLSearchParams();
  if (params.pageType) query.set('page_type', params.pageType);
  if (typeof params.limit === 'number') query.set('limit', params.limit.toString());
  if (typeof params.offset === 'number') query.set('offset', params.offset.toString());
  if (params.status) query.set('status', params.status);
  if (typeof params.indexable === 'boolean') query.set('indexable', params.indexable ? 'true' : 'false');
  const qs = query.toString();
  return `${apiBase}/api/site/seo_pages${qs ? `?${qs}` : ''}`;
}

export async function loadSeoPages(
  params: SeoPagesQuery = {},
  mode?: FetchMode,
): Promise<SeoPage[]> {
  try {
    const res = await fetchWithTimeout(
      buildSeoPagesUrl(params),
      buildFetchInit(mode),
    );
    if (!res.ok) return [];
    return (await res.json()) as SeoPage[];
  } catch {
    return [];
  }
}

export async function loadSeoPage(
  pageType: string,
  slug: string,
  mode?: FetchMode,
): Promise<SeoPage | null> {
  const normalized = slug?.trim();
  if (!normalized) return null;
  try {
    const res = await fetchWithTimeout(
      `${apiBase}/api/site/seo_pages/${pageType}/${encodeURIComponent(normalized)}`,
      buildFetchInit(mode),
    );
    if (!res.ok) return null;
    return (await res.json()) as SeoPage;
  } catch {
    return null;
  }
}

export function filtersFromPayload(payload: SeoPagePayload) {
  const brand =
    payload.brand_slug || (payload.brand ? slugify(payload.brand) : undefined);
  const model =
    payload.model_slug || (payload.model ? slugify(payload.model) : undefined);
  const category =
    payload.category_slug || (payload.category ? slugify(payload.category) : undefined);
  const rawQuery = payload.car?.trim()
    || [payload.brand, payload.model].filter(Boolean).join(' ').trim()
    || undefined;
  return {
    brand,
    model,
    category,
    query: rawQuery,
  };
}
