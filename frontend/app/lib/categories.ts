export type CategoryNode = {
  id: string;
  name: string;
  children: CategoryNode[];
  slug?: string;
  path?: string;
  canonical?: string;
  seo_title?: string;
  seo_description?: string;
  seo_text?: string;
  product_count?: number;
  indexable?: boolean;
  image_url?: string;
};

const apiBase =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY || process.env.NEXT_PUBLIC_SITE_API_KEY;
const serverHeaders: HeadersInit = siteApiKey ? { 'x-api-key': siteApiKey } : {};
const isBrowser = typeof window !== 'undefined';
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

function buildFetchInit(mode?: FetchMode): RequestInit {
  const revalidate = mode?.revalidate ?? 0;
  const init: RequestInit = { headers: isBrowser ? {} : serverHeaders };
  if (!isBrowser && revalidate > 0) {
    init.next = { revalidate };
  } else {
    init.cache = mode?.cache ?? 'no-store';
  }
  return init;
}

function resolveEndpoint(path: 'categories' | 'car_categories') {
  if (isBrowser) return `/api/${path}`;
  return `${apiBase}/api/site/${path}`;
}

export async function loadCategories(mode?: FetchMode): Promise<CategoryNode[]> {
  return loadProductCategories(mode);
}

export async function loadProductCategories(mode?: FetchMode): Promise<CategoryNode[]> {
  try {
    const res = await fetchWithTimeout(
      resolveEndpoint('categories'),
      buildFetchInit(mode),
    );
    if (!res.ok) throw new Error('Failed');
    const data = (await res.json()) as CategoryNode[];
    if (!Array.isArray(data)) return [];
    return data;
  } catch {
    return [];
  }
}

export async function loadCarCategories(mode?: FetchMode): Promise<CategoryNode[]> {
  try {
    const res = await fetchWithTimeout(
      resolveEndpoint('car_categories'),
      buildFetchInit(mode),
    );
    if (!res.ok) throw new Error('Failed');
    return (await res.json()) as CategoryNode[];
  } catch {
    return [];
  }
}

export async function loadModelCategories(
  params: { brand?: string; model?: string },
  mode?: FetchMode,
): Promise<string[]> {
  try {
    const query = new URLSearchParams();
    if (params.brand) query.set('brand', params.brand);
    if (params.model) query.set('model', params.model);
    const qs = query.toString();
    const res = await fetchWithTimeout(
      `${apiBase}/api/site/model_categories${qs ? `?${qs}` : ''}`,
      buildFetchInit(mode),
    );
    if (!res.ok) throw new Error('Failed');
    const data = (await res.json()) as string[];
    return Array.isArray(data) ? data : [];
  } catch {
    return [];
  }
}

export function flattenBrands(tree: CategoryNode[]): string[] {
  return tree.map((c) => c.name);
}

export function findBrand(tree: CategoryNode[], slug: string, slugify: (s: string) => string) {
  return tree.find((b) => (b.slug || slugify(b.name)) === slug);
}

export function findModel(
  brand: CategoryNode | undefined,
  modelSlug: string,
  slugify: (s: string) => string,
) {
  return brand?.children.find((m) => (m.slug || slugify(m.name)) === modelSlug);
}
