export type Review = {
  id: number;
  name: string;
  text: string;
  rating: number;
  photos: string[];
  createdAt: number;
};

export type ReviewsQuery = {
  product?: string;
  limit?: number;
  offset?: number;
};

export type ReviewCreatePayload = {
  product?: string;
  name: string;
  text: string;
  rating: number;
  photos?: string[];
};

const apiBase =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.NEXT_PUBLIC_SITE_API_KEY || process.env.SITE_API_KEY;
const isBrowser = typeof window !== 'undefined';
const apiHeaders: HeadersInit = !isBrowser && siteApiKey ? { 'x-api-key': siteApiKey } : {};
const REQUEST_TIMEOUT_MS = 15000;

type FetchMode = {
  revalidate?: number;
  cache?: RequestCache;
};

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

function resolveEndpoint(path: string): string {
  if (isBrowser) return `/api/${path}`;
  return `${apiBase}/api/site/${path}`;
}

function buildReviewsUrl(params: ReviewsQuery): string {
  const query = new URLSearchParams();
  if (params.product) query.set('product', params.product);
  if (typeof params.limit === 'number') query.set('limit', params.limit.toString());
  if (typeof params.offset === 'number') query.set('offset', params.offset.toString());
  const qs = query.toString();
  return `${resolveEndpoint('reviews')}${qs ? `?${qs}` : ''}`;
}

export async function loadReviews(
  params: ReviewsQuery = {},
  mode?: FetchMode,
): Promise<Review[]> {
  try {
    const res = await fetchWithTimeout(
      buildReviewsUrl(params),
      buildFetchInit(apiHeaders, mode),
    );
    if (!res.ok) return [];
    return (await res.json()) as Review[];
  } catch {
    return [];
  }
}

export async function submitReview(payload: ReviewCreatePayload): Promise<{
  ok: boolean;
  review?: Review;
  error?: string;
}> {
  try {
    const res = await fetchWithTimeout(resolveEndpoint('reviews'), {
      method: 'POST',
      headers: {
        ...apiHeaders,
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      return { ok: false, error: 'request_failed' };
    }
    return (await res.json()) as { ok: boolean; review?: Review; error?: string };
  } catch {
    return { ok: false, error: 'request_failed' };
  }
}
