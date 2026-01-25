import { NextResponse } from 'next/server';
import { getCachedValue, setCachedValue } from '../../lib/server-cache';

const apiBase = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY || process.env.NEXT_PUBLIC_SITE_API_KEY;
const CACHE_TTL_MS = 300_000;
const CACHE_CONTROL = 'public, max-age=120, s-maxage=300, stale-while-revalidate=600';

export async function GET(request: Request) {
  if (!siteApiKey) {
    return NextResponse.json([], { status: 500 });
  }
  const url = new URL(request.url);
  const limitRaw = Number.parseInt(url.searchParams.get('limit') || '24', 10);
  if (Number.isFinite(limitRaw) && limitRaw > 30) {
    url.searchParams.set('limit', '30');
  }
  const cacheKey = url.toString();
  const cached = getCachedValue(cacheKey);
  if (cached) {
    const headers = new Headers(cached.headers || {});
    headers.set('Content-Type', 'application/json');
    headers.set('Cache-Control', CACHE_CONTROL);
    return new NextResponse(cached.value, { status: 200, headers });
  }
  const target = `${apiBase}/api/site/products${url.search}`;
  const headers: HeadersInit = {};
  if (siteApiKey) headers['x-api-key'] = siteApiKey;

  try {
    const res = await fetch(target, { headers, cache: 'no-store' });
    const total = res.headers.get('x-total-count');
    if (!res.ok) {
      return NextResponse.json([], { status: res.status });
    }
    const data = await res.json();
    const responseHeaders = new Headers();
    if (total) responseHeaders.set('x-total-count', total);
    responseHeaders.set('Content-Type', 'application/json');
    responseHeaders.set('Cache-Control', CACHE_CONTROL);
    const body = JSON.stringify(data);
    setCachedValue(
      cacheKey,
      body,
      CACHE_TTL_MS,
      total ? { 'x-total-count': total } : undefined,
    );
    return new NextResponse(body, { headers: responseHeaders });
  } catch {
    return NextResponse.json([], { status: 502 });
  }
}
