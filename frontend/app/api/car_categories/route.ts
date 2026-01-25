import { NextResponse } from 'next/server';
import { getCachedValue, setCachedValue } from '../../lib/server-cache';

const apiBase = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY || process.env.NEXT_PUBLIC_SITE_API_KEY;
const CACHE_TTL_MS = 3_600_000;
const CACHE_CONTROL = 'public, max-age=300, s-maxage=3600, stale-while-revalidate=7200';

export async function GET() {
  if (!siteApiKey) {
    return NextResponse.json([], { status: 500 });
  }
  const cacheKey = 'car_categories';
  const cached = getCachedValue(cacheKey);
  if (cached) {
    const headers = new Headers(cached.headers || {});
    headers.set('Content-Type', 'application/json');
    headers.set('Cache-Control', CACHE_CONTROL);
    return new NextResponse(cached.value, { status: 200, headers });
  }
  const target = `${apiBase}/api/site/car_categories`;
  const headers: HeadersInit = { 'x-api-key': siteApiKey };

  try {
    const res = await fetch(target, { headers, cache: 'no-store' });
    if (!res.ok) {
      return NextResponse.json([], { status: res.status });
    }
    const data = await res.json();
    const body = JSON.stringify(data);
    setCachedValue(cacheKey, body, CACHE_TTL_MS);
    return new NextResponse(body, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': CACHE_CONTROL,
      },
    });
  } catch {
    return NextResponse.json([], { status: 502 });
  }
}
