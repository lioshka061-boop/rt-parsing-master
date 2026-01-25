import { NextResponse, type NextRequest } from 'next/server';

const apiBase =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY;

type RateEntry = {
  count: number;
  resetAt: number;
};

const RATE_STORE = new Map<string, RateEntry>();

const BOT_UA_RE = /(curl|python|wget|bot|crawler|spider)/i;

const RATE_LIMITS = {
  global: { max: 100, windowMs: 60_000 },
  search: { max: 20, windowMs: 10_000 },
  cart: { max: 10, windowMs: 10_000 },
  bot: { max: 30, windowMs: 60_000 },
};

function getClientIp(request: NextRequest): string {
  const forwarded = request.headers.get('x-forwarded-for');
  if (forwarded) {
    const first = forwarded.split(',')[0]?.trim();
    if (first) return first;
  }
  return request.ip || 'unknown';
}

function shouldSkipPath(pathname: string): boolean {
  return (
    pathname.startsWith('/_next/') ||
    pathname.startsWith('/favicon') ||
    pathname.startsWith('/robots.txt') ||
    pathname.startsWith('/sitemap.xml')
  );
}

function isSearchPath(pathname: string, search: string): boolean {
  if (pathname.startsWith('/search')) return true;
  if (pathname.startsWith('/api/products')) return true;
  if (pathname === '/catalog' && search) return true;
  return false;
}

function isCartPath(pathname: string): boolean {
  return pathname.startsWith('/cart') || pathname.startsWith('/favorites');
}

function hitRateLimit(key: string, max: number, windowMs: number) {
  const now = Date.now();
  const existing = RATE_STORE.get(key);
  if (!existing || now >= existing.resetAt) {
    RATE_STORE.set(key, { count: 1, resetAt: now + windowMs });
    return { ok: true, retryAfter: windowMs / 1000 };
  }
  existing.count += 1;
  if (existing.count > max) {
    const retryAfter = Math.max(1, Math.ceil((existing.resetAt - now) / 1000));
    return { ok: false, retryAfter };
  }
  return { ok: true, retryAfter: Math.max(1, Math.ceil((existing.resetAt - now) / 1000)) };
}

function enforceRateLimits(request: NextRequest): NextResponse | null {
  const url = request.nextUrl;
  const pathname = url.pathname;
  if (shouldSkipPath(pathname)) return null;

  const ip = getClientIp(request);
  const ua = request.headers.get('user-agent') || '';
  const isBot = BOT_UA_RE.test(ua);

  const checks: Array<{ key: string; max: number; windowMs: number }> = [
    { key: `global:${ip}`, ...RATE_LIMITS.global },
  ];
  if (isBot) {
    checks.unshift({ key: `bot:${ip}`, ...RATE_LIMITS.bot });
  }
  if (isSearchPath(pathname, url.search)) {
    checks.unshift({ key: `search:${ip}`, ...RATE_LIMITS.search });
  }
  if (isCartPath(pathname)) {
    checks.unshift({ key: `cart:${ip}`, ...RATE_LIMITS.cart });
  }

  for (const check of checks) {
    const result = hitRateLimit(check.key, check.max, check.windowMs);
    if (!result.ok) {
      return new NextResponse('Too Many Requests', {
        status: 429,
        headers: {
          'Retry-After': result.retryAfter.toString(),
          'Cache-Control': 'no-store',
        },
      });
    }
  }
  return null;
}

export async function middleware(request: NextRequest) {
  const limited = enforceRateLimits(request);
  if (limited) return limited;

  if (!siteApiKey) return NextResponse.next();
  const { pathname } = request.nextUrl;
  if (!pathname.startsWith('/item/')) return NextResponse.next();
  const segment = pathname.slice('/item/'.length);
  if (!segment) return NextResponse.next();
  if (segment.includes('/')) return NextResponse.next();

  try {
    const decoded = decodeURIComponent(segment);
    const res = await fetch(
      `${apiBase}/api/site/products/${encodeURIComponent(decoded)}`,
      {
        headers: { 'x-api-key': siteApiKey },
        cache: 'no-store',
      },
    );
    if (!res.ok) return NextResponse.next();
    const product = (await res.json()) as { path?: string };
    const path = product?.path;
    if (!path || path === pathname || !path.startsWith('/item/')) {
      return NextResponse.next();
    }
    const url = request.nextUrl.clone();
    url.pathname = path;
    url.search = '';
    return NextResponse.redirect(url, 301);
  } catch {
    return NextResponse.next();
  }
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico|robots.txt|sitemap.xml).*)'],
};
