import { NextResponse } from 'next/server';

const apiBase = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY || process.env.NEXT_PUBLIC_SITE_API_KEY;

export async function GET(request: Request) {
  const url = new URL(request.url);
  const headers: HeadersInit = {};
  if (siteApiKey) headers['x-api-key'] = siteApiKey;
  const target = `${apiBase}/api/site/reviews${url.search}`;

  try {
    const res = await fetch(target, { headers, cache: 'no-store' });
    if (!res.ok) {
      return NextResponse.json([], { status: res.status });
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json([], { status: 502 });
  }
}

export async function POST(request: Request) {
  let payload: Record<string, unknown> | null = null;
  try {
    payload = await request.json();
  } catch {
    payload = null;
  }
  if (!payload) {
    return NextResponse.json({ ok: false, error: 'invalid_payload' }, { status: 400 });
  }

  const headers: HeadersInit = { 'Content-Type': 'application/json' };
  if (siteApiKey) headers['x-api-key'] = siteApiKey;

  try {
    const res = await fetch(`${apiBase}/api/site/reviews`, {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    return NextResponse.json(data, { status: res.status });
  } catch {
    return NextResponse.json({ ok: false, error: 'upstream_unavailable' }, { status: 502 });
  }
}
