import { NextResponse } from 'next/server';

const apiBase = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY || process.env.NEXT_PUBLIC_SITE_API_KEY;

export async function POST(request: Request) {
  let payload: { phone?: string; article?: string; title?: string } | null = null;
  try {
    payload = await request.json();
  } catch {
    payload = null;
  }

  if (!payload?.phone) {
    return NextResponse.json({ ok: false, error: 'missing_phone' }, { status: 400 });
  }

  const headers: HeadersInit = { 'Content-Type': 'application/json' };
  if (siteApiKey) headers['x-api-key'] = siteApiKey;

  try {
    const res = await fetch(`${apiBase}/api/site/quick_order`, {
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
