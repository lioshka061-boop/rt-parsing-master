import { NextResponse } from 'next/server';

type SitemapEntry = {
  loc: string;
  lastmod: string;
};

const apiBase = process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, '') || 'http://localhost:8080';
const siteApiKey = process.env.SITE_API_KEY;

export const revalidate = 3600; // ISR для зменшення crawl-cost

export async function GET() {
  if (!siteApiKey) {
    return new NextResponse('SITE_API_KEY is required', { status: 500 });
  }
  const headers: HeadersInit = { 'x-api-key': siteApiKey };

  try {
    const res = await fetch(`${apiBase}/api/site/sitemap`, {
      headers,
      next: { revalidate },
    });
    if (res.status === 401 || res.status === 403) {
      return new NextResponse('Unauthorized', { status: 401 });
    }
    if (!res.ok) {
      return new NextResponse('Upstream error', { status: 502 });
    }
    const items = (await res.json()) as SitemapEntry[];
    const urlset = items
      .map(
        (item) => `
  <url>
    <loc>${item.loc}</loc>
    <lastmod>${item.lastmod}</lastmod>
  </url>`,
      )
      .join('');

    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urlset}
</urlset>`;

    return new NextResponse(xml, {
      status: 200,
      headers: {
        'Content-Type': 'application/xml; charset=utf-8',
        'Cache-Control': 's-maxage=3600, stale-while-revalidate=7200',
      },
    });
  } catch {
    return new NextResponse('Upstream error', { status: 502 });
  }
}
