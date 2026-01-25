import { NextResponse } from 'next/server';

export async function GET() {
  const siteUrl = process.env.NEXT_PUBLIC_SITE_URL;
  if (!siteUrl) {
    return new NextResponse('NEXT_PUBLIC_SITE_URL is required', { status: 500 });
  }
  const base = siteUrl.replace(/\/$/, '');
  const body = [
    'User-agent: *',
    'Disallow: /cart',
    'Disallow: /checkout',
    'Disallow: /search',
    'Disallow: /favorites',
    'Disallow: /compare',
    'Disallow: /profile',
    'Allow: /',
    `Sitemap: ${base}/sitemap.xml`,
    '',
  ].join('\n');

  return new NextResponse(body, {
    status: 200,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
    },
  });
}
