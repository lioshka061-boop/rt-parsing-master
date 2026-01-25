import { NextResponse } from 'next/server';
import { loadProduct } from '../../../lib/products';

export async function GET(
  request: Request,
  { params }: { params: { article: string } },
) {
  const article = decodeURIComponent(params.article);
  const product = await loadProduct(article, { revalidate: 300 });

  if (!product?.path) {
    return new NextResponse('Not Found', { status: 404 });
  }

  return NextResponse.redirect(new URL(product.path, request.url), 301);
}
