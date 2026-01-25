'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { LoadProductsParams, Product } from '../lib/products';
import {
  formatPrice,
  formatMonthlyInstallment,
  plainText,
  productLink,
  pickPrimaryImage,
  resolveAvailability,
} from '../lib/products';
import Link from 'next/link';
import { AddToCartButton } from './AddToCartButton';
import { FavoriteButton } from './FavoriteButton';

type Props = {
  initialItems: Product[];
  total: number;
  perPage: number;
  initialOffset?: number;
  query: Omit<LoadProductsParams, 'limit' | 'offset' | 'includeTotal'>;
  resetKey: string;
  emptyTitle?: string;
  emptyText?: string;
  nextHref?: string | null;
};

const EMPTY_THUMB =
  'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="400" height="300" viewBox="0 0 400 300" fill="none"><rect width="400" height="300" fill="%23f2f5fa"/><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="%235b6474" font-family="Arial" font-size="16">Фото оновлюється</text></svg>';

const REQUEST_TIMEOUT_MS = 15000;

function availabilityRank(product: Product) {
  const availability = resolveAvailability(product);
  if (availability.status === 'in_stock') return 0;
  if (availability.status === 'on_order') return 1;
  return 2;
}

function sortByAvailability(items: Product[]) {
  return [...items].sort((a, b) => {
    const rankDiff = availabilityRank(a) - availabilityRank(b);
    if (rankDiff !== 0) return rankDiff;
    return a.title.localeCompare(b.title);
  });
}

function uniqueByArticle(items: Product[]) {
  const map = new Map<string, Product>();
  items.forEach((item) => {
    const key = (item.article || item.path || item.title).toLowerCase();
    if (!key) return;
    if (!map.has(key)) map.set(key, item);
  });
  return Array.from(map.values());
}

function buildQuery(params: LoadProductsParams): string {
  const query = new URLSearchParams();
  if (typeof params.limit === 'number') query.set('limit', params.limit.toString());
  if (typeof params.offset === 'number') query.set('offset', params.offset.toString());
  if (params.brand) query.set('brand', params.brand);
  if (params.model) query.set('model', params.model);
  if (params.category) query.set('category', params.category);
  if (params.query) query.set('q', params.query);
  if (params.compact) query.set('compact', 'true');
  return query.toString();
}

async function fetchProductsClient(
  params: LoadProductsParams,
): Promise<{ items: Product[]; total?: number }> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const qs = buildQuery(params);
    const res = await fetch(`/api/products${qs ? `?${qs}` : ''}`, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`Fetch failed: ${res.status}`);
    }
    const items = (await res.json()) as Product[];
    const totalHeader = res.headers.get('x-total-count');
    const total = totalHeader ? Number.parseInt(totalHeader, 10) : undefined;
    const normalizedTotal = Number.isFinite(total) ? total : undefined;
    return { items, total: normalizedTotal };
  } finally {
    clearTimeout(timeout);
  }
}

export function ProductInfiniteGrid({
  initialItems,
  total,
  perPage,
  initialOffset = 0,
  query,
  resetKey,
  emptyTitle,
  emptyText,
  nextHref = null,
}: Props) {
  const [items, setItems] = useState<Product[]>(() =>
    sortByAvailability(uniqueByArticle(initialItems)),
  );
  const [offset, setOffset] = useState(initialOffset + initialItems.length);
  const [clientTotal, setClientTotal] = useState(total);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(initialOffset + initialItems.length < total);
  const [error, setError] = useState<string | null>(null);
  const loadingRef = useRef(false);

  const fetchParams = useMemo(
    () => ({
      ...query,
      limit: perPage,
    }),
    [query, perPage],
  );

  useEffect(() => {
    setItems(sortByAvailability(uniqueByArticle(initialItems)));
    const nextOffset = initialOffset + initialItems.length;
    setOffset(nextOffset);
    setClientTotal(total);
    setHasMore(nextOffset < total);
    setLoading(false);
    setError(null);
    loadingRef.current = false;
  }, [resetKey, initialItems, initialOffset, total]);

  useEffect(() => {
    if (items.length || loadingRef.current) return;
    loadingRef.current = true;
    setLoading(true);
    setError(null);
    fetchProductsClient({ ...fetchParams, offset: 0 })
      .then(({ items: next, total: nextTotal }) => {
        if (nextTotal !== undefined) setClientTotal(nextTotal);
        if (!next.length) {
          setHasMore(false);
          return;
        }
        const resolvedTotal = nextTotal ?? Math.max(total, next.length);
        setItems(sortByAvailability(uniqueByArticle(next)));
        setOffset(next.length);
        setHasMore(next.length < resolvedTotal);
      })
      .catch(() => {
        setError('Не вдалося завантажити товари. Спробуйте ще раз.');
      })
      .finally(() => {
        loadingRef.current = false;
        setLoading(false);
      });
  }, [fetchParams, items.length, total]);

  const loadMore = useCallback(() => {
    if (loadingRef.current || loading || !hasMore) return;
    loadingRef.current = true;
    setLoading(true);
    setError(null);
    fetchProductsClient({ ...fetchParams, offset })
      .then(({ items: next, total: nextTotal }) => {
        if (nextTotal !== undefined) {
          setClientTotal(nextTotal);
        }
        if (next.length === 0) {
          setHasMore(false);
          return;
        }
        const resolvedTotal = nextTotal ?? clientTotal;
        setItems((prev) => sortByAvailability(uniqueByArticle([...prev, ...next])));
        setOffset((prev) => {
          const nextOffset = prev + next.length;
          const totalCount = resolvedTotal ?? nextOffset;
          setHasMore(nextOffset < totalCount);
          return nextOffset;
        });
      })
      .catch(() => {
        setError('Не вдалося підвантажити товари. Спробуйте ще раз.');
      })
      .finally(() => {
        loadingRef.current = false;
        setLoading(false);
      });
  }, [clientTotal, fetchParams, hasMore, loading, offset]);

  useEffect(() => {
    if (!hasMore) return;
    const onScroll = () => {
      if (loading || !hasMore) return;
      const nearBottom = window.innerHeight + window.scrollY >= document.body.offsetHeight - 600;
      if (nearBottom) loadMore();
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', onScroll);
    onScroll();
    return () => {
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('resize', onScroll);
    };
  }, [hasMore, loadMore, loading]);

  if (items.length === 0 && !loading) {
    return (
      <div className="product-grid">
        <div className="not-found" style={{ gridColumn: '1 / -1' }}>
          <div className="badge">{emptyTitle || 'Порожньо'}</div>
          <p>{emptyText || 'Товарів поки немає.'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="product-grid">
      {items.map((p) => {
        const img = pickPrimaryImage(p.images);
        const thumb = img || EMPTY_THUMB;
        const installment = formatMonthlyInstallment(p.price);
        return (
          <article className="product-card" key={p.article}>
            <Link className="product-link" href={productLink(p)} prefetch={false}>
              <div className="thumb" data-images={p.images?.join('|')} style={{ backgroundImage: `url('${thumb}')` }} />
              <div className="info">
                <h4>{plainText(p.title)}</h4>
                <p className="meta">
                  {plainText(p.model)} • {plainText(p.brand)}
                </p>
              </div>
            </Link>
            <div className="price-row">
              <div className="price-stack">
                <span className="price">{formatPrice(p.price)}</span>
                {installment && <span className="price-installment">{installment}</span>}
              </div>
              <div className="product-actions">
                <AddToCartButton
                  article={p.article}
                  title={plainText(p.title)}
                  price={p.price}
                  image={p.images?.[0]}
                  path={p.path}
                />
                <FavoriteButton
                  article={p.article}
                  title={plainText(p.title)}
                  image={p.images?.[0]}
                  path={p.path}
                />
              </div>
            </div>
          </article>
        );
      })}
      {loading && (
        <div className="not-found" style={{ gridColumn: '1 / -1' }}>
          <div className="badge">Завантаження…</div>
          <p>Підвантажуємо більше товарів.</p>
        </div>
      )}
      {error && !loading && (
        <div className="not-found" style={{ gridColumn: '1 / -1' }}>
          <div className="badge">Помилка</div>
          <p>{error}</p>
          <button className="primary" type="button" onClick={loadMore}>
            Спробувати ще
          </button>
        </div>
      )}
      {hasMore && nextHref && !loading && !error && (
        <div className="catalog-more" style={{ gridColumn: '1 / -1' }}>
          <a className="ghost" href={nextHref} rel="next">
            Показати ще
          </a>
        </div>
      )}
      {!hasMore && items.length > 0 && (
        <div className="not-found" style={{ gridColumn: '1 / -1' }}>
          <div className="badge">Все показано</div>
          <p>Більше товарів немає.</p>
        </div>
      )}
    </div>
  );
}
