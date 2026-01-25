'use client';

import Link from 'next/link';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { LoadProductsParams, Product } from '../lib/products';
import { formatPrice, formatMonthlyInstallment, plainText, productLink, pickPrimaryImage } from '../lib/products';
import { FavoriteButton } from './FavoriteButton';

type Props = {
  products: Product[];
  title?: string;
  badgeLabel?: string;
  fallbackQuery?: LoadProductsParams;
};

const REQUEST_TIMEOUT_MS = 15000;

function buildQuery(params: LoadProductsParams): string {
  const query = new URLSearchParams();
  if (typeof params.limit === 'number') query.set('limit', params.limit.toString());
  if (typeof params.offset === 'number') query.set('offset', params.offset.toString());
  if (params.brand) query.set('brand', params.brand);
  if (params.model) query.set('model', params.model);
  if (params.category) query.set('category', params.category);
  if (params.query) query.set('q', params.query);
  if (params.compact) query.set('compact', 'true');
  if (params.hit) query.set('hit', 'true');
  return query.toString();
}

async function fetchProductsClient(params: LoadProductsParams): Promise<Product[]> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const qs = buildQuery(params);
    const res = await fetch(`/api/products${qs ? `?${qs}` : ''}`, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) return [];
    return (await res.json()) as Product[];
  } finally {
    clearTimeout(timeout);
  }
}

export function NewArrivalsSlider({
  products,
  title = 'Новинки',
  badgeLabel = 'новинка',
  fallbackQuery,
}: Props) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const [canPrev, setCanPrev] = useState(false);
  const [canNext, setCanNext] = useState(true);
  const [itemsSource, setItemsSource] = useState<Product[]>(products);

  const navLabel = title.toLowerCase();
  const items = useMemo(() => itemsSource.slice(0, 16), [itemsSource]);

  useEffect(() => {
    setItemsSource(products);
  }, [products]);

  useEffect(() => {
    const el = trackRef.current;
    if (!el) return;

    const update = () => {
      const max = el.scrollWidth - el.clientWidth;
      setCanPrev(el.scrollLeft > 4);
      setCanNext(el.scrollLeft < max - 4);
    };

    update();
    el.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update);
    return () => {
      el.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
    };
  }, []);

  useEffect(() => {
    if (itemsSource.length || !fallbackQuery) return;
    let active = true;
    fetchProductsClient(fallbackQuery).then((next) => {
      if (!active || next.length === 0) return;
      setItemsSource(next);
    });
    return () => {
      active = false;
    };
  }, [fallbackQuery, itemsSource.length]);

  const scrollByPage = (dir: -1 | 1) => {
    const el = trackRef.current;
    if (!el) return;
    el.scrollBy({ left: dir * Math.round(el.clientWidth * 0.9), behavior: 'smooth' });
  };

  if (!items.length) return null;

  return (
    <div className="new-arrivals-block" aria-label={title}>
      <div className="new-arrivals-slider">
        <button
          type="button"
          className="new-arrivals-arrow prev"
          aria-label={`Попередні ${navLabel}`}
          onClick={() => scrollByPage(-1)}
          disabled={!canPrev}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
            <path
              fill="currentColor"
              d="M15.5 19a1 1 0 0 1-.7-.29l-6-6a1 1 0 0 1 0-1.42l6-6a1 1 0 1 1 1.4 1.42L10.91 12l5.29 5.29A1 1 0 0 1 15.5 19Z"
            />
          </svg>
        </button>

        <div className="new-arrivals-track" ref={trackRef}>
          {items.map((p) => {
            const href = productLink(p);
            const img = pickPrimaryImage(p.images);
            const installment = formatMonthlyInstallment(p.price);
            return (
              <article className="new-arrivals-card hover-grow" key={p.article}>
                <Link
                  className="new-arrivals-cardLink"
                  href={href}
                  prefetch={false}
                  aria-label={plainText(p.title) || 'Товар'}
                >
                  <div
                    className="thumb new-arrivals-thumb"
                    data-images={p.images?.join('|')}
                    style={img ? { backgroundImage: `url('${img}')` } : undefined}
                  >
                    <span className="new-arrivals-badge">{badgeLabel}</span>
                    {!img && <span className="new-arrivals-noimg">Фото оновлюється</span>}
                  </div>
                  <div className="new-arrivals-body">
                    <h4 className="new-arrivals-title">{plainText(p.title)}</h4>
                    <div className="new-arrivals-price">
                      {formatPrice(p.price)}
                      {installment && <span className="new-arrivals-installment">{installment}</span>}
                    </div>
                  </div>
                </Link>

                <div className="new-arrivals-actions">
                  <Link className="new-arrivals-buy" href={href} prefetch={false}>
                    Купити →
                  </Link>
                  <FavoriteButton article={p.article} title={plainText(p.title)} image={img} path={p.path} />
                </div>
              </article>
            );
          })}
        </div>

        <button
          type="button"
          className="new-arrivals-arrow next"
          aria-label={`Наступні ${navLabel}`}
          onClick={() => scrollByPage(1)}
          disabled={!canNext}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
            <path
              fill="currentColor"
              d="M8.5 19a1 1 0 0 1-.7-1.71L13.09 12 7.8 6.71a1 1 0 1 1 1.4-1.42l6 6a1 1 0 0 1 0 1.42l-6 6a1 1 0 0 1-.7.29Z"
            />
          </svg>
        </button>
      </div>
    </div>
  );
}
