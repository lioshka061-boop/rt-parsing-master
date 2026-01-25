'use client';

import Link from 'next/link';
import { useState } from 'react';
import { AddToCartButton } from './AddToCartButton';
import { formatPrice as formatCatalogPrice, formatMonthlyInstallment, productLink } from '../lib/products';

export type Recommendation = {
  article: string;
  title: string;
  price?: number;
  image?: string;
  model?: string;
  brand?: string;
  category?: string;
  path?: string;
};

type Props = {
  items: Recommendation[];
};

function truncateWords(text: string, limit = 4) {
  const trimmed = text.trim();
  if (!trimmed) return text;
  const words = trimmed.split(/\s+/);
  if (words.length <= limit) return trimmed;
  return `${words.slice(0, limit).join(' ')}...`;
}

export function ProductRecommendations({ items }: Props) {
  const [expanded, setExpanded] = useState(false);

  if (!items || items.length === 0) return null;

  return (
    <>
      <div className={`reco-grid ${expanded ? 'is-expanded' : 'is-collapsed'}`}>
        {items.map((item) => {
          const installment = formatMonthlyInstallment(item.price);
          return (
            <div key={item.article} className="reco-card">
              <Link
                className="reco-link"
                href={productLink({ article: item.article, path: item.path })}
                prefetch={false}
              >
                <div className="reco-thumb">
                  {item.image ? (
                    <img src={item.image} alt={item.title} loading="lazy" decoding="async" width={180} height={120} />
                  ) : (
                    <span style={{ color: 'var(--muted)' }}>Фото оновлюється</span>
                  )}
                </div>
                <div className="reco-info">
                  <p className="reco-title">{truncateWords(item.title)}</p>
                  <p className="reco-meta">{item.model || item.brand}</p>
                  <div className="reco-footer">
                    <div className="reco-price-group">
                      <span className="reco-price">{formatCatalogPrice(item.price)}</span>
                      {installment && <span className="reco-installment">{installment}</span>}
                    </div>
                    <AddToCartButton
                      article={item.article}
                      title={item.title}
                      price={item.price}
                      image={item.image}
                      path={item.path}
                      label="В кошик"
                      className="reco-buy-btn"
                    />
                  </div>
                </div>
              </Link>
            </div>
          );
        })}
      </div>
      {items.length > 2 && !expanded && (
        <button type="button" className="ghost reco-more mobile-only" onClick={() => setExpanded(true)}>
          Показати більше
        </button>
      )}
    </>
  );
}
