'use client';

import { useMemo, useState } from 'react';
import { AddToCartButton } from './AddToCartButton';
import { FavoriteButton } from './FavoriteButton';
import { QuickOrderForm } from './QuickOrderForm';

type Props = {
  article: string;
  title: string;
  price?: number;
  image?: string;
  path?: string;
};

export function ProductPurchaseActions({ article, title, price, image, path }: Props) {
  const [qty, setQty] = useState(1);

  const safeQty = useMemo(() => {
    if (!Number.isFinite(qty) || qty < 1) return 1;
    if (qty > 99) return 99;
    return Math.floor(qty);
  }, [qty]);

  return (
    <div className="product-cta">
      <div className="product-cta-bar">
        <div className="product-cta-row">
          <div className="qty-control">
            <button
              type="button"
              aria-label="Зменшити кількість"
              onClick={() => setQty((v) => Math.max(1, v - 1))}
            >
              −
            </button>
            <input
              type="number"
              min={1}
              max={99}
              value={safeQty}
              onChange={(event) => {
                const next = Number.parseInt(event.target.value, 10);
                setQty(Number.isFinite(next) ? next : 1);
              }}
            />
            <button
              type="button"
              aria-label="Збільшити кількість"
              onClick={() => setQty((v) => Math.min(99, v + 1))}
            >
              +
            </button>
          </div>
          <AddToCartButton
            article={article}
            title={title}
            price={price}
            image={image}
            path={path}
            label="Додати до кошика"
            className="buy-btn--primary"
            quantity={safeQty}
          />
          <FavoriteButton article={article} title={title} image={image} path={path} />
        </div>
      </div>
      <div className="product-cta-extra">
        <div className="product-cta-row">
          <QuickOrderForm article={article} title={title} />
        </div>
      </div>
    </div>
  );
}
