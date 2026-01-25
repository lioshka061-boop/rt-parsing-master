'use client';

import { addToCart } from '../lib/cart';
import { useEffect, useState } from 'react';

type Props = {
  article: string;
  title: string;
  price?: number;
  image?: string;
  path?: string;
  label?: string;
  className?: string;
  quantity?: number;
};

export function AddToCartButton({ article, title, price, image, path, label, className, quantity }: Props) {
  const [pulse, setPulse] = useState(false);

  useEffect(() => {
    if (!pulse) return;
    const t = setTimeout(() => setPulse(false), 450);
    return () => clearTimeout(t);
  }, [pulse]);

  return (
    <button
      type="button"
      className={`buy-btn ${className || ''} ${pulse ? 'pulse' : ''}`.trim()}
      onClick={(e) => {
        e.preventDefault();
        addToCart({ article, title, price, image, quantity, path });
        window.dispatchEvent(
          new CustomEvent('cart:add', { detail: { article, title, price, image, quantity, path } }),
        );
        setPulse(true);
      }}
    >
      <i className="ri-shopping-cart-2-line"></i> {label || 'Купити'}
    </button>
  );
}
