'use client';

import { useEffect, useState } from 'react';
import { CartItem, getCart } from '../lib/cart';

type ToastState = {
  item: CartItem | null;
  visible: boolean;
};

export function CartToaster() {
  const [toast, setToast] = useState<ToastState>({ item: null, visible: false });

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent<CartItem>).detail;
      if (!detail) return;
      setToast({ item: detail, visible: true });
      const t = setTimeout(() => setToast((prev) => ({ ...prev, visible: false })), 3200);
      return () => clearTimeout(t);
    };
    window.addEventListener('cart:add', handler as EventListener);
    return () => window.removeEventListener('cart:add', handler as EventListener);
  }, []);

  if (!toast.visible || !toast.item) return null;

  const { item } = toast;
  const total = getCart().reduce((sum, i) => sum + (i.price || 0) * (i.quantity || 1), 0);

  return (
    <div className="cart-toast">
      <div className="cart-toast__head">
        <span className="badge">Додано</span>
        <button
          type="button"
          aria-label="Закрити"
          onClick={() => setToast({ item: null, visible: false })}
        >
          <i className="ri-close-line"></i>
        </button>
      </div>
      <div className="cart-toast__body">
        {item.image ? (
          <img src={item.image} alt={item.title} loading="lazy" decoding="async" />
        ) : (
          <div className="cart-toast__ph">Фото</div>
        )}
        <div>
          <div className="cart-toast__title">{item.title}</div>
          <div className="cart-toast__meta">Артикул: {item.article}</div>
          {item.quantity && item.quantity > 1 && (
            <div className="cart-toast__meta">К-сть: {item.quantity}</div>
          )}
          <div className="cart-toast__price">
            {item.price ? new Intl.NumberFormat('uk-UA').format(item.price) + ' ₴' : 'Ціну уточнюйте'}
          </div>
          <div className="cart-toast__total">Разом у кошику: {new Intl.NumberFormat('uk-UA').format(total)} ₴</div>
        </div>
      </div>
      <div className="cart-toast__actions">
        <a className="ghost" href="/cart">
          Перейти в кошик
        </a>
        <a className="primary" href="/checkout">
          Оформити
        </a>
      </div>
    </div>
  );
}
