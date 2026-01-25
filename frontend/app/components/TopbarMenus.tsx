'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { formatPrice, productLink } from '../lib/products';
import { getCart, removeFromCart, incrementCartItem, setCartItemQuantity, type CartItem } from '../lib/cart';
import { slugify } from '../lib/slug';

type FavoriteItem = { article: string; title: string; image?: string; path?: string };
type GarageItem = { brand: string; model?: string };

function readLocal<T>(key: string, fallback: T): T {
  if (typeof window === 'undefined') return fallback;
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function useDropdown() {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onClick = (event: MouseEvent) => {
      if (!ref.current) return;
      if (!ref.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('click', onClick);
    return () => document.removeEventListener('click', onClick);
  }, []);

  return { open, setOpen, ref };
}

function CartMenu() {
  const [items, setItems] = useState<CartItem[]>([]);
  const { open, setOpen, ref } = useDropdown();

  const refresh = useCallback(() => {
    setItems(getCart());
  }, []);

  useEffect(() => {
    refresh();
    const handler = () => refresh();
    window.addEventListener('cart:add', handler as EventListener);
    window.addEventListener('cart:update', handler as EventListener);
    return () => {
      window.removeEventListener('cart:add', handler as EventListener);
      window.removeEventListener('cart:update', handler as EventListener);
    };
  }, [refresh]);

  const total = useMemo(
    () => items.reduce((sum, item) => sum + (item.price || 0) * (item.quantity || 1), 0),
    [items],
  );
  const totalQty = useMemo(
    () => items.reduce((sum, item) => sum + (item.quantity || 1), 0),
    [items],
  );
  const totalLabel = total > 0 ? formatPrice(total) : '0 ₴';

  return (
    <div className={`top-action ${items.length > 0 ? 'has-items' : ''}`} ref={ref}>
      <button type="button" className="action-btn" onClick={() => setOpen((v) => !v)}>
        <i className="ri-shopping-cart-2-line"></i>
        <span>Кошик</span>
        {items.length > 0 && <span className="action-badge">{totalQty}</span>}
      </button>
      {open && (
        <div className="action-dropdown">
          <div className="dropdown-head">Кошик</div>
          {items.length === 0 ? (
            <div className="dropdown-empty">Кошик порожній.</div>
          ) : (
            <div className="dropdown-list">
              {items.map((item) => (
                <div key={item.article} className="dropdown-item">
                  <div className="dropdown-thumb">
                    {item.image ? <img src={item.image} alt={item.title} /> : <span>Фото</span>}
                  </div>
                  <div className="dropdown-info">
                    <div className="dropdown-title">{item.title}</div>
                    <div className="dropdown-meta">
                      {formatPrice(item.price)} {item.quantity && item.quantity > 1 ? `× ${item.quantity}` : ''}
                    </div>
                  </div>
                  <div className="dropdown-actions">
                    <div className="dropdown-qty">
                      <button
                        type="button"
                        aria-label="Зменшити кількість"
                        onClick={() => {
                          incrementCartItem(item.article, -1);
                          window.dispatchEvent(new Event('cart:update'));
                        }}
                      >
                        −
                      </button>
                      <input
                        type="number"
                        min={1}
                        max={99}
                        value={item.quantity || 1}
                        onChange={(event) => {
                          const next = Number.parseInt(event.target.value, 10);
                          setCartItemQuantity(item.article, Number.isFinite(next) ? next : 1);
                          window.dispatchEvent(new Event('cart:update'));
                        }}
                      />
                      <button
                        type="button"
                        aria-label="Збільшити кількість"
                        onClick={() => {
                          incrementCartItem(item.article, 1);
                          window.dispatchEvent(new Event('cart:update'));
                        }}
                      >
                        +
                      </button>
                    </div>
                    <Link
                      className="dropdown-open"
                      href={productLink({ article: item.article, path: item.path })}
                      prefetch={false}
                    >
                      Відкрити
                    </Link>
                    <button
                      type="button"
                      className="dropdown-remove"
                      onClick={() => {
                        removeFromCart(item.article);
                        window.dispatchEvent(new Event('cart:update'));
                      }}
                    >
                      Видалити
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
          {items.length > 0 && (
            <div className="dropdown-footer">
              <div className="dropdown-total">Разом: {totalLabel}</div>
              <Link className="dropdown-primary" href="/cart">
                Перейти в кошик
              </Link>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function FavoritesMenu() {
  const [items, setItems] = useState([] as FavoriteItem[]);
  const { open, setOpen, ref } = useDropdown();

  const refresh = useCallback(() => {
    setItems(readLocal<FavoriteItem[]>('favorites', []));
  }, []);

  useEffect(() => {
    refresh();
    const handler = () => refresh();
    window.addEventListener('favorites:update', handler as EventListener);
    return () => window.removeEventListener('favorites:update', handler as EventListener);
  }, [refresh]);

  return (
    <div className={`top-action ${items.length > 0 ? 'has-items' : ''}`} ref={ref}>
      <button type="button" className="action-btn" onClick={() => setOpen((v) => !v)}>
        <i className="ri-star-line"></i>
        <span>Обране</span>
        {items.length > 0 && <span className="action-badge">{items.length}</span>}
      </button>
      {open && (
        <div className="action-dropdown">
          <div className="dropdown-head">Обране</div>
          {items.length === 0 ? (
            <div className="dropdown-empty">Немає збережених товарів.</div>
          ) : (
            <div className="dropdown-list">
              {items.map((item) => (
                <div key={item.article} className="dropdown-item">
                  <div className="dropdown-thumb">
                    {item.image ? <img src={item.image} alt={item.title} /> : <span>Фото</span>}
                  </div>
                  <div className="dropdown-info">
                    <div className="dropdown-title">{item.title}</div>
                    <div className="dropdown-meta">Артикул: {item.article}</div>
                  </div>
                  <div className="dropdown-actions">
                    <Link
                      className="dropdown-open"
                      href={productLink({ article: item.article, path: item.path })}
                      prefetch={false}
                    >
                      Відкрити
                    </Link>
                    <button
                      type="button"
                      className="dropdown-remove"
                      onClick={() => {
                        const next = items.filter((f) => f.article !== item.article);
                        localStorage.setItem('favorites', JSON.stringify(next));
                        window.dispatchEvent(new Event('favorites:update'));
                      }}
                    >
                      Видалити
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
          {items.length > 0 && (
            <div className="dropdown-footer">
              <span className="dropdown-total">Товарів: {items.length}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function GarageMenu() {
  const [items, setItems] = useState([] as GarageItem[]);
  const { open, setOpen, ref } = useDropdown();

  const refresh = useCallback(() => {
    setItems(readLocal<GarageItem[]>('garageCars', []));
  }, []);

  useEffect(() => {
    refresh();
    const handler = () => refresh();
    window.addEventListener('garage:update', handler as EventListener);
    return () => window.removeEventListener('garage:update', handler as EventListener);
  }, [refresh]);

  return (
    <div className={`top-action ${items.length > 0 ? 'has-items' : ''}`} ref={ref}>
      <button type="button" className="action-btn" onClick={() => setOpen((v) => !v)}>
        <i className="ri-car-line"></i>
        <span>Гараж</span>
        {items.length > 0 && <span className="action-badge">{items.length}</span>}
      </button>
      {open && (
        <div className="action-dropdown">
          <div className="dropdown-head">Гараж</div>
          {items.length === 0 ? (
            <div className="dropdown-empty">Немає збережених авто.</div>
          ) : (
            <div className="dropdown-list">
              {items.map((item, idx) => {
                const brandSlug = slugify(item.brand || '');
                const modelSlug = slugify(item.model || '');
                const href = modelSlug ? `/catalog/${brandSlug}/${modelSlug}` : `/catalog/${brandSlug}`;
                return (
                  <div key={`${item.brand}-${item.model || idx}`} className="dropdown-item">
                    <div className="dropdown-thumb car">
                      <i className="ri-car-line"></i>
                    </div>
                    <div className="dropdown-info">
                      <div className="dropdown-title">{item.brand}</div>
                      <div className="dropdown-meta">{item.model || 'Без моделі'}</div>
                    </div>
                    <div className="dropdown-actions">
                      <Link className="dropdown-open" href={href}>
                        Відкрити
                      </Link>
                      <button
                        type="button"
                        className="dropdown-remove"
                        onClick={() => {
                          const next = items.filter((_, i) => i !== idx);
                          localStorage.setItem('garageCars', JSON.stringify(next));
                          window.dispatchEvent(new Event('garage:update'));
                        }}
                      >
                        Видалити
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

type TopbarMenusProps = {
  variant?: 'full' | 'mobile';
};

export function TopbarMenus({ variant = 'full' }: TopbarMenusProps) {
  if (variant === 'mobile') {
    return <CartMenu />;
  }
  return (
    <>
      <FavoritesMenu />
      <GarageMenu />
      <CartMenu />
    </>
  );
}
