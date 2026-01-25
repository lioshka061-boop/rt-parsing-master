'use client';

import { useEffect, useState } from 'react';
import { getCart, removeFromCart, incrementCartItem, setCartItemQuantity, CartItem } from '../lib/cart';
import { SimpleTopbar } from '../components/SimpleTopbar';

export default function CartPage() {
  const [items, setItems] = useState<CartItem[]>([]);

  useEffect(() => {
    setItems(getCart());
  }, []);

  const total = items.reduce((sum, i) => sum + (i.price || 0) * (i.quantity || 1), 0);

  return (
    <>
      <SimpleTopbar />
      <main className="page cart-page">
        <section className="categories">
          <nav className="breadcrumbs checkout-breadcrumbs" aria-label="Breadcrumbs">
            <a href="/">Головна</a>
            <span>/</span>
            <span>Кошик</span>
          </nav>
          <div className="checkout-progress">
            <div className="progress-circle">1/4</div>
            <div className="progress-info">
              <div className="progress-title">Кошик</div>
              <div className="progress-sub">
                Вартість кошика: {new Intl.NumberFormat('uk-UA').format(total)} ₴
              </div>
            </div>
            <div className="progress-line" aria-hidden="true"></div>
          </div>
          <div className="section-head">
            <div>
              <p className="eyebrow">Кошик</p>
              <h2>Мій кошик</h2>
            </div>
          </div>
        {items.length === 0 ? (
          <div className="not-found">
            <div className="badge">Порожньо</div>
            <p>Додайте товари до кошика з каталогу.</p>
          </div>
        ) : (
          <>
            <div className="cart-list">
              {items.map((i) => (
                <div key={i.article} className="cart-item">
                  <div
                    className="cart-thumb"
                    style={
                      i.image ? { backgroundImage: `url('${i.image}')`, backgroundSize: 'cover' } : undefined
                    }
                  />
                  <div className="cart-info">
                    <div className="cart-title">{i.title}</div>
                    <div className="cart-meta">Артикул: {i.article}</div>
                  </div>
                  <div className="cart-qty">
                    <button
                      type="button"
                      aria-label="Зменшити кількість"
                      onClick={() => {
                        incrementCartItem(i.article, -1);
                        setItems(getCart());
                      }}
                    >
                      −
                    </button>
                    <input
                      type="number"
                      min={1}
                      max={99}
                      value={i.quantity || 1}
                      onChange={(event) => {
                        const next = Number.parseInt(event.target.value, 10);
                        setCartItemQuantity(i.article, Number.isFinite(next) ? next : 1);
                        setItems(getCart());
                      }}
                    />
                    <button
                      type="button"
                      aria-label="Збільшити кількість"
                      onClick={() => {
                        incrementCartItem(i.article, 1);
                        setItems(getCart());
                      }}
                    >
                      +
                    </button>
                  </div>
                  <div className="cart-price">
                    {i.price ? `${i.price} ₴` : 'Ціну уточнюйте'}
                  </div>
                  <button
                    className="cart-remove"
                    type="button"
                    onClick={() => {
                      removeFromCart(i.article);
                      setItems(getCart());
                    }}
                  >
                    Видалити
                  </button>
                </div>
              ))}
            </div>
            <div className="cart-summary">
              <div className="cart-total">
                Разом: {new Intl.NumberFormat('uk-UA').format(total)} ₴
              </div>
              <div className="promo-box promo-box--card">
                <label className="promo-box__label" htmlFor="promoCode">Промокод</label>
                <div className="promo-box__input">
                  <input id="promoCode" type="text" name="promo" placeholder="Введіть промокод" />
                  <button type="button" aria-label="Застосувати промокод">{'>'}</button>
                </div>
              </div>
              <a className="primary" href="/checkout">Перейти далі</a>
            </div>
          </>
        )}
        </section>
      </main>
    </>
  );
}
