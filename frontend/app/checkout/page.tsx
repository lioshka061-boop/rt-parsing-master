'use client';

import { useEffect, useState } from 'react';
import { clearCart, getCart, CartItem, removeFromCart } from '../lib/cart';
import { SimpleTopbar } from '../components/SimpleTopbar';

export default function CheckoutPage() {
  const [items, setItems] = useState<CartItem[]>([]);
  const [step, setStep] = useState(2);
  const [error, setError] = useState('');
  const [npError, setNpError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [cityQuery, setCityQuery] = useState('');
  const [cities, setCities] = useState<{ Ref: string; Description: string }[]>([]);
  const [branches, setBranches] = useState<{ Ref: string; Description: string }[]>([]);
  const [loadingCities, setLoadingCities] = useState(false);
  const [loadingBranches, setLoadingBranches] = useState(false);
  const [cityName, setCityName] = useState('');
  const [branchName, setBranchName] = useState('');
  const [cityOpen, setCityOpen] = useState(false);
  const [form, setForm] = useState({
    email: '',
    phone: '',
    lastName: '',
    firstName: '',
    middleName: '',
    delivery: '',
    city: '',
    branch: '',
    comment: '',
    payment: 'cod',
    news: false,
    terms: false,
  });

  useEffect(() => {
    setItems(getCart());
  }, []);

  const total = items.reduce((sum, i) => sum + (i.price || 0) * (i.quantity || 1), 0);
  const progressStep = Math.min(step, 4);
  const stageInfo =
    step === 2
      ? { title: 'Контактні дані', subtitle: 'Отримувач і доставка' }
      : step === 3
        ? { title: 'Оплата', subtitle: 'Вибір способу оплати' }
        : step === 4
          ? { title: 'Перевірка', subtitle: 'Перевірте замовлення' }
          : { title: 'Успіх', subtitle: 'Замовлення прийняте' };

  const onRemove = (article: string) => {
    removeFromCart(article);
    setItems(getCart());
  };

  const update = (key: keyof typeof form, value: string | boolean) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  useEffect(() => {
    if (cityQuery.trim().length < 2) {
      setCities([]);
      setCityOpen(false);
      return;
    }
    const controller = new AbortController();
    const t = window.setTimeout(async () => {
      try {
        setLoadingCities(true);
        setNpError('');
        const res = await fetch(`/api/novaposhta/cities?q=${encodeURIComponent(cityQuery.trim())}`, {
          signal: controller.signal,
        });
        const data = await res.json();
      if (!data?.ok) {
        setNpError('Не вдалося завантажити міста Нової пошти.');
        setCities([]);
        setCityOpen(false);
        return;
      }
      setCities(data.data || []);
      setCityOpen(true);
      } catch {
        if (!controller.signal.aborted) {
        setNpError('Не вдалося завантажити міста Нової пошти.');
        setCities([]);
        setCityOpen(false);
      }
      } finally {
        setLoadingCities(false);
      }
    }, 350);
    return () => {
      controller.abort();
      window.clearTimeout(t);
    };
  }, [cityQuery]);

  useEffect(() => {
    if (!form.city || form.delivery === 'pickup') {
      setBranches([]);
      return;
    }
    const controller = new AbortController();
    const t = window.setTimeout(async () => {
      try {
        setLoadingBranches(true);
        setNpError('');
        const res = await fetch(`/api/novaposhta/warehouses?cityRef=${encodeURIComponent(form.city)}`, {
          signal: controller.signal,
        });
        const data = await res.json();
        if (!data?.ok) {
          setNpError('Не вдалося завантажити відділення Нової пошти.');
          setBranches([]);
          return;
        }
        setBranches(data.data || []);
      } catch {
        if (!controller.signal.aborted) {
          setNpError('Не вдалося завантажити відділення Нової пошти.');
          setBranches([]);
        }
      } finally {
        setLoadingBranches(false);
      }
    }, 200);
    return () => {
      controller.abort();
      window.clearTimeout(t);
    };
  }, [form.city]);

  const goNext = () => {
    setError('');
    if (step === 2) {
      if (!form.phone || !form.lastName || !form.firstName) {
        setError('Заповніть обовʼязкові поля: телефон, прізвище та імʼя.');
        return;
      }
      if (!form.delivery) {
        setError('Оберіть спосіб доставки.');
        return;
      }
      if (form.delivery !== 'pickup' && (!form.city || !form.branch)) {
        setError('Оберіть місто та відділення.');
        return;
      }
      setStep(3);
      return;
    }
    if (step === 3) {
      if (!form.payment) {
        setError('Оберіть спосіб оплати.');
        return;
      }
      setStep(4);
    }
  };

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError('');
    if (!form.terms) {
      setError('Потрібно погодитись з умовами доставки.');
      return;
    }
    if (submitting) return;
    setSubmitting(true);
    try {
      const payload = {
        email: form.email || undefined,
        phone: form.phone,
        last_name: form.lastName,
        first_name: form.firstName,
        middle_name: form.middleName || undefined,
        delivery: form.delivery,
        city_name: cityName || undefined,
        branch_name: branchName || undefined,
        comment: form.comment || undefined,
        payment: form.payment,
        news: form.news,
        items: items.map((item) => ({
          article: item.article,
          title: item.title,
          price: item.price,
          quantity: item.quantity || 1,
        })),
      };
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.ok) {
        setError('Не вдалося створити замовлення. Спробуйте ще раз.');
        return;
      }
      clearCart();
      setItems([]);
      window.dispatchEvent(new Event('cart:update'));
      setStep(5);
    } catch {
      setError('Не вдалося створити замовлення. Спробуйте ще раз.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <>
      <SimpleTopbar />
      <main className="page no-footer">
        <section className="categories">
          <nav className="breadcrumbs checkout-breadcrumbs" aria-label="Breadcrumbs">
            <a href="/">Головна</a>
            <span>/</span>
            <span>Оформлення</span>
          </nav>
          <div className="checkout-progress">
            <div className="progress-circle">{progressStep}/4</div>
            <div className="progress-info">
              <div className="progress-title">{stageInfo.title}</div>
              <div className="progress-sub">{stageInfo.subtitle}</div>
            </div>
            <div className="progress-line" aria-hidden="true"></div>
          </div>
          <div className="section-head">
            <div>
              <p className="eyebrow">Оформлення</p>
              <h2>Підтвердити замовлення</h2>
              <p className="muted">Перевірте товари та заповніть дані.</p>
            </div>
          </div>

        {items.length === 0 ? (
          <div className="not-found">
            <div className="badge">Кошик порожній</div>
            <p>Додайте товари, щоб оформити замовлення.</p>
            <a className="primary" href="/catalog">
              До каталогу
            </a>
          </div>
        ) : (
          <>
            <div className="checkout-panel">
              <div className="checkout-total">
                <span>Разом:</span>
                <strong>{new Intl.NumberFormat('uk-UA').format(total)} ₴</strong>
              </div>
              {step === 2 && (
                <div className="checkout-stage">
                  <h3>Контактні дані</h3>
                  <div className="checkout-grid">
                    <label>
                      Контакти (електронна пошта)
                      <input
                        type="email"
                        name="email"
                        placeholder="example@email.com"
                        value={form.email}
                        onChange={(e) => update('email', e.target.value)}
                      />
                    </label>
                    <label>
                      Телефон *
                      <input
                        type="tel"
                        name="phone"
                        placeholder="+380..."
                        required
                        value={form.phone}
                        onChange={(e) => update('phone', e.target.value)}
                      />
                    </label>
                    <label>
                      Прізвище *
                      <input
                        type="text"
                        name="lastName"
                        required
                        value={form.lastName}
                        onChange={(e) => update('lastName', e.target.value)}
                      />
                    </label>
                    <label>
                      Імʼя *
                      <input
                        type="text"
                        name="firstName"
                        required
                        value={form.firstName}
                        onChange={(e) => update('firstName', e.target.value)}
                      />
                    </label>
                    <label>
                      По батькові
                      <input
                        type="text"
                        name="middleName"
                        value={form.middleName}
                        onChange={(e) => update('middleName', e.target.value)}
                      />
                    </label>
                  </div>
                  <h3>Доставка</h3>
                  <div className="checkout-grid">
                    <label>
                      Спосіб доставки *
                      <select
                        name="delivery"
                        required
                        value={form.delivery}
                        onChange={(e) => {
                          const value = e.target.value;
                          update('delivery', value);
                          if (value === 'pickup') {
                            update('city', '');
                            update('branch', '');
                            setCityQuery('');
                            setCityName('');
                            setBranchName('');
                            setCities([]);
                            setBranches([]);
                            setCityOpen(false);
                          }
                        }}
                      >
                        <option value="">Оберіть спосіб доставки</option>
                        <option value="nova-poshta-branch">Нова пошта (на відділення)</option>
                        <option value="nova-poshta-courier">Нова пошта (курʼєр)</option>
                        <option value="pickup">Самовивіз</option>
                      </select>
                    </label>
                    <label>
                      Місто *
                      <input
                        type="text"
                        placeholder={loadingCities ? 'Завантаження…' : 'Почніть вводити місто'}
                        value={cityQuery}
                        onChange={(e) => {
                          const value = e.target.value;
                          setCityQuery(value);
                          const match = cities.find((c) => c.Description.toLowerCase() === value.toLowerCase());
                          if (match) {
                            update('city', match.Ref);
                            setCityName(match.Description);
                          } else {
                            update('city', '');
                            setCityName('');
                          }
                          update('branch', '');
                          setBranchName('');
                          setCityOpen(true);
                        }}
                        disabled={form.delivery === 'pickup'}
                        onFocus={() => {
                          if (cities.length > 0) setCityOpen(true);
                        }}
                        onBlur={() => {
                          window.setTimeout(() => setCityOpen(false), 150);
                        }}
                      />
                      {cityOpen && cities.length > 0 && !loadingCities && (
                        <div className="city-suggest" role="listbox">
                          {cities.map((city) => (
                            <button
                              key={city.Ref}
                              type="button"
                              className="city-suggest-item"
                              onMouseDown={() => {
                                setCityQuery(city.Description);
                                update('city', city.Ref);
                                setCityName(city.Description);
                                update('branch', '');
                                setBranchName('');
                                setCityOpen(false);
                              }}
                            >
                              {city.Description}
                            </button>
                          ))}
                        </div>
                      )}
                    </label>
                    <label>
                      Відділення *
                      <select
                        name="branch"
                        required
                        value={form.branch}
                        onChange={(e) => {
                          const ref = e.target.value;
                          const match = branches.find((b) => b.Ref === ref);
                          update('branch', ref);
                          setBranchName(match?.Description || '');
                        }}
                        disabled={form.delivery === 'pickup'}
                      >
                        <option value="">{loadingBranches ? 'Завантаження…' : 'Оберіть відділення'}</option>
                        {branches.map((branch) => (
                          <option key={branch.Ref} value={branch.Ref}>
                            {branch.Description}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                  {npError && <div className="checkout-error">{npError}</div>}
                  {error && <div className="checkout-error">{error}</div>}
                  <div className="checkout-actions">
                    <a className="ghost" href="/cart">Назад</a>
                    <button className="primary" type="button" onClick={goNext}>
                      Далі
                    </button>
                  </div>
                </div>
              )}

              {step === 3 && (
                <div className="checkout-stage">
                  <h3>Оплата</h3>
                  <div className="checkout-payments">
                    <div className="checkout-payments-title">Види оплати</div>
                    <label>
                      <input
                        type="radio"
                        name="payment"
                        value="cod"
                        checked={form.payment === 'cod'}
                        onChange={() => update('payment', 'cod')}
                      />
                      Накладений платіж (післяплата)
                    </label>
                    <label>
                      <input
                        type="radio"
                        name="payment"
                        value="wayforpay"
                        checked={form.payment === 'wayforpay'}
                        onChange={() => update('payment', 'wayforpay')}
                      />
                      Оплата онлайн Wayforpay
                      <span className="payment-note">Visa, MasterCard, Apple Pay, Google Pay</span>
                    </label>
                    <label className="payment-installment">
                      <input
                        type="radio"
                        name="payment"
                        value="installments"
                        checked={form.payment === 'installments'}
                        onChange={() => update('payment', 'installments')}
                      />
                      Оплата частинами
                      <span className="payment-note">72% клієнтів обрали цей метод</span>
                      <span className="payment-amount">
                        {new Intl.NumberFormat('uk-UA').format(Math.ceil(total * 1.15 / 10))} ₴/міс.
                      </span>
                    </label>
                    <label>
                      <input
                        type="radio"
                        name="payment"
                        value="invoice"
                        checked={form.payment === 'invoice'}
                        onChange={() => update('payment', 'invoice')}
                      />
                      Оплата на рахунок
                    </label>
                  </div>
                  {error && <div className="checkout-error">{error}</div>}
                  <div className="checkout-actions">
                    <button className="ghost" type="button" onClick={() => setStep(2)}>
                      Назад
                    </button>
                    <button className="primary" type="button" onClick={goNext}>
                      Далі
                    </button>
                  </div>
                </div>
              )}

              {step === 4 && (
                <form className="checkout-stage" onSubmit={submit}>
                  <h3>Перевірка замовлення</h3>
                  <div className="checkout-review">
                    <div>
                      <strong>Отримувач</strong>
                      <div>{form.lastName} {form.firstName} {form.middleName}</div>
                      <div>{form.phone}</div>
                      {form.email && <div>{form.email}</div>}
                    </div>
                    <div>
                      <strong>Доставка</strong>
                      <div>{form.delivery || '—'}</div>
                      <div>{cityName || '—'}</div>
                      <div>{branchName || '—'}</div>
                    </div>
                  </div>
                  <div className="product-grid">
                    {items.map((i) => (
                      <div key={i.article} className="product-card">
                        <div
                          className="thumb"
                          style={
                            i.image ? { backgroundImage: `url('${i.image}')`, backgroundSize: 'cover' } : undefined
                          }
                        />
                        <div className="info">
                          <h4>{i.title}</h4>
                          <p className="meta">Артикул: {i.article}</p>
                          <div className="price-row">
                            <span className="price">
                              {i.price ? `${i.price} ₴` : 'Ціну уточнюйте'}
                              {i.quantity && i.quantity > 1 ? ` × ${i.quantity}` : ''}
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                  <label className="checkout-full">
                    Коментар
                    <textarea
                      name="comment"
                      placeholder="Коментар до замовлення"
                      rows={3}
                      value={form.comment}
                      onChange={(e) => update('comment', e.target.value)}
                    />
                  </label>
                  <div className="checkout-checks">
                    <label>
                      <input
                        type="checkbox"
                        name="news"
                        checked={form.news}
                        onChange={(e) => update('news', e.target.checked)}
                      />
                      Я хочу отримувати інформацію про товари, новинки та акції на пошту
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        name="terms"
                        checked={form.terms}
                        onChange={(e) => update('terms', e.target.checked)}
                        required
                      />
                      Я згоден з умовами доставки
                    </label>
                  </div>
                  {error && <div className="checkout-error">{error}</div>}
                  <div className="checkout-actions">
                    <button className="ghost" type="button" onClick={() => setStep(3)}>
                      Назад
                    </button>
                    <button className="primary" type="submit" disabled={submitting}>
                      {submitting ? 'Надсилаємо…' : 'ОФОРМИТИ ЗАМОВЛЕННЯ'}
                    </button>
                  </div>
                </form>
              )}

              {step === 5 && (
                <div className="checkout-stage checkout-thanks">
                  <h3>Дякуємо за замовлення!</h3>
                  <p>Ми отримали вашу заявку та звʼяжемось найближчим часом.</p>
                  <a className="primary" href="/catalog">Повернутись до каталогу</a>
                </div>
              )}
            </div>
          </>
        )}
        </section>
      </main>
    </>
  );
}
