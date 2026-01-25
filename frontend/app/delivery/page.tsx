import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Доставка та оплата | O&P Tuning',
  description: 'Умови доставки та доступні способи оплати.',
};

export default function DeliveryPage() {
  return (
    <main className="page">
      <section className="home-seo-text">
        <p className="eyebrow">Інформація</p>
        <h1>Доставка та оплата</h1>
        <p>
          Опишіть актуальні способи доставки, строки та можливості оплати. Додайте дані про
          передплату, оплату частинами та перевірку сумісності перед відправкою.
        </p>
        <p>
          Заповніть цей розділ фактичними умовами компанії для прозорої комунікації з клієнтами.
        </p>
      </section>
    </main>
  );
}
