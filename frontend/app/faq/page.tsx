import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'FAQ | O&P Tuning',
  description: 'Відповіді на часті запитання щодо товарів, підбору та доставки.',
};

export default function FaqPage() {
  return (
    <main className="page">
      <section className="home-seo-text">
        <p className="eyebrow">Інформація</p>
        <h1>FAQ</h1>
        <p>
          Додайте відповіді на найчастіші запитання клієнтів: сумісність, строки доставки,
          гарантію та оплату частинами.
        </p>
      </section>
    </main>
  );
}
