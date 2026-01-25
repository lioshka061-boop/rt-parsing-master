import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Постачальникам | O&P Tuning',
  description: 'Інформація для постачальників та партнерів.',
};

export default function SuppliersPage() {
  return (
    <main className="page">
      <section className="home-seo-text">
        <p className="eyebrow">Інформація</p>
        <h1>Постачальникам</h1>
        <p>
          Якщо ви постачальник автотоварів і хочете співпрацювати з O&amp;P Tuning, залиште
          ваші контакти та умови. Ми розглянемо пропозицію та звʼяжемось.
        </p>
        <p>Контакт для пропозицій: partners@example.com</p>
      </section>
    </main>
  );
}
