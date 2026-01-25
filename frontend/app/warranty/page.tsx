import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Гарантія та повернення | O&P Tuning',
  description: 'Правила гарантії, повернення та обміну товарів.',
};

export default function WarrantyPage() {
  return (
    <main className="page">
      <section className="home-seo-text">
        <p className="eyebrow">Інформація</p>
        <h1>Гарантія та повернення</h1>
        <p>
          Розпишіть умови гарантії, обміну та повернення. Вкажіть строки, процедуру та
          контакт для узгодження.
        </p>
        <p>Цей текст має бути реальним перед публікацією.</p>
      </section>
    </main>
  );
}
