import type { Metadata } from 'next';
import {
  CONTACT_EMAIL,
  CONTACT_INSTAGRAM,
  CONTACT_PHONE,
  CONTACT_PHONE_DISPLAY,
  CONTACT_TELEGRAM,
  CONTACT_VIBER,
  CONTACT_WHATSAPP,
} from '../lib/site';

export const metadata: Metadata = {
  title: 'Контакти | O&P Tuning',
  description: 'Контактні дані для звʼязку з менеджерами та підтримкою.',
};

export default function ContactsPage() {
  return (
    <main className="page">
      <section className="home-seo-text">
        <p className="eyebrow">Інформація</p>
        <h1>Контакти</h1>
        <p>
          Телефон: <a href={`tel:${CONTACT_PHONE}`}>{CONTACT_PHONE_DISPLAY}</a>
        </p>
        <p>
          Email: <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>
        </p>
        <p>Графік: Пн–Сб, 9:00–19:00</p>
        <p>
          Месенджери:{' '}
          <a href={CONTACT_TELEGRAM} target="_blank" rel="noopener noreferrer">
            Telegram
          </a>
          ,{' '}
          <a href={CONTACT_VIBER}>
            Viber
          </a>
          ,{' '}
          <a href={CONTACT_WHATSAPP} target="_blank" rel="noopener noreferrer">
            WhatsApp
          </a>
          ,{' '}
          <a href={CONTACT_INSTAGRAM} target="_blank" rel="noopener noreferrer">
            Instagram
          </a>
        </p>
      </section>
    </main>
  );
}
