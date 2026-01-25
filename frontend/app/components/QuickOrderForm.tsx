'use client';

import { useMemo, useState, type FormEvent } from 'react';

type Props = {
  article?: string;
  title?: string;
};

const phoneRe = /^\+380\d{9}$/;

function normalizePhone(input: string) {
  const trimmed = input.trim();
  if (!trimmed) return '';
  const digits = trimmed.replace(/[^\d+]/g, '');
  if (digits.startsWith('+')) {
    return digits;
  }
  if (digits.startsWith('380')) {
    return `+${digits}`;
  }
  if (digits.startsWith('0') && digits.length === 10) {
    return `+38${digits}`;
  }
  return digits;
}

export function QuickOrderForm({ article, title }: Props) {
  const [phone, setPhone] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const [status, setStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [message, setMessage] = useState('');

  const normalizedPhone = useMemo(() => normalizePhone(phone), [phone]);
  const isValid = phoneRe.test(normalizedPhone);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isValid) {
      setStatus('error');
      setMessage('Вкажіть номер у форматі +380XXXXXXXXX.');
      return;
    }

    setStatus('loading');
    setMessage('');
    try {
      const res = await fetch('/api/quick_order', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone: normalizedPhone, article, title }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || data?.ok === false) {
        throw new Error('request_failed');
      }
      setStatus('success');
      setMessage('Дякуємо! Менеджер зателефонує для уточнення деталей.');
      setPhone('');
    } catch {
      setStatus('error');
      setMessage('Не вдалося надіслати номер. Спробуйте ще раз.');
    }
  }

  return (
    <div className={`quick-order-wrap ${isOpen ? 'is-open' : 'is-closed'}`}>
      <button
        type="button"
        className="quick-order-trigger"
        onClick={() => {
          setIsOpen((prev) => !prev);
          setStatus('idle');
          setMessage('');
        }}
      >
        Замовити в 1 клік
      </button>
      {isOpen && (
        <div className="quick-order-panel">
          <p className="quick-order-hint">
            Залиште номер — менеджер зателефонує для уточнення деталей.
          </p>
          <form className="quick-order-form" onSubmit={submit}>
            <input
              className="quick-order-input"
              type="tel"
              inputMode="numeric"
              placeholder="+380XXXXXXXXX"
              required
              value={phone}
              onChange={(event) => {
                setStatus('idle');
                setMessage('');
                setPhone(event.target.value);
              }}
            />
            <button className="quick-order-btn" type="submit" disabled={status === 'loading'}>
              {status === 'loading' ? 'Надсилаємо…' : 'Надіслати номер'}
            </button>
          </form>
          {message && (
            <p className={`quick-order-msg ${status === 'error' ? 'error' : 'success'}`}>
              {message}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
