'use client';

import { useEffect, useMemo, useState } from 'react';
import { loadReviews, Review, submitReview } from '../lib/reviews';

type Props = {
  productKey: string;
  isMaxton?: boolean;
};

export function ReviewsSection({ productKey, isMaxton }: Props) {
  const key = useMemo(
    () => (isMaxton ? 'maxton_global' : productKey),
    [productKey, isMaxton],
  );
  const [items, setItems] = useState<Review[]>([]);
  const [name, setName] = useState('');
  const [text, setText] = useState('');
  const [rating, setRating] = useState(5);
  const [photos, setPhotos] = useState<string[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  const formatDate = (value: string | number) => {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const year = date.getUTCFullYear();
    return `${day}.${month}.${year}`;
  };

  useEffect(() => {
    let active = true;
    setLoading(true);
    loadReviews({ product: key, limit: 50 })
      .then((data) => {
        if (!active) return;
        setItems(data);
      })
      .catch(() => {
        if (!active) return;
        setError('Не вдалося завантажити відгуки.');
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [key]);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const cleanName = name.trim();
    const cleanText = text.trim();
    if (!cleanName || !cleanText) {
      setError('Заповніть імʼя та відгук.');
      return;
    }
    if (rating < 1 || rating > 5) {
      setError('Оберіть оцінку від 1 до 5.');
      return;
    }
    setSubmitting(true);
    const res = await submitReview({
      product: key,
      name: cleanName,
      text: cleanText,
      rating,
      photos,
    });
    if (!res.ok || !res.review) {
      setError('Не вдалося зберегти відгук.');
      setSubmitting(false);
      return;
    }
    setItems((prev) => (res.review ? [res.review, ...prev] : prev));
    setName('');
    setText('');
    setRating(5);
    setPhotos([]);
    setError('');
    setSubmitting(false);
  }

  function readFiles(files: FileList | null) {
    if (!files || files.length === 0) return;
    const picks = Array.from(files).slice(0, 3);
    const oversize = picks.find((file) => file.size > 1024 * 1024);
    if (oversize) {
      setError('Фото мають бути до 1MB кожне.');
      return;
    }
    Promise.all(
      picks.map(
        (file) =>
          new Promise<string>((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result || ''));
            reader.onerror = () => reject(new Error('read_failed'));
            reader.readAsDataURL(file);
          }),
      ),
    )
      .then((urls) => {
        setPhotos(urls);
        setError('');
      })
      .catch(() => {
        setError('Не вдалося завантажити фото.');
      });
  }

  return (
    <div className="reviews">
      <div className="reviews-head">
        <div>
          <h3>Відгуки</h3>
          {isMaxton && <p>Спільні для всієї продукції Maxton.</p>}
        </div>
        <div className="reviews-count">{items.length} відгуків</div>
      </div>

      {loading ? (
        <div className="reviews-empty">Завантажуємо відгуки…</div>
      ) : items.length === 0 ? (
        <div className="reviews-empty">Поки що немає відгуків. Будьте першим!</div>
      ) : (
        <div className="reviews-list">
          {items.map((item) => (
            <div key={item.id} className="review-card">
              <div className="review-meta">
                <strong>{item.name}</strong>
                <span>{formatDate(item.createdAt)}</span>
              </div>
              <div className="review-stars" aria-label={`Оцінка ${item.rating} з 5`}>
                {'★'.repeat(item.rating)}
                {'☆'.repeat(5 - item.rating)}
              </div>
              <p>{item.text}</p>
              {item.photos.length > 0 && (
                <div className="review-photos">
                  {item.photos.map((src, idx) => (
                    <img key={`${item.id}-photo-${idx}`} src={src} alt="Фото відгуку" loading="lazy" />
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      <form className="review-form" onSubmit={submit}>
        <div className="review-stars-input">
          <span>Оцінка</span>
          <div className="stars">
            {[1, 2, 3, 4, 5].map((value) => (
              <button
                key={value}
                type="button"
                className={`star ${rating >= value ? 'active' : ''}`}
                onClick={() => setRating(value)}
                aria-label={`Оцінка ${value}`}
              >
                ★
              </button>
            ))}
          </div>
        </div>
        <div className="review-row">
          <input
            type="text"
            placeholder="Ваше імʼя"
            value={name}
            onChange={(event) => setName(event.target.value)}
          />
        </div>
        <div className="review-row">
          <textarea
            rows={3}
            placeholder="Ваш відгук"
            value={text}
            onChange={(event) => setText(event.target.value)}
          />
        </div>
        <div className="review-row">
          <label className="review-upload">
            <input
              type="file"
              accept="image/*"
              multiple
              onChange={(event) => readFiles(event.target.files)}
            />
            Додати фото (до 3 шт.)
          </label>
          {photos.length > 0 && (
            <div className="review-photos preview">
              {photos.map((src, idx) => (
                <img key={`preview-${idx}`} src={src} alt="Превʼю фото" />
              ))}
            </div>
          )}
        </div>
        {error && <div className="review-error">{error}</div>}
        <button type="submit" disabled={submitting}>
          {submitting ? 'Зберігаємо…' : 'Додати відгук'}
        </button>
      </form>
    </div>
  );
}
