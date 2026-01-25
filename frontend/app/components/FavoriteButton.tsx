'use client';

import { useEffect, useState } from 'react';

type Props = {
  article: string;
  title: string;
  image?: string;
  path?: string;
};

export function FavoriteButton({ article, title, image, path }: Props) {
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    try {
      const stored = localStorage.getItem('favorites');
      if (stored) {
        const list = JSON.parse(stored) as Array<{ article: string }>;
        setSaved(list.some((f) => f.article === article));
      }
    } catch {
      setSaved(false);
    }
  }, [article]);

  const toggle = () => {
    try {
      const stored = localStorage.getItem('favorites');
      const list: Array<{ article: string; title: string; image?: string; path?: string }> = stored ? JSON.parse(stored) : [];
      const exists = list.find((f) => f.article === article);
      let next = list;
      if (exists) {
        next = list.filter((f) => f.article !== article);
        setSaved(false);
      } else {
        next = [...list, { article, title, image, path }];
        setSaved(true);
      }
      localStorage.setItem('favorites', JSON.stringify(next));
      window.dispatchEvent(new Event('favorites:update'));
    } catch {
      /* ignore */
    }
  };

  return (
    <button
      type="button"
      onClick={toggle}
      aria-label={saved ? 'Прибрати з обраних' : 'Додати в обране'}
      className={`favorite-btn ${saved ? 'active' : ''}`}
    >
      {saved ? '★' : '☆'}
    </button>
  );
}
