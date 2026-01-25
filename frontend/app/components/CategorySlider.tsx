'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

type CategoryCard = {
  label: string;
  slug: string;
  path?: string;
  image: string;
};

type Props = {
  items: CategoryCard[];
};

export function CategorySlider({ items }: Props) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const [canPrev, setCanPrev] = useState(false);
  const [canNext, setCanNext] = useState(true);

  const cards = useMemo(() => items, [items]);

  useEffect(() => {
    const el = trackRef.current;
    if (!el) return;

    const update = () => {
      const max = el.scrollWidth - el.clientWidth;
      setCanPrev(el.scrollLeft > 4);
      setCanNext(el.scrollLeft < max - 4);
    };

    update();
    el.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update);
    return () => {
      el.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
    };
  }, []);

  const scrollByPage = (dir: -1 | 1) => {
    const el = trackRef.current;
    if (!el) return;
    el.scrollBy({ left: dir * Math.round(el.clientWidth * 0.9), behavior: 'smooth' });
  };

  if (!cards.length) return null;

  return (
    <div className="category-slider" aria-label="Категорії товарів">
      <button
        type="button"
        className="category-arrow prev"
        aria-label="Попередні категорії"
        onClick={() => scrollByPage(-1)}
        disabled={!canPrev}
      >
        <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
          <path
            fill="currentColor"
            d="M15.5 19a1 1 0 0 1-.7-.29l-6-6a1 1 0 0 1 0-1.42l6-6a1 1 0 1 1 1.4 1.42L10.91 12l5.29 5.29A1 1 0 0 1 15.5 19Z"
          />
        </svg>
      </button>

      <div className="category-track" ref={trackRef}>
        {cards.map((cat) => (
          <a
            key={cat.slug}
            className="category-card category-card--slide"
            href={cat.path || `/catalog?pcat=${encodeURIComponent(cat.slug)}`}
          >
            <div className="category-thumb" style={{ backgroundImage: `url('${cat.image}')` }} />
            <div className="category-info">
              <h3>{cat.label}</h3>
            </div>
          </a>
        ))}
      </div>

      <button
        type="button"
        className="category-arrow next"
        aria-label="Наступні категорії"
        onClick={() => scrollByPage(1)}
        disabled={!canNext}
      >
        <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
          <path
            fill="currentColor"
            d="M8.5 19a1 1 0 0 1-.7-1.71L13.09 12 7.8 6.71a1 1 0 1 1 1.4-1.42l6 6a1 1 0 0 1 0 1.42l-6 6a1 1 0 0 1-.7.29Z"
          />
        </svg>
      </button>

    </div>
  );
}
