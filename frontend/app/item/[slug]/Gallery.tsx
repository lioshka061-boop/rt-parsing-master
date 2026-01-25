'use client';

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type Props = {
  images: string[];
  title: string;
};

export function Gallery({ images, title }: Props) {
  const pics = useMemo(() => (images || []).filter(Boolean), [images]);
  const [activeIdx, setActiveIdx] = useState(0);
  const [isLightbox, setIsLightbox] = useState(false);
  const thumbRowRef = useRef<HTMLDivElement>(null);
  const hasImages = pics.length > 0;

  const prev = useCallback(() => {
    if (!hasImages) return;
    setActiveIdx((idx) => (idx === 0 ? pics.length - 1 : idx - 1));
  }, [hasImages, pics.length]);

  const next = useCallback(() => {
    if (!hasImages) return;
    setActiveIdx((idx) => (idx + 1) % pics.length);
  }, [hasImages, pics.length]);

  useEffect(() => {
    if (activeIdx >= pics.length) setActiveIdx(0);
  }, [pics, activeIdx]);

  useEffect(() => {
    if (!isLightbox) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setIsLightbox(false);
      if (e.key === 'ArrowRight') next();
      if (e.key === 'ArrowLeft') prev();
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isLightbox, next, prev]);

  const active = pics[activeIdx];

  const openLightbox = () => hasImages && setIsLightbox(true);
  const closeLightbox = () => setIsLightbox(false);

  const scrollThumbs = (dir: -1 | 1) => {
    if (!thumbRowRef.current) return;
    thumbRowRef.current.scrollBy({ left: dir * 200, behavior: 'smooth' });
  };

  return (
    <>
      <div className="product-gallery">
        <div className="product-gallery-main">
          {hasImages && (
            <>
              <button className="product-gallery-arrow left" onClick={prev} aria-label="Попереднє фото">
                <i className="ri-arrow-left-s-line"></i>
              </button>
              <button className="product-gallery-arrow right" onClick={next} aria-label="Наступне фото">
                <i className="ri-arrow-right-s-line"></i>
              </button>
            </>
          )}
          <div
            className="product-gallery-image"
            onClick={openLightbox}
            role="button"
            aria-label="Відкрити фото"
            
          >
            {active ? (
              <img
                src={active}
                alt={`${title} — головне фото`}
                loading="eager"
                fetchPriority="high"
                decoding="async"
                width={1280}
                height={820}
                style={{ objectFit: 'contain', width: '100%', height: '100%' }}
              />
            ) : (
              <span style={{ color: 'var(--muted)' }}>Фото буде додано</span>
            )}
          </div>
          {hasImages && (
            <button className="product-gallery-zoom" onClick={openLightbox}>
              <i className="ri-fullscreen-line"></i>
              <span>На весь екран</span>
            </button>
          )}
        </div>
        <div className="product-thumb-strip">
          {hasImages && pics.length > 4 && (
            <button
              className="product-thumb-nav"
              aria-label="Прокрутити вліво"
              type="button"
              onClick={() => scrollThumbs(-1)}
            >
              <i className="ri-arrow-left-s-line"></i>
            </button>
          )}
          <div className="product-thumb-row" ref={thumbRowRef}>
            {hasImages ? (
              pics.map((img, idx) => (
                <button
                  key={img}
                  className={`product-thumb-chip ${idx === activeIdx ? 'active' : ''}`}
                  onClick={() => setActiveIdx(idx)}
                  aria-label={`Переглянути фото ${idx + 1}`}
                >
                  <img
                    src={img}
                    alt={`${title} — прев'ю ${idx + 1}`}
                    loading="lazy"
                    decoding="async"
                    width={120}
                    height={90}
                    style={{ objectFit: 'cover', width: '100%', height: '100%' }}
                  />
                </button>
              ))
            ) : (
              <div className="product-thumb placeholder">Фото немає</div>
            )}
          </div>
          {hasImages && pics.length > 4 && (
            <button
              className="product-thumb-nav"
              aria-label="Прокрутити вправо"
              type="button"
              onClick={() => scrollThumbs(1)}
            >
              <i className="ri-arrow-right-s-line"></i>
            </button>
          )}
        </div>
      </div>

      {isLightbox && (
        <div className="lightbox" role="dialog" aria-modal="true">
          <button className="lightbox-close" onClick={closeLightbox} aria-label="Закрити перегляд">
            <i className="ri-close-line"></i>
          </button>
          {hasImages && (
            <>
              <button className="lightbox-arrow left" onClick={prev} aria-label="Попереднє фото">
                <i className="ri-arrow-left-s-line"></i>
              </button>
              <button className="lightbox-arrow right" onClick={next} aria-label="Наступне фото">
                <i className="ri-arrow-right-s-line"></i>
              </button>
            </>
          )}
            <div className="lightbox-body">
              <div className="lightbox-thumbs">
              {pics.map((img, idx) => (
                <button
                  key={img}
                  className={`lightbox-thumb ${idx === activeIdx ? 'active' : ''}`}
                  onClick={() => setActiveIdx(idx)}
                  aria-label={`Вибрати фото ${idx + 1}`}
                >
                  <img
                    src={img}
                    alt={`${title} — фото ${idx + 1}`}
                    loading="lazy"
                    decoding="async"
                    width={86}
                    height={86}
                    style={{ objectFit: 'cover', width: '100%', height: '100%' }}
                  />
                </button>
              ))}
              </div>
            <div className="lightbox-main">
              {active ? (
                <img
                  src={active}
                  alt={`${title} — збільшене фото`}
                  loading="eager"
                  decoding="async"
                  width={1400}
                  height={900}
                  style={{ objectFit: 'contain', width: '100%', height: '100%', maxHeight: '80vh' }}
                />
              ) : (
                <span style={{ color: 'var(--muted)' }}>Фото буде додано</span>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
