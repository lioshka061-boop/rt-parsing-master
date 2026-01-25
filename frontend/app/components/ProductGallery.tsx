'use client';

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styles from './ProductGallery.module.css';

type Props = {
  images: string[];
  title: string;
};

export function ProductGallery({ images, title }: Props) {
  const pics = useMemo(() => (images || []).filter(Boolean), [images]);
  const [activeIdx, setActiveIdx] = useState(0);
  const [isLightbox, setIsLightbox] = useState(false);
  const [fitMode, setFitMode] = useState<'contain' | 'cover'>('contain');
  const [failedImages, setFailedImages] = useState<Set<string>>(new Set());
  const thumbRowRef = useRef<HTMLDivElement>(null);
  const touchMovedRef = useRef(false);
  const touchStartRef = useRef<number | null>(null);
  const carouselRef = useRef<HTMLDivElement>(null);
  const scrollRafRef = useRef<number | null>(null);
  const lightboxTouchStartRef = useRef<number | null>(null);
  const hasImages = pics.length > 0;

  const scrollToIndex = useCallback((idx: number, behavior: ScrollBehavior = 'smooth') => {
    const container = carouselRef.current;
    if (!container) return;
    const target = container.children[idx] as HTMLElement | undefined;
    if (!target) return;
    target.scrollIntoView({ behavior, inline: 'start', block: 'nearest' });
  }, []);

  const prev = useCallback(() => {
    if (!hasImages) return;
    const nextIdx = activeIdx === 0 ? pics.length - 1 : activeIdx - 1;
    setActiveIdx(nextIdx);
    scrollToIndex(nextIdx);
  }, [activeIdx, hasImages, pics.length, scrollToIndex]);

  const next = useCallback(() => {
    if (!hasImages) return;
    const nextIdx = (activeIdx + 1) % pics.length;
    setActiveIdx(nextIdx);
    scrollToIndex(nextIdx);
  }, [activeIdx, hasImages, pics.length, scrollToIndex]);

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

  const openLightbox = () => {
    if (!hasImages || touchMovedRef.current) return;
    setIsLightbox(true);
  };
  const closeLightbox = () => setIsLightbox(false);
  const handleImageLoad = (event: React.SyntheticEvent<HTMLImageElement>) => {
    const { naturalWidth, naturalHeight } = event.currentTarget;
    if (!naturalWidth || !naturalHeight) return;
    const ratio = naturalWidth / naturalHeight;
    if (ratio > 1.65 || ratio < 0.9) {
      setFitMode('cover');
    } else {
      setFitMode('contain');
    }
  };

  const handleImageError = (src: string) => {
    setFailedImages((prev) => {
      const next = new Set(prev);
      next.add(src);
      return next;
    });
  };

  useEffect(() => {
    return () => {
      if (scrollRafRef.current) {
        window.cancelAnimationFrame(scrollRafRef.current);
      }
    };
  }, []);

  const onTouchStart = (event: React.TouchEvent<HTMLDivElement>) => {
    touchStartRef.current = event.touches[0]?.clientX ?? null;
    touchMovedRef.current = false;
  };

  const onTouchMove = (event: React.TouchEvent<HTMLDivElement>) => {
    if (touchStartRef.current === null) return;
    const currentX = event.touches[0]?.clientX ?? touchStartRef.current;
    if (Math.abs(currentX - touchStartRef.current) > 6) {
      touchMovedRef.current = true;
    }
  };

  const onLightboxTouchStart = (event: React.TouchEvent<HTMLDivElement>) => {
    lightboxTouchStartRef.current = event.touches[0]?.clientX ?? null;
  };

  const onLightboxTouchEnd = (event: React.TouchEvent<HTMLDivElement>) => {
    if (lightboxTouchStartRef.current === null) return;
    const endX = event.changedTouches[0]?.clientX ?? lightboxTouchStartRef.current;
    const diff = lightboxTouchStartRef.current - endX;
    lightboxTouchStartRef.current = null;
    if (Math.abs(diff) < 40) return;
    if (diff > 0) {
      next();
    } else {
      prev();
    }
  };

  const handleScroll = () => {
    if (!carouselRef.current) return;
    if (scrollRafRef.current) return;
    scrollRafRef.current = window.requestAnimationFrame(() => {
      const container = carouselRef.current;
      if (!container) return;
      const width = container.clientWidth || 1;
      const nextIdx = Math.round(container.scrollLeft / width);
      if (nextIdx !== activeIdx) {
        setActiveIdx(Math.min(Math.max(nextIdx, 0), pics.length - 1));
      }
      scrollRafRef.current = null;
    });
  };

  const scrollThumbs = (dir: -1 | 1) => {
    if (!thumbRowRef.current) return;
    thumbRowRef.current.scrollBy({ left: dir * 200, behavior: 'smooth' });
  };

  return (
    <>
      <div className={styles.gallery}>
        <div className={styles.main}>
          {hasImages && (
            <>
              <button
                className={`${styles.arrow} ${styles.left}`}
                onClick={prev}
                aria-label="Попереднє фото"
              >
                <i className="ri-arrow-left-s-line"></i>
              </button>
              <button
                className={`${styles.arrow} ${styles.right}`}
                onClick={next}
                aria-label="Наступне фото"
              >
                <i className="ri-arrow-right-s-line"></i>
              </button>
            </>
          )}
          <div
            className={styles.carousel}
            ref={carouselRef}
            onScroll={handleScroll}
            onTouchStart={onTouchStart}
            onTouchMove={onTouchMove}
            role="list"
            aria-label="Галерея фото"
          >
            {hasImages ? (
              pics.map((img, idx) => (
                <button
                  key={`${img}-${idx}`}
                  type="button"
                  className={styles.slide}
                  onClick={() => {
                    if (touchMovedRef.current) return;
                    setActiveIdx(idx);
                    openLightbox();
                  }}
                  aria-label={`Відкрити фото ${idx + 1}`}
                >
                  {failedImages.has(img) ? (
                    <span className={styles.placeholder}>Фото оновлюється</span>
                  ) : (
                    <img
                      src={img}
                      alt={`${title} — фото ${idx + 1}`}
                      loading={idx === 0 ? 'eager' : 'lazy'}
                      fetchPriority={idx === 0 ? 'high' : 'auto'}
                      decoding="async"
                      width={1280}
                      height={820}
                      onLoad={idx === 0 ? handleImageLoad : undefined}
                      onError={() => handleImageError(img)}
                      className={fitMode === 'cover' ? styles.cover : styles.contain}
                    />
                  )}
                </button>
              ))
            ) : (
              <div className={styles.slide}>
                <span className={styles.placeholder}>Фото буде додано</span>
              </div>
            )}
          </div>
        </div>
        {hasImages && (
          <div className={styles.dots} role="tablist" aria-label="Перегляд фото">
            {pics.map((_, idx) => (
              <button
                key={`dot-${idx}`}
                type="button"
                className={`${styles.dot} ${idx === activeIdx ? styles.dotActive : ''}`}
                aria-label={`Фото ${idx + 1}`}
                onClick={() => {
                  setActiveIdx(idx);
                  scrollToIndex(idx);
                }}
              />
            ))}
          </div>
        )}
        <div className={styles.thumbStrip}>
          {hasImages && pics.length > 4 && (
            <button
              className={styles.thumbNav}
              aria-label="Прокрутити вліво"
              type="button"
              onClick={() => scrollThumbs(-1)}
            >
              <i className="ri-arrow-left-s-line"></i>
            </button>
          )}
          <div className={styles.thumbRow} ref={thumbRowRef}>
            {hasImages ? (
              pics.map((img, idx) => (
                <button
                  key={`${img}-${idx}`}
                  className={`${styles.thumbChip} ${idx === activeIdx ? styles.active : ''}`}
                  onClick={() => {
                    setActiveIdx(idx);
                    scrollToIndex(idx);
                  }}
                  aria-label={`Переглянути фото ${idx + 1}`}
                >
                  <img
                    src={img}
                    alt={`${title} — прев'ю ${idx + 1}`}
                    loading="lazy"
                    decoding="async"
                    width={120}
                    height={90}
                  />
                </button>
              ))
            ) : (
              <div className={`${styles.thumbChip} ${styles.thumbPlaceholder}`}>Фото немає</div>
            )}
          </div>
          {hasImages && pics.length > 4 && (
            <button
              className={styles.thumbNav}
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
        <div className={styles.lightbox} role="dialog" aria-modal="true">
          <button className={styles.lightboxClose} onClick={closeLightbox} aria-label="Закрити перегляд">
            <i className="ri-close-line"></i>
          </button>
          {hasImages && (
            <>
              <button
                className={`${styles.lightboxArrow} ${styles.left}`}
                onClick={prev}
                aria-label="Попереднє фото"
              >
                <i className="ri-arrow-left-s-line"></i>
              </button>
              <button
                className={`${styles.lightboxArrow} ${styles.right}`}
                onClick={next}
                aria-label="Наступне фото"
              >
                <i className="ri-arrow-right-s-line"></i>
              </button>
            </>
          )}
          <div className={styles.lightboxBody}>
            <div className={styles.lightboxThumbs}>
              {pics.map((img, idx) => (
                <button
                  key={`${img}-${idx}`}
                  className={`${styles.lightboxThumb} ${idx === activeIdx ? styles.active : ''}`}
                  onClick={() => {
                    setActiveIdx(idx);
                    scrollToIndex(idx);
                  }}
                  aria-label={`Вибрати фото ${idx + 1}`}
                >
                  {failedImages.has(img) ? (
                    <span className={styles.thumbFallback}>Фото</span>
                  ) : (
                    <img
                      src={img}
                      alt={`${title} — фото ${idx + 1}`}
                      loading="lazy"
                      decoding="async"
                      width={86}
                      height={86}
                      onError={() => handleImageError(img)}
                    />
                  )}
                </button>
              ))}
            </div>
            <div
              className={styles.lightboxMain}
              onTouchStart={onLightboxTouchStart}
              onTouchEnd={onLightboxTouchEnd}
            >
              {active ? (
                failedImages.has(active) ? (
                  <span className={styles.placeholder}>Фото оновлюється</span>
                ) : (
                  <img
                    src={active}
                    alt={`${title} — збільшене фото`}
                    loading="eager"
                    decoding="async"
                    width={1400}
                    height={900}
                    onError={() => handleImageError(active)}
                  />
                )
              ) : (
                <span className={styles.placeholder}>Фото буде додано</span>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
