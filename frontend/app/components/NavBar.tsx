'use client';

import Link from 'next/link';
import { usePathname, useSearchParams } from 'next/navigation';
import { useEffect, useRef, useState } from 'react';
import { SearchFilter } from './SearchFilter';
import { TopbarMenus } from './TopbarMenus';
import { formatPrice, plainText, productLink, type Product } from '../lib/products';
import type { CategoryNode } from '../lib/categories';
import { MobileCarSelector } from './MobileCarSelector';

type NavBarProps = {
  carCategories?: CategoryNode[];
};

export function NavBar({ carCategories = [] }: NavBarProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const initialQuery = searchParams?.get('q') || '';
  const [query, setQuery] = useState(initialQuery);
  const [mobileQuery, setMobileQuery] = useState(initialQuery);
  const [suggestions, setSuggestions] = useState<Product[]>([]);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [mobileSuggestions, setMobileSuggestions] = useState<Product[]>([]);
  const [mobileSuggestOpen, setMobileSuggestOpen] = useState(false);
  const [garageCount, setGarageCount] = useState(0);
  const suggestRef = useRef<HTMLDivElement | null>(null);
  const closeSheet = () => {
    if (typeof window !== 'undefined') {
      window.location.hash = '';
    }
  };
  const handleMobileNavClick = () => {
    closeSheet();
  };

  useEffect(() => {
    const onClick = (event: MouseEvent) => {
      if (!suggestRef.current) return;
      if (!suggestRef.current.contains(event.target as Node)) {
        setSuggestOpen(false);
      }
    };
    document.addEventListener('click', onClick);
    return () => document.removeEventListener('click', onClick);
  }, []);

  useEffect(() => {
    const readGarage = () => {
      try {
        const raw = localStorage.getItem('garageCars');
        if (!raw) return 0;
        const parsed = JSON.parse(raw) as Array<{ brand: string; model?: string }>;
        return Array.isArray(parsed) ? parsed.length : 0;
      } catch {
        return 0;
      }
    };
    const refresh = () => setGarageCount(readGarage());
    refresh();
    const handler = () => refresh();
    window.addEventListener('garage:update', handler as EventListener);
    return () => window.removeEventListener('garage:update', handler as EventListener);
  }, []);

  useEffect(() => {
    const onScroll = () => {
      const compact = window.scrollY > 24;
      document.body.classList.toggle('topbar-compact', compact);
    };
    onScroll();
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  useEffect(() => {
    const q = query.trim();
    if (q.length < 2) {
      setSuggestions([]);
      return;
    }
    const t = window.setTimeout(async () => {
      try {
        const res = await fetch(`/api/products?q=${encodeURIComponent(q)}&limit=6&compact=true`, {
          cache: 'no-store',
        });
        if (!res.ok) {
          setSuggestions([]);
          return;
        }
        const data = (await res.json()) as Product[];
        setSuggestions(data.slice(0, 6));
        setSuggestOpen(true);
      } catch {
        setSuggestions([]);
      }
    }, 250);
    return () => window.clearTimeout(t);
  }, [query]);

  useEffect(() => {
    const q = mobileQuery.trim();
    if (q.length < 2) {
      setMobileSuggestions([]);
      setMobileSuggestOpen(false);
      return;
    }
    const t = window.setTimeout(async () => {
      try {
        const res = await fetch(`/api/products?q=${encodeURIComponent(q)}&limit=6&compact=true`, {
          cache: 'no-store',
        });
        if (!res.ok) {
          setMobileSuggestions([]);
          return;
        }
        const data = (await res.json()) as Product[];
        setMobileSuggestions(data.slice(0, 6));
        setMobileSuggestOpen(true);
      } catch {
        setMobileSuggestions([]);
      }
    }, 250);
    return () => window.clearTimeout(t);
  }, [mobileQuery]);

  const shortenTitle = (title: string) => {
    const words = plainText(title).split(/\s+/).filter(Boolean);
    if (words.length <= 4) return words.join(' ');
    return `${words.slice(0, 4).join(' ')}…`;
  };

  // Ховаємо навбар для checkout/cart, якщо з'являться
  if (pathname?.startsWith('/checkout') || pathname?.startsWith('/cart')) {
    return null;
  }

  return (
    <>
      <header className="topbar">
        <div className="topbar-inner desktop-only">
          <Link className="brand" href="/">
            <div className="brand-mark" aria-hidden="true">&amp;</div>
            <div>
              <div className="brand-name">O&P Tuning</div>
              <div className="brand-sub">tuning & parts</div>
            </div>
          </Link>
          <div className="nav-search" ref={suggestRef}>
            <form
              className="nav-search-form"
              action="/search"
              onSubmit={(e) => {
                if (!query.trim()) {
                  e.preventDefault();
                }
              }}
            >
              <input
                name="q"
                type="search"
                placeholder="Пошук товарів…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onFocus={() => {
                  if (suggestions.length > 0) setSuggestOpen(true);
                }}
              />
              <button type="submit" aria-label="Шукати">
                <i className="ri-search-line"></i>
              </button>
            </form>
            {suggestOpen && suggestions.length > 0 && (
              <div className="search-suggest">
                {suggestions.map((item) => {
                  return (
                    <Link
                      key={item.article}
                      href={productLink(item)}
                      className="suggest-item"
                      prefetch={false}
                    >
                      <div className="suggest-thumb">
                        {item.images?.[0] ? <img src={item.images[0]} alt={item.title} /> : <span>Фото</span>}
                      </div>
                      <div className="suggest-info">
                        <div className="suggest-title" title={plainText(item.title)}>
                          {shortenTitle(item.title)}
                        </div>
                        <div className="suggest-meta">{item.model || item.brand}</div>
                      </div>
                      <div className="suggest-price">{formatPrice(item.price)}</div>
                    </Link>
                  );
                })}
              </div>
            )}
          </div>
          <nav className="topnav">
            <Link href="/catalog">Каталог</Link>
            <Link href="/new">Новинки</Link>
            <Link href="/contacts">Контакти</Link>
          </nav>
          <div className="top-actions">
            <TopbarMenus />
            <Link className="primary" href="/register" prefetch={false}>
              Вхід
            </Link>
          </div>
        </div>

        <div className="topbar-mobile mobile-only">
          <Link className="mobile-brand" href="/">
            <span className="mobile-brand-mark" aria-hidden="true">&amp;</span>
            <span className="mobile-brand-text">O&P Tuning</span>
          </Link>
          <div className="mobile-icon-row">
            <a className="icon-btn menu" href="#mobile-menu" aria-label="Відкрити меню">
              <i className="ri-menu-line"></i>
              <span>MENU</span>
            </a>
            <div className="mobile-icon-actions">
            <a className="icon-btn" href="#mobile-search" aria-label="Пошук">
              <i className="ri-search-line"></i>
            </a>
            <a className="icon-btn has-badge" href="#mobile-car" aria-label="Підібрати авто">
              <i className="ri-car-line"></i>
              {garageCount > 0 && <span className="action-badge">{garageCount}</span>}
            </a>
            <div className="mobile-cart">
              <TopbarMenus variant="mobile" />
            </div>
            </div>
          </div>
          <a className="mobile-car-cta" href="#mobile-car">
            <span>Підбір по авто</span>
          </a>
        </div>

        <div className="search-shell desktop-only">
          <SearchFilter
            carCategories={carCategories}
            initialQuery={initialQuery}
            initialBrand={searchParams?.get('brand') || ''}
            initialModel={searchParams?.get('model') || ''}
            variant="compact"
          />
        </div>
      </header>

      <div id="mobile-menu" className="mobile-sheet">
        <div className="mobile-sheet__panel">
          <div className="mobile-sheet__head">
            <div>
              <p className="mobile-sheet__eyebrow">Меню</p>
              <h3>Навігація</h3>
            </div>
            <a
              className="mobile-sheet__close"
              href="#"
              onClick={(event) => {
                event.preventDefault();
                closeSheet();
              }}
              aria-label="Закрити меню"
            >
              ✕
            </a>
          </div>
          <nav className="mobile-menu-links">
            <Link href="/catalog" onClick={handleMobileNavClick}>Каталог</Link>
            <Link href="/new" onClick={handleMobileNavClick}>Новинки</Link>
            <Link href="/contacts" onClick={handleMobileNavClick}>Контакти</Link>
            <Link href="/register" onClick={handleMobileNavClick}>Вхід</Link>
          </nav>
        </div>
      </div>

      <div id="mobile-search" className="mobile-sheet">
        <div className="mobile-sheet__panel">
          <div className="mobile-sheet__head">
            <div>
              <p className="mobile-sheet__eyebrow">Пошук</p>
              <h3>Швидкий пошук</h3>
            </div>
            <a
              className="mobile-sheet__close"
              href="#"
              onClick={(event) => {
                event.preventDefault();
                closeSheet();
              }}
              aria-label="Закрити пошук"
            >
              ✕
            </a>
          </div>
          <form
            className="mobile-search-form"
            action="/search"
            onSubmit={(event) => {
              if (!mobileQuery.trim()) {
                event.preventDefault();
                return;
              }
              closeSheet();
            }}
          >
            <input
              name="q"
              type="search"
              placeholder="Пошук товарів…"
              value={mobileQuery}
              onChange={(event) => setMobileQuery(event.target.value)}
              onFocus={() => {
                if (mobileSuggestions.length > 0) setMobileSuggestOpen(true);
              }}
            />
            <button
              type="submit"
              className="mobile-search-btn"
              aria-label="Шукати"
              onClick={() => {
                if (mobileQuery.trim()) {
                  closeSheet();
                }
              }}
            >
              <i className="ri-search-line"></i>
            </button>
          </form>
          {mobileSuggestOpen && mobileSuggestions.length > 0 && (
            <div className="mobile-search-suggest">
              {mobileSuggestions.map((item) => {
                return (
                  <Link
                    key={item.article}
                    href={productLink(item)}
                    className="suggest-item"
                    onClick={closeSheet}
                    prefetch={false}
                  >
                    <div className="suggest-thumb">
                      {item.images?.[0] ? <img src={item.images[0]} alt={item.title} /> : <span>Фото</span>}
                    </div>
                    <div className="suggest-info">
                      <div className="suggest-title" title={plainText(item.title)}>
                        {shortenTitle(item.title)}
                      </div>
                      <div className="suggest-meta">{item.model || item.brand}</div>
                    </div>
                    <div className="suggest-price">{formatPrice(item.price)}</div>
                  </Link>
                );
              })}
            </div>
          )}
        </div>
      </div>

      <div id="mobile-car" className="mobile-sheet">
        <div className="mobile-sheet__panel">
          <div className="mobile-sheet__head">
            <div>
              <p className="mobile-sheet__eyebrow">Авто</p>
              <h3>Підібрати авто</h3>
            </div>
            <a
              className="mobile-sheet__close"
              href="#"
              onClick={(event) => {
                event.preventDefault();
                closeSheet();
              }}
              aria-label="Закрити підбір авто"
            >
              ✕
            </a>
          </div>
          <MobileCarSelector carCategories={carCategories} />
        </div>
      </div>
    </>
  );
}
