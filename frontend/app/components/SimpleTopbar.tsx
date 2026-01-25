"use client";

import Link from "next/link";

export function SimpleTopbar() {
  const closeSheet = () => {
    if (typeof window !== "undefined") {
      window.location.hash = "";
    }
  };

  return (
    <>
      <header className="topbar topbar-simple">
        <div className="topbar-inner">
          <Link className="brand" href="/">
            <div className="brand-mark" aria-hidden="true">&amp;</div>
            <div>
              <div className="brand-name">O&amp;P Tuning</div>
              <div className="brand-sub">design &amp; aerodynamics</div>
            </div>
          </Link>
          <nav className="topnav">
            <Link href="/catalog">Каталог</Link>
            <Link href="/new">Новинки</Link>
            <Link href="/contacts">Контакти</Link>
          </nav>
          <a className="icon-btn menu simple-menu-btn mobile-only" href="#checkout-menu" aria-label="Меню">
            <i className="ri-menu-line"></i>
            <span>MENU</span>
          </a>
          <div className="checkout-safe">
            <i className="ri-shield-check-line" aria-hidden="true"></i>
            <span>Безпечна оплата</span>
          </div>
        </div>
      </header>

      <div id="checkout-menu" className="mobile-sheet">
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
            <Link href="/catalog" onClick={closeSheet}>Каталог</Link>
            <Link href="/new" onClick={closeSheet}>Новинки</Link>
            <Link href="/contacts" onClick={closeSheet}>Контакти</Link>
            <Link href="/cart" onClick={closeSheet}>Кошик</Link>
          </nav>
        </div>
      </div>
    </>
  );
}
