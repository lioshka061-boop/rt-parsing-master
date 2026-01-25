'use client';

import { useEffect, useState } from 'react';

type GarageCar = { brand: string; model?: string };

export function GarageButton() {
  const [count, setCount] = useState(0);

  useEffect(() => {
    try {
      const stored = localStorage.getItem('garageCars');
      if (stored) {
        const parsed = JSON.parse(stored) as GarageCar[];
        setCount(parsed.length);
      }
    } catch {
      setCount(0);
    }
  }, []);

  return (
    <button
      type="button"
      className="link garage-btn"
      onClick={() => {
        window.dispatchEvent(new Event('garage-open'));
      }}
      title="Гараж (обрані авто)"
    >
      <i className="ri-car-line"></i> Гараж
      {count > 0 && <span className="garage-badge">{count}</span>}
    </button>
  );
}
