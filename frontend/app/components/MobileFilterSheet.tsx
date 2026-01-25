'use client';

import { useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

type Option = {
  label: string;
  value: string;
  count?: number;
};

type Props = {
  id: string;
  title: string;
  basePath: string;
  options: Option[];
  activeValue?: string;
};

export function MobileFilterSheet({ id, title, basePath, options, activeValue }: Props) {
  const router = useRouter();
  const [selected, setSelected] = useState(activeValue || '');

  const filteredOptions = useMemo(
    () => options.filter((option) => option.value),
    [options],
  );

  const apply = () => {
    if (typeof window !== 'undefined') {
      window.location.hash = '';
    }
    if (!selected) {
      router.push(basePath);
      return;
    }
    const params = new URLSearchParams();
    params.set('pcat', selected);
    router.push(`${basePath}?${params.toString()}`);
  };

  const clear = () => {
    setSelected('');
    if (typeof window !== 'undefined') {
      window.location.hash = '';
    }
    router.push(basePath);
  };

  return (
    <section id={id} className="mobile-sheet">
      <div className="mobile-sheet__panel">
        <div className="mobile-sheet__head">
          <div>
            <p className="mobile-sheet__eyebrow">Фільтри</p>
            <h3>{title}</h3>
          </div>
          <a className="mobile-sheet__close" href="#" aria-label="Закрити фільтри">
            ✕
          </a>
        </div>

        <div className="mobile-sheet__body">
          {filteredOptions.length === 0 && (
            <p className="muted">Фільтрів поки немає.</p>
          )}
          {filteredOptions.length > 0 && (
            <div className="sheet-filters">
              {filteredOptions.map((option) => {
                const checked = selected === option.value;
                return (
                  <label key={option.value} className="sheet-checkbox">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => setSelected(checked ? '' : option.value)}
                    />
                    <span className="checkmark" aria-hidden="true" />
                    <span>{option.label}</span>
                    {typeof option.count === 'number' && (
                      <span className="count">{option.count}</span>
                    )}
                  </label>
                );
              })}
            </div>
          )}
        </div>

      <div className="mobile-sheet__actions">
        <button type="button" className="primary" onClick={apply}>
          Застосувати
        </button>
        <button type="button" className="ghost" onClick={clear}>
          Очистити
        </button>
      </div>
      </div>
    </section>
  );
}
