'use client';

import { useState } from 'react';

type Tab = {
  id: string;
  label: string;
  content: React.ReactNode;
};

export function ProductTabs({ tabs }: { tabs: Tab[] }) {
  const [active, setActive] = useState(tabs[0]?.id || '');

  const current = tabs.find((t) => t.id === active) || tabs[0];

  return (
    <div className="product-tabs">
      <div className="tab-row">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={`tab ${tab.id === active ? 'active' : ''}`}
            onClick={() => setActive(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {current && <div className="tab-panel">{current.content}</div>}
    </div>
  );
}
