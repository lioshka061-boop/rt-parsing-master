'use client';

import { useState } from 'react';

type Tab = {
  id: string;
  label: string;
  content: React.ReactNode;
};

export function ItemTabs({ tabs }: { tabs: Tab[] }) {
  const [active, setActive] = useState(tabs[0]?.id || '');
  const current = tabs.find((t) => t.id === active) || tabs[0];

  return (
    <div className="item-tabs">
      <div className="item-tab-row desktop-only">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={`item-tab ${tab.id === active ? 'active' : ''}`}
            onClick={() => setActive(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {current && <div className="item-tab-panel desktop-only">{current.content}</div>}
      <div className="item-accordion mobile-only">
        {tabs.map((tab) => (
          <details key={tab.id} className="item-accordion-item">
            <summary>{tab.label}</summary>
            <div className="item-accordion-body">{tab.content}</div>
          </details>
        ))}
      </div>
    </div>
  );
}
