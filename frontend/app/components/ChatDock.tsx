'use client';

import { useEffect, useRef, useState } from 'react';
import {
  CONTACT_INSTAGRAM,
  CONTACT_TELEGRAM,
  CONTACT_VIBER,
  CONTACT_WHATSAPP,
} from '../lib/site';

const actions = [
  { label: 'Telegram', href: CONTACT_TELEGRAM, icon: 'ri-telegram-line', external: true },
  { label: 'Viber', href: CONTACT_VIBER, icon: 'ri-phone-line', external: false },
  { label: 'WhatsApp', href: CONTACT_WHATSAPP, icon: 'ri-whatsapp-line', external: true },
  { label: 'Instagram', href: CONTACT_INSTAGRAM, icon: 'ri-instagram-line', external: true },
];

export function ChatDock() {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      if (!rootRef.current || rootRef.current.contains(event.target as Node)) return;
      setOpen(false);
    };
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('click', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('click', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, []);

  return (
    <div className={`chat-dock${open ? ' is-open' : ''}`} ref={rootRef}>
      <div className="chat-menu" role="menu" aria-hidden={!open} id="chat-dock-menu">
        <div className="chat-menu-title">Написати в чат</div>
        {actions.map((action) => (
          <a
            key={action.label}
            className="chat-action"
            href={action.href}
            role="menuitem"
            aria-label={action.label}
            target={action.external ? '_blank' : undefined}
            rel={action.external ? 'noopener noreferrer' : undefined}
            onClick={() => setOpen(false)}
          >
            <i className={action.icon} aria-hidden="true"></i>
            <span>{action.label}</span>
          </a>
        ))}
      </div>
      <button
        type="button"
        className="chat-toggle"
        aria-expanded={open}
        aria-controls="chat-dock-menu"
        onClick={() => setOpen((value) => !value)}
      >
        <i className="ri-chat-3-line" aria-hidden="true"></i>
        <span>Чат</span>
      </button>
    </div>
  );
}
