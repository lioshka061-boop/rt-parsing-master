'use client';

import { useEffect } from 'react';
import { usePathname } from 'next/navigation';

export function ForceNewTabLinks() {
  const pathname = usePathname();

  useEffect(() => {
    const links = document.querySelectorAll<HTMLAnchorElement>('a[href]');
    links.forEach((link) => {
      const href = link.getAttribute('href') || '';
      if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
    });
  }, [pathname]);

  return null;
}
