import './globals.css';
import NavBarShell from './components/NavBarShell';
import { ThumbCycleHydrator } from './components/ThumbCycleHydrator';
import { Suspense } from 'react';
import { CartToaster } from './components/CartToaster';
import { ScrollToTop } from './components/ScrollToTop';
import { SiteFooter } from './components/SiteFooter';
import { ChatDock } from './components/ChatDock';
import { CONTACT_EMAIL, CONTACT_PHONE } from './lib/site';

export const metadata = {
  title: 'O&P Tuning | Магазин тюнінгу та запчастин',
  description:
    'Аеродинаміка, обвіси, спліттери, дифузори та аксесуари для авто. Замовляйте онлайн з доставкою.',
  icons: {
    icon: '/installments/O%26P%20favikon.png',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const organizationSchema = {
    '@context': 'https://schema.org',
    '@type': 'Organization',
    name: 'O&P Tuning',
    url: process.env.NEXT_PUBLIC_SITE_URL || 'https://example.com',
    logo: `${process.env.NEXT_PUBLIC_SITE_URL || ''}/logo.png`,
    email: CONTACT_EMAIL,
    contactPoint: [
      {
        '@type': 'ContactPoint',
        telephone: CONTACT_PHONE,
        contactType: 'customer support',
        areaServed: 'UA',
        availableLanguage: ['uk', 'en'],
      },
    ],
  };

  return (
    <html lang="uk">
      <head>
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/remixicon@3.5.0/fonts/remixicon.css"
        />
        <link rel="icon" href="/installments/O%26P%20favikon.png" />
      </head>
      <body>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(organizationSchema) }}
        />
        <Suspense fallback={null}>
        <NavBarShell />
      </Suspense>
      <ThumbCycleHydrator />
      <CartToaster />
      <ScrollToTop />
      <ChatDock />
      {children}
      <SiteFooter />
      </body>
    </html>
  )
}
