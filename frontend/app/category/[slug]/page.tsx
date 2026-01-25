import { permanentRedirect } from 'next/navigation';

export default function CategoryPage({ params }: { params: { slug: string } }) {
  const slug = params.slug.toLowerCase();
  permanentRedirect(`/catalog?pcat=${encodeURIComponent(slug)}`);
}
