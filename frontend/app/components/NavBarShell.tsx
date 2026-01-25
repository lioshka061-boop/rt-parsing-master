import { loadCarCategories, type CategoryNode } from '../lib/categories';
import { loadProducts, plainText } from '../lib/products';
import { slugify } from '../lib/slug';
import { NavBar } from './NavBar';

function buildCarCategories(products: { brand: string; model: string }[]): CategoryNode[] {
  const map = new Map<string, Set<string>>();
  for (const p of products) {
    const brand = plainText(p.brand || '').trim();
    const model = plainText(p.model || '').trim();
    if (!brand) continue;
    if (!map.has(brand)) map.set(brand, new Set());
    if (model) map.get(brand)!.add(model);
  }
  return Array.from(map.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([brand, models]) => ({
      id: slugify(brand),
      name: brand,
      children: Array.from(models)
        .sort((a, b) => a.localeCompare(b))
        .map((model) => ({
          id: slugify(model),
          name: model,
          children: [],
        })),
    }));
}

export default async function NavBarShell() {
  const carCategoriesRaw = await loadCarCategories({ revalidate: 900 });
  const carCategories =
    carCategoriesRaw.length > 0
      ? carCategoriesRaw
      : buildCarCategories(await loadProducts({ limit: 30, compact: true }, { revalidate: 300 }));

  return <NavBar carCategories={carCategories} />;
}
