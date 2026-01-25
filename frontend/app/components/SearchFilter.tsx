'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { equalsSlug, slugify } from '../lib/slug';
import { loadCarCategories, loadProductCategories, CategoryNode } from '../lib/categories';

type Props = {
  carCategories?: CategoryNode[];
  productCategories?: CategoryNode[];
  initialQuery?: string;
  initialBrand?: string;
  initialModel?: string;
  initialProductCategory?: string;
  variant?: 'full' | 'compact';
};

export function SearchFilter({
  carCategories: initialCarCats,
  productCategories: initialProductCats,
  initialQuery = '',
  initialBrand = '',
  initialModel = '',
  initialProductCategory = '',
  variant = 'full',
}: Props) {
  const [carCategories, setCarCategories] = useState<CategoryNode[]>(initialCarCats || []);
  const [productCategories, setProductCategories] = useState<CategoryNode[]>(initialProductCats || []);
  const [loadingCats, setLoadingCats] = useState(false);
  const [query] = useState(initialQuery);
  const [brand, setBrand] = useState(initialBrand);
  const [model, setModel] = useState(initialModel);
  const [productCategory, setProductCategory] = useState(initialProductCategory);
  const router = useRouter();

  useEffect(() => {
    const hasCarCats = carCategories.length > 0;
    const hasProductCats = productCategories.length > 0;
    if (hasCarCats && hasProductCats) return;
    setLoadingCats(true);
    Promise.all([
      hasCarCats ? Promise.resolve(carCategories) : loadCarCategories(),
      hasProductCats ? Promise.resolve(productCategories) : loadProductCategories(),
    ])
      .then(([cars, products]) => {
        if (!hasCarCats) setCarCategories(cars);
        if (!hasProductCats) setProductCategories(products);
      })
      .catch(() => {
        if (!hasCarCats) setCarCategories([]);
        if (!hasProductCats) setProductCategories([]);
      })
      .finally(() => setLoadingCats(false));
  }, [carCategories, productCategories]);

  const brandOptions = useMemo(() => carCategories.map((c) => c.name), [carCategories]);
  const modelOptions = useMemo(() => {
    if (!brand) return [];
    const parent = carCategories.find((c) => equalsSlug(c.name, brand));
    return parent ? parent.children.map((m) => m.name) : [];
  }, [carCategories, brand]);

  const productCategoryOptions = useMemo(
    () => productCategories.map((c) => c.name),
    [productCategories],
  );

  const onBrandChange = (val: string) => {
    setBrand(val);
    setModel('');
  };

  const submit = () => {
    if (variant === 'compact') {
      const params = new URLSearchParams();
      if (productCategory) params.set('pcat', slugify(productCategory));
      if (brand && model) {
        const suffix = params.toString();
        router.push(`/catalog/${slugify(brand)}/${slugify(model)}${suffix ? `?${suffix}` : ''}`);
        return;
      }
      if (brand) {
        const suffix = params.toString();
        router.push(`/catalog/${slugify(brand)}${suffix ? `?${suffix}` : ''}`);
        return;
      }
      if (productCategory) {
        router.push(`/catalog?pcat=${encodeURIComponent(slugify(productCategory))}`);
        return;
      }
      router.push('/catalog');
      return;
    }
    const params = new URLSearchParams();
    if (query.trim()) params.set('q', query.trim());
    if (brand) params.set('brand', slugify(brand));
    if (model) params.set('model', slugify(model));
    if (productCategory) params.set('pcat', slugify(productCategory));
    router.push(`/search?${params.toString()}`);
  };

  const saveToGarage = () => {
    if (!brand) return;
    try {
      const stored = localStorage.getItem('garageCars');
      const parsed: Array<{ brand: string; model?: string }> = stored ? JSON.parse(stored) : [];
      parsed.push({ brand, model: model || undefined });
      const unique = parsed
        .filter((c) => c.brand)
        .filter(
          (c, idx, arr) =>
            arr.findIndex(
              (x) => slugify(x.brand) === slugify(c.brand) && slugify(x.model || '') === slugify(c.model || ''),
            ) === idx,
        );
      localStorage.setItem('garageCars', JSON.stringify(unique));
      window.dispatchEvent(new Event('garage:update'));
    } catch {
      /* ignore */
    }
  };

  const formClass = variant === 'compact' ? 'filters tight' : 'filters tight';
  const controlStyle = { minWidth: 140, maxWidth: 220, height: 46, flex: '1 1 auto' };

  return (
    <form
      className={formClass}
      style={{ position: 'relative' }}
      onSubmit={(e) => {
        e.preventDefault();
        submit();
      }}
      aria-label="Фільтр пошуку"
      id="garage"
    >
      <label className="visually-hidden" htmlFor={`brand-${variant}`}>Марка</label>
      <select
        id={`brand-${variant}`}
        aria-label="Марка"
        value={brand}
        onChange={(e) => onBrandChange(e.target.value)}
        disabled={loadingCats}
        style={controlStyle}
      >
        <option value="">Марка</option>
        {brandOptions.map((b) => (
          <option key={b} value={b}>
            {b}
          </option>
        ))}
      </select>
      <label className="visually-hidden" htmlFor={`model-${variant}`}>Модель</label>
      <select
        id={`model-${variant}`}
        aria-label="Модель"
        value={model}
        onChange={(e) => setModel(e.target.value)}
        disabled={!brand || modelOptions.length === 0}
        style={controlStyle}
      >
        <option value="">Модель</option>
        {modelOptions.map((m) => (
          <option key={m} value={m}>
            {m}
          </option>
        ))}
      </select>
      <label className="visually-hidden" htmlFor={`pcat-${variant}`}>Категорія</label>
      <select
        id={`pcat-${variant}`}
        aria-label="Категорія"
        value={productCategory}
        onChange={(e) => setProductCategory(e.target.value)}
        disabled={loadingCats || productCategoryOptions.length === 0}
        style={controlStyle}
      >
        <option value="">Категорія</option>
        {productCategoryOptions.map((c) => (
          <option key={c} value={c}>
            {c}
          </option>
        ))}
      </select>
      <button
        type="button"
        className="ghost"
        onClick={saveToGarage}
        disabled={!brand}
        title="Додати обране авто до гаража"
        style={controlStyle}
      >
        <i className="ri-car-line"></i> Додати в гараж
      </button>
      <button className="primary" type="submit" style={controlStyle}>
        <i className="ri-search-line"></i>Підібрати
      </button>
    </form>
  );
}
