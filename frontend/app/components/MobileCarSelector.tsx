'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { equalsSlug, slugify } from '../lib/slug';
import { loadCarCategories, type CategoryNode } from '../lib/categories';

type Props = {
  carCategories?: CategoryNode[];
};

type LoadingStep = 'model' | 'generation' | null;

export function MobileCarSelector({ carCategories: initialCars = [] }: Props) {
  const router = useRouter();
  const [carCategories, setCarCategories] = useState<CategoryNode[]>(initialCars);
  const [loadingCats, setLoadingCats] = useState(false);
  const [loadingStep, setLoadingStep] = useState<LoadingStep>(null);
  const [brand, setBrand] = useState('');
  const [model, setModel] = useState('');
  const [generation, setGeneration] = useState('');
  const loaderTimerRef = useRef<number | null>(null);

  useEffect(() => {
    if (carCategories.length > 0) return;
    setLoadingCats(true);
    loadCarCategories()
      .then((data) => setCarCategories(Array.isArray(data) ? data : []))
      .catch(() => setCarCategories([]))
      .finally(() => setLoadingCats(false));
  }, [carCategories.length]);

  useEffect(() => {
    return () => {
      if (loaderTimerRef.current) {
        window.clearTimeout(loaderTimerRef.current);
      }
    };
  }, []);

  const brandOptions = useMemo(
    () => carCategories.map((c) => c.name).filter(Boolean),
    [carCategories],
  );

  const selectedBrand = useMemo(() => {
    if (!brand) return null;
    return carCategories.find((c) => equalsSlug(c.name, brand)) || null;
  }, [brand, carCategories]);

  const modelOptions = useMemo(() => {
    if (!selectedBrand) return [];
    return selectedBrand.children.map((m) => m.name).filter(Boolean);
  }, [selectedBrand]);

  const selectedModel = useMemo(() => {
    if (!selectedBrand || !model) return null;
    return selectedBrand.children.find((m) => equalsSlug(m.name, model)) || null;
  }, [selectedBrand, model]);

  const generationOptions = useMemo(() => {
    if (!selectedModel) return [];
    return selectedModel.children.map((g) => g.name).filter(Boolean);
  }, [selectedModel]);

  const showGeneration = model && generationOptions.length > 0;

  const triggerLoader = (step: LoadingStep) => {
    setLoadingStep(step);
    if (loaderTimerRef.current) {
      window.clearTimeout(loaderTimerRef.current);
    }
    loaderTimerRef.current = window.setTimeout(() => {
      setLoadingStep(null);
    }, 420);
  };

  const onBrandChange = (value: string) => {
    setBrand(value);
    setModel('');
    setGeneration('');
    if (value) {
      triggerLoader('model');
    }
  };

  const onModelChange = (value: string) => {
    setModel(value);
    setGeneration('');
    if (value) {
      triggerLoader('generation');
    }
  };

  const clearAll = () => {
    setBrand('');
    setModel('');
    setGeneration('');
    setLoadingStep(null);
  };

  const canSubmit = Boolean(brand);

  const submit = () => {
    if (!brand) return;
    const params = new URLSearchParams();
    params.set('brand', slugify(brand));
    if (model) params.set('model', slugify(model));
    if (generation) {
      const normalized = generation.split('[')[0]?.trim() || generation;
      params.set('gen', slugify(normalized));
    }
    if (typeof window !== 'undefined') {
      window.location.hash = '';
    }
    router.push(`/catalog?${params.toString()}`);
  };

  return (
    <div className="mobile-car-selector">
      <div className="mobile-car-head">
        <p>Знайди своє авто</p>
        {brand && <strong>{brand}</strong>}
      </div>

      <div className="mobile-car-fields">
        {loadingCats && (
          <div className="selector-loader" aria-live="polite">
            Завантажуємо марки…
          </div>
        )}
        <label className="car-field">
          <span>Марка</span>
          <select
            value={brand}
            onChange={(e) => onBrandChange(e.target.value)}
            disabled={loadingCats}
          >
            <option value="">Марка</option>
            {brandOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>

        {brand && loadingStep === 'model' && (
          <div className="selector-loader" aria-live="polite">
            Завантажуємо моделі…
          </div>
        )}

        {brand && (
          <label className="car-field">
            <span>Модель</span>
            <select
              value={model}
              onChange={(e) => onModelChange(e.target.value)}
              disabled={modelOptions.length === 0}
            >
              <option value="">Модель</option>
              {modelOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        )}

        {showGeneration && loadingStep === 'generation' && (
          <div className="selector-loader" aria-live="polite">
            Завантажуємо покоління…
          </div>
        )}

        {showGeneration && (
          <label className="car-field">
            <span>Генерація / роки</span>
            <select
              value={generation}
              onChange={(e) => setGeneration(e.target.value)}
              disabled={generationOptions.length === 0}
            >
              <option value="">Генерація / роки</option>
              {generationOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>

      <div className="mobile-car-actions">
        <button type="button" className="primary" onClick={submit} disabled={!canSubmit}>
          Шукати →
        </button>
        <button type="button" className="ghost" onClick={clearAll}>
          Очистити ✕
        </button>
      </div>
    </div>
  );
}
