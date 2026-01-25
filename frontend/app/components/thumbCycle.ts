'use client';

let bound = false;

function resolveThumbTarget(target: EventTarget | null): HTMLElement | null {
  if (!target || !(target instanceof Element)) return null;
  return target.closest<HTMLElement>('.thumb[data-images]');
}

function getThumbImages(el: HTMLElement): [string, string] | null {
  const cachedFirst = el.dataset.cycleFirst;
  const cachedSecond = el.dataset.cycleSecond;
  if (cachedFirst && cachedSecond) {
    return [cachedFirst, cachedSecond];
  }
  const raw = el.dataset.images || '';
  const imgs = raw.split('|').filter(Boolean);
  if (imgs.length < 2) return null;
  const [first, second] = imgs;
  el.dataset.cycleFirst = first;
  el.dataset.cycleSecond = second;
  return [first, second];
}

export function attachThumbCycle() {
  if (bound || typeof document === 'undefined') return;
  bound = true;
  document.addEventListener('mouseover', (event) => {
    const el = resolveThumbTarget(event.target);
    if (!el) return;
    const related = event.relatedTarget as Node | null;
    if (related && el.contains(related)) return;
    const imgs = getThumbImages(el);
    if (!imgs) return;
    const [, second] = imgs;
    el.style.backgroundImage = `url('${second}')`;
  });
  document.addEventListener('mouseout', (event) => {
    const el = resolveThumbTarget(event.target);
    if (!el) return;
    const related = event.relatedTarget as Node | null;
    if (related && el.contains(related)) return;
    const imgs = getThumbImages(el);
    if (!imgs) return;
    const [first] = imgs;
    if (first) {
      el.style.backgroundImage = `url('${first}')`;
    }
  });
}
