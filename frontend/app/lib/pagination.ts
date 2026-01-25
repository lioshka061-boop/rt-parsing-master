export type PageItem = { type: 'page'; page: number } | { type: 'gap' };

export function buildPagination(current: number, total: number): PageItem[] {
  if (total <= 1) return [];
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => ({ type: 'page', page: i + 1 }));
  }
  if (current <= 3) {
    return [
      { type: 'page', page: 1 },
      { type: 'page', page: 2 },
      { type: 'page', page: 3 },
      { type: 'page', page: 4 },
      { type: 'gap' },
      { type: 'page', page: total },
    ];
  }
  if (current >= total - 2) {
    return [
      { type: 'page', page: 1 },
      { type: 'gap' },
      { type: 'page', page: total - 3 },
      { type: 'page', page: total - 2 },
      { type: 'page', page: total - 1 },
      { type: 'page', page: total },
    ];
  }
  return [
    { type: 'page', page: 1 },
    { type: 'gap' },
    { type: 'page', page: current - 1 },
    { type: 'page', page: current },
    { type: 'page', page: current + 1 },
    { type: 'gap' },
    { type: 'page', page: total },
  ];
}
