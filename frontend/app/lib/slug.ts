const cyrMap: Record<string, string> = {
  а: 'a',
  б: 'b',
  в: 'v',
  г: 'h',
  ґ: 'g',
  д: 'd',
  е: 'e',
  є: 'ie',
  ж: 'zh',
  з: 'z',
  и: 'y',
  і: 'i',
  ї: 'i',
  й: 'i',
  к: 'k',
  л: 'l',
  м: 'm',
  н: 'n',
  о: 'o',
  п: 'p',
  р: 'r',
  с: 's',
  т: 't',
  у: 'u',
  ф: 'f',
  х: 'kh',
  ц: 'ts',
  ч: 'ch',
  ш: 'sh',
  щ: 'shch',
  ю: 'iu',
  я: 'ia',
  ь: '',
  ъ: '',
  ы: 'y',
  э: 'e',
};

function translit(str: string): string {
  return str
    .toLowerCase()
    .split('')
    .map((ch) => cyrMap[ch] ?? ch)
    .join('');
}

export function slugify(input: string): string {
  const base = translit(input.normalize('NFKD'));
  return base
    .replace(/[^a-z0-9\s-]/g, '')
    .trim()
    .replace(/[\s_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

export function equalsSlug(value: string, slug: string): boolean {
  return slugify(value) === slug.toLowerCase();
}
