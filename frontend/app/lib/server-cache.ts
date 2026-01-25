type CacheEntry = {
  expiresAt: number;
  value: string;
  headers?: Record<string, string>;
};

const CACHE = new Map<string, CacheEntry>();

export function getCachedValue(key: string) {
  const entry = CACHE.get(key);
  if (!entry) return null;
  if (Date.now() >= entry.expiresAt) {
    CACHE.delete(key);
    return null;
  }
  return entry;
}

export function setCachedValue(key: string, value: string, ttlMs: number, headers?: Record<string, string>) {
  CACHE.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
    headers,
  });
}
