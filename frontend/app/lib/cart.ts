'use client';

export type CartItem = {
  article: string;
  title: string;
  price?: number;
  image?: string;
  quantity?: number;
  path?: string;
};

const KEY = 'cart_items_v1';

function normalize(items: CartItem[]): CartItem[] {
  return items.map((item) => ({
    ...item,
    quantity: item.quantity && item.quantity > 0 ? item.quantity : 1,
  }));
}

function read(): CartItem[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return [];
    const items = JSON.parse(raw) as CartItem[];
    return normalize(items);
  } catch {
    return [];
  }
}

function write(items: CartItem[]) {
  if (typeof window === 'undefined') return;
  localStorage.setItem(KEY, JSON.stringify(normalize(items)));
}

export function addToCart(item: CartItem) {
  const items = read();
  const qty = item.quantity && item.quantity > 0 ? item.quantity : 1;
  const existing = items.find((i) => i.article === item.article);
  if (existing) {
    existing.quantity = (existing.quantity || 1) + qty;
    if (item.path) {
      existing.path = item.path;
    }
  } else {
    items.push({ ...item, quantity: qty });
  }
  write(items);
}

export function removeFromCart(article: string) {
  const items = read().filter((i) => i.article !== article);
  write(items);
}

export function setCartItemQuantity(article: string, quantity: number) {
  const items = read();
  const nextQty = Math.max(1, Math.min(99, Math.floor(quantity)));
  const target = items.find((i) => i.article === article);
  if (!target) return;
  target.quantity = nextQty;
  write(items);
}

export function incrementCartItem(article: string, delta: number) {
  const items = read();
  const target = items.find((i) => i.article === article);
  if (!target) return;
  const nextQty = (target.quantity || 1) + delta;
  target.quantity = Math.max(1, Math.min(99, Math.floor(nextQty)));
  write(items);
}

export function getCart(): CartItem[] {
  return read();
}

export function clearCart() {
  if (typeof window === 'undefined') return;
  localStorage.removeItem(KEY);
}
