// Pages anchored to a NAME, not to an index (owner ruling 2026-08-19). The
// list is live: topics and consumer groups appear and vanish between polls. An
// index-based page would then shift under the reader — showing a row twice
// when something is inserted earlier in the list, or skipping one when
// something is deleted. Comparing against the anchor's value instead means an
// earlier change is a non-event for the page being read, and the anchor keeps
// working even after the item it names is gone.

/// The `size` items that sort after `anchor` (from the start when null).
export function pageFrom<T>(items: T[], key: (item: T) => string, anchor: string | null, size: number): T[] {
  const start = anchor === null ? 0 : items.findIndex((item) => key(item) > anchor)
  if (start < 0) return []
  return items.slice(start, start + size)
}

/// The anchor for the following page, or null when this page ends the list.
export function nextAnchor<T>(items: T[], key: (item: T) => string, anchor: string | null, size: number): string | null {
  const page = pageFrom(items, key, anchor, size)
  if (page.length === 0) return null
  const last = key(page[page.length - 1])
  return items.some((item) => key(item) > last) ? last : null
}

/// The anchor one page back, or null when the previous page is the first.
export function prevAnchor<T>(items: T[], key: (item: T) => string, anchor: string | null, size: number): string | null {
  if (anchor === null) return null
  const first = items.findIndex((item) => key(item) > anchor)
  const start = first < 0 ? Math.max(0, items.length - size) : Math.max(0, first - size)
  return start === 0 ? null : key(items[start - 1])
}
