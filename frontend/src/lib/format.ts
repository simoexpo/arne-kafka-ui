export function formatAgo(asOfMs: number, nowMs: number): string {
  const delta = Math.max(0, nowMs - asOfMs)
  if (delta < 12_000) return 'just now'
  if (delta < 60_000) return `${Math.floor(delta / 1_000)}s ago`
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}h ago`
  return `${Math.floor(delta / 86_400_000)}d ago`
}

export function formatCount(n: number): string {
  if (n < 1_000) return String(n)
  if (n < 1_000_000) return `${(n / 1_000).toFixed(1)}k`
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  return `${(n / 1_000_000_000).toFixed(1)}B`
}

// Compact oldest -> newest range for the currently loaded window (owner
// feedback 2026-08-15: replaces the raw "{n} messages" count, which
// saturated — and lied — at the store's 2000-row cap). UTC, not local time
// (matches MessageRow's own per-row `toISOString()` convention, and stays
// deterministic regardless of the reader's or CI's timezone).
//
// The date is NEVER omitted (owner feedback, same day: "having just the
// time could be misleading") — a same-UTC-day window shows the shared date
// ONCE, then bare oldest -> newest times (e.g. "2026-08-15 09:12 → 17:40
// UTC"); a window spanning a day boundary gives each side its own full
// date + time (e.g. "2023-12-31 23:59 → 2024-01-01 00:01 UTC"). Minute
// precision (no seconds): this is a compact header, not a precise log —
// matches the owner's own suggested format.
export function formatWindowRange(oldestMs: number | null, newestMs: number | null): string {
  if (oldestMs === null || newestMs === null) return '—'
  const oldestIso = new Date(oldestMs).toISOString()
  const newestIso = new Date(newestMs).toISOString()
  const oldestDate = oldestIso.slice(0, 10)
  const newestDate = newestIso.slice(0, 10)
  const time = (iso: string) => iso.slice(11, 16)
  return oldestDate === newestDate
    ? `${oldestDate} ${time(oldestIso)} → ${time(newestIso)} UTC`
    : `${oldestDate} ${time(oldestIso)} → ${newestDate} ${time(newestIso)} UTC`
}

export function formatRetentionValue(raw: string | null): string {
  if (raw == null) return '—'
  if (Number(raw) === -1) return '∞'
  return raw
}

export function retentionMsHint(raw: string | null): string | null {
  if (raw == null) return null
  const n = Number(raw)
  if (!Number.isFinite(n) || n <= 0) return null
  const days = n / 86_400_000
  if (Number.isInteger(days)) return `${days}d`
  const hours = n / 3_600_000
  if (Number.isInteger(hours)) return `${hours}h`
  return null
}
