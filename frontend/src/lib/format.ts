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
// deterministic regardless of the reader's or CI's timezone): same-UTC-day
// windows show bare times; a window spanning a day boundary prefixes both
// sides with their own date so the range is never ambiguous.
export function formatWindowRange(oldestMs: number | null, newestMs: number | null): string {
  if (oldestMs === null || newestMs === null) return '—'
  const oldestIso = new Date(oldestMs).toISOString()
  const newestIso = new Date(newestMs).toISOString()
  const oldestDate = oldestIso.slice(0, 10)
  const newestDate = newestIso.slice(0, 10)
  const time = (iso: string) => iso.slice(11, 19)
  return oldestDate === newestDate
    ? `${time(oldestIso)} → ${time(newestIso)} UTC`
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
