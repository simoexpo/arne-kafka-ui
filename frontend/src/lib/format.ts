import type { TimeDisplayMode } from './timeDisplayMode'

function pad(n: number, len = 2): string {
  return String(n).padStart(len, '0')
}

// Local-zone equivalent of `.toISOString()` — same precision (down to the
// millisecond), but read off the `Date` object's own LOCAL getters (not the
// UTC ones `toISOString` uses), and explicitly suffixed "local" so it's
// never mistaken for the UTC rendering `formatTimestamp(ms, 'utc')` (and
// historically, every row) produces.
function isoLocal(ms: number): string {
  const d = new Date(ms)
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
  )
}

// UTC/local display toggle (owner ruling 2026-08-15, `lib/timeDisplayMode`):
// formats one absolute instant for direct display — message rows and the
// jump-timestamp preview both render already-loaded/already-computed
// epoch-ms values through this single function, so both zones are always
// computed exactly the same way. 'utc' matches the historical per-row
// `toISOString()` convention (unlabelled beyond its own trailing `Z`,
// exactly as today); 'local' is always explicitly labelled.
export function formatTimestamp(ms: number, mode: TimeDisplayMode): string {
  return mode === 'utc' ? new Date(ms).toISOString() : `${isoLocal(ms)} local`
}

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
// `mode` (owner ruling 2026-08-15, UTC/local display toggle): defaults to
// 'utc' — every existing caller predates the toggle and keeps rendering
// exactly as before. 'local' repeats the same shared-date-once / day-
// boundary logic against the browser's own zone instead, labelled "local"
// rather than "UTC" so the two are never confused.
export function formatWindowRange(
  oldestMs: number | null,
  newestMs: number | null,
  mode: TimeDisplayMode = 'utc',
): string {
  if (oldestMs === null || newestMs === null) return '—'
  if (mode === 'utc') {
    const oldestIso = new Date(oldestMs).toISOString()
    const newestIso = new Date(newestMs).toISOString()
    const oldestDate = oldestIso.slice(0, 10)
    const newestDate = newestIso.slice(0, 10)
    const time = (iso: string) => iso.slice(11, 16)
    return oldestDate === newestDate
      ? `${oldestDate} ${time(oldestIso)} → ${time(newestIso)} UTC`
      : `${oldestDate} ${time(oldestIso)} → ${newestDate} ${time(newestIso)} UTC`
  }
  const localDate = (ms: number) => {
    const d = new Date(ms)
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
  }
  const localTime = (ms: number) => {
    const d = new Date(ms)
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`
  }
  const oldestDate = localDate(oldestMs)
  const newestDate = localDate(newestMs)
  return oldestDate === newestDate
    ? `${oldestDate} ${localTime(oldestMs)} → ${localTime(newestMs)} local`
    : `${oldestDate} ${localTime(oldestMs)} → ${newestDate} ${localTime(newestMs)} local`
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
