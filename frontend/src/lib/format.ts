import type { TimeDisplayMode } from './timeDisplayMode'

function pad(n: number, len = 2): string {
  return String(n).padStart(len, '0')
}

// ONE format family for both modes, replacing a prior split between a bare
// `toISOString()` for UTC (`T`/`Z` ISO shape) and
// a differently-laid-out, word-labelled "local" string: same
// "yyyy-mm-dd hh:mm:ss[.mmm] <ZONE>" layout in both modes — only the
// trailing zone SUFFIX differs (see `zoneSuffix` below). This is the one
// place per-component date math for DISPLAY is allowed to live; every
// caller (rows, the window-range header, the picker's zone badge) goes
// through here or `zoneSuffix` rather than re-deriving fields itself.
function dateAndTime(ms: number, mode: TimeDisplayMode, millis: boolean): string {
  const d = new Date(ms)
  const y = mode === 'utc' ? d.getUTCFullYear() : d.getFullYear()
  const mo = mode === 'utc' ? d.getUTCMonth() : d.getMonth()
  const day = mode === 'utc' ? d.getUTCDate() : d.getDate()
  const h = mode === 'utc' ? d.getUTCHours() : d.getHours()
  const mi = mode === 'utc' ? d.getUTCMinutes() : d.getMinutes()
  const s = mode === 'utc' ? d.getUTCSeconds() : d.getSeconds()
  const date = `${y}-${pad(mo + 1)}-${pad(day)}`
  const time = `${pad(h)}:${pad(mi)}:${pad(s)}`
  if (!millis) return `${date} ${time}`
  const ms3 = mode === 'utc' ? d.getUTCMilliseconds() : d.getMilliseconds()
  return `${date} ${time}.${pad(ms3, 3)}`
}

// The zone suffix half of the format family: always "UTC" in utc mode; in
// local mode, a NUMERIC, per-timestamp, DST-honest offset ("UTC-5",
// "UTC+5:30" for half-hour zones) derived from THIS timestamp's own
// `getTimezoneOffset()` — never the word "local".
// Per-timestamp (not "now") on purpose: two rows a DST transition apart
// carry different, both-correct offsets. `getTimezoneOffset()` is minutes to
// ADD to local time to reach UTC, so the DISPLAYED offset is its negation.
export function zoneSuffix(ms: number, mode: TimeDisplayMode): string {
  if (mode === 'utc') return 'UTC'
  const totalMin = -new Date(ms).getTimezoneOffset()
  if (totalMin === 0) return 'UTC'
  const sign = totalMin < 0 ? '-' : '+'
  const abs = Math.abs(totalMin)
  const hours = Math.floor(abs / 60)
  const mins = abs % 60
  return mins === 0 ? `UTC${sign}${hours}` : `UTC${sign}${hours}:${pad(mins)}`
}

// UTC/local display toggle (owner ruling 2026-08-15, `lib/timeDisplayMode`):
// formats one absolute instant for direct display — message rows
// (MessageRow) and the jump-timestamp preview (JumpControl) both render
// already-loaded/already-computed epoch-ms values through this single
// function, so every zone is computed exactly the same way everywhere. (The
// window-range header does NOT go through here — see `formatWindowRange`'s
// own comment for why.) `millis` defaults to true; no current caller passes
// `{ millis: false }`, so that branch is unused today.
export function formatTimestamp(ms: number, mode: TimeDisplayMode, opts: { millis?: boolean } = {}): string {
  const { millis = true } = opts
  return `${dateAndTime(ms, mode, millis)} ${zoneSuffix(ms, mode)}`
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

// I5: a topic's message-count estimate can be missing for two different
// reasons — an internal topic (skipped on purpose, `estimate_error` null) or
// a genuine per-partition watermark-fetch failure (`estimate_error` set).
// Both render '—'; only the latter gets this tooltip, attributing the gap to
// Kafka rather than leaving the reader to guess.
export function estimateErrorTitle(error: string): string {
  return `Kafka couldn't provide a count — ${error}`
}

// Byte-scaled (1024-based, unlike `formatCount`'s decimal message-count
// shorthand) — a size in bytes should read as KB/MB/GB, not as unit-less "k".
export function formatBytes(n: number): string {
  const KB = 1024
  const MB = KB * 1024
  const GB = MB * 1024
  if (n < KB) return `${n} B`
  if (n < MB) return `${(n / KB).toFixed(1)} KB`
  if (n < GB) return `${(n / MB).toFixed(1)} MB`
  return `${(n / GB).toFixed(1)} GB`
}

function dateOnly(ms: number, mode: TimeDisplayMode): string {
  const d = new Date(ms)
  return mode === 'utc'
    ? `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
    : `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

function timeOnly(ms: number, mode: TimeDisplayMode): string {
  const d = new Date(ms)
  return mode === 'utc'
    ? `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
    : `${pad(d.getHours())}:${pad(d.getMinutes())}`
}

// Compact oldest -> newest range for the currently loaded window (owner
// feedback 2026-08-15: replaces the raw "{n} messages" count, which
// saturated — and lied — at the store's 2000-row cap).
//
// The date is NEVER omitted (owner feedback, same day: "having just the
// time could be misleading") — a same-day window shows the shared date ONCE,
// then bare oldest -> newest times (e.g. "2026-08-15 09:12 → 17:40 UTC"); a
// window spanning a day boundary gives each side its own full date + time
// (e.g. "2023-12-31 23:59 → 2024-01-01 00:01 UTC"). Minute precision (no
// seconds/millis): this is a compact header, not a precise log — matches the
// owner's own suggested format, and is why it calls `formatTimestamp`'s
// shared building blocks directly rather than `formatTimestamp` itself
// (which always carries seconds).
//
// `mode` (owner ruling 2026-08-15, UTC/local display toggle; suffix family
// unified across modes): defaults to 'utc' — every existing caller predates
// the toggle and keeps rendering exactly as before. 'local' repeats the same
// shared-date-once / day-boundary logic against the browser's own zone
// instead, suffixed with the numeric offset (`zoneSuffix`, honest per the
// NEWEST timestamp) rather than the word "local".
export function formatWindowRange(
  oldestMs: number | null,
  newestMs: number | null,
  mode: TimeDisplayMode = 'utc',
): string {
  if (oldestMs === null || newestMs === null) return '—'
  const oldestDate = dateOnly(oldestMs, mode)
  const newestDate = dateOnly(newestMs, mode)
  const suffix = zoneSuffix(newestMs, mode)
  return oldestDate === newestDate
    ? `${oldestDate} ${timeOnly(oldestMs, mode)} → ${timeOnly(newestMs, mode)} ${suffix}`
    : `${oldestDate} ${timeOnly(oldestMs, mode)} → ${newestDate} ${timeOnly(newestMs, mode)} ${suffix}`
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
