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
