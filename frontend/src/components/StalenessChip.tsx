import { useEffect, useState } from 'react'
import { formatAgo } from '../lib/format'

const AGING_THRESHOLD_MS = 36_000
const STALE_THRESHOLD_MS = 120_000

type Tier = 'fresh' | 'aging' | 'stale' | 'refreshing' | 'failed' | 'neutral'

// Precedence: failed > neutral > refreshing > age-based tiers. A query that
// just errored is the loudest signal regardless of anything else. `neutral`
// (design spec v1.3 "the detached chip is NEUTRAL") comes next: a detached
// (historical) window is immutable and cannot go stale, so its chip must
// never show the aging/alarm colors no matter how old the timestamp is —
// but an actual query failure is still the loudest signal even then, so
// failed still wins over neutral.
function tierOf(asOf: number | null, now: number, refreshing: boolean, failed: boolean, neutral: boolean): Tier | null {
  if (failed) return 'failed'
  if (asOf === null) return null
  if (neutral) return 'neutral'
  if (refreshing) return 'refreshing'
  const delta = now - asOf
  if (delta > STALE_THRESHOLD_MS) return 'stale'
  if (delta > AGING_THRESHOLD_MS) return 'aging'
  return 'fresh'
}

const TIER_CLASSES: Record<Tier, string> = {
  fresh: 'border-zinc-300 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400',
  aging: 'border-amber-400 text-amber-600 dark:border-amber-600 dark:text-amber-400',
  stale: 'border-red-400 text-red-600 dark:border-red-600 dark:text-red-400',
  // A cached-then-revalidate flash (stale chip renders instantly from cache,
  // then flips to fresh the instant the refetch lands) is honest but reads
  // as alarming. While a refetch for this data is in flight, suppress tier
  // styling and show the true age in the neutral zinc style instead.
  refreshing: 'border-zinc-300 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400',
  failed: 'border-red-400 text-red-600 dark:border-red-600 dark:text-red-400',
  // Same zinc styling as fresh/refreshing, kept as its own tier (rather than
  // reusing 'fresh') so `data-staleness="neutral"` is independently
  // testable/observable — position-in-history, not a freshness claim.
  neutral: 'border-zinc-300 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400',
}

export function StalenessChip({
  asOf,
  now,
  refreshing = false,
  failed = false,
  neutral = false,
}: {
  asOf: number | null
  now?: number
  refreshing?: boolean
  failed?: boolean
  // Design spec v1.3: the detached-window chip communicates position in
  // history, not freshness — a historical page is immutable and cannot be
  // stale. Same timestamp text, always the neutral zinc style. See tierOf's
  // precedence comment for failed-vs-neutral.
  neutral?: boolean
}) {
  const [tick, setTick] = useState(() => now ?? Date.now())
  useEffect(() => {
    if (now !== undefined) return
    const id = setInterval(() => setTick(Date.now()), 1_000)
    return () => clearInterval(id)
  }, [now])
  const current = now ?? tick
  const tier = tierOf(asOf, current, refreshing, failed, neutral)
  return (
    <span
      className={`rounded-full border px-2 py-0.5 text-xs ${TIER_CLASSES[tier ?? 'fresh']}`}
      data-staleness={tier ?? undefined}
    >
      {asOf === null ? 'no data yet' : formatAgo(asOf, current)}
    </span>
  )
}
