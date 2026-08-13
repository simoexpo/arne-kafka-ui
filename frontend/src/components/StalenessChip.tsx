import { useEffect, useState } from 'react'
import { formatAgo } from '../lib/format'

const AGING_THRESHOLD_MS = 36_000
const STALE_THRESHOLD_MS = 120_000

type Tier = 'fresh' | 'aging' | 'stale'

function tierOf(asOf: number | null, now: number): Tier | null {
  if (asOf === null) return null
  const delta = now - asOf
  if (delta > STALE_THRESHOLD_MS) return 'stale'
  if (delta > AGING_THRESHOLD_MS) return 'aging'
  return 'fresh'
}

const TIER_CLASSES: Record<Tier, string> = {
  fresh: 'border-zinc-300 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400',
  aging: 'border-amber-400 text-amber-600 dark:border-amber-600 dark:text-amber-400',
  stale: 'border-red-400 text-red-600 dark:border-red-600 dark:text-red-400',
}

export function StalenessChip({ asOf, now }: { asOf: number | null; now?: number }) {
  const [tick, setTick] = useState(() => now ?? Date.now())
  useEffect(() => {
    if (now !== undefined) return
    const id = setInterval(() => setTick(Date.now()), 1_000)
    return () => clearInterval(id)
  }, [now])
  const current = now ?? tick
  const tier = tierOf(asOf, current)
  return (
    <span
      className={`rounded-full border px-2 py-0.5 text-xs ${TIER_CLASSES[tier ?? 'fresh']}`}
      data-staleness={tier ?? undefined}
    >
      {asOf === null ? 'no data yet' : formatAgo(asOf, current)}
    </span>
  )
}
