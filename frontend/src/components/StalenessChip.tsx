import { useEffect, useState } from 'react'
import { formatAgo } from '../lib/format'

export function StalenessChip({ asOf, now }: { asOf: number | null; now?: number }) {
  const [tick, setTick] = useState(() => now ?? Date.now())
  useEffect(() => {
    if (now !== undefined) return
    const id = setInterval(() => setTick(Date.now()), 1_000)
    return () => clearInterval(id)
  }, [now])
  const current = now ?? tick
  return (
    <span className="rounded-full border border-zinc-300 px-2 py-0.5 text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
      {asOf === null ? 'no data yet' : formatAgo(asOf, current)}
    </span>
  )
}
