import { useEffect, useState } from 'react'

/**
 * True only once `active` has held continuously for `delayMs` — flips back
 * to false the instant `active` does, with no trailing delay. Guards a
 * gesture that resolves fast enough that showing progress for it at all
 * would be noise a human could never read (PROGRESS_SHOW_DELAY_MS in
 * Timeline.tsx): only a gesture a human could plausibly watch ever renders
 * anything here.
 */
export function useShowDelay(active: boolean, delayMs: number): boolean {
  const [shown, setShown] = useState(false)
  useEffect(() => {
    if (!active) {
      setShown(false)
      return
    }
    const id = setTimeout(() => setShown(true), delayMs)
    return () => clearTimeout(id)
  }, [active, delayMs])
  return shown
}
