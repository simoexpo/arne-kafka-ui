import { useRef } from 'react'

/**
 * True whenever the most recent CHANGE in `value` was true → false — i.e.
 * the "just finished" level, which stays true until `value` rises again.
 * Fed to a `useEffect` dependency array it yields exactly one effect run per
 * falling edge, which is what both call sites in Timeline.tsx want (the
 * empty-page auto-continue and the jump landing). Each caller needs its own
 * instance: the two concerns are independent and must not consume each
 * other's edge.
 *
 * Deliberately a LEVEL, not a one-render pulse. A pulse would have to know
 * how many times it has been rendered, and a render is not a fact a hook is
 * allowed to count: React renders a component more than once per commit
 * under `<StrictMode>` (main.tsx — every development render is
 * double-invoked), and may discard a render entirely under concurrent
 * rendering. This version writes its ref only when `value` actually differs
 * from the last value it saw, so re-running the render N times produces the
 * same answer every time. Both consumers already clear their own pending-work
 * ref on entry (`pendingDirectionRef`, `pendingScrollEdgeRef`), so a level
 * that stays true past its edge still does its work exactly once.
 */
export function useFallingEdge(value: boolean): boolean {
  const seenRef = useRef<{ previous: boolean; latest: boolean }>({ previous: false, latest: false })
  if (seenRef.current.latest !== value) {
    seenRef.current = { previous: seenRef.current.latest, latest: value }
  }
  return seenRef.current.previous && !value
}
