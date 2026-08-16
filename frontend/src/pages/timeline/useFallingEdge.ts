import { useRef } from 'react'

/**
 * True exactly on the render where `value` went true → false. Each caller
 * needs its own instance (e.g. the empty-page auto-continue effect and the
 * jump-landing effect in Timeline.tsx are independent and must not consume
 * each other's edge).
 */
export function useFallingEdge(value: boolean): boolean {
  const wasRef = useRef(false)
  const was = wasRef.current
  wasRef.current = value
  return was && !value
}
