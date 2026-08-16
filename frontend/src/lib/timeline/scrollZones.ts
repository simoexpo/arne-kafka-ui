// Scroll offset below which the viewport counts as "pinned to top" —
// roughly half a row, per the design spec's threshold.
const TOP_PIN_THRESHOLD = 20
// How close to the bottom (in px of unscrolled remaining distance) counts as
// "reached the last row" for the bottom-sentinel scroll-triggered
// pagination (spec: "scroll down -> next 100 older"). Same order of
// magnitude as TOP_PIN_THRESHOLD, kept as its own named constant since it
// guards a different feature (load-older, not live-pause).
const BOTTOM_PIN_THRESHOLD = 20

export interface ScrollMetrics {
  scrollTop: number
  scrollHeight: number
  clientHeight: number
}

export interface ScrollZones {
  pinnedTop: boolean
  nearBottom: boolean
}

export function classifyScroll({ scrollTop, scrollHeight, clientHeight }: ScrollMetrics): ScrollZones {
  return {
    pinnedTop: scrollTop < TOP_PIN_THRESHOLD,
    nearBottom: scrollHeight - (scrollTop + clientHeight) < BOTTOM_PIN_THRESHOLD,
  }
}
