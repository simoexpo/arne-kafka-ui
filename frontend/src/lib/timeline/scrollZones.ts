// Threshold: roughly half a row.
const TOP_PIN_THRESHOLD = 20
// Scroll down -> next 100 older.
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
