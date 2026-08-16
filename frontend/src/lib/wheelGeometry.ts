// Looping wheel-column mechanics (owner ruling 2026-08-16, wheel-picker
// follow-up): hour/minute/second are rendered as an iOS-style endless
// wheel — the value list is repeated 3× back-to-back (so there's always
// more list both above and below whatever's centered) and the SELECTED
// value's row in the MIDDLE copy is kept perfectly vertically centered in
// the viewport. Real drag/wheel scroll physics only exist in a real
// browser (jsdom has no layout engine — `scrollTop` is just a stored
// number, `scrollHeight`/`clientHeight` are always 0), so the geometry is
// factored into small pure functions below, unit-tested directly; both
// click-to-select and scroll-to-select (settle → centered value becomes the
// selection, see `WHEEL_SETTLE_MS` below) are unit-tested through the DOM —
// scroll via fake timers + stubbed `scrollTop`, since jsdom has no real
// scroll physics — and the actual drag/momentum FEEL is verified in the
// browser pass, not here.
export const WHEEL_ROW_HEIGHT_PX = 24
export const WHEEL_VISIBLE_ROWS = 5
export const WHEEL_VIEWPORT_HEIGHT_PX = WHEEL_ROW_HEIGHT_PX * WHEEL_VISIBLE_ROWS

// Owner ruling 2026-08-17: scrolling a wheel doesn't just pan it, it
// SELECTS — real wheel-picker semantics. `onScroll` debounces by this many
// ms after the LAST scroll event (not on every event — a continuous drag
// would otherwise fire dozens of spurious selections mid-flight); once
// settled, whichever value is resting at the vertical center is selected.
export const WHEEL_SETTLE_MS = 120

// Row index (within the 3×-tripled list) of the MIDDLE copy of `selected`.
export function wheelCenteredIndex(selected: number, count: number): number {
  return count + selected
}

// `scrollTop` that puts row `idx` (top-aligned at `idx * rowHeight`)
// exactly vertically centered within a `viewportHeight`-tall viewport.
export function wheelScrollTopForIndex(
  idx: number,
  rowHeight: number = WHEEL_ROW_HEIGHT_PX,
  viewportHeight: number = WHEEL_VIEWPORT_HEIGHT_PX,
): number {
  return idx * rowHeight + rowHeight / 2 - viewportHeight / 2
}

// Standard "3× list, silently re-center near an edge" infinite-scroll
// technique: while the reader is scrolling anywhere within the middle
// third, do nothing; once they've drifted into the outer thirds (meaning
// they're approaching the start/end of the tripled DOM list), jump the
// scroll position by exactly one block-height (`count * rowHeight`) in the
// opposite direction. Every block is an identical copy of the same values,
// so the jump is invisible to the reader — the value under the centerline
// doesn't change, only which copy is now "current". Returns null when no
// rewrap is needed yet.
export function wheelRewrapScrollTop(
  scrollTop: number,
  count: number,
  rowHeight: number = WHEEL_ROW_HEIGHT_PX,
): number | null {
  const blockHeight = count * rowHeight
  if (scrollTop < blockHeight * 0.5) return scrollTop + blockHeight
  if (scrollTop > blockHeight * 1.5) return scrollTop - blockHeight
  return null
}

// The exact inverse of `wheelScrollTopForIndex`: given a `scrollTop`, which
// (tripled-list, absolute) row index is currently centered in the viewport.
// Rounds to the nearest row, so a scroll that settles a few px off an exact
// row boundary still resolves to that row rather than its neighbor. This is
// the geometry half of scroll-to-select; the caller reduces the result mod
// `count` to get the actual value (any of the 3 tripled copies of a value
// resolves to the same selection).
export function wheelIndexForScrollTop(
  scrollTop: number,
  rowHeight: number = WHEEL_ROW_HEIGHT_PX,
  viewportHeight: number = WHEEL_VIEWPORT_HEIGHT_PX,
): number {
  return Math.round((scrollTop + viewportHeight / 2 - rowHeight / 2) / rowHeight)
}
