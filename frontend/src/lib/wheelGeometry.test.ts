import { describe, expect, it } from 'vitest'
import { wheelCenteredIndex, wheelIndexForScrollTop, wheelRewrapScrollTop, wheelScrollTopForIndex } from './wheelGeometry'

// Pure functions — jsdom has no real scroll physics, so this is
// unit-tested directly; see `DateTimePicker.test.tsx` for the DOM-level
// click-to-select/scroll-to-select coverage that consumes these.
describe('wheel geometry', () => {
  it('wheelCenteredIndex is the middle (2nd) copy of the tripled list', () => {
    expect(wheelCenteredIndex(9, 24)).toBe(24 + 9)
    expect(wheelCenteredIndex(0, 60)).toBe(60)
    expect(wheelCenteredIndex(45, 60)).toBe(105)
  })

  it('wheelScrollTopForIndex centers the given row in the default 120px/5-row viewport', () => {
    // row 33 top-aligned at 33*24=792, +half a row (12) - half the viewport (60)
    expect(wheelScrollTopForIndex(33)).toBe(792 + 12 - 60)
    expect(wheelScrollTopForIndex(0)).toBe(0 + 12 - 60)
  })

  it('wheelRewrapScrollTop silently jumps by one block only near an edge, otherwise no-ops', () => {
    const count = 60
    const rowHeight = 24
    const blockHeight = count * rowHeight // 1440
    // Deep in the middle third: no rewrap.
    expect(wheelRewrapScrollTop(blockHeight, count, rowHeight)).toBeNull()
    expect(wheelRewrapScrollTop(blockHeight * 1.4, count, rowHeight)).toBeNull()
    // Drifted into the top third: jump forward by one block.
    expect(wheelRewrapScrollTop(100, count, rowHeight)).toBe(100 + blockHeight)
    // Drifted into the bottom third: jump backward by one block.
    expect(wheelRewrapScrollTop(blockHeight * 2 - 50, count, rowHeight)).toBe(blockHeight * 2 - 50 - blockHeight)
  })

  it('wheelIndexForScrollTop is the exact inverse of wheelScrollTopForIndex', () => {
    expect(wheelIndexForScrollTop(wheelScrollTopForIndex(33))).toBe(33)
    expect(wheelIndexForScrollTop(wheelScrollTopForIndex(0))).toBe(0)
    expect(wheelIndexForScrollTop(wheelScrollTopForIndex(105))).toBe(105)
  })

  it('wheelIndexForScrollTop rounds to the nearest row when scrollTop sits between two rows', () => {
    // Row 33 is centered at scrollTop 744; a few px off either way still
    // resolves to row 33 rather than its neighbor.
    expect(wheelIndexForScrollTop(744 + 5)).toBe(33)
    expect(wheelIndexForScrollTop(744 - 5)).toBe(33)
  })
})
