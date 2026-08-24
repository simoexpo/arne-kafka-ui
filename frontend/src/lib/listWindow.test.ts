import { describe, expect, it } from 'vitest'
import { grownCount, viewportSlice } from './listWindow'

// A 100-row list rendered at ~33px/row inside a 600px scroller.
const metrics = (scrollTop: number) => ({ scrollTop, scrollHeight: 3300, clientHeight: 600 })

describe('grownCount', () => {
  it('grows by one step when the reader is within a viewport of the bottom', () => {
    expect(grownCount(100, 250, 100, metrics(2200))).toBe(200)
  })

  it('stays put while the bottom is more than a viewport away', () => {
    expect(grownCount(100, 250, 100, metrics(0))).toBe(100)
  })

  it('never exceeds the total', () => {
    expect(grownCount(200, 250, 100, metrics(2800))).toBe(250)
  })

  it('never shrinks, even when everything is already rendered', () => {
    expect(grownCount(250, 250, 100, metrics(2800))).toBe(250)
  })

  it('never shrinks when a refetch removed rows out from under the window', () => {
    expect(grownCount(200, 50, 100, metrics(2800))).toBe(200)
  })
})

describe('viewportSlice', () => {
  it('covers the top chunk before any scrolling', () => {
    expect(viewportSlice(metrics(0), 100, 50)).toEqual({ start: 0, end: 50 })
  })

  it('slides to the chunk the viewport scrolled into', () => {
    // rows ~60..~78 in view: chunk quantization covers [50, 100)
    expect(viewportSlice(metrics(2000), 100, 50)).toEqual({ start: 50, end: 100 })
  })

  it('spans both chunks when the viewport straddles a boundary', () => {
    // rows ~40..~59 in view
    expect(viewportSlice(metrics(1350), 100, 50)).toEqual({ start: 0, end: 100 })
  })

  it('falls back to the first chunk when the scroller reports no geometry (jsdom, unmounted)', () => {
    expect(viewportSlice({ scrollTop: 0, scrollHeight: 0, clientHeight: 0 }, 100, 50)).toEqual({ start: 0, end: 50 })
  })
})
