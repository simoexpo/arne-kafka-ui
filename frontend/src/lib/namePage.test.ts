import { describe, expect, it } from 'vitest'
import { pageFrom, nextAnchor, prevAnchor } from './namePage'

const names = ['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot']

describe('name-anchored pagination', () => {
  it('the first page starts at the beginning', () => {
    expect(pageFrom(names, (n) => n, null, 2)).toEqual(['alpha', 'bravo'])
  })

  it('a page is the items after its anchor', () => {
    expect(pageFrom(names, (n) => n, 'bravo', 2)).toEqual(['charlie', 'delta'])
  })

  // The whole reason for anchoring by name (owner ruling 2026-08-19): an item
  // appearing or vanishing EARLIER in the list must not shift the page under
  // the reader's eyes, which is exactly what index-based paging does.
  it('an insertion before the page leaves the page untouched', () => {
    const withNew = ['alpha', 'apricot', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot']
    expect(pageFrom(withNew, (n) => n, 'bravo', 2)).toEqual(['charlie', 'delta'])
  })

  it('a deletion before the page leaves the page untouched', () => {
    const without = ['bravo', 'charlie', 'delta', 'echo', 'foxtrot']
    expect(pageFrom(without, (n) => n, 'bravo', 2)).toEqual(['charlie', 'delta'])
  })

  it('the anchor itself disappearing does not break the page', () => {
    const without = ['alpha', 'charlie', 'delta', 'echo']
    expect(pageFrom(without, (n) => n, 'bravo', 2)).toEqual(['charlie', 'delta'])
  })

  it('the last page is short rather than padded', () => {
    expect(pageFrom(names, (n) => n, 'echo', 2)).toEqual(['foxtrot'])
  })

  it('next anchor is the last name shown, and null at the end', () => {
    expect(nextAnchor(names, (n) => n, 'bravo', 2)).toBe('delta')
    expect(nextAnchor(names, (n) => n, 'echo', 2)).toBeNull()
  })

  it('previous anchor steps back one page, and null on the first', () => {
    expect(prevAnchor(names, (n) => n, 'delta', 2)).toBe('bravo')
    expect(prevAnchor(names, (n) => n, 'bravo', 2)).toBeNull()
    expect(prevAnchor(names, (n) => n, null, 2)).toBeNull()
  })
})
