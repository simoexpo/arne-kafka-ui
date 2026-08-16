import { describe, expect, it } from 'vitest'
import { decidePostPage, type PostPageInput } from './postPage'

const base: PostPageInput = {
  error: false,
  exhausted: false,
  matched: false,
  filterActive: false,
  pageMatches: 0,
  pageLimit: 100,
  nextCursor: 'c1',
  iteration: 0,
  iterationCap: 20,
}

describe('decidePostPage', () => {
  it('an errored page drops the uncommitted overlay, regardless of anything else', () => {
    expect(decidePostPage({ ...base, error: true, exhausted: true, matched: true })).toBe('error-drop-overlay')
  })

  it('a genuinely exhausted direction stops', () => {
    expect(decidePostPage({ ...base, exhausted: true })).toBe('stop')
  })

  it('a matched, unfiltered page stops (scroll sentinels cover continuing)', () => {
    expect(decidePostPage({ ...base, matched: true, filterActive: false })).toBe('stop')
  })

  it('a matched, filtered page that filled the page limit stops', () => {
    expect(decidePostPage({ ...base, matched: true, filterActive: true, pageMatches: 100, pageLimit: 100 })).toBe('stop')
  })

  it('a matched, filtered page that stopped short of the page limit offers to continue', () => {
    expect(decidePostPage({ ...base, matched: true, filterActive: true, pageMatches: 42, pageLimit: 100 })).toBe(
      'offer-continue',
    )
  })

  it('no next cursor stops, even unmatched', () => {
    expect(decidePostPage({ ...base, nextCursor: null })).toBe('stop')
  })

  it('hitting the iteration cap offers to continue', () => {
    expect(decidePostPage({ ...base, iteration: 20, iterationCap: 20 })).toBe('offer-continue')
  })

  it('an empty page under the cap, with a cursor, auto-continues with that cursor', () => {
    expect(decidePostPage({ ...base, nextCursor: 'c2', iteration: 5, iterationCap: 20 })).toEqual({
      kind: 'continue',
      cursor: 'c2',
    })
  })
})
