import { describe, expect, it } from 'vitest'
import { classifyScroll } from './scrollZones'

describe('classifyScroll', () => {
  it('pinnedTop is true right at the top', () => {
    expect(classifyScroll({ scrollTop: 0, scrollHeight: 1000, clientHeight: 300 }).pinnedTop).toBe(true)
  })

  it('pinnedTop stays true within the pin threshold', () => {
    expect(classifyScroll({ scrollTop: 19, scrollHeight: 1000, clientHeight: 300 }).pinnedTop).toBe(true)
  })

  it('pinnedTop is false once scrolled past the threshold', () => {
    expect(classifyScroll({ scrollTop: 20, scrollHeight: 1000, clientHeight: 300 }).pinnedTop).toBe(false)
  })

  it('nearBottom is true once the unscrolled remaining distance is under the threshold', () => {
    // scrollHeight 1000, clientHeight 300 -> bottom at scrollTop 700; 19px short of it
    expect(classifyScroll({ scrollTop: 681, scrollHeight: 1000, clientHeight: 300 }).nearBottom).toBe(true)
  })

  it('nearBottom is false at exactly the threshold distance from the bottom', () => {
    expect(classifyScroll({ scrollTop: 680, scrollHeight: 1000, clientHeight: 300 }).nearBottom).toBe(false)
  })

  it('nearBottom is true when scrolled past the bottom (overscroll)', () => {
    expect(classifyScroll({ scrollTop: 700, scrollHeight: 1000, clientHeight: 300 }).nearBottom).toBe(true)
  })

  it('both zones can be true at once for short content (fits within one screen)', () => {
    const zones = classifyScroll({ scrollTop: 0, scrollHeight: 300, clientHeight: 300 })
    expect(zones).toEqual({ pinnedTop: true, nearBottom: true })
  })

  it('neither zone is true in the middle of a tall scroll', () => {
    expect(classifyScroll({ scrollTop: 350, scrollHeight: 1000, clientHeight: 300 })).toEqual({
      pinnedTop: false,
      nearBottom: false,
    })
  })
})
