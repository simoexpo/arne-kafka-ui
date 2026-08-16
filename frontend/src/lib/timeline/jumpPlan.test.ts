import { describe, expect, it } from 'vitest'
import { planJump } from './jumpPlan'

const PAGE_LIMIT = 100

describe('planJump', () => {
  it('now: lands top, attached, resumes live, no highlight', () => {
    expect(planJump({ kind: 'now' }, PAGE_LIMIT)).toEqual({
      anchorContext: 'default',
      pauseIntent: 'none',
      scrollEdge: 'top',
      attach: true,
      highlight: null,
      params: { direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' },
    })
  })

  it('beginning: lands bottom, detached, auto-paused, reads forward from the topic start', () => {
    expect(planJump({ kind: 'beginning' }, PAGE_LIMIT)).toEqual({
      anchorContext: 'beginning',
      pauseIntent: 'auto',
      scrollEdge: 'bottom',
      attach: false,
      highlight: null,
      params: { direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' },
    })
  })

  it('offset: lands bottom, detached, highlights the target row, reads forward from it', () => {
    expect(planJump({ kind: 'offset', partition: 2, offset: 42 }, PAGE_LIMIT)).toEqual({
      anchorContext: 'default',
      pauseIntent: 'auto',
      scrollEdge: 'bottom',
      attach: false,
      highlight: { partition: 2, offset: 42 },
      params: { direction: 'forward', limit: PAGE_LIMIT, anchor: 'offset', partition: 2, offset: 42 },
    })
  })

  it('timestamp: lands bottom, detached, no highlight, reads forward from the resolved instant', () => {
    expect(planJump({ kind: 'timestamp', ts_ms: 12345 }, PAGE_LIMIT)).toEqual({
      anchorContext: 'default',
      pauseIntent: 'auto',
      scrollEdge: 'bottom',
      attach: false,
      highlight: null,
      params: { direction: 'forward', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: 12345 },
    })
  })
})
