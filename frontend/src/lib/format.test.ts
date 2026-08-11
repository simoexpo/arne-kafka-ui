import { describe, expect, it } from 'vitest'
import { formatAgo, formatCount } from './format'

describe('formatAgo', () => {
  const now = 1_000_000_000
  it('sub-2s is "just now"', () => expect(formatAgo(now - 1500, now)).toBe('just now'))
  it('seconds', () => expect(formatAgo(now - 3_000, now)).toBe('3s ago'))
  it('minutes', () => expect(formatAgo(now - 120_000, now)).toBe('2m ago'))
  it('hours', () => expect(formatAgo(now - 3_600_000, now)).toBe('1h ago'))
  it('days', () => expect(formatAgo(now - 172_800_000, now)).toBe('2d ago'))
})

describe('formatCount', () => {
  it('small numbers verbatim', () => expect(formatCount(999)).toBe('999'))
  it('thousands', () => expect(formatCount(1500)).toBe('1.5k'))
  it('millions', () => expect(formatCount(2_000_000)).toBe('2.0M'))
  it('billions', () => expect(formatCount(3_100_000_000)).toBe('3.1B'))
})
