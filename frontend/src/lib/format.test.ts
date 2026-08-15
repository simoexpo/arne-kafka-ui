import { describe, expect, it } from 'vitest'
import { formatAgo, formatCount, formatRetentionValue, formatWindowRange, retentionMsHint } from './format'

describe('formatAgo', () => {
  const now = 1_000_000_000
  it('sub-12s is "just now"', () => expect(formatAgo(now - 1500, now)).toBe('just now'))
  it('3s is still "just now"', () => expect(formatAgo(now - 3_000, now)).toBe('just now'))
  it('10s is still "just now"', () => expect(formatAgo(now - 10_000, now)).toBe('just now'))
  it('13s ago', () => expect(formatAgo(now - 13_000, now)).toBe('13s ago'))
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

describe('formatRetentionValue', () => {
  it('shows a dash for a missing value', () => expect(formatRetentionValue(null)).toBe('—'))
  it('shows infinity for -1', () => expect(formatRetentionValue('-1')).toBe('∞'))
  it('shows the raw value otherwise', () => expect(formatRetentionValue('604800000')).toBe('604800000'))
})

describe('formatWindowRange', () => {
  it('shows oldest -> newest, UTC, same-day compact (no date)', () => {
    // 2024-01-01T00:00:00Z + 5s / + 65s
    expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000)).toBe('00:00:05 → 00:01:05 UTC')
  })

  it('prefixes both sides with their date when the window spans a UTC day boundary', () => {
    // 2023-12-31T23:59:00Z .. 2024-01-01T00:01:00Z
    expect(formatWindowRange(1_704_067_140_000, 1_704_067_260_000)).toBe(
      '2023-12-31 23:59:00 → 2024-01-01 00:01:00 UTC',
    )
  })

  it('is a dash when either endpoint is null (no timestamp to anchor on)', () => {
    expect(formatWindowRange(null, 1_704_067_265_000)).toBe('—')
    expect(formatWindowRange(1_704_067_265_000, null)).toBe('—')
    expect(formatWindowRange(null, null)).toBe('—')
  })
})

describe('retentionMsHint', () => {
  it('computes whole days', () => expect(retentionMsHint('604800000')).toBe('7d'))
  it('computes whole hours when not a whole number of days', () => expect(retentionMsHint('7200000')).toBe('2h'))
  it('returns null for -1 (infinite retention)', () => expect(retentionMsHint('-1')).toBeNull())
  it('returns null for a missing value', () => expect(retentionMsHint(null)).toBeNull())
  it('returns null when not cleanly expressible in days or hours', () => expect(retentionMsHint('1234567')).toBeNull())
})
