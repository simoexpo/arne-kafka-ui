import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatAgo, formatCount, formatRetentionValue, formatTimestamp, formatWindowRange, retentionMsHint } from './format'

// `@types/node` isn't in this app's tsconfig `types` (browser app) — declared
// locally for the fixed-TZ tests below rather than widening ambient types.
declare const process: { env: Record<string, string | undefined> }

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
  // Owner feedback 2026-08-15: "having just the time could be misleading" —
  // the date must never be omitted entirely, even within one UTC day; when
  // both ends share a day, show it once rather than twice.
  it('shows the shared date once, then oldest -> newest time, when both ends fall on the same UTC day', () => {
    // 2024-01-01T00:00:05Z .. 2024-01-01T00:01:05Z
    expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000)).toBe('2024-01-01 00:00 → 00:01 UTC')
  })

  it('gives each side its own full date + time when the window spans a UTC day boundary', () => {
    // 2023-12-31T23:59:00Z .. 2024-01-01T00:01:00Z
    expect(formatWindowRange(1_704_067_140_000, 1_704_067_260_000)).toBe(
      '2023-12-31 23:59 → 2024-01-01 00:01 UTC',
    )
  })

  it('is a dash when either endpoint is null (no timestamp to anchor on)', () => {
    expect(formatWindowRange(null, 1_704_067_265_000)).toBe('—')
    expect(formatWindowRange(1_704_067_265_000, null)).toBe('—')
    expect(formatWindowRange(null, null)).toBe('—')
  })

  // UTC/local display toggle (owner ruling 2026-08-15): a third, optional
  // mode argument re-renders the exact same instants in the reader's own
  // browser zone instead, explicitly labelled so it's never mistaken for
  // UTC. Fixed non-UTC TZ so a mode that silently fell back to UTC math
  // would fail these.
  describe('local mode (fixed TZ=America/New_York)', () => {
    const ORIGINAL_TZ = process.env.TZ
    beforeAll(() => { process.env.TZ = 'America/New_York' })
    afterAll(() => { process.env.TZ = ORIGINAL_TZ })

    it('shows the shared LOCAL date once, then oldest -> newest LOCAL time, labelled "local"', () => {
      // 2024-01-01T00:00:05Z .. 2024-01-01T00:01:05Z == 2023-12-31 19:00:05 .. 19:01:05 America/New_York (EST, UTC-5)
      expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000, 'local')).toBe(
        '2023-12-31 19:00 → 19:01 local',
      )
    })

    it('defaults to utc when no mode is given (backward compatible)', () => {
      expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000)).toBe('2024-01-01 00:00 → 00:01 UTC')
    })
  })
})

describe('formatTimestamp', () => {
  const ORIGINAL_TZ = process.env.TZ
  beforeAll(() => { process.env.TZ = 'America/New_York' })
  afterAll(() => { process.env.TZ = ORIGINAL_TZ })

  it('utc mode matches the historical toISOString() row rendering', () => {
    expect(formatTimestamp(1_704_067_205_000, 'utc')).toBe(new Date(1_704_067_205_000).toISOString())
  })

  it('local mode renders the browser\'s own zone, explicitly labelled so it is never mistaken for UTC', () => {
    // 2024-01-01T00:00:05Z == 2023-12-31 19:00:05 America/New_York (EST, UTC-5)
    expect(formatTimestamp(1_704_067_205_000, 'local')).toBe('2023-12-31 19:00:05.000 local')
  })
})

describe('retentionMsHint', () => {
  it('computes whole days', () => expect(retentionMsHint('604800000')).toBe('7d'))
  it('computes whole hours when not a whole number of days', () => expect(retentionMsHint('7200000')).toBe('2h'))
  it('returns null for -1 (infinite retention)', () => expect(retentionMsHint('-1')).toBeNull())
  it('returns null for a missing value', () => expect(retentionMsHint(null)).toBeNull())
  it('returns null when not cleanly expressible in days or hours', () => expect(retentionMsHint('1234567')).toBeNull())
})
