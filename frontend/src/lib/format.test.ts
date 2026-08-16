import { describe, expect, it } from 'vitest'
import { estimateErrorTitle, formatAgo, formatBytes, formatCount, formatRetentionValue, formatTimestamp, formatWindowRange, retentionMsHint, zoneSuffix } from './format'
import { withFixedTZ } from '../test/timezone'

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

describe('estimateErrorTitle', () => {
  // Kafka attribution, product voice — never the raw error alone, and never
  // the internal operation name the backend called it with (only the
  // underlying reason — see admin.rs's `estimate_error_message`).
  it('attributes a watermark-fetch failure to Kafka, including the reason', () => {
    expect(estimateErrorTitle('counting messages timed out')).toBe(
      "Kafka couldn't provide a count — counting messages timed out",
    )
  })
})

describe('formatBytes', () => {
  it('sub-KB verbatim with a B suffix', () => expect(formatBytes(999)).toBe('999 B'))
  it('kilobytes', () => expect(formatBytes(1536)).toBe('1.5 KB'))
  it('megabytes', () => expect(formatBytes(2 * 1024 * 1024)).toBe('2.0 MB'))
  it('gigabytes', () => expect(formatBytes(3.1 * 1024 * 1024 * 1024)).toBe('3.1 GB'))
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

  // ONE format family in both modes — same layout,
  // only the zone SUFFIX differs. The word "local" is gone; local mode
  // instead carries a NUMERIC, per-timestamp, DST-honest UTC offset (e.g.
  // "UTC-5" / "UTC+5:30"). Fixed non-UTC TZ so a mode that silently fell
  // back to UTC math (or hardcoded a single offset regardless of DST) would
  // fail these.
  describe('local mode (fixed TZ=America/New_York)', () => {
    withFixedTZ('America/New_York')

    it('shows the shared LOCAL date once, then oldest -> newest LOCAL time, suffixed with the numeric UTC offset', () => {
      // 2024-01-01T00:00:05Z .. 2024-01-01T00:01:05Z == 2023-12-31 19:00:05 .. 19:01:05 America/New_York (EST, UTC-5)
      expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000, 'local')).toBe(
        '2023-12-31 19:00 → 19:01 UTC-5',
      )
    })

    it('defaults to utc when no mode is given (backward compatible)', () => {
      expect(formatWindowRange(1_704_067_205_000, 1_704_067_265_000)).toBe('2024-01-01 00:00 → 00:01 UTC')
    })
  })
})

describe('formatTimestamp', () => {
  withFixedTZ('America/New_York')

  // One format family for both modes —
  // "yyyy-mm-dd hh:mm:ss.mmm <ZONE>" — only the trailing zone suffix
  // differs ("UTC" vs a numeric per-timestamp offset). This REPLACES the
  // historical bare `toISOString()` row rendering (`T`/`Z` ISO shape).
  it('utc mode: the shared family, suffixed "UTC"', () => {
    expect(formatTimestamp(1_704_067_205_000, 'utc')).toBe('2024-01-01 00:00:05.000 UTC')
  })

  it('local mode: the same family in local wall-clock time, suffixed with a NUMERIC offset — never the word "local"', () => {
    // 2024-01-01T00:00:05Z == 2023-12-31 19:00:05 America/New_York (EST, UTC-5)
    expect(formatTimestamp(1_704_067_205_000, 'local')).toBe('2023-12-31 19:00:05.000 UTC-5')
  })

  it('the offset is honest per-timestamp: DST and non-DST instants in the same zone carry different offsets', () => {
    // Aug 15 2026 America/New_York is EDT (UTC-4).
    expect(formatTimestamp(Date.UTC(2026, 7, 15, 12, 0, 0), 'local')).toBe('2026-08-15 08:00:00.000 UTC-4')
    // Jan 15 2026 America/New_York is EST (UTC-5).
    expect(formatTimestamp(Date.UTC(2026, 0, 15, 12, 0, 0), 'local')).toBe('2026-01-15 07:00:00.000 UTC-5')
  })

  it('millis:false omits the trailing .mmm (unused by any current caller, but exercised here directly)', () => {
    expect(formatTimestamp(1_704_067_205_000, 'utc', { millis: false })).toBe('2024-01-01 00:00:05 UTC')
  })
})

describe('zoneSuffix', () => {
  withFixedTZ('America/New_York')

  it('is always "UTC" in utc mode', () => {
    expect(zoneSuffix(1_704_067_205_000, 'utc')).toBe('UTC')
  })

  it('is a numeric, sign-prefixed offset in local mode', () => {
    expect(zoneSuffix(Date.UTC(2026, 7, 15, 12, 0, 0), 'local')).toBe('UTC-4') // EDT
    expect(zoneSuffix(Date.UTC(2026, 0, 15, 12, 0, 0), 'local')).toBe('UTC-5') // EST
  })

  it('formats half-hour zone offsets with minutes (e.g. UTC+5:30)', () => {
    // A half-hour-offset zone doesn't depend on the fixed America/New_York
    // TZ above — this stubs getTimezoneOffset directly to prove the minutes
    // branch, independent of any real IANA zone being available in CI.
    const ms = Date.UTC(2026, 7, 15, 12, 0, 0)
    const original = Date.prototype.getTimezoneOffset
    Date.prototype.getTimezoneOffset = () => -330 // UTC+5:30 (e.g. India)
    try {
      expect(zoneSuffix(ms, 'local')).toBe('UTC+5:30')
    } finally {
      Date.prototype.getTimezoneOffset = original
    }
  })
})

describe('retentionMsHint', () => {
  it('computes whole days', () => expect(retentionMsHint('604800000')).toBe('7d'))
  it('computes whole hours when not a whole number of days', () => expect(retentionMsHint('7200000')).toBe('2h'))
  it('returns null for -1 (infinite retention)', () => expect(retentionMsHint('-1')).toBeNull())
  it('returns null for a missing value', () => expect(retentionMsHint(null)).toBeNull())
  it('returns null when not cleanly expressible in days or hours', () => expect(retentionMsHint('1234567')).toBeNull())
})
