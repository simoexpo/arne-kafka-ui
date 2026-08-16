import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { getTimeDisplayMode } from '../lib/timeDisplayMode'
import { withFixedTZ } from '../test/timezone'
import { TimeZoneToggle } from './TimeZoneToggle'

// UTC/local display toggle: moved out of the sidebar (`layout/AppShell.tsx`)
// into its own component so it can be rendered wherever its effect is
// actually felt — the Messages tab's Timeline header (see Timeline.tsx),
// next to the window-range display it rewrites. This suite covers the
// toggle CONTROL in isolation; Timeline.test.tsx covers it wired into the
// header (row re-render, zero network, persistence across remount).
//
// Owner ruling (relabel — reverts an earlier dynamic-offset label): the two
// halves read the MODE NAMES, "UTC" / "local", never a live
// numeric offset — a mode selector labeled with a current value contradicts
// rows spanning a DST transition (which legitimately carry different
// offsets) and implies a choice of offsets that doesn't exist. The local
// half instead carries a `title` tooltip telling the full story in product
// voice ("browser time — currently UTC+2"), computed live off "now" via the
// same `zoneSuffix` family every timestamp display uses (half-hour zones
// included). Fixed TZ + fake system time make "current offset" deterministic
// here.
describe('TimeZoneToggle', () => {
  withFixedTZ('America/New_York')

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date(Date.UTC(2026, 7, 15, 12, 0, 0))) // Aug 15: EDT, UTC-4
  })
  afterEach(() => vi.useRealTimers())

  it('defaults to UTC, labels both halves by mode name, and tooltips the current offset on the local half', () => {
    render(<TimeZoneToggle />)
    const button = screen.getByRole('button', { name: /time zone/i })
    expect(button).toHaveAttribute('data-mode', 'utc')
    expect(button).toHaveTextContent('UTC')
    expect(button).toHaveTextContent('local')
    expect(screen.getByText('local')).toHaveAttribute('title', 'browser time — currently UTC-4')
  })

  // `fireEvent` (not `userEvent`) — userEvent's own internal delays don't
  // mix with fake timers without extra setup, and these tests only need a
  // plain click (same pattern as DateTimePicker's "today marker" test).
  it('clicking flips to local and persists the choice', () => {
    render(<TimeZoneToggle />)
    const button = screen.getByRole('button', { name: /time zone/i })
    fireEvent.click(button)
    expect(button).toHaveAttribute('data-mode', 'local')
    expect(getTimeDisplayMode()).toBe('local')
  })

  it('clicking again flips back to utc', () => {
    render(<TimeZoneToggle />)
    const button = screen.getByRole('button', { name: /time zone/i })
    fireEvent.click(button)
    fireEvent.click(button)
    expect(button).toHaveAttribute('data-mode', 'utc')
  })
})
