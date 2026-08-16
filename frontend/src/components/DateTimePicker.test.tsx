import type { ComponentProps } from 'react'
import { act, fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { withFixedTZ } from '../test/timezone'
import { DateTimePicker, formatDateTimeMillis, parseDateTimeMillis } from './DateTimePicker'
import { setTimeDisplayMode } from '../lib/timeDisplayMode'
import { wheelCenteredIndex, wheelScrollTopForIndex } from '../lib/wheelGeometry'

// Betrachtung has no native-popup fallback left to lean on (see report:
// Chrome/Firefox's own `datetime-local` calendar popup is themed by the
// OS/browser's `prefers-color-scheme`, NOT by page CSS — verified, not
// fixable from here). This is a fully hand-rolled popover, so both themes
// come from the app's own `dark:` classes — nothing to negotiate with the
// browser.
//
// picker3 feedback round (owner ruling 2026-08-16): the component is now
// self-contained like `FilterInput` — it owns the outer epoch-ms textbox
// AND a calendar-icon trigger embedded at its right edge — and it follows
// the app-wide UTC/local display toggle throughout the popover (calendar,
// time columns, "today" marker, internal datetime textbox). The time
// columns are looping wheels (3×-tripled list, selection always centered);
// jsdom has no real scroll physics, so the wrap/center geometry is tested
// as pure functions, and click-to-select is tested through the DOM as
// before.
function Picker(props: Partial<ComponentProps<typeof DateTimePicker>> = {}) {
  return (
    <DateTimePicker
      valueMs={null}
      onChange={vi.fn()}
      ariaLabel="pick timestamp"
      textValue=""
      onTextChange={vi.fn()}
      textTestId="outer-text"
      textAriaLabel="timestamp (epoch ms)"
      textPlaceholder="epoch ms"
      {...props}
    />
  )
}

describe('DateTimePicker', () => {
  describe('outer control (self-contained, FilterInput-style: textbox + embedded icon)', () => {
    it('is closed by default: one textbox (epoch ms) with the calendar icon embedded at its right edge, no separate sibling button', () => {
      render(<Picker textValue="1700000000000" />)
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()

      const textbox = screen.getByTestId('outer-text')
      expect(textbox).toHaveValue('1700000000000')
      expect(textbox).toHaveAttribute('placeholder', 'epoch ms')

      const icon = screen.getByTestId('datetime-picker-trigger')
      // "Embedded" (not a separate flow sibling): same positioning parent,
      // icon absolutely placed over/inside the textbox's own box.
      expect(textbox.parentElement).toBe(icon.parentElement)
      expect(icon.className).toMatch(/absolute/)
    })

    it('the outer textbox is editable raw epoch ms — typing calls onTextChange, Enter calls onTextEnter', async () => {
      const user = userEvent.setup()
      const onTextChange = vi.fn()
      const onTextEnter = vi.fn()
      render(<Picker textValue="123" onTextChange={onTextChange} onTextEnter={onTextEnter} />)

      await user.type(screen.getByTestId('outer-text'), '4')
      expect(onTextChange).toHaveBeenCalled()

      await user.type(screen.getByTestId('outer-text'), '{Enter}')
      expect(onTextEnter).toHaveBeenCalledTimes(1)
    })

    it("the icon trigger's aria-label reflects the active UTC/local display mode", () => {
      render(<Picker ariaLabel="pick timestamp" />)
      expect(screen.getByLabelText('pick timestamp (UTC)')).toBeInTheDocument()

      // `setTimeDisplayMode` notifies `useSyncExternalStore` subscribers
      // outside of any React event handler — wrap in `act` so the
      // resulting re-render is flushed before asserting, same as any other
      // external-store-driven update in a test.
      act(() => setTimeDisplayMode('local'))
      expect(screen.getByLabelText('pick timestamp (local)')).toBeInTheDocument()
    })
  })

  describe('zone-aware format/parse helpers (fixed TZ=America/New_York)', () => {
    withFixedTZ('America/New_York')

    // 2026-08-15 14:32:10 America/New_York (EDT, UTC-4) == 2026-08-15T18:32:10.005Z
    const MS = Date.UTC(2026, 7, 15, 18, 32, 10, 5)

    it('formats in UTC using UTC getters', () => {
      expect(formatDateTimeMillis(MS, 'utc')).toBe('2026-08-15 18:32:10.005')
    })

    it('formats in local using local (zone) getters', () => {
      expect(formatDateTimeMillis(MS, 'local')).toBe('2026-08-15 14:32:10.005')
    })

    it('round-trips through format/parse in either mode back to the same epoch ms', () => {
      expect(parseDateTimeMillis(formatDateTimeMillis(MS, 'utc'), 'utc')).toBe(MS)
      expect(parseDateTimeMillis(formatDateTimeMillis(MS, 'local'), 'local')).toBe(MS)
    })

    it('rejects malformed or out-of-range input regardless of mode', () => {
      expect(parseDateTimeMillis('not a date', 'utc')).toBeNull()
      expect(parseDateTimeMillis('2026-02-30 10:00:00.000', 'local')).toBeNull() // Feb 30 doesn't exist
      expect(parseDateTimeMillis('2026-08-15 25:00:00.000', 'utc')).toBeNull() // hour out of range
    })
  })

  // Fixed, non-UTC test zone so a picker that (incorrectly) treated a
  // "local" value as UTC (or vice versa) would produce a different, wrong
  // ms value and fail these tests. America/New_York is UTC-4 (EDT) on the
  // dates used.
  describe('fixed TZ=America/New_York', () => {
    withFixedTZ('America/New_York')

    // 2026-08-15 14:32:10 America/New_York (EDT, UTC-4) == 2026-08-15T18:32:10Z
    const SEED_MS = Date.UTC(2026, 7, 15, 18, 32, 10)

    describe('default mode (UTC — nothing toggled)', () => {
      it('opens seeded with the value read as UTC: month/day/time/textbox/zone-label all UTC', async () => {
        const user = userEvent.setup()
        render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
        await user.click(screen.getByTestId('datetime-picker-trigger'))

        const popover = screen.getByTestId('datetime-picker-popover')
        expect(within(popover).getByText('August 2026')).toBeInTheDocument()
        expect(within(popover).getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-hour-18')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 18:32:10.000')
        expect(screen.getByTestId('datetime-picker-zone-label')).toHaveTextContent('UTC')
      })
    })

    describe('local mode (toggled on)', () => {
      it('opens seeded with the value read as local wall-clock: month/day/time/textbox/zone-label all local, badge showing the numeric offset (never the word "local")', async () => {
        setTimeDisplayMode('local')
        const user = userEvent.setup()
        render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
        await user.click(screen.getByTestId('datetime-picker-trigger'))

        const popover = screen.getByTestId('datetime-picker-popover')
        expect(within(popover).getByText('August 2026')).toBeInTheDocument()
        expect(within(popover).getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-hour-14')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 14:32:10.000')
        // Aug 15 America/New_York is EDT (UTC-4).
        expect(screen.getByTestId('datetime-picker-zone-label')).toHaveTextContent('UTC-4')
      })

      it('picking a day and time columns, then Apply, converts local wall-clock to the correct epoch ms and closes', async () => {
        setTimeDisplayMode('local')
        const user = userEvent.setup()
        const onChange = vi.fn()
        render(<Picker valueMs={SEED_MS} onChange={onChange} />)
        await user.click(screen.getByTestId('datetime-picker-trigger'))

        await user.click(screen.getByTestId('datetime-picker-day-20'))
        await user.click(screen.getByTestId('datetime-picker-hour-09'))
        await user.click(screen.getByTestId('datetime-picker-minute-05'))
        await user.click(screen.getByTestId('datetime-picker-second-00'))

        expect(onChange).not.toHaveBeenCalled()
        await user.click(screen.getByTestId('datetime-picker-apply'))

        // 2026-08-20 09:05:00 America/New_York (EDT, UTC-4) == 2026-08-20T13:05:00Z
        expect(onChange).toHaveBeenCalledExactlyOnceWith(Date.UTC(2026, 7, 20, 13, 5, 0))
        expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      })
    })

    // The core invariant of following the toggle: a ts_ms whose UTC and
    // America/New_York calendar DAYS differ must show a different grid/
    // column selection per mode, yet re-toggling and hitting Apply without
    // touching anything else always reproduces the exact same epoch — the
    // toggle only changes how the SAME instant is displayed, never what it
    // actually is.
    describe('following the UTC/local toggle live, without drifting the epoch', () => {
      // 2026-08-15 02:00:00 UTC == 2026-08-14 22:00:00 America/New_York
      // (EDT, UTC-4) — a different CALENDAR DAY in each zone.
      const CROSS_DAY_MS = Date.UTC(2026, 7, 15, 2, 0, 0)

      it('shifts the grid/columns/textbox when the toggle changes while the popover stays open, and Apply yields the identical epoch in either mode', async () => {
        const user = userEvent.setup()
        const onChange = vi.fn()
        render(<Picker valueMs={CROSS_DAY_MS} onChange={onChange} />)
        await user.click(screen.getByTestId('datetime-picker-trigger'))

        // Default mode is UTC: Aug 15, 02:00:00.
        expect(screen.getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-hour-02')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 02:00:00.000')
        expect(screen.getByTestId('datetime-picker-zone-label')).toHaveTextContent('UTC')

        // Toggle to local WHILE the popover is open: America/New_York
        // reads the SAME instant as Aug 14, 22:00:00 — a different day.
        // `act` flushes the resulting re-render synchronously (`findBy*`
        // alone would be the wrong tool here: day-14's button ALREADY
        // exists in the DOM before the toggle, just unselected, so it
        // would resolve on its first check rather than waiting for the
        // attribute to actually change).
        act(() => setTimeDisplayMode('local'))
        expect(screen.getByTestId('datetime-picker-day-14')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-hour-22')).toHaveAttribute('aria-pressed', 'true')
        expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-14 22:00:00.000')
        // Still August (EDT, UTC-4) — never the word "local".
        expect(screen.getByTestId('datetime-picker-zone-label')).toHaveTextContent('UTC-4')

        // Nothing else touched — Apply must reproduce the exact same epoch.
        await user.click(screen.getByTestId('datetime-picker-apply'))
        expect(onChange).toHaveBeenCalledExactlyOnceWith(CROSS_DAY_MS)
      })
    })

    it('selecting a day resets milliseconds to 0, updating the textbox', async () => {
      const user = userEvent.setup()
      const seedWithMillis = SEED_MS + 500
      render(<Picker valueMs={seedWithMillis} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 18:32:10.500')

      await user.click(screen.getByTestId('datetime-picker-day-20'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-20 18:32:10.000')
    })

    it('selecting a time column value resets milliseconds to 0, updating the textbox', async () => {
      const user = userEvent.setup()
      const seedWithMillis = SEED_MS + 500
      render(<Picker valueMs={seedWithMillis} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 18:32:10.500')

      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 09:32:10.000')
    })

    it('the internal textbox is editable — typing a custom millisecond value flows into Apply\'s epoch ms', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<Picker valueMs={SEED_MS} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      const textbox = screen.getByTestId('datetime-picker-text')
      await user.clear(textbox)
      await user.type(textbox, '2026-08-15 18:32:10.777')
      await user.click(screen.getByTestId('datetime-picker-apply'))

      expect(onChange).toHaveBeenCalledExactlyOnceWith(SEED_MS + 777)
    })

    it('editing the internal textbox to a different date/time also re-highlights the calendar and time columns', async () => {
      const user = userEvent.setup()
      render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      const textbox = screen.getByTestId('datetime-picker-text')
      await user.clear(textbox)
      await user.type(textbox, '2026-08-20 09:05:00.000')

      expect(screen.getByTestId('datetime-picker-day-20')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-05')).toHaveAttribute('aria-pressed', 'true')
    })

    it('invalid internal textbox input disables Apply and shows an honest hint; correcting it re-enables Apply', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<Picker valueMs={SEED_MS} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      const textbox = screen.getByTestId('datetime-picker-text')
      await user.clear(textbox)
      await user.type(textbox, 'garbage')

      expect(screen.getByTestId('datetime-picker-apply')).toBeDisabled()
      expect(screen.getByTestId('datetime-picker-invalid-hint')).toBeInTheDocument()

      await user.clear(textbox)
      await user.type(textbox, '2026-08-20 09:05:00.000')
      expect(screen.getByTestId('datetime-picker-apply')).toBeEnabled()
      expect(screen.queryByTestId('datetime-picker-invalid-hint')).not.toBeInTheDocument()

      await user.click(screen.getByTestId('datetime-picker-apply'))
      // Default mode (UTC): the typed fields are taken as UTC verbatim.
      expect(onChange).toHaveBeenCalledExactlyOnceWith(Date.UTC(2026, 7, 20, 9, 5, 0))
    })

    it('navigating to the next/previous month changes the shown grid without calling onChange', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<Picker valueMs={SEED_MS} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      await user.click(screen.getByLabelText('next month'))
      expect(screen.getByTestId('datetime-picker-popover')).toHaveTextContent('September 2026')
      await user.click(screen.getByLabelText('previous month'))
      await user.click(screen.getByLabelText('previous month'))
      expect(screen.getByTestId('datetime-picker-popover')).toHaveTextContent('July 2026')
      expect(onChange).not.toHaveBeenCalled()
    })

    it('clamps the selected day when navigating to a shorter month, so Apply stays in the viewed month', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      // 2026-01-31 10:00:00 UTC (default mode)
      const jan31 = Date.UTC(2026, 0, 31, 10, 0, 0)
      render(<Picker valueMs={jan31} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      await user.click(screen.getByLabelText('next month'))
      expect(screen.getByTestId('datetime-picker-popover')).toHaveTextContent('February 2026')
      // 2026 is not a leap year: the selection clamps to Feb 28.
      expect(screen.getByTestId('datetime-picker-day-28')).toHaveAttribute('aria-pressed', 'true')

      await user.click(screen.getByTestId('datetime-picker-apply'))
      // NOT rolled into March.
      expect(onChange).toHaveBeenCalledExactlyOnceWith(Date.UTC(2026, 1, 28, 10, 0, 0))
    })

    it('closes on Escape without calling onChange, discarding in-popover edits', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<Picker valueMs={SEED_MS} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      await user.click(screen.getByTestId('datetime-picker-day-20'))

      await user.keyboard('{Escape}')
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      expect(onChange).not.toHaveBeenCalled()
    })

    it('closes on an outside click without calling onChange', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(
        <div>
          <button type="button">outside</button>
          <Picker valueMs={SEED_MS} onChange={onChange} />
        </div>,
      )
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      expect(screen.getByTestId('datetime-picker-popover')).toBeInTheDocument()

      await user.click(screen.getByText('outside'))
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      expect(onChange).not.toHaveBeenCalled()
    })

    it('the cancel button dismisses without calling onChange, discarding in-popover edits', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<Picker valueMs={SEED_MS} onChange={onChange} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      await user.click(screen.getByTestId('datetime-picker-day-20'))
      await user.click(screen.getByTestId('datetime-picker-close'))
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      expect(onChange).not.toHaveBeenCalled()
    })

    it('marks today distinctly from the selected day', async () => {
      const user = userEvent.setup()
      render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      // SEED_MS's own day (Aug 15) being "today" is NOT guaranteed — the
      // component derives "today" from the real clock, not SEED_MS. What's
      // asserted is only the selected-vs-not distinction, which IS
      // deterministic: day 15 (the seeded value) is marked selected.
      expect(screen.getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-day-20')).toHaveAttribute('aria-pressed', 'false')
    })

    // Owner ruling 2026-08-16 (second feedback round): emerald is
    // semantically reserved elsewhere (live/healthy, the jump-target row) —
    // the picker's own selection highlight must never collide with it.
    it('the selected day and the "today" marker use sky, never emerald', () => {
      // Fixed system time so "today" is deterministic and DISTINCT from the
      // seeded/selected day (Aug 15) — this is the one test in the file
      // that needs "today" to be a specific, known day rather than
      // "whatever the real clock says" (see the sibling test above).
      // `fireEvent` (not `userEvent`) — `userEvent`'s own internal delays
      // don't mix with fake timers without extra setup, and this test only
      // needs one plain click.
      vi.useFakeTimers()
      vi.setSystemTime(new Date(2026, 7, 10, 12, 0, 0))
      try {
        render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
        fireEvent.click(screen.getByTestId('datetime-picker-trigger'))

        const selectedDay = screen.getByTestId('datetime-picker-day-15')
        expect(selectedDay.className).toMatch(/\bbg-sky-/)
        expect(selectedDay.className).not.toMatch(/emerald/)

        const todayDay = screen.getByTestId('datetime-picker-day-10')
        expect(todayDay).toHaveAttribute('aria-pressed', 'false')
        expect(todayDay.className).toMatch(/\btext-sky-/)
        expect(todayDay.className).not.toMatch(/emerald/)
      } finally {
        vi.useRealTimers()
      }
    })

    it('the selected wheel-column value uses sky, never emerald', async () => {
      const user = userEvent.setup()
      render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      // Default mode (UTC, no toggle in this test): SEED_MS's hour is 18.
      const selectedHour = screen.getByTestId('datetime-picker-hour-18')
      expect(selectedHour.className).toMatch(/\bbg-sky-/)
      expect(selectedHour.className).not.toMatch(/emerald/)
    })

    it('renders each time column as a looping wheel: only the canonical (accessible) copy is exposed, at the full 00-23/00-59/00-59 range', async () => {
      const user = userEvent.setup()
      render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      // `getAllByRole` excludes `aria-hidden` elements by default, so this
      // counts only the ONE canonical (middle) copy per value — even
      // though the list is internally tripled for the wrap illusion.
      expect(within(screen.getByTestId('datetime-picker-hour-list')).getAllByRole('button')).toHaveLength(24)
      expect(within(screen.getByTestId('datetime-picker-minute-list')).getAllByRole('button')).toHaveLength(60)
      expect(within(screen.getByTestId('datetime-picker-second-list')).getAllByRole('button')).toHaveLength(60)
    })

    it('clicking a value in one time column does not change the selection of the other columns', async () => {
      const user = userEvent.setup()
      render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
      await user.click(screen.getByTestId('datetime-picker-trigger'))

      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
    })

    // Scrolling a wheel doesn't just pan it, it SELECTS — once the scroll
    // settles (no further scroll events for
    // ~120ms), whichever value is resting at the vertical center becomes
    // the selection, snapped exactly into place. jsdom has no real scroll
    // physics, so `scrollTop` is driven directly via stubbed `fireEvent.scroll`
    // (matching the pure geometry helpers tested above) and settling is
    // driven via fake timers — the actual wheel drag/momentum FEEL is
    // verified in the browser pass, not here.
    describe('scroll-to-select (fake timers drive the settle debounce)', () => {
      const scrollTopForHour = (h: number) => wheelScrollTopForIndex(wheelCenteredIndex(h, 24))

      it('scrolling a wheel column and letting it settle selects the centered value, snapping it to center', () => {
        vi.useFakeTimers()
        try {
          render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
          fireEvent.click(screen.getByTestId('datetime-picker-trigger'))

          const hourList = screen.getByTestId('datetime-picker-hour-list')
          fireEvent.scroll(hourList, { target: { scrollTop: scrollTopForHour(9) } })
          // Not yet selected — still settling.
          expect(screen.getByTestId('datetime-picker-hour-18')).toHaveAttribute('aria-pressed', 'true')

          act(() => { vi.advanceTimersByTime(150) })

          expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'true')
          expect(screen.getByTestId('datetime-picker-hour-18')).toHaveAttribute('aria-pressed', 'false')
          // Selecting via scroll zeroes millis exactly like a click selection.
          expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 09:32:10.000')
        } finally {
          vi.useRealTimers()
        }
      })

      it('does not select while still scrolling (before the settle window elapses)', () => {
        vi.useFakeTimers()
        try {
          render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
          fireEvent.click(screen.getByTestId('datetime-picker-trigger'))

          const hourList = screen.getByTestId('datetime-picker-hour-list')
          fireEvent.scroll(hourList, { target: { scrollTop: scrollTopForHour(9) } })
          act(() => { vi.advanceTimersByTime(80) }) // < the ~120ms settle window

          expect(screen.getByTestId('datetime-picker-hour-18')).toHaveAttribute('aria-pressed', 'true')
          expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'false')
        } finally {
          vi.useRealTimers()
        }
      })

      it('rapid successive scroll events reset the settle timer — only the final resting position is selected', () => {
        vi.useFakeTimers()
        try {
          render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
          fireEvent.click(screen.getByTestId('datetime-picker-trigger'))

          const hourList = screen.getByTestId('datetime-picker-hour-list')
          fireEvent.scroll(hourList, { target: { scrollTop: scrollTopForHour(9) } })
          act(() => { vi.advanceTimersByTime(80) }) // resets the debounce below
          fireEvent.scroll(hourList, { target: { scrollTop: scrollTopForHour(3) } })
          act(() => { vi.advanceTimersByTime(150) })

          expect(screen.getByTestId('datetime-picker-hour-03')).toHaveAttribute('aria-pressed', 'true')
          expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'false')
          expect(screen.getByTestId('datetime-picker-hour-18')).toHaveAttribute('aria-pressed', 'false')
        } finally {
          vi.useRealTimers()
        }
      })

      it('scrolling one column does not affect the others', () => {
        vi.useFakeTimers()
        try {
          render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
          fireEvent.click(screen.getByTestId('datetime-picker-trigger'))

          const hourList = screen.getByTestId('datetime-picker-hour-list')
          fireEvent.scroll(hourList, { target: { scrollTop: scrollTopForHour(9) } })
          act(() => { vi.advanceTimersByTime(150) })

          expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
          expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
        } finally {
          vi.useRealTimers()
        }
      })

      it('click-to-select still works unchanged alongside scroll-to-select', async () => {
        const user = userEvent.setup()
        render(<Picker valueMs={SEED_MS} onChange={vi.fn()} />)
        await user.click(screen.getByTestId('datetime-picker-trigger'))
        await user.click(screen.getByTestId('datetime-picker-minute-05'))
        expect(screen.getByTestId('datetime-picker-minute-05')).toHaveAttribute('aria-pressed', 'true')
      })
    })
  })

  it('the popover panel carries dark-mode classes alongside its light ones (both themes, no OS/browser dependency)', async () => {
    const user = userEvent.setup()
    render(<Picker />)
    await user.click(screen.getByTestId('datetime-picker-trigger'))
    const popover = screen.getByTestId('datetime-picker-popover')
    expect(popover.className).toMatch(/\bdark:/)
  })
})
