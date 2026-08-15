import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { DateTimePicker, formatDateTimeMillis, parseDateTimeMillis } from './DateTimePicker'

// `@types/node` isn't in this app's tsconfig `types` (it's a browser app) —
// this is the one Node global the fixed-TZ tests below need, declared
// locally rather than widening the whole app's ambient types for one test
// file's sake.
declare const process: { env: Record<string, string | undefined> }

// Betrachtung has no native-popup fallback left to lean on (see report:
// Chrome/Firefox's own `datetime-local` calendar popup is themed by the
// OS/browser's `prefers-color-scheme`, NOT by page CSS — verified, not
// fixable from here). This is a fully hand-rolled popover, so both themes
// come from the app's own `dark:` classes — nothing to negotiate with the
// browser.
//
// picker3 (owner ruling 2026-08-16): the outer control is now a bare
// calendar-icon trigger button — the "current value" textbox this used to
// render itself moved one level up to the caller (JumpControl owns the
// single outer textbox now). Inside the popover: calendar grid on the
// LEFT, three independently-scrollable time columns (hour/minute/second)
// on the RIGHT, then a single editable "datetime with milliseconds"
// textbox below, then cancel (left) / apply (right).
describe('DateTimePicker', () => {
  it('is closed by default, rendering only the calendar-icon trigger button', () => {
    render(<DateTimePicker valueMs={null} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
    expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
    const trigger = screen.getByLabelText('pick timestamp (local time)')
    expect(trigger.tagName).toBe('BUTTON')
  })

  describe('format/parse helpers (round-trip through local wall-clock getters)', () => {
    it('formats an epoch-ms instant as a local yyyy-mm-dd hh:mm:ss.mmm string', () => {
      const ms = new Date(2026, 7, 15, 14, 32, 10, 5).getTime()
      expect(formatDateTimeMillis(ms)).toBe('2026-08-15 14:32:10.005')
    })

    it('parses a well-formed string back to the same epoch ms', () => {
      const ms = new Date(2026, 7, 15, 14, 32, 10, 5).getTime()
      expect(parseDateTimeMillis(formatDateTimeMillis(ms))).toBe(ms)
    })

    it('rejects malformed or out-of-range input', () => {
      expect(parseDateTimeMillis('not a date')).toBeNull()
      expect(parseDateTimeMillis('2026-02-30 10:00:00.000')).toBeNull() // Feb 30 doesn't exist
      expect(parseDateTimeMillis('2026-08-15 25:00:00.000')).toBeNull() // hour out of range
    })
  })

  // Fixed, non-UTC test zone so a picker that (incorrectly) treated the
  // value as UTC instead of local would produce a different, wrong ms value
  // and fail these tests. America/New_York is UTC-4 (EDT) on the date used.
  describe('fixed TZ=America/New_York', () => {
    const ORIGINAL_TZ = process.env.TZ

    beforeAll(() => {
      process.env.TZ = 'America/New_York'
    })

    afterAll(() => {
      process.env.TZ = ORIGINAL_TZ
    })

    // 2026-08-15 14:32:10 America/New_York (EDT, UTC-4) == 2026-08-15T18:32:10Z
    const SEED_MS = Date.UTC(2026, 7, 15, 18, 32, 10)

    it('opens on trigger click, seeded with the value\'s month/day/time and full datetime text', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={SEED_MS} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

      const popover = screen.getByTestId('datetime-picker-popover')
      expect(within(popover).getByText('August 2026')).toBeInTheDocument()
      expect(within(popover).getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
      expect(within(popover).getByTestId('datetime-picker-day-31')).toBeInTheDocument()
      expect(within(popover).queryByTestId('datetime-picker-day-32')).not.toBeInTheDocument()

      expect(screen.getByTestId('datetime-picker-hour-14')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 14:32:10.000')
    })

    it('opens seeded at the current moment when no value is set yet', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={null} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      const now = new Date()
      const monthLabel = now.toLocaleString('en-US', { month: 'long' })
      expect(screen.getByTestId('datetime-picker-popover')).toHaveTextContent(`${monthLabel} ${now.getFullYear()}`)
    })

    it('picking a day and time columns, then Apply, converts local wall-clock to the correct epoch ms and closes', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

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

    it('selecting a day resets milliseconds to 0, updating the textbox', async () => {
      const user = userEvent.setup()
      const seedWithMillis = SEED_MS + 500
      render(<DateTimePicker valueMs={seedWithMillis} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 14:32:10.500')

      await user.click(screen.getByTestId('datetime-picker-day-20'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-20 14:32:10.000')
    })

    it('selecting a time column value resets milliseconds to 0, updating the textbox', async () => {
      const user = userEvent.setup()
      const seedWithMillis = SEED_MS + 500
      render(<DateTimePicker valueMs={seedWithMillis} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 14:32:10.500')

      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      expect(screen.getByTestId('datetime-picker-text')).toHaveValue('2026-08-15 09:32:10.000')
    })

    it('the textbox is editable — typing a custom millisecond value flows into Apply\'s epoch ms', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

      const textbox = screen.getByTestId('datetime-picker-text')
      await user.clear(textbox)
      await user.type(textbox, '2026-08-15 14:32:10.777')
      await user.click(screen.getByTestId('datetime-picker-apply'))

      expect(onChange).toHaveBeenCalledExactlyOnceWith(SEED_MS + 777)
    })

    it('editing the textbox to a different date/time also re-highlights the calendar and time columns', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={SEED_MS} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

      const textbox = screen.getByTestId('datetime-picker-text')
      await user.clear(textbox)
      await user.type(textbox, '2026-08-20 09:05:00.000')

      expect(screen.getByTestId('datetime-picker-day-20')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-05')).toHaveAttribute('aria-pressed', 'true')
    })

    it('invalid textbox input disables Apply and shows an honest hint; correcting it re-enables Apply', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

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
      expect(onChange).toHaveBeenCalledExactlyOnceWith(Date.UTC(2026, 7, 20, 13, 5, 0))
    })

    it('navigating to the next/previous month changes the shown grid without calling onChange', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

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
      // 2026-01-31 10:00:00 America/New_York (EST, UTC-5) == 2026-01-31T15:00:00Z
      const jan31 = Date.UTC(2026, 0, 31, 15, 0, 0)
      render(<DateTimePicker valueMs={jan31} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

      await user.click(screen.getByLabelText('next month'))
      expect(screen.getByTestId('datetime-picker-popover')).toHaveTextContent('February 2026')
      // 2026 is not a leap year: the selection clamps to Feb 28.
      expect(screen.getByTestId('datetime-picker-day-28')).toHaveAttribute('aria-pressed', 'true')

      await user.click(screen.getByTestId('datetime-picker-apply'))
      // 2026-02-28 10:00:00 America/New_York (EST, UTC-5) == 2026-02-28T15:00:00Z — NOT rolled into March.
      expect(onChange).toHaveBeenCalledExactlyOnceWith(Date.UTC(2026, 1, 28, 15, 0, 0))
    })

    it('closes on Escape without calling onChange, discarding in-popover edits', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
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
          <DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />
        </div>,
      )
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      expect(screen.getByTestId('datetime-picker-popover')).toBeInTheDocument()

      await user.click(screen.getByText('outside'))
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      expect(onChange).not.toHaveBeenCalled()
    })

    it('the cancel button dismisses without calling onChange, discarding in-popover edits', async () => {
      const user = userEvent.setup()
      const onChange = vi.fn()
      render(<DateTimePicker valueMs={SEED_MS} onChange={onChange} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      await user.click(screen.getByTestId('datetime-picker-day-20'))
      await user.click(screen.getByTestId('datetime-picker-close'))
      expect(screen.queryByTestId('datetime-picker-popover')).not.toBeInTheDocument()
      expect(onChange).not.toHaveBeenCalled()
    })

    it('marks today distinctly from the selected day', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={SEED_MS} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      // SEED_MS's own day (Aug 15) being "today" is NOT guaranteed — the
      // component derives "today" from the real clock, not SEED_MS. What's
      // asserted is only the selected-vs-not distinction, which IS
      // deterministic: day 15 (the seeded value) is marked selected.
      expect(screen.getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-day-20')).toHaveAttribute('aria-pressed', 'false')
    })

    it('renders each time column as an independently-scrollable list of the full 00-23/00-59/00-59 range', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={SEED_MS} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))
      expect(within(screen.getByTestId('datetime-picker-hour-list')).getAllByRole('button')).toHaveLength(24)
      expect(within(screen.getByTestId('datetime-picker-minute-list')).getAllByRole('button')).toHaveLength(60)
      expect(within(screen.getByTestId('datetime-picker-second-list')).getAllByRole('button')).toHaveLength(60)
    })

    it('clicking a value in one time column does not change the selection of the other columns', async () => {
      const user = userEvent.setup()
      render(<DateTimePicker valueMs={SEED_MS} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
      await user.click(screen.getByLabelText('pick timestamp (local time)'))

      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      expect(screen.getByTestId('datetime-picker-hour-09')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
    })
  })

  it('the popover panel carries dark-mode classes alongside its light ones (both themes, no OS/browser dependency)', async () => {
    const user = userEvent.setup()
    render(<DateTimePicker valueMs={null} onChange={vi.fn()} ariaLabel="pick timestamp (local time)" />)
    await user.click(screen.getByLabelText('pick timestamp (local time)'))
    const popover = screen.getByTestId('datetime-picker-popover')
    expect(popover.className).toMatch(/\bdark:/)
  })
})
