import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { formatDateTimeMillis } from '../../components/DateTimePicker'
import { setTimeDisplayMode } from '../../lib/timeDisplayMode'
import { JumpControl } from './JumpControl'

// `@types/node` isn't in this app's tsconfig `types` (it's a browser app) —
// this is the one Node global the fixed-TZ tests below need, declared
// locally rather than widening the whole app's ambient types for one test
// file's sake.
declare const process: { env: Record<string, string | undefined> }

describe('JumpControl', () => {
  it('jumping to "now" calls onJump immediately with kind now, no expansion', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-now'))
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'now' })
    expect(screen.queryByTestId('jump-offset-input')).not.toBeInTheDocument()
  })

  it('jumping to "beginning" calls onJump immediately with kind beginning', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-beginning'))
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'beginning' })
  })

  it('has aria-labels on every segment', () => {
    render(<JumpControl onJump={vi.fn()} />)
    expect(screen.getByLabelText('jump to now')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to beginning')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to offset')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to timestamp')).toBeInTheDocument()
  })

  it('clicking "offset…" expands partition + offset inputs without calling onJump', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    expect(screen.getByTestId('jump-offset-partition-input')).toBeInTheDocument()
    expect(screen.getByTestId('jump-offset-value-input')).toBeInTheDocument()
    expect(onJump).not.toHaveBeenCalled()
  })

  it('offset apply is disabled until both partition and offset are valid numbers', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    const apply = screen.getByTestId('jump-offset-apply')
    expect(apply).toBeDisabled()

    await user.type(screen.getByTestId('jump-offset-partition-input'), '2')
    expect(apply).toBeDisabled() // offset still empty

    await user.type(screen.getByTestId('jump-offset-value-input'), 'abc')
    expect(apply).toBeDisabled() // non-numeric

    await user.clear(screen.getByTestId('jump-offset-value-input'))
    await user.type(screen.getByTestId('jump-offset-value-input'), '42')
    expect(apply).toBeEnabled()

    await user.click(apply)
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'offset', partition: 2, offset: 42 })
  })

  it('offset input applies on Enter when valid', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    await user.type(screen.getByTestId('jump-offset-partition-input'), '0')
    await user.type(screen.getByTestId('jump-offset-value-input'), '100{Enter}')
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'offset', partition: 0, offset: 100 })
  })

  it('Enter does not apply while offset inputs are invalid', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    await user.type(screen.getByTestId('jump-offset-partition-input'), '0')
    await user.type(screen.getByTestId('jump-offset-value-input'), '-5{Enter}')
    expect(onJump).not.toHaveBeenCalled()
  })

  it('clicking "timestamp…" expands a datetime-with-ms input, disabled apply until well-formed, applies on Enter', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-timestamp'))
    const apply = screen.getByTestId('jump-timestamp-apply')
    expect(apply).toBeDisabled()

    await user.type(screen.getByTestId('jump-timestamp-input'), 'nope')
    expect(apply).toBeDisabled()

    await user.clear(screen.getByTestId('jump-timestamp-input'))
    // Round-trips through the same format/parse pair the component itself
    // uses, so this is TZ-agnostic regardless of the machine running it.
    await user.type(screen.getByTestId('jump-timestamp-input'), `${formatDateTimeMillis(1700000000000)}{Enter}`)
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'timestamp', ts_ms: 1700000000000 })
  })

  it('switching segments resets the previous expansion', async () => {
    const user = userEvent.setup()
    render(<JumpControl onJump={vi.fn()} />)
    await user.click(screen.getByTestId('jump-offset'))
    expect(screen.getByTestId('jump-offset-partition-input')).toBeInTheDocument()
    await user.click(screen.getByTestId('jump-timestamp'))
    expect(screen.queryByTestId('jump-offset-partition-input')).not.toBeInTheDocument()
    expect(screen.getByTestId('jump-timestamp-input')).toBeInTheDocument()
  })

  // Fixed, non-UTC test zone so a picker that (incorrectly) treated its
  // value as UTC instead of local would produce a different, wrong ms value
  // and fail these tests. America/New_York is UTC-4 (EDT) on the date used.
  // The picker itself (calendar grid, day click, time fields, its own
  // Apply/Escape/outside-click handling) has its own dedicated suite —
  // ../../components/DateTimePicker.test.tsx. These tests are about the
  // INTEGRATION: JumpControl feeds it `tsMs`/receives ms back into `tsText`,
  // and the picker's own Apply is independent of the segment's "jump"
  // button — only the latter ever calls onJump.
  describe('timestamp picker integration (fixed TZ=America/New_York)', () => {
    const ORIGINAL_TZ = process.env.TZ

    beforeAll(() => {
      process.env.TZ = 'America/New_York'
    })

    afterAll(() => {
      process.env.TZ = ORIGINAL_TZ
    })

    it('labels the picker as local time, distinct from the UTC rows', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      expect(screen.getByLabelText(/pick timestamp.*local/i)).toBeInTheDocument()
    })

    it('picking via the popover fills the ms field; only the segment\'s jump button fires onJump', async () => {
      const user = userEvent.setup()
      const onJump = vi.fn()
      render(<JumpControl onJump={onJump} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      // Seed a known month/day by typing the outer datetime field first, so
      // the popover opens on August 2026 rather than whatever month the
      // test happens to run in. 2026-08-15T18:32:10Z == 2026-08-15 14:32:10
      // America/New_York (EDT, UTC-4).
      await user.type(screen.getByTestId('jump-timestamp-input'), '2026-08-15 14:32:10.000')
      await user.click(screen.getByLabelText(/pick timestamp.*local/i))
      await user.click(screen.getByTestId('datetime-picker-day-20'))
      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      await user.click(screen.getByTestId('datetime-picker-minute-05'))
      await user.click(screen.getByTestId('datetime-picker-second-00'))
      await user.click(screen.getByTestId('datetime-picker-apply'))

      // 2026-08-20 09:05:00 America/New_York (EDT, UTC-4) == 2026-08-20T13:05:00Z
      const expectedMs = Date.UTC(2026, 7, 20, 13, 5, 0)
      expect(screen.getByTestId('jump-timestamp-input')).toHaveValue('2026-08-20 09:05:00.000')
      expect(onJump).not.toHaveBeenCalled()

      await user.click(screen.getByTestId('jump-timestamp-apply'))
      expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'timestamp', ts_ms: expectedMs })
    })

    it('shows the picked instant\'s absolute UTC time so the local-time picker never misleads', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), '2026-08-15 14:32:10.000')
      expect(screen.getByTestId('jump-timestamp-preview')).toHaveTextContent('2026-08-15T18:32:10')
    })

    // UTC/local display toggle (owner ruling 2026-08-15): the preview
    // mirrors whatever zone the rest of the app (rows, header) is currently
    // showing, so it's always speaking the SAME language as the data the
    // reader is about to jump into — never hardcoded to UTC regardless of
    // the toggle.
    it('preview follows the UTC/local display toggle, showing the picked instant in the browser\'s local zone when toggled', async () => {
      setTimeDisplayMode('local')
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), '2026-08-15 14:32:10.000')
      // Same instant as above (2026-08-15T18:32:10Z), now shown local.
      expect(screen.getByTestId('jump-timestamp-preview')).toHaveTextContent('2026-08-15 14:32:10.000 local')
    })

    it('typing a datetime value directly also seeds the popover to match (bidirectional sync)', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      // 2026-08-15T18:32:10Z == 2026-08-15 14:32:10 America/New_York (EDT, UTC-4)
      await user.type(screen.getByTestId('jump-timestamp-input'), '2026-08-15 14:32:10.000')
      await user.click(screen.getByLabelText(/pick timestamp.*local/i))
      expect(screen.getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-hour-14')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
    })
  })
})
