import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { setTimeDisplayMode } from '../../lib/timeDisplayMode'
import { JumpControl } from './JumpControl'

describe('JumpControl', () => {
  it('jumping to "now" calls onJump immediately with kind now, and collapses any open offset expansion', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    expect(screen.getByTestId('jump-offset-partition-input')).toBeInTheDocument()
    await user.click(screen.getByTestId('jump-now'))
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'now' })
    expect(screen.queryByTestId('jump-offset-partition-input')).not.toBeInTheDocument()
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

  it('clicking "timestamp…" expands an epoch-ms input, disabled apply until numeric, applies on Enter', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-timestamp'))
    const apply = screen.getByTestId('jump-timestamp-apply')
    expect(apply).toBeDisabled()

    await user.type(screen.getByTestId('jump-timestamp-input'), 'nope')
    expect(apply).toBeDisabled()

    await user.clear(screen.getByTestId('jump-timestamp-input'))
    await user.type(screen.getByTestId('jump-timestamp-input'), '1700000000000{Enter}')
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
  // The picker itself (calendar grid, day click, time columns, its own
  // Apply/Escape/outside-click handling, and following the UTC/local
  // toggle) has its own dedicated suite —
  // ../../components/DateTimePicker.test.tsx. These tests are about the
  // INTEGRATION: JumpControl's outer field is the raw epoch-ms number
  // (unchanged by the toggle) feeding/fed by the popover via `tsMs`/
  // `onChange`, and the picker's own Apply is independent of the segment's
  // "jump" button — only the latter ever calls onJump.
  describe('timestamp picker integration (fixed TZ=America/New_York)', () => {
    const ORIGINAL_TZ = process.env.TZ

    beforeAll(() => {
      process.env.TZ = 'America/New_York'
    })

    afterAll(() => {
      process.env.TZ = ORIGINAL_TZ
    })

    it("the icon trigger's aria-label reflects the current UTC/local toggle state", async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      expect(screen.getByLabelText('pick timestamp (UTC)')).toBeInTheDocument()

      // `setTimeDisplayMode` notifies `useSyncExternalStore` subscribers
      // outside any React event handler — wrap in `act` to flush the
      // resulting re-render before asserting.
      act(() => setTimeDisplayMode('local'))
      expect(screen.getByLabelText('pick timestamp (local)')).toBeInTheDocument()
    })

    it('picking via the popover (in local mode) fills the outer epoch-ms field; only the segment\'s jump button fires onJump', async () => {
      setTimeDisplayMode('local')
      const user = userEvent.setup()
      const onJump = vi.fn()
      render(<JumpControl onJump={onJump} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      // Seed a known month/day by typing the outer epoch-ms field first, so
      // the popover opens on August 2026 rather than whatever month the
      // test happens to run in. 2026-08-15T18:32:10Z == 2026-08-15 14:32:10
      // America/New_York (EDT, UTC-4).
      await user.type(screen.getByTestId('jump-timestamp-input'), String(Date.UTC(2026, 7, 15, 18, 32, 10)))
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      await user.click(screen.getByTestId('datetime-picker-day-20'))
      await user.click(screen.getByTestId('datetime-picker-hour-09'))
      await user.click(screen.getByTestId('datetime-picker-minute-05'))
      await user.click(screen.getByTestId('datetime-picker-second-00'))
      await user.click(screen.getByTestId('datetime-picker-apply'))

      // 2026-08-20 09:05:00 America/New_York (EDT, UTC-4) == 2026-08-20T13:05:00Z
      const expectedMs = Date.UTC(2026, 7, 20, 13, 5, 0)
      expect(screen.getByTestId('jump-timestamp-input')).toHaveValue(String(expectedMs))
      expect(onJump).not.toHaveBeenCalled()

      await user.click(screen.getByTestId('jump-timestamp-apply'))
      expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'timestamp', ts_ms: expectedMs })
    })

    it('shows the picked instant\'s absolute UTC time in the preview by default', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), String(Date.UTC(2026, 7, 15, 18, 32, 10)))
      expect(screen.getByTestId('jump-timestamp-preview')).toHaveTextContent('2026-08-15 18:32:10.000 UTC')
    })

    // UTC/local display toggle (owner ruling 2026-08-15): the preview
    // mirrors whatever zone the rest of the app (rows, header) is currently
    // showing, so it's always speaking the SAME language as the data the
    // reader is about to jump into — never hardcoded to UTC regardless of
    // the toggle. The outer epoch-ms field itself never changes shape, only
    // the preview and (per the picker's own suite) the popover follow it.
    it('preview follows the UTC/local display toggle, showing the picked instant in the browser\'s local zone when toggled', async () => {
      setTimeDisplayMode('local')
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), String(Date.UTC(2026, 7, 15, 18, 32, 10)))
      // Same instant as above (2026-08-15T18:32:10Z), now shown local
      // (America/New_York, EDT: UTC-4).
      expect(screen.getByTestId('jump-timestamp-preview')).toHaveTextContent('2026-08-15 14:32:10.000 UTC-4')
    })

    it('typing an epoch-ms value directly also seeds the popover to match (bidirectional sync, local mode)', async () => {
      setTimeDisplayMode('local')
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      // 2026-08-15T18:32:10Z == 2026-08-15 14:32:10 America/New_York (EDT, UTC-4)
      await user.type(screen.getByTestId('jump-timestamp-input'), String(Date.UTC(2026, 7, 15, 18, 32, 10)))
      await user.click(screen.getByTestId('datetime-picker-trigger'))
      expect(screen.getByTestId('datetime-picker-day-15')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-hour-14')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-minute-32')).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('datetime-picker-second-10')).toHaveAttribute('aria-pressed', 'true')
    })
  })
})

describe('partition dropdown', () => {
  it('renders the known partitions as a select and jumps with the chosen one', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} partitionIds={[0, 1, 2]} />)
    await user.click(screen.getByTestId('jump-offset'))
    const select = screen.getByTestId('jump-offset-partition-input')
    expect(select.tagName).toBe('SELECT')
    expect(select.querySelectorAll('option[value]:not([value=""])')).toHaveLength(3)
    await user.selectOptions(select, '2')
    await user.type(screen.getByTestId('jump-offset-value-input'), '77')
    await user.click(screen.getByTestId('jump-offset-apply'))
    expect(onJump).toHaveBeenCalledWith({ kind: 'offset', partition: 2, offset: 77 })
  })
})
