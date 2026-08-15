import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
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
  describe('datetime-local picker (fixed TZ=America/New_York)', () => {
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

    it('picking a date+time converts from the picker\'s local zone to the correct UTC epoch ms and fills the ms field', async () => {
      const user = userEvent.setup()
      const onJump = vi.fn()
      render(<JumpControl onJump={onJump} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      const picker = screen.getByTestId('jump-timestamp-picker')
      fireEvent.change(picker, { target: { value: '2026-08-15T14:32:10' } })

      // 2026-08-15 14:32:10 America/New_York (EDT, UTC-4) == 2026-08-15T18:32:10Z
      const expectedMs = Date.UTC(2026, 7, 15, 18, 32, 10)
      expect(screen.getByTestId('jump-timestamp-input')).toHaveValue(String(expectedMs))

      await user.click(screen.getByTestId('jump-timestamp-apply'))
      expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'timestamp', ts_ms: expectedMs })
    })

    it('shows the picked instant\'s absolute UTC time so the local-time picker never misleads', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      const picker = screen.getByTestId('jump-timestamp-picker')
      fireEvent.change(picker, { target: { value: '2026-08-15T14:32:10' } })
      expect(screen.getByTestId('jump-timestamp-utc-preview')).toHaveTextContent('2026-08-15T18:32:10')
    })

    it('typing an epoch-ms value directly also updates the picker to match (bidirectional sync)', async () => {
      const user = userEvent.setup()
      render(<JumpControl onJump={vi.fn()} />)
      await user.click(screen.getByTestId('jump-timestamp'))
      // 2026-08-15T18:32:10Z == 2026-08-15 14:32:10 America/New_York (EDT, UTC-4)
      await user.type(screen.getByTestId('jump-timestamp-input'), String(Date.UTC(2026, 7, 15, 18, 32, 10)))
      const picker = screen.getByTestId('jump-timestamp-picker') as HTMLInputElement
      expect(picker.value).toMatch(/^2026-08-15T14:32:10/)
    })
  })
})
