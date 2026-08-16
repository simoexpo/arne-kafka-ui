import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StrictMode } from 'react'
import userEvent from '@testing-library/user-event'
import { MessageRow } from './MessageRow'
import type { MessageOut } from '../../api/types'
import { setTimeDisplayMode } from '../../lib/timeDisplayMode'
import { withFixedTZ } from '../../test/timezone'

const msg = (overrides: Partial<MessageOut> = {}): MessageOut => ({
  partition: 2, offset: 1337, timestamp_ms: 1754900000000,
  key: { encoding: 'utf8', text: 'order-42', schema_id: null, error: null },
  value: { encoding: 'json', text: '{"total": 99}', schema_id: null, error: null },
  headers: [{ key: 'trace-id', value: 'abc' }],
  ...overrides,
})

describe('MessageRow', () => {
  it('shows offset, key and value preview collapsed', () => {
    render(<MessageRow message={msg()} />)
    expect(screen.getByText('p2·1337')).toBeInTheDocument()
    expect(screen.getByText('order-42')).toBeInTheDocument()
    expect(screen.getByText(/"total": 99/)).toBeInTheDocument()
    expect(screen.queryByText('trace-id')).not.toBeInTheDocument()
  })
  it('expands on click to full payloads and headers', async () => {
    render(<MessageRow message={msg()} />)
    await userEvent.click(screen.getByTestId('message-row'))
    expect(screen.getByText('trace-id')).toBeInTheDocument()
    expect(screen.getByText('total')).toBeInTheDocument() // JsonView key
  })
  it('marks an out-of-order timestamp with the alert icon', () => {
    render(<MessageRow message={msg({ timestamp_ms: 100 })} tsInverted />)
    const icon = screen.getByTestId('ts-inversion')
    expect(icon).toBeInTheDocument()
    // tooltip explains the ruling in product voice
    expect(icon.querySelector('title')?.textContent).toMatch(/same-partition order/i)
  })
  it('renders no icon by default', () => {
    render(<MessageRow message={msg({ timestamp_ms: 100 })} />)
    expect(screen.queryByTestId('ts-inversion')).not.toBeInTheDocument()
  })
  it('marks the jump target with a highlighted background and a marker', () => {
    render(<MessageRow message={msg()} isJumpTarget />)
    const row = screen.getByTestId('message-row')
    expect(row.className).toMatch(/emerald/)
    expect(screen.getByTestId('jump-target')).toBeInTheDocument()
  })
  it('renders no jump-target marker or highlight by default', () => {
    render(<MessageRow message={msg()} />)
    expect(screen.queryByTestId('jump-target')).not.toBeInTheDocument()
    expect(screen.getByTestId('message-row').className).not.toMatch(/emerald/)
  })

  // Design spec v1.7 "Inspection pause": Timeline needs to know when a row
  // opens/closes to count live inspections.
  describe('onExpandChange', () => {
    it('fires with true on open, then false on close', async () => {
      const onExpandChange = vi.fn()
      const user = userEvent.setup()
      render(<MessageRow message={msg()} onExpandChange={onExpandChange} />)

      await user.click(screen.getByTestId('message-row'))
      expect(onExpandChange).toHaveBeenNthCalledWith(1, true)

      await user.click(screen.getByTestId('message-row'))
      expect(onExpandChange).toHaveBeenNthCalledWith(2, false)
      expect(onExpandChange).toHaveBeenCalledTimes(2)
    })

    it('fires exactly once per click under StrictMode double-rendering', async () => {
      const onExpandChange = vi.fn()
      const user = userEvent.setup()
      render(
        <StrictMode>
          <MessageRow message={msg()} onExpandChange={onExpandChange} />
        </StrictMode>,
      )

      await user.click(screen.getByTestId('message-row'))
      expect(onExpandChange).toHaveBeenCalledTimes(1)
      expect(onExpandChange).toHaveBeenCalledWith(true)
    })

    it('is optional — omitting it does not break the existing toggle behavior', async () => {
      const user = userEvent.setup()
      render(<MessageRow message={msg()} />)
      await user.click(screen.getByTestId('message-row'))
      expect(screen.getByText('trace-id')).toBeInTheDocument()
    })
  })

  // UTC/local display toggle (owner ruling 2026-08-15): rows re-render the
  // SAME epoch-ms value in either zone — no refetch, pure formatting swap.
  describe('time display mode (fixed TZ=America/New_York)', () => {
    withFixedTZ('America/New_York')

    it('shows UTC by default (unchanged historical behavior)', () => {
      render(<MessageRow message={msg({ timestamp_ms: 1_704_067_205_000 })} />)
      expect(screen.getByText('2024-01-01 00:00:05.000 UTC')).toBeInTheDocument()
    })

    it('shows the browser\'s local zone, suffixed with its numeric offset, once the toggle is set to local', () => {
      setTimeDisplayMode('local')
      render(<MessageRow message={msg({ timestamp_ms: 1_704_067_205_000 })} />)
      // 2024-01-01T00:00:05Z == 2023-12-31 19:00:05 America/New_York (EST, UTC-5)
      expect(screen.getByText('2023-12-31 19:00:05.000 UTC-5')).toBeInTheDocument()
    })
  })
})
