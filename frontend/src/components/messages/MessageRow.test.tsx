import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
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
    render(<MessageRow message={msg()} expanded={false} onToggle={() => {}} />)
    expect(screen.getByText('p2·1337')).toBeInTheDocument()
    expect(screen.getByText('order-42')).toBeInTheDocument()
    expect(screen.getByText(/"total": 99/)).toBeInTheDocument()
    expect(screen.queryByText('trace-id')).not.toBeInTheDocument()
  })

  // Fix: expansion state survives virtualization, owned by identity —
  // MessageRow is a fully CONTROLLED component now (no local `useState`):
  // whether it renders expanded is entirely a function of the `expanded`
  // prop, and clicking only ever reports the click via `onToggle`. Timeline
  // (via MessageList) owns the actual expanded/collapsed truth, keyed by
  // (partition, offset) identity — see Timeline.tsx's `expandedKeysRef`.
  it('renders expanded content when `expanded` is true, with no click needed', () => {
    render(<MessageRow message={msg()} expanded onToggle={() => {}} />)
    expect(screen.getByText('trace-id')).toBeInTheDocument()
    expect(screen.getByText('total')).toBeInTheDocument() // JsonView key
  })

  it('clicking calls onToggle, but does not itself change what is rendered', async () => {
    const onToggle = vi.fn()
    const user = userEvent.setup()
    render(<MessageRow message={msg()} expanded={false} onToggle={onToggle} />)

    await user.click(screen.getByTestId('message-row'))
    expect(onToggle).toHaveBeenCalledTimes(1)
    // Still collapsed: MessageRow never manages its own expansion anymore —
    // only a parent re-rendering with `expanded` changed would show content.
    expect(screen.queryByText('trace-id')).not.toBeInTheDocument()
  })

  it('calls onToggle exactly once per click under StrictMode double-rendering', async () => {
    const onToggle = vi.fn()
    const user = userEvent.setup()
    render(
      <StrictMode>
        <MessageRow message={msg()} expanded={false} onToggle={onToggle} />
      </StrictMode>,
    )

    await user.click(screen.getByTestId('message-row'))
    expect(onToggle).toHaveBeenCalledTimes(1)
  })

  it('marks an out-of-order timestamp with the alert icon', () => {
    render(<MessageRow message={msg({ timestamp_ms: 100 })} tsInverted expanded={false} onToggle={() => {}} />)
    const icon = screen.getByTestId('ts-inversion')
    expect(icon).toBeInTheDocument()
    // tooltip explains the ruling in product voice
    expect(icon.querySelector('title')?.textContent).toMatch(/same-partition order/i)
  })
  it('renders no icon by default', () => {
    render(<MessageRow message={msg({ timestamp_ms: 100 })} expanded={false} onToggle={() => {}} />)
    expect(screen.queryByTestId('ts-inversion')).not.toBeInTheDocument()
  })
  it('marks the jump target with a highlighted background and a marker', () => {
    render(<MessageRow message={msg()} isJumpTarget expanded={false} onToggle={() => {}} />)
    const row = screen.getByTestId('message-row')
    expect(row.className).toMatch(/emerald/)
    expect(screen.getByTestId('jump-target')).toBeInTheDocument()
  })
  it('renders no jump-target marker or highlight by default', () => {
    render(<MessageRow message={msg()} expanded={false} onToggle={() => {}} />)
    expect(screen.queryByTestId('jump-target')).not.toBeInTheDocument()
    expect(screen.getByTestId('message-row').className).not.toMatch(/emerald/)
  })

  describe('keyboard operability', () => {
    it('exposes the row as a button with aria-expanded reflecting the prop', () => {
      const { rerender } = render(<MessageRow message={msg()} expanded={false} onToggle={() => {}} />)
      const row = screen.getByTestId('message-row')
      expect(row).toHaveAttribute('role', 'button')
      expect(row).toHaveAttribute('tabIndex', '0')
      expect(row).toHaveAttribute('aria-expanded', 'false')

      rerender(<MessageRow message={msg()} expanded onToggle={() => {}} />)
      expect(screen.getByTestId('message-row')).toHaveAttribute('aria-expanded', 'true')
    })

    it('Enter toggles the row', () => {
      const onToggle = vi.fn()
      render(<MessageRow message={msg()} expanded={false} onToggle={onToggle} />)
      fireEvent.keyDown(screen.getByTestId('message-row'), { key: 'Enter' })
      expect(onToggle).toHaveBeenCalledTimes(1)
    })

    it('Space toggles the row and prevents the page from scrolling', () => {
      const onToggle = vi.fn()
      render(<MessageRow message={msg()} expanded={false} onToggle={onToggle} />)
      const event = fireEvent.keyDown(screen.getByTestId('message-row'), { key: ' ' })
      expect(onToggle).toHaveBeenCalledTimes(1)
      expect(event).toBe(false) // fireEvent returns false when preventDefault() was called
    })

    it('ignores other keys', () => {
      const onToggle = vi.fn()
      render(<MessageRow message={msg()} expanded={false} onToggle={onToggle} />)
      fireEvent.keyDown(screen.getByTestId('message-row'), { key: 'a' })
      expect(onToggle).not.toHaveBeenCalled()
    })

    it('a copy button inside the expanded row is independently focusable and does not toggle the row on Enter', async () => {
      const onToggle = vi.fn()
      const user = userEvent.setup()
      const writeText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
      render(<MessageRow message={msg()} expanded onToggle={onToggle} />)

      const copyKey = screen.getByLabelText('copy key')
      copyKey.focus()
      expect(copyKey).toHaveFocus()
      await user.keyboard('{Enter}')
      expect(writeText).toHaveBeenCalled()
      expect(onToggle).not.toHaveBeenCalled()
    })

    // JsonView's <summary> is a nested disclosure control, exactly like the
    // copy button above — a click on it (whether from a mouse, or the real
    // bubbling `click` the HTML spec has the browser synthesize when
    // Enter/Space activates a focused <summary>) must never also toggle the
    // row underneath it. jsdom cannot synthesize that keyboard-triggered
    // activation itself (there is no way to fire it from a keydown here),
    // but a plain click on the <summary> element exercises the exact same
    // bubbling path that activation's synthesized click would take — this
    // is as far as this suite can verify without a real browser.
    it('a click on JsonView\'s <summary> inside the expanded row does not toggle the row', () => {
      const onToggle = vi.fn()
      const { container } = render(<MessageRow message={msg()} expanded onToggle={onToggle} />)
      const summary = container.querySelector('summary')
      expect(summary).not.toBeNull()
      fireEvent.click(summary!)
      expect(onToggle).not.toHaveBeenCalled()
    })
  })

  // UTC/local display toggle (owner ruling 2026-08-15): rows re-render the
  // SAME epoch-ms value in either zone — no refetch, pure formatting swap.
  describe('time display mode (fixed TZ=America/New_York)', () => {
    withFixedTZ('America/New_York')

    it('shows UTC by default (unchanged historical behavior)', () => {
      render(<MessageRow message={msg({ timestamp_ms: 1_704_067_205_000 })} expanded={false} onToggle={() => {}} />)
      expect(screen.getByText('2024-01-01 00:00:05.000 UTC')).toBeInTheDocument()
    })

    it('shows the browser\'s local zone, suffixed with its numeric offset, once the toggle is set to local', () => {
      setTimeDisplayMode('local')
      render(<MessageRow message={msg({ timestamp_ms: 1_704_067_205_000 })} expanded={false} onToggle={() => {}} />)
      // 2024-01-01T00:00:05Z == 2023-12-31 19:00:05 America/New_York (EST, UTC-5)
      expect(screen.getByText('2023-12-31 19:00:05.000 UTC-5')).toBeInTheDocument()
    })
  })
})
