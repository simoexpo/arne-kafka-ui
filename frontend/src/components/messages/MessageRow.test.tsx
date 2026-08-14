import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MessageRow } from './MessageRow'
import type { MessageOut } from '../../api/types'

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
})
