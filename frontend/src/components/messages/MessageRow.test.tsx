import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MessageRow } from './MessageRow'
import type { MessageOut } from '../../api/types'

const msg: MessageOut = {
  partition: 2, offset: 1337, timestamp_ms: 1754900000000,
  key: { encoding: 'utf8', text: 'order-42', schema_id: null, error: null },
  value: { encoding: 'json', text: '{"total": 99}', schema_id: null, error: null },
  headers: [{ key: 'trace-id', value: 'abc' }],
}

describe('MessageRow', () => {
  it('shows offset, key and value preview collapsed', () => {
    render(<MessageRow message={msg} />)
    expect(screen.getByText('p2·1337')).toBeInTheDocument()
    expect(screen.getByText('order-42')).toBeInTheDocument()
    expect(screen.getByText(/"total": 99/)).toBeInTheDocument()
    expect(screen.queryByText('trace-id')).not.toBeInTheDocument()
  })
  it('expands on click to full payloads and headers', async () => {
    render(<MessageRow message={msg} />)
    await userEvent.click(screen.getByTestId('message-row'))
    expect(screen.getByText('trace-id')).toBeInTheDocument()
    expect(screen.getByText('total')).toBeInTheDocument() // JsonView key
  })
})
