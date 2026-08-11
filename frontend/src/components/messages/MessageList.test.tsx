import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MessageList } from './MessageList'
import type { MessageOut } from '../../api/types'

const mk = (offset: number): MessageOut => ({
  partition: 0, offset, timestamp_ms: 1,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
})

describe('MessageList', () => {
  it('renders visible rows through the virtualizer', () => {
    render(<MessageList messages={Array.from({ length: 200 }, (_, i) => mk(i))} />)
    expect(screen.getByText('p0·0')).toBeInTheDocument()
    expect(screen.getAllByTestId('message-row').length).toBeLessThan(60) // virtualized, not 200
  })
  it('shows an empty state', () => {
    render(<MessageList messages={[]} />)
    expect(screen.getByText('no messages')).toBeInTheDocument()
  })
})
