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

const msg = (overrides: Partial<MessageOut> = {}): MessageOut => ({
  partition: 0, offset: 0, timestamp_ms: 1,
  key: null,
  value: { encoding: 'utf8', text: 'v', schema_id: null, error: null },
  headers: [],
  ...overrides,
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
  it('flags exactly the rows whose timestamp is newer than the row above', () => {
    // newest-first merged order with a same-partition inversion:
    // [p1o5 ts=80, p0o2 ts=50, p0o1 ts=100] → only the ts=100 row is marked
    const rows = [
      msg({ partition: 1, offset: 5, timestamp_ms: 80 }),
      msg({ partition: 0, offset: 2, timestamp_ms: 50 }),
      msg({ partition: 0, offset: 1, timestamp_ms: 100 }),
    ]
    render(<MessageList messages={rows} />)
    expect(screen.getAllByTestId('ts-inversion')).toHaveLength(1)
    const marked = screen.getAllByTestId('message-row')[2]
    expect(marked.querySelector('[data-testid="ts-inversion"]')).toBeInTheDocument()
  })
  it('null timestamps neither get marked nor mark their neighbors', () => {
    const rows = [
      msg({ partition: 0, offset: 3, timestamp_ms: null }),
      msg({ partition: 0, offset: 2, timestamp_ms: 100 }),
      msg({ partition: 0, offset: 1, timestamp_ms: null }),
    ]
    render(<MessageList messages={rows} />)
    expect(screen.queryAllByTestId('ts-inversion')).toHaveLength(0)
  })
})
