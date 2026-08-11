import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../test/utils'
import { ConsumersTab } from './TopicDetailPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getTopicConsumers: vi.fn(),
  getThroughput: vi.fn(),
}))

describe('ConsumersTab', () => {
  it('renders current rate and consumer groups with lag', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [
        { ts_ms: 1000, msgs_per_sec: 2.0, bytes_per_sec: null },
        { ts_ms: 2000, msgs_per_sec: 5.5, bytes_per_sec: null },
      ],
      as_of: 2000,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{
        group_id: 'billing', state: 'Stable', total_lag: 7,
        partitions: [{ topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 }],
      }],
      as_of: 2000,
    })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('5.5 msg/s')).toBeInTheDocument()
    expect(screen.getByText('billing')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
  })

  it('shows empty state when no group consumes the topic', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], as_of: 1000 })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText(/no consumer groups/i)).toBeInTheDocument()
  })
})
