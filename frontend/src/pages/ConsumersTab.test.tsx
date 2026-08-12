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
      as_of: Date.now(),
    })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('5.5 msg/s')).toBeInTheDocument()
    expect(screen.getByText('billing')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
    // lag data carries its own sample timestamp, distinct from the throughput panel's
    expect(screen.getByText('just now')).toBeInTheDocument()
  })

  it('shows empty state when no group consumes the topic', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], as_of: 1000 })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText(/no consumer groups/i)).toBeInTheDocument()
  })

  it('caption states the sparkline span so the chart never lies about its window', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: 1000, msgs_per_sec: 2.0, bytes_per_sec: null }],
      as_of: 1000,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], as_of: 1000 })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('last 15m')).toBeInTheDocument()
  })

  it('excludes samples older than 15 minutes before the newest sample from the sparkline', async () => {
    const newest = 20 * 60_000 // 20 minutes on the sample clock
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [
        { ts_ms: 0, msgs_per_sec: 999, bytes_per_sec: null }, // 20 min before newest -> excluded
        { ts_ms: newest - 10 * 60_000, msgs_per_sec: 3.0, bytes_per_sec: null }, // 10 min before -> included
        { ts_ms: newest, msgs_per_sec: 5.5, bytes_per_sec: null },
      ],
      as_of: newest,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], as_of: newest })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByText('5.5 msg/s')
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // only 2 of the 3 samples fall within the 15m window -> only 2 points plotted
    expect(polyline?.getAttribute('points')?.trim().split(' ')).toHaveLength(2)
  })
})
