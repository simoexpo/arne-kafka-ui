import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../test/utils'
import { ConsumersTab } from './ConsumersTab'
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
        group_id: 'billing', state: 'Stable', total_lag: 7, error: null,
        partitions: [{ topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 }],
      }],
      unchecked: [],
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
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: 1000 })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText(/no consumer groups/i)).toBeInTheDocument()
  })

  // Owner ruling 2026-08-19: an assigned group with no committed offset is
  // consuming — where it reads is its own business until it commits — so it is
  // listed with an undetermined lag, never a fake 0.
  it('shows an assigned group that has not committed with a dash instead of zero lag', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{ group_id: 'fresh', state: 'Stable', total_lag: null, error: null, partitions: [] }],
      unchecked: [],
      as_of: 1000,
    })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('fresh')).toBeInTheDocument()
    const lag = screen.getByTestId('group-lag')
    expect(lag).toHaveTextContent('—')
    expect(lag).toHaveAttribute('title', expect.stringContaining("hasn't committed"))
    expect(screen.queryByText('0')).not.toBeInTheDocument()
  })

  it('attributes a failed lag lookup to Kafka on the row that failed', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{
        group_id: 'billing', state: 'Stable', total_lag: null,
        error: 'Broker: Not coordinator for group', partitions: [],
      }],
      unchecked: [],
      as_of: 1000,
    })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('billing')).toBeInTheDocument()
    expect(screen.getByTestId('group-lag')).toHaveAttribute(
      'title',
      "Kafka couldn't read this group's offsets — Broker: Not coordinator for group",
    )
  })

  it('discloses groups it could not check instead of dropping them silently', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [],
      unchecked: [
        { group_id: 'ghost-a', error: 'Broker: Not coordinator for group' },
        { group_id: 'ghost-b', error: 'Broker: Not coordinator for group' },
      ],
      as_of: 1000,
    })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    const note = await screen.findByTestId('unchecked-groups')
    expect(note).toHaveTextContent(/2 groups couldn't be checked/i)
    expect(note).toHaveTextContent('ghost-a')
    expect(note).toHaveTextContent('Broker: Not coordinator for group')
  })

  it('caption states the sparkline span so the chart never lies about its window', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: 1000, msgs_per_sec: 2.0, bytes_per_sec: null }],
      as_of: 1000,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: 1000 })
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
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: newest })
    renderWithQuery(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByText('5.5 msg/s')
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // only 2 of the 3 samples fall within the 15m window -> only 2 points plotted
    expect(polyline?.getAttribute('points')?.trim().split(' ')).toHaveLength(2)
  })
})
