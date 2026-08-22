import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithRouter } from '../test/utils'
import { ConsumersTab } from './ConsumersTab'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getTopicConsumers: vi.fn(),
  getThroughput: vi.fn(),
}))

describe('ConsumersTab', () => {
  // "no samples yet" already covers the empty state; a dash next to it would
  // be a second unknown-marker saying the same thing.
  it('renders no rate placeholder when there are no samples', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: 1000 })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByText('no samples yet')
    expect(screen.queryByTestId('current-rate')).toBeNull()
  })

  // Owner ruling 2026-08-22: clicking a row expands it (kept), so the way to
  // the group's own page is a separate link on the row — one that must NOT
  // also toggle the expansion.
  it('links to the group page without toggling the row expansion', async () => {
    const user = userEvent.setup()
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{
        group_id: 'billing', state: 'Stable', total_lag: 3, unreadable_partitions: 0, error: null,
        partitions: [{ topic: 'orders', partition: 0, committed_offset: 1, end_offset: 4, lag: 3 }],
      }],
      unchecked: [],
      as_of: 1000,
    })
    const { router } = await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByText('billing')
    const link = screen.getByRole('link', { name: /open billing/i })
    expect(link).toHaveAttribute('href', '/c/prod/consumers/billing')
    await user.click(link)
    expect(router.state.location.pathname).toBe('/c/prod/consumers/billing')
    // the expansion did not open as a side effect of following the link
    expect(screen.queryByText('committed')).toBeNull()
  })

  it('renders current rate and consumer groups with lag', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [
        { ts_ms: 1000, msgs_per_sec: 2.0, window_ms: 10_000, continuous: true, bytes_per_sec: null },
        { ts_ms: 2000, msgs_per_sec: 5.5, window_ms: 10_000, continuous: true, bytes_per_sec: null },
      ],
      as_of: 2000,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{
        group_id: 'billing', state: 'Stable', total_lag: 7, unreadable_partitions: 0, error: null,
        partitions: [{ topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 }],
      }],
      unchecked: [],
      as_of: Date.now(),
    })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByTestId('current-rate')).toBeInTheDocument()
    expect(screen.getByText('billing')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
    // lag data carries its own sample timestamp, distinct from the throughput panel's
    expect(screen.getByText('just now')).toBeInTheDocument()
  })

  // Owner ruling 2026-08-19: each section's freshness chip belongs at the
  // right of that section's own header, not floating inside its body.
  it('each panel carries its own freshness chip in its header', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: Date.now(), msgs_per_sec: 1, window_ms: 10_000, continuous: true, bytes_per_sec: null }],
      as_of: Date.now(),
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders', groups: [], unchecked: [], as_of: Date.now(),
    })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByText(/no consumer groups/i)
    const headers = screen.getAllByTestId('panel-header')
    const throughput = headers.find((h) => h.textContent?.includes('Throughput'))!
    const consumers = headers.find((h) => h.textContent?.includes('Consumer groups'))!
    expect(throughput).toHaveTextContent(/just now/i)
    expect(consumers).toHaveTextContent(/just now/i)
  })

  // The counter must say what window it measured: a rate taken across a
  // return visit's gap is an average over that gap, not a current rate.
  it('labels the rate with its window when the sample spans a gap', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: Date.now(), msgs_per_sec: 0.4, window_ms: 180_000, continuous: false, bytes_per_sec: null }],
      as_of: Date.now(),
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: Date.now() })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByTestId('current-rate')).toBeInTheDocument()
    expect(screen.getByTestId('rate-window')).toHaveTextContent(/3m/)
  })

  it('a rate measured over one ordinary interval needs no window caveat', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: Date.now(), msgs_per_sec: 5.5, window_ms: 10_000, continuous: true, bytes_per_sec: null }],
      as_of: Date.now(),
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: Date.now() })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByTestId('current-rate')).toBeInTheDocument()
    expect(screen.queryByTestId('rate-window')).not.toBeInTheDocument()
  })

  it('shows empty state when no group consumes the topic', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: 1000 })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText(/no consumer groups/i)).toBeInTheDocument()
  })

  // Owner ruling 2026-08-19: an assigned group with no committed offset is
  // Owner ruling 2026-08-20: one rule everywhere a lag total is shown — the
  // partitions we read prove a floor, and the tooltip says the rest is unread.
  it('states a topic total as a lower bound when one of its partitions is unreadable', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{
        group_id: 'billing', state: 'Stable', total_lag: 4_200, unreadable_partitions: 1,
        error: null, partitions: [],
      }],
      unchecked: [],
      as_of: 1000,
    })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('billing')).toBeInTheDocument()
    const lag = screen.getByTestId('group-lag')
    expect(lag).toHaveTextContent('≥ 4.2k')
    expect(lag).toHaveAttribute('title', expect.stringContaining('1'))
  })

  // consuming — where it reads is its own business until it commits — so it is
  // listed with an undetermined lag, never a fake 0.
  it('shows an assigned group that has not committed with a dash instead of zero lag', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({ topic: 'orders', samples: [], as_of: null })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({
      topic: 'orders',
      groups: [{ group_id: 'fresh', state: 'Stable', total_lag: null, unreadable_partitions: 0, error: null, partitions: [] }],
      unchecked: [],
      as_of: 1000,
    })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
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
        group_id: 'billing', state: 'Stable', total_lag: null, unreadable_partitions: 0,
        error: 'Broker: Not coordinator for group', partitions: [],
      }],
      unchecked: [],
      as_of: 1000,
    })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
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
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    const note = await screen.findByTestId('unchecked-groups')
    expect(note).toHaveTextContent(/2 groups couldn't be checked/i)
    expect(note).toHaveTextContent('ghost-a')
    expect(note).toHaveTextContent('Broker: Not coordinator for group')
  })

  it('caption states the sparkline span so the chart never lies about its window', async () => {
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [{ ts_ms: 1000, msgs_per_sec: 2.0, window_ms: 10_000, continuous: true, bytes_per_sec: null }],
      as_of: 1000,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: 1000 })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('last 15m')).toBeInTheDocument()
  })

  it('excludes samples older than 15 minutes before the newest sample from the sparkline', async () => {
    const newest = 20 * 60_000 // 20 minutes on the sample clock
    vi.mocked(client.getThroughput).mockResolvedValue({
      topic: 'orders',
      samples: [
        { ts_ms: 0, msgs_per_sec: 999, window_ms: 10_000, continuous: true, bytes_per_sec: null }, // 20 min before newest -> excluded
        { ts_ms: newest - 10 * 60_000, msgs_per_sec: 3.0, window_ms: 10_000, continuous: true, bytes_per_sec: null }, // 10 min before -> included
        { ts_ms: newest, msgs_per_sec: 5.5, window_ms: 10_000, continuous: true, bytes_per_sec: null },
      ],
      as_of: newest,
    })
    vi.mocked(client.getTopicConsumers).mockResolvedValue({ topic: 'orders', groups: [], unchecked: [], as_of: newest })
    await renderWithRouter(<ConsumersTab cluster="prod" topic="orders" />)
    await screen.findByTestId('current-rate')
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // only 2 of the 3 samples fall within the 15m window -> only 2 points plotted
    expect(polyline?.getAttribute('points')?.trim().split(' ')).toHaveLength(2)
  })
})
