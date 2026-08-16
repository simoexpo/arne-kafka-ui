import { describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithRouter } from '../test/utils'
import { TopicsView } from './TopicsPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getTopics: vi.fn(),
}))

const topics = {
  topics: [
    { name: 'orders', partitions: 3, replication_factor: 2, message_estimate: 1200, estimate_error: null, size_bytes: null, internal: false },
    { name: 'payments', partitions: 1, replication_factor: 2, message_estimate: 50, estimate_error: null, size_bytes: null, internal: false },
    { name: '__consumer_offsets', partitions: 50, replication_factor: 3, message_estimate: 0, estimate_error: null, size_bytes: null, internal: true },
  ],
  as_of: Date.now(),
}

describe('TopicsView', () => {
  it('lists topics, hides internal by default, links to detail via SPA navigation', async () => {
    vi.mocked(client.getTopics).mockResolvedValue(topics)
    await renderWithRouter(<TopicsView cluster="prod" />, { initialPath: '/c/prod/topics/orders' })
    expect(await screen.findByText('orders')).toBeInTheDocument()
    expect(screen.queryByText('__consumer_offsets')).not.toBeInTheDocument()
    const link = screen.getByRole('link', { name: /orders/ })
    expect(link).toHaveAttribute('href', '/c/prod/topics/orders')
    // router-rendered <Link>, not a plain <a> full-reload anchor: the router
    // marks the link matching the current location as active itself
    expect(link).toHaveAttribute('data-status', 'active')
  })

  it('renders — for a topic whose message estimate is null', async () => {
    vi.mocked(client.getTopics).mockResolvedValue({
      // size_bytes is non-null so the only '—' in the row is attributable to
      // the message-estimate cell, not the (already-guarded) size cell
      topics: [
        { name: 'flaky', partitions: 2, replication_factor: 1, message_estimate: null, estimate_error: null, size_bytes: 100, internal: false },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<TopicsView cluster="prod" />)
    expect(await screen.findByText('flaky')).toBeInTheDocument()
    const row = screen.getByText('flaky').closest('tr')
    expect(row).toHaveTextContent('—')
    expect(row).not.toHaveTextContent('null')
  })

  // I5: an internal topic's null estimate (skipped on purpose) carries no
  // `estimate_error` and gets no tooltip; a genuine watermark-fetch failure
  // does, in product voice with kafka attribution.
  it('shows a title explaining WHY the estimate is missing when a watermark fetch failed', async () => {
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        {
          name: 'flaky',
          partitions: 2,
          replication_factor: 1,
          message_estimate: null,
          estimate_error: 'fetch watermarks: broker transport failure',
          size_bytes: 100,
          internal: false,
        },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('flaky')
    const row = screen.getByText('flaky').closest('tr')!
    const dash = within(row).getByText('—')
    expect(dash).toHaveAttribute('title', "Kafka couldn't provide a count — fetch watermarks: broker transport failure")
  })

  it('the size-only "—" carries no estimate-error tooltip', async () => {
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        { name: 'flaky', partitions: 2, replication_factor: 1, message_estimate: null, estimate_error: null, size_bytes: 100, internal: false },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('flaky')
    const row = screen.getByText('flaky').closest('tr')!
    expect(within(row).getByText('—')).not.toHaveAttribute('title')
  })

  it('encodes topic names with spaces and slashes in the detail link href', async () => {
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        { name: 'order events', partitions: 1, replication_factor: 1, message_estimate: 0, estimate_error: null, size_bytes: null, internal: false },
        { name: 'a/b', partitions: 1, replication_factor: 1, message_estimate: 0, estimate_error: null, size_bytes: null, internal: false },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<TopicsView cluster="prod" />)
    expect(await screen.findByText('order events')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'order events' })).toHaveAttribute(
      'href',
      '/c/prod/topics/order%20events',
    )
    expect(screen.getByRole('link', { name: 'a/b' })).toHaveAttribute('href', '/c/prod/topics/a%2Fb')
  })

  it('filter narrows instantly, the clear button resets it, and show-internal reveals internals', async () => {
    vi.mocked(client.getTopics).mockResolvedValue(topics)
    await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('orders')
    await userEvent.type(screen.getByPlaceholderText('filter topics…'), 'pay')
    expect(screen.queryByText('orders')).not.toBeInTheDocument()
    expect(screen.getByText('payments')).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'clear filter' }))
    expect(screen.getByText('orders')).toBeInTheDocument() // list restored
    await userEvent.click(screen.getByRole('switch', { name: /internal/i }))
    expect(screen.getByText('__consumer_offsets')).toBeInTheDocument()
  })

  it('show-internal switch label is content-sized, not stretched to fill the row', async () => {
    vi.mocked(client.getTopics).mockResolvedValue(topics)
    await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('orders')
    const label = screen.getByRole('switch', { name: /internal/i }).closest('label')
    expect(label?.className).toMatch(/\bw-fit\b/)
  })

  it('RF column header explains replication factor via a tooltip', async () => {
    vi.mocked(client.getTopics).mockResolvedValue(topics)
    await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('orders')
    expect(screen.getByText('RF')).toHaveAttribute('title', 'replication factor')
  })

  it('a failed background refresh keeps the topic list visible with a compact error banner', async () => {
    vi.mocked(client.getTopics)
      .mockResolvedValueOnce(topics)
      .mockRejectedValueOnce(
        new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
      )
    const { queryClient } = await renderWithRouter(<TopicsView cluster="prod" />)
    await screen.findByText('orders') // initial load succeeded
    await queryClient.refetchQueries({ queryKey: ['topics', 'prod'] })
    await waitFor(() => expect(screen.getByTestId('panel-error-banner')).toBeInTheDocument())
    expect(screen.getByText('orders')).toBeInTheDocument() // stale data stays visible
    expect(screen.queryByText(/retriable/i)).not.toBeInTheDocument() // banner, not the full-block takeover
  })

  it('copies the topic name without navigating when the row copy button is clicked', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    vi.mocked(client.getTopics).mockResolvedValue(topics)
    const { router } = await renderWithRouter(<TopicsView cluster="prod" />, { initialPath: '/c/prod/topics' })
    await screen.findByText('orders')
    await userEvent.click(screen.getByRole('button', { name: 'copy orders' }))
    expect(writeText).toHaveBeenCalledWith('orders')
    expect(await screen.findByText(/copied/i)).toBeInTheDocument()
    expect(router.state.location.pathname).toBe('/c/prod/topics')
  })
})
