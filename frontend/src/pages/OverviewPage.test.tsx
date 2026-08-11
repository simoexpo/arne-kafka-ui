import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../test/utils'
import { OverviewView } from './OverviewPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getOverview: vi.fn(),
  getTopics: vi.fn(),
  getClusters: vi.fn().mockResolvedValue({ clusters: [] }),
}))

describe('OverviewView', () => {
  it('renders broker count, URP, and top topics by size', async () => {
    vi.mocked(client.getOverview).mockResolvedValue({
      brokers: [{ id: 1, host: 'b1', port: 9092 }],
      controller_id: null,
      topic_count: 4,
      partition_count: 12,
      under_replicated_partitions: 0,
      as_of: Date.now(),
    })
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        { name: 'big', partitions: 3, replication_factor: 1, message_estimate: 5000, size_bytes: null, internal: false },
        { name: 'small', partitions: 1, replication_factor: 1, message_estimate: 10, size_bytes: null, internal: false },
        { name: '__internal', partitions: 1, replication_factor: 1, message_estimate: 99999, size_bytes: null, internal: true },
      ],
      as_of: Date.now(),
    })
    renderWithQuery(<OverviewView cluster="prod" />)
    expect(await screen.findByText('b1:9092')).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument() // partition count
    const topTopics = await screen.findAllByTestId('top-topic')
    expect(topTopics[0]).toHaveTextContent('big')
    expect(screen.queryByText('__internal')).not.toBeInTheDocument() // internal excluded
  })

  it('renders an inline error in the failing panel only', async () => {
    vi.mocked(client.getOverview).mockRejectedValue(
      new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
    )
    vi.mocked(client.getTopics).mockResolvedValue({ topics: [], as_of: Date.now() })
    renderWithQuery(<OverviewView cluster="prod" />)
    expect(await screen.findByText(/kafka_timeout/)).toBeInTheDocument()
    expect(screen.getByText('Top topics')).toBeInTheDocument() // sibling panel intact
  })
})
