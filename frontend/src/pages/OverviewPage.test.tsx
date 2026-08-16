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

  it('renders — for top topics whose message estimate is null, sorted after ranked ones', async () => {
    vi.mocked(client.getOverview).mockResolvedValue({
      brokers: [{ id: 1, host: 'b1', port: 9092 }],
      controller_id: null,
      topic_count: 2,
      partition_count: 4,
      under_replicated_partitions: 0,
      as_of: Date.now(),
    })
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        { name: 'flaky', partitions: 1, replication_factor: 1, message_estimate: null, size_bytes: null, internal: false },
        { name: 'ranked', partitions: 1, replication_factor: 1, message_estimate: 10, size_bytes: null, internal: false },
      ],
      as_of: Date.now(),
    })
    renderWithQuery(<OverviewView cluster="prod" />)
    const topTopics = await screen.findAllByTestId('top-topic')
    expect(topTopics[0]).toHaveTextContent('ranked')
    expect(topTopics[1]).toHaveTextContent('flaky')
    expect(topTopics[1]).toHaveTextContent('—')
  })

  it('renders an inline error in the failing panel only', async () => {
    vi.mocked(client.getOverview).mockRejectedValue(
      new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
    )
    vi.mocked(client.getTopics).mockResolvedValue({ topics: [], as_of: Date.now() })
    renderWithQuery(<OverviewView cluster="prod" />)
    const errors = await screen.findAllByText(/kafka_timeout/)
    expect(errors).toHaveLength(2) // both overview panels show error
    expect(screen.getByText('Top topics')).toBeInTheDocument() // sibling panel intact
  })

  it('renders an inline error in Top topics only when the topics query fails', async () => {
    vi.mocked(client.getOverview).mockResolvedValue({
      brokers: [{ id: 1, host: 'b1', port: 9092 }],
      controller_id: null,
      topic_count: 4,
      partition_count: 12,
      under_replicated_partitions: 0,
      as_of: Date.now(),
    })
    vi.mocked(client.getTopics).mockRejectedValue(
      new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
    )
    renderWithQuery(<OverviewView cluster="prod" />)
    const errors = await screen.findAllByText(/kafka_timeout/)
    expect(errors).toHaveLength(1) // only the Top topics panel shows error
    expect(await screen.findByText('b1:9092')).toBeInTheDocument() // Cluster/Brokers panels intact
  })

  it('renders an inline error in every panel when both queries fail', async () => {
    vi.mocked(client.getOverview).mockRejectedValue(
      new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
    )
    vi.mocked(client.getTopics).mockRejectedValue(
      new client.ApiError(502, 'kafka_unreachable', 'no brokers reachable', 'prod', true),
    )
    renderWithQuery(<OverviewView cluster="prod" />)
    expect((await screen.findAllByText(/kafka_timeout/)).length).toBe(2) // Cluster + Brokers
    expect(screen.getAllByText(/kafka_unreachable/)).toHaveLength(1) // Top topics
  })
})
