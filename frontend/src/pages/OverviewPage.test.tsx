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
  it('broker addresses are copyable like other identifiers', async () => {
    vi.mocked(client.getOverview).mockResolvedValue({
      brokers: [{ id: 1, host: 'b1', port: 9092 }],
      controller_id: null,
      topic_count: 2,
      partition_count: 4,
      under_replicated_partitions: 0,
      top_topics: [],
      as_of: Date.now(),
    })
    renderWithQuery(<OverviewView cluster="prod" />)
    expect(await screen.findByLabelText('copy b1:9092')).toBeInTheDocument()
  })

  // Owner ruling 2026-08-18: top topics rank by partition count served by the
  // overview call itself — no watermark-priced message estimates, no second
  // query. Revisit when librdkafka ships DescribeLogDirs (librdkafka#5333).
  it('renders broker count, URP, and top topics by partitions from the overview payload alone', async () => {
    vi.mocked(client.getOverview).mockResolvedValue({
      brokers: [{ id: 1, host: 'b1', port: 9092 }],
      controller_id: null,
      topic_count: 4,
      partition_count: 12,
      under_replicated_partitions: 0,
      top_topics: [
        { name: 'big', partitions: 8 },
        { name: 'small', partitions: 1 },
      ],
      as_of: Date.now(),
    })
    renderWithQuery(<OverviewView cluster="prod" />)
    expect(await screen.findByText('b1:9092')).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument() // partition count
    const topTopics = await screen.findAllByTestId('top-topic')
    expect(topTopics[0]).toHaveTextContent('big')
    expect(topTopics[0]).toHaveTextContent('8')
    expect(topTopics[1]).toHaveTextContent('small')
    expect(client.getTopics).not.toHaveBeenCalled()
  })

  it('renders an inline error in every panel when the overview query fails', async () => {
    vi.mocked(client.getOverview).mockRejectedValue(
      new client.ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true),
    )
    renderWithQuery(<OverviewView cluster="prod" />)
    const errors = await screen.findAllByText(/kafka_timeout/)
    expect(errors).toHaveLength(3) // Cluster, Brokers, Top topics
  })
})
