import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery } from '../test/utils'
import { TopicDetailView } from './TopicDetailPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getTopicDetail: vi.fn(),
  getTopicConsumers: vi.fn().mockResolvedValue({ topic: 'orders', groups: [], as_of: 0 }),
  getThroughput: vi.fn().mockResolvedValue({ topic: 'orders', samples: [], as_of: null }),
  getMessages: vi.fn().mockResolvedValue({ messages: [], as_of: 0 }),
}))

const detail = {
  name: 'orders',
  partitions: [
    { id: 0, leader: 1, replicas: [1, 2], isr: [1, 2], start_offset: 0, end_offset: 42 },
    { id: 1, leader: 2, replicas: [2, 1], isr: [2], start_offset: 5, end_offset: 40 },
  ],
  configs: [
    { name: 'retention.ms', value: '604800000', is_default: false },
    { name: 'cleanup.policy', value: 'delete', is_default: true },
  ],
  as_of: Date.now(),
}

describe('TopicDetailView', () => {
  it('shows partitions with offsets and flags shrunken ISR', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Partitions' }))
    expect(await screen.findByText('42')).toBeInTheDocument() // end offset p0
    const rows = screen.getAllByTestId('partition-row')
    expect(rows).toHaveLength(2)
    expect(rows[1]).toHaveTextContent('under-replicated') // isr < replicas
  })

  it('config tab lists entries and marks non-defaults', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    expect(await screen.findByText('retention.ms')).toBeInTheDocument()
    const overridden = screen.getByTestId('config-retention.ms')
    expect(overridden).toHaveTextContent('overridden')
  })

  it('messages tab is shown by default', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    expect(await screen.findByText('no messages')).toBeInTheDocument()
    expect(client.getMessages).toHaveBeenCalled()
  })
})
