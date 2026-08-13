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
    { name: 'retention.bytes', value: '-1', is_default: true },
    { name: 'max.message.bytes', value: '1048576', is_default: true },
  ],
  as_of: Date.now(),
}

describe('TopicDetailView', () => {
  it('shows partitions with offsets and flags shrunken ISR', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Partitions' }))
    expect(await screen.findByText('42')).toBeInTheDocument() // end offset p0
    expect(screen.getByRole('heading', { name: '2 partitions' })).toBeInTheDocument()
    const rows = screen.getAllByTestId('partition-row')
    expect(rows).toHaveLength(2)
    expect(rows[1]).toHaveTextContent('under-replicated') // isr < replicas
  })

  it('config tab summary shows partitions, cleanup policy, and retention with a human hint', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    expect(await screen.findByTestId('stat-partitions')).toHaveTextContent('2')
    expect(screen.getByRole('heading', { name: 'Summary' })).toBeInTheDocument()
    expect(screen.getByTestId('stat-cleanup.policy')).toHaveTextContent('delete')
    expect(screen.getByTestId('stat-retention.ms')).toHaveTextContent('604800000 (7d)')
    expect(screen.getByTestId('stat-retention.bytes')).toHaveTextContent('∞')
  })

  it('config tab summary shows a dash when cleanup.policy is absent', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue({
      ...detail,
      configs: detail.configs.filter((c) => c.name !== 'cleanup.policy'),
    })
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    expect(await screen.findByTestId('stat-cleanup.policy')).toHaveTextContent('—')
  })

  it('config tab table shows only overridden entries by default', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    expect(await screen.findByTestId('config-retention.ms')).toHaveTextContent('overridden')
    expect(screen.queryByTestId('config-cleanup.policy')).not.toBeInTheDocument()
    expect(screen.queryByTestId('config-all-retention.ms')).not.toBeInTheDocument() // full table collapsed
  })

  it('config tab shows an empty-state message when nothing is overridden', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue({
      ...detail,
      configs: detail.configs.map((c) => ({ ...c, is_default: true })),
    })
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    expect(await screen.findByText('no overrides — all values are broker defaults')).toBeInTheDocument()
  })

  it('config tab "show all configs" expands the full table with defaults and overrides', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    await screen.findByTestId('config-retention.ms')
    expect(screen.queryByTestId('config-all-retention.ms')).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('switch', { name: 'show all configs' }))
    expect(await screen.findByTestId('config-all-retention.ms')).toHaveTextContent('overridden')
    expect(screen.getByTestId('config-all-cleanup.policy')).toHaveTextContent('default')
    expect(screen.getByTestId('config-all-max.message.bytes')).toHaveTextContent('default')
  })

  it('config tab filter box is hidden until "show all configs" is on', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    await screen.findByTestId('config-retention.ms')
    expect(screen.queryByLabelText('filter configs')).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('switch', { name: 'show all configs' }))
    expect(await screen.findByPlaceholderText('filter configs…')).toBeInTheDocument()
  })

  it('config tab filter box narrows the full table to matching names', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    await userEvent.click(screen.getByRole('switch', { name: 'show all configs' }))
    await userEvent.type(await screen.findByLabelText('filter configs'), 'retention')
    expect(screen.getByTestId('config-all-retention.ms')).toBeInTheDocument()
    expect(screen.getByTestId('config-all-retention.bytes')).toBeInTheDocument()
    expect(screen.queryByTestId('config-all-cleanup.policy')).not.toBeInTheDocument()
    expect(screen.queryByTestId('config-all-max.message.bytes')).not.toBeInTheDocument()
  })

  it('config tab filter box shows a message when nothing matches', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    await userEvent.click(screen.getByRole('switch', { name: 'show all configs' }))
    await userEvent.type(await screen.findByLabelText('filter configs'), 'zzz-nope')
    expect(await screen.findByText('no matching configs')).toBeInTheDocument()
    expect(screen.queryByTestId('config-all-retention.ms')).not.toBeInTheDocument()
  })

  it('config tab filter box resets when the switch is turned off and back on', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'Config' }))
    const toggle = screen.getByRole('switch', { name: 'show all configs' })
    await userEvent.click(toggle)
    await userEvent.type(await screen.findByLabelText('filter configs'), 'retention')
    await userEvent.click(toggle) // off
    await userEvent.click(toggle) // on again
    expect(await screen.findByLabelText('filter configs')).toHaveValue('')
    expect(screen.getByTestId('config-all-cleanup.policy')).toBeInTheDocument()
  })

  it('messages tab is shown by default', async () => {
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    expect(await screen.findByText('no messages')).toBeInTheDocument()
    expect(client.getMessages).toHaveBeenCalled()
  })

  it('header has a button to copy the topic name', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    vi.mocked(client.getTopicDetail).mockResolvedValue(detail)
    renderWithQuery(<TopicDetailView cluster="prod" topic="orders" />)
    await userEvent.click(screen.getByRole('button', { name: 'copy orders' }))
    expect(writeText).toHaveBeenCalledWith('orders')
    expect(await screen.findByText(/copied/i)).toBeInTheDocument()
  })
})
