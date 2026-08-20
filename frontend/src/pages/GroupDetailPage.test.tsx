import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery } from '../test/utils'
import { GroupDetailView } from './GroupDetailPage'
import * as client from '../api/client'
import type { GroupDetail } from '../api/types'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getGroupDetail: vi.fn(),
}))

describe('GroupDetailView', () => {
  const detail = (over: Partial<GroupDetail> = {}): GroupDetail => ({
    group_id: 'billing',
    state: 'Stable',
    members: [{ member_id: 'm-1', client_id: 'billing-app', client_host: '/10.0.0.5' }],
    partitions: [
      { topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 },
      { topic: 'orders', partition: 1, committed_offset: 10, end_offset: 12, lag: 2 },
      { topic: 'users', partition: 0, committed_offset: 5, end_offset: 5, lag: 0 },
    ],
    unreadable_partitions: 0,
    as_of: Date.now(),
    ...over,
  })

  it('shows members and per-partition lag', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByText('billing-app')).toBeInTheDocument()
    expect(screen.getAllByText('orders').length).toBeGreaterThan(0)
    expect(screen.getByText('7')).toBeInTheDocument()
  })

  // Owner ruling 2026-08-20: organised like the overview page — a stats panel
  // and the members beside it, with the per-partition table below.
  it('summarises what the group consumes: topics, partitions, total lag', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByTestId('stat-topics')).toHaveTextContent('2')
    expect(screen.getByTestId('stat-partitions')).toHaveTextContent('3')
    expect(screen.getByTestId('stat-total lag')).toHaveTextContent('9')
  })

  // Owner ruling 2026-08-20: lag is not an alarm — a healthy consumer on a
  // busy topic sits thousands of messages behind and is perfectly fine.
  it('does not colour a non-zero total as a failure', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const value = (await screen.findByTestId('stat-total lag')).querySelector('dd')!
    expect(value.className).not.toMatch(/red|amber/)
  })

  // Owner ruling 2026-08-20: the readable sum is worth stating as a lower
  // bound — "unknown" throws away a magnitude the operator needs — but it
  // must never read as a complete total, hence `>=` plus the tooltip.
  it('states the readable total as a lower bound when a partition could not be read', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ unreadable_partitions: 2 }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const total = await screen.findByTestId('stat-total lag')
    expect(total).toHaveTextContent('≥ 9')
    expect(total).toHaveAttribute('title', expect.stringContaining('2'))
    // the partitions it commits on include the ones we could not read
    expect(screen.getByTestId('stat-partitions')).toHaveTextContent('5')
  })

  it('the group id is copyable and the state sits with the members, not in the title', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    await screen.findByText('billing-app')
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toHaveTextContent('billing')
    expect(heading.parentElement).not.toHaveTextContent('Stable')
    await userEvent.click(screen.getByRole('button', { name: 'copy billing' }))
    expect(writeText).toHaveBeenCalledWith('billing')
    // the state is disclosed beside the members it describes
    const members = screen.getAllByTestId('panel-header').find((h) => h.textContent?.includes('Members'))!
    expect(members).toHaveTextContent('Stable')
  })

  it('renders 404 error inline', async () => {
    vi.mocked(client.getGroupDetail).mockRejectedValue(
      new client.ApiError(404, 'group_not_found', "consumer group 'ghost' does not exist", 'prod', false),
    )
    renderWithQuery(<GroupDetailView cluster="prod" group="ghost" />)
    // one per panel: Group, Members, Partition lag — a failing query must not
    // leave a panel looking like it has nothing to show
    const errors = await screen.findAllByText(/group_not_found/)
    expect(errors).toHaveLength(3)
  })
})
