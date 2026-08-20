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
    assignment_strategy: 'range',
    as_of: Date.now(),
    ...over,
  })

  // Owner ruling 2026-08-20: broker order is arbitrary and reshuffles between
  // polls, which makes a ten-member roster unreadable. Sorted by client id,
  // and the header states how many there are so nobody counts rows.
  it('lists members in a stable order and states how many there are', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm3', client_id: 'worker-3', client_host: '/10.0.0.3' },
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1' },
        { member_id: 'm2', client_id: 'worker-2', client_host: '/10.0.0.2' },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const items = await screen.findAllByTestId('group-member')
    expect(items.map((li) => li.textContent?.split(' ')[0])).toEqual(['worker-1', 'worker-2', 'worker-3'])
    const header = screen.getAllByTestId('panel-header').find((h) => h.textContent?.includes('Members'))!
    expect(header).toHaveTextContent('3')
  })

  // Owner ruling 2026-08-20: named by its assignor class, alongside the other
  // three figures as a plain fourth stat — same style, same row.
  it('names the assignment strategy by its class', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ assignment_strategy: 'cooperative-sticky' }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByTestId('stat-assignment strategy')).toHaveTextContent('CooperativeSticky')
  })

  // Owner ruling 2026-08-20: it sits against the right edge of the box while
  // the three figures stay left, so the row reads as numbers-then-strategy.
  it('parks the strategy against the right edge of the panel', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect((await screen.findByTestId('stat-assignment strategy')).className).toMatch(/text-right/)
    expect(screen.getByTestId('stat-topics').className).not.toMatch(/text-right/)
  })

  // An unknown protocol is passed through, never mapped to a wrong class: a
  // custom assignor or a KIP-848 group must read as what the broker said.
  it('passes an unrecognised assignor through untranslated', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ assignment_strategy: 'uniform' }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByTestId('stat-assignment strategy')).toHaveTextContent('uniform')
  })

  // An empty group negotiated nothing — saying "Range" would be an invention.
  it('does not invent a strategy for a group with no members', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ assignment_strategy: '', members: [], state: 'Empty' }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByText('no active members')).toBeInTheDocument()
    expect(screen.getByTestId('stat-assignment strategy')).toHaveTextContent('—')
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
