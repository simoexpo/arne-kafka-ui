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
    members: [{ member_id: 'm-1', client_id: 'billing-app', client_host: '/10.0.0.5', assigned: [] }],
    partitions: [
      { topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 },
      { topic: 'orders', partition: 1, committed_offset: 10, end_offset: 12, lag: 2 },
      { topic: 'users', partition: 0, committed_offset: 5, end_offset: 5, lag: 0 },
    ],
    unreadable_partitions: 0,
    assignment_strategy: 'range',
    group_type: 'Classic',
    as_of: Date.now(),
    ...over,
  })

  // Owner ruling 2026-08-20: broker order is arbitrary and reshuffles between
  // polls, which makes a ten-member roster unreadable. Sorted by client id,
  // and the header states how many there are so nobody counts rows.
  it('lists members in a stable order and states how many there are', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm3', client_id: 'worker-3', client_host: '/10.0.0.3', assigned: [] },
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1', assigned: [] },
        { member_id: 'm2', client_id: 'worker-2', client_host: '/10.0.0.2', assigned: [] },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const items = await screen.findAllByTestId('group-member')
    expect(items.map((li) => li.textContent?.split(' ')[0])).toEqual(['worker-1', 'worker-2', 'worker-3'])
    const header = screen.getAllByTestId('panel-header').find((h) => h.textContent?.includes('Members'))!
    expect(header).toHaveTextContent('3')
    // Owner ruling 2026-08-20: the count and nothing else — state and protocol
    // are group facts and live in the Group card.
    expect(header).not.toHaveTextContent('Stable')
    expect(header).not.toHaveTextContent('Classic')
  })

  // Owner ruling 2026-08-20: the group's own facts sit together in the Group
  // card — what it consumes on the first row, how it behaves on the second,
  // named by assignor class.
  it('states the strategy, status and protocol on the group card', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ group_type: 'Consumer' }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByTestId('stat-assignment strategy')).toHaveTextContent('Range')
    expect(screen.getByTestId('stat-status')).toHaveTextContent('Stable')
    expect(screen.getByTestId('stat-protocol')).toHaveTextContent('Consumer')
  })

  it('names the assignment strategy by its class', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({ assignment_strategy: 'cooperative-sticky' }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByTestId('stat-assignment strategy')).toHaveTextContent('CooperativeSticky')
  })

  it('spreads both rows of figures across the card', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const first = (await screen.findByTestId('stat-topics')).parentElement!
    const second = screen.getByTestId('stat-assignment strategy').parentElement!
    expect(first.className).toMatch(/justify-between/)
    expect(second.className).toMatch(/justify-between/)
    expect(first).not.toBe(second)
    // what it consumes on top, how it behaves below
    expect(first.lastElementChild).toBe(screen.getByTestId('stat-total lag'))
    expect(second.lastElementChild).toBe(screen.getByTestId('stat-protocol'))
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

  // The lag table says which partitions are behind; only the assignment says
  // WHO is behind. Both come from calls the page already makes.
  it('names the member that owns each partition', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1',
          assigned: [{ topic: 'orders', partitions: [0, 1] }] },
        { member_id: 'm2', client_id: 'worker-2', client_host: '/10.0.0.2',
          assigned: [{ topic: 'users', partitions: [0] }] },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const rows = await screen.findAllByTestId('partition-row')
    expect(rows.map((r) => r.querySelector('[data-testid="partition-owner"]')?.textContent))
      .toEqual(['worker-1', 'worker-1', 'worker-2'])
  })

  // Lag on a partition nobody owns is nobody's job to work off — the case
  // worth spotting during an incident.
  it('marks a partition that no live member owns', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1',
          assigned: [{ topic: 'orders', partitions: [0] }] },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const rows = await screen.findAllByTestId('partition-row')
    const owners = rows.map((r) => r.querySelector('[data-testid="partition-owner"]')?.textContent)
    expect(owners).toEqual(['worker-1', 'unassigned', 'unassigned'])
  })

  // More members than partitions: the standbys own nothing and the page says
  // so, instead of looking like ten working consumers.
  it('marks a member that owns no partition as idle', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1',
          assigned: [{ topic: 'orders', partitions: [0, 1] }] },
        { member_id: 'm2', client_id: 'worker-2', client_host: '/10.0.0.2', assigned: [] },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const items = await screen.findAllByTestId('group-member')
    expect(items[0]).not.toHaveTextContent('idle')
    expect(items[1]).toHaveTextContent('idle')
  })

  // An undecodable blob is not an empty one: claiming "idle" or "unassigned"
  // would be inventing knowledge we do not have.
  it('never calls an undecodable assignment idle or unassigned', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail({
      members: [
        { member_id: 'm1', client_id: 'worker-1', client_host: '/10.0.0.1', assigned: null },
      ],
    }))
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const items = await screen.findAllByTestId('group-member')
    expect(items[0]).not.toHaveTextContent('idle')
    const rows = screen.getAllByTestId('partition-row')
    const owners = rows.map((r) => r.querySelector('[data-testid="partition-owner"]')?.textContent)
    expect(owners).toEqual(['—', '—', '—'])
  })

  // Auto layout handed the slack to the widest header and squeezed the
  // offsets into 70px; the columns are sized on purpose instead.
  it('sizes the lag columns deliberately rather than by content', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue(detail())
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    const table = (await screen.findAllByTestId('partition-row'))[0].closest('table')!
    expect(table.className).toMatch(/table-fixed/)
    const widths = [...table.querySelectorAll('thead th')].map((th) => th.className.match(/w-\[\d+%\]/)?.[0])
    expect(widths.every(Boolean)).toBe(true)
    // the two offsets hold the same kind of value, so they get the same room
    expect(widths[2]).toBe(widths[3])
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

  it('the group id is copyable and the state is out of the title', async () => {
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
    // the state is disclosed as one of the group's own figures
    expect(screen.getByTestId('stat-status')).toHaveTextContent('Stable')
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
