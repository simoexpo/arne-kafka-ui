import { describe, expect, it, vi } from 'vitest'
import { fireEvent, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithRouter } from '../test/utils'
import { GroupsView } from './GroupsPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getGroups: vi.fn(),
  getGroupLag: vi.fn().mockResolvedValue({ groups: [], as_of: Date.now() }),
}))

describe('GroupsView', () => {
  it('lists groups with state and members, and links to detail via SPA navigation', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2, group_type: 'Classic' },
        { group_id: 'audit', state: 'Empty', protocol_type: 'consumer', member_count: 0, group_type: 'Classic' },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />, { initialPath: '/c/prod/consumers/billing' })
    expect(await screen.findByText('billing')).toBeInTheDocument()
    expect(screen.getByText('Stable')).toBeInTheDocument()
    const link = screen.getByRole('link', { name: /billing/ })
    expect(link).toHaveAttribute('href', '/c/prod/consumers/billing')
    // router-rendered <Link>, not a plain <a> full-reload anchor: the router
    // marks the link matching the current location as active itself
    expect(link).toHaveAttribute('data-status', 'active')
  })

  it('encodes group ids with spaces and slashes in the detail link href', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing team', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
        { group_id: 'a/b', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    expect(await screen.findByText('billing team')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'billing team' })).toHaveAttribute(
      'href',
      '/c/prod/consumers/billing%20team',
    )
    expect(screen.getByRole('link', { name: 'a/b' })).toHaveAttribute('href', '/c/prod/consumers/a%2Fb')
  })

  // Same affordance as the topics list (owner ruling 2026-08-19): the filter
  // narrows the whole list instantly, client-side, with no extra request.
  it('filter narrows the list instantly and the clear button restores it', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
        { group_id: 'analytics', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('billing')
    await userEvent.type(screen.getByPlaceholderText('filter consumers…'), 'anal')
    expect(screen.queryByText('billing')).not.toBeInTheDocument()
    expect(screen.getByText('analytics')).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'clear filter' }))
    expect(screen.getByText('billing')).toBeInTheDocument()
  })

  it('the panel title counts what the filter left visible', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
        { group_id: 'analytics', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    expect(await screen.findByText('2 groups')).toBeInTheDocument()
    await userEvent.type(screen.getByPlaceholderText('filter consumers…'), 'billing')
    expect(screen.getByText('1 groups')).toBeInTheDocument()
  })

  // Owner design 2026-08-19: lag costs one broker request per group, so the
  // page asks for it ONLY for the rows it is showing.
  it('requests lag for the visible page only, and renders it per row', async () => {
    const many = Array.from({ length: 60 }, (_, i) => ({
      group_id: `g-${String(i).padStart(2, '0')}`,
      state: 'Stable',
      protocol_type: 'consumer',
      group_type: 'Classic',
      member_count: 1,
    }))
    vi.mocked(client.getGroups).mockResolvedValue({ groups: many, as_of: Date.now() })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'g-00', total_lag: 42, unreadable_partitions: 0, member_count: null, error: null }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('g-00')
    const asked = vi.mocked(client.getGroupLag).mock.calls.at(-1)![1]
    expect(asked).toHaveLength(50)
    expect(asked).toContain('g-00')
    expect(asked).not.toContain('g-50')
    expect(await screen.findByText('42')).toBeInTheDocument()
  })

  it('scrolls only inside the list: the panel owns a scroller and no ancestor scrolls', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('billing')
    const scroller = screen.getByTestId('list-scroller')
    expect(scroller.className).toContain('overflow-y-auto')
    for (let p = scroller.parentElement; p; p = p.parentElement) {
      expect(p.className || '').not.toContain('overflow-y-auto')
    }
  })

  it('scrolling extends the window without unmounting loaded rows, and slides the lag viewport with the reader', async () => {
    const many = Array.from({ length: 120 }, (_, i) => ({
      group_id: `g-${String(i).padStart(3, '0')}`,
      state: 'Stable',
      protocol_type: 'consumer',
      group_type: 'Classic',
      member_count: 1,
    }))
    vi.mocked(client.getGroups).mockResolvedValue({ groups: many, as_of: Date.now() })
    vi.mocked(client.getGroupLag).mockResolvedValue({ groups: [], as_of: Date.now() })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('g-000')
    // first window: 100 rows mounted, lag asked for the top chunk only
    expect(screen.getAllByTestId('group-row')).toHaveLength(100)
    expect(vi.mocked(client.getGroupLag).mock.calls.at(-1)![1]).not.toContain('g-050')

    // reader scrolls deep: rows ~85–103 in view (100 rows ≈ 33px each)
    const scroller = screen.getByTestId('list-scroller')
    Object.defineProperty(scroller, 'scrollHeight', { value: 3300, configurable: true })
    Object.defineProperty(scroller, 'clientHeight', { value: 600, configurable: true })
    fireEvent.scroll(scroller, { target: { scrollTop: 2800 } })

    // the window grew to the whole list, nothing unmounted
    await waitFor(() => expect(screen.getAllByTestId('group-row')).toHaveLength(120))
    expect(screen.getByText('g-000')).toBeInTheDocument()
    // lag follows the viewport: the chunk in view, never the whole scrollback
    const asked = vi.mocked(client.getGroupLag).mock.calls.at(-1)![1]
    expect(asked).toContain('g-099')
    expect(asked).not.toContain('g-000')
    expect(asked.length).toBeLessThanOrEqual(100)
  })

  it('a row whose lag is undetermined shows a dash, not a zero', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'fresh', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' }],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'fresh', total_lag: null, unreadable_partitions: 0, member_count: null, error: null }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('fresh')
    expect(await screen.findByTestId('group-total-lag')).toHaveTextContent('—')
  })

  it('states a group with an unreadable partition as a lower bound, with the reason', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'partial', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' }],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'partial', total_lag: 12_000, unreadable_partitions: 2, member_count: null, error: null }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('partial')
    const cell = await screen.findByTestId('group-total-lag')
    expect(cell).toHaveTextContent('≥ 12.0k')
    expect(cell).toHaveAttribute('title', expect.stringContaining('2'))
  })

  // Owner ruling 2026-08-20: the list must agree with the coordinator. A
  // KIP-848 group's members cannot be counted from the calls this page makes,
  // so it says so rather than printing the classic describe's phantom zero.
  it('names the protocol and withholds an uncountable member count', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'next', state: 'Stable', protocol_type: '', group_type: 'Consumer', member_count: null },
        { group_id: 'old', state: 'Stable', protocol_type: 'consumer', group_type: 'Classic', member_count: 2 },
      ],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({ groups: [], as_of: Date.now() })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('next')
    const rows = screen.getAllByTestId('group-row')
    expect(rows[0]).toHaveTextContent('Consumer')
    const members = rows[0].querySelector('[data-testid="group-members"]')!
    expect(members).toHaveTextContent('—')
    expect(members).toHaveAttribute('title', expect.stringContaining('KIP-848'))
    expect(rows[1]).toHaveTextContent('Classic')
    expect(rows[1].querySelector('[data-testid="group-members"]')).toHaveTextContent('2')
  })

  // The count the list cannot take arrives with the visible page's lag, so the
  // row fills in rather than staying a dash.
  it('fills an uncountable member count from the visible page batch', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'next', state: 'Stable', protocol_type: '', group_type: 'Consumer', member_count: null }],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'next', total_lag: 5, unreadable_partitions: 0, member_count: 3, error: null }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('next')
    expect(await screen.findByTestId('group-members')).toHaveTextContent('3')
  })

  // A broker too old to report the protocol must not be labelled either way.
  it('falls back to the raw protocol type when the broker cannot name it', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'old-broker', state: 'Stable', protocol_type: 'consumer', group_type: 'Unknown', member_count: 1 }],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({ groups: [], as_of: Date.now() })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('old-broker')
    const row = screen.getAllByTestId('group-row')[0]
    expect(row).toHaveTextContent('consumer')
    expect(row).not.toHaveTextContent('Unknown')
  })

  it('copies the group id without navigating when the row copy button is clicked', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2, group_type: 'Classic' }],
      as_of: Date.now(),
    })
    const { router } = await renderWithRouter(<GroupsView cluster="prod" />, { initialPath: '/c/prod/consumers' })
    await screen.findByText('billing')
    await userEvent.click(screen.getByRole('button', { name: 'copy billing' }))
    expect(writeText).toHaveBeenCalledWith('billing')
    expect(await screen.findByText(/copied/i)).toBeInTheDocument()
    expect(router.state.location.pathname).toBe('/c/prod/consumers')
  })
})
