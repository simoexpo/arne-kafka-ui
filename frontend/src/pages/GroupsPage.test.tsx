import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
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
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2 },
        { group_id: 'audit', state: 'Empty', protocol_type: 'consumer', member_count: 0 },
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
        { group_id: 'billing team', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
        { group_id: 'a/b', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
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
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
        { group_id: 'analytics', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
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
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
        { group_id: 'analytics', state: 'Stable', protocol_type: 'consumer', member_count: 1 },
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
      member_count: 1,
    }))
    vi.mocked(client.getGroups).mockResolvedValue({ groups: many, as_of: Date.now() })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'g-00', total_lag: 42, error: null }],
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

  it('paginates by name, so the next page asks lag for its own rows', async () => {
    const many = Array.from({ length: 60 }, (_, i) => ({
      group_id: `g-${String(i).padStart(2, '0')}`,
      state: 'Stable',
      protocol_type: 'consumer',
      member_count: 1,
    }))
    vi.mocked(client.getGroups).mockResolvedValue({ groups: many, as_of: Date.now() })
    vi.mocked(client.getGroupLag).mockResolvedValue({ groups: [], as_of: Date.now() })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('g-00')
    expect(screen.queryByText('g-50')).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: /next/i }))
    expect(await screen.findByText('g-50')).toBeInTheDocument()
    expect(screen.queryByText('g-00')).not.toBeInTheDocument()
    const asked = vi.mocked(client.getGroupLag).mock.calls.at(-1)![1]
    expect(asked).toContain('g-50')
    expect(asked).not.toContain('g-00')
  })

  it('a row whose lag is undetermined shows a dash, not a zero', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'fresh', state: 'Stable', protocol_type: 'consumer', member_count: 1 }],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroupLag).mockResolvedValue({
      groups: [{ group_id: 'fresh', total_lag: null, error: null }],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />)
    await screen.findByText('fresh')
    expect(await screen.findByTestId('group-total-lag')).toHaveTextContent('—')
  })

  it('copies the group id without navigating when the row copy button is clicked', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2 }],
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
