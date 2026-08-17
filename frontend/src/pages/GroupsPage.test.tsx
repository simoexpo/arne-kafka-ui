import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { GroupsView } from './GroupsPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getGroups: vi.fn(),
}))

describe('GroupsView', () => {
  it('lists groups with state, members, aggregate lag, and links to detail via SPA navigation', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2, total_lag: 42 },
        { group_id: 'audit', state: 'Empty', protocol_type: 'consumer', member_count: 0, total_lag: 0 },
      ],
      as_of: Date.now(),
    })
    await renderWithRouter(<GroupsView cluster="prod" />, { initialPath: '/c/prod/consumers/billing' })
    expect(await screen.findByText('billing')).toBeInTheDocument()
    expect(screen.getByText('Stable')).toBeInTheDocument()
    expect(screen.getByText('42')).toBeInTheDocument()
    const link = screen.getByRole('link', { name: /billing/ })
    expect(link).toHaveAttribute('href', '/c/prod/consumers/billing')
    // router-rendered <Link>, not a plain <a> full-reload anchor: the router
    // marks the link matching the current location as active itself
    expect(link).toHaveAttribute('data-status', 'active')
  })

  it('encodes group ids with spaces and slashes in the detail link href', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing team', state: 'Stable', protocol_type: 'consumer', member_count: 1, total_lag: 0 },
        { group_id: 'a/b', state: 'Stable', protocol_type: 'consumer', member_count: 1, total_lag: 0 },
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
})
