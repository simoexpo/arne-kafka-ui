import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../test/utils'
import { GroupsView } from './GroupsPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getGroups: vi.fn(),
}))

describe('GroupsView', () => {
  it('lists groups with state, members and aggregate lag', async () => {
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [
        { group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 2, total_lag: 42 },
        { group_id: 'audit', state: 'Empty', protocol_type: 'consumer', member_count: 0, total_lag: 0 },
      ],
      as_of: Date.now(),
    })
    renderWithQuery(<GroupsView cluster="prod" />)
    expect(await screen.findByText('billing')).toBeInTheDocument()
    expect(screen.getByText('Stable')).toBeInTheDocument()
    expect(screen.getByText('42')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /billing/ })).toHaveAttribute('href', '/c/prod/groups/billing')
  })
})
