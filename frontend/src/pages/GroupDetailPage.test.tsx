import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithQuery } from '../test/utils'
import { GroupDetailView } from './GroupDetailPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getGroupDetail: vi.fn(),
}))

describe('GroupDetailView', () => {
  it('shows members and per-partition lag', async () => {
    vi.mocked(client.getGroupDetail).mockResolvedValue({
      group_id: 'billing',
      state: 'Stable',
      members: [{ member_id: 'm-1', client_id: 'billing-app', client_host: '/10.0.0.5' }],
      partitions: [{ topic: 'orders', partition: 0, committed_offset: 35, end_offset: 42, lag: 7 }],
      as_of: Date.now(),
    })
    renderWithQuery(<GroupDetailView cluster="prod" group="billing" />)
    expect(await screen.findByText('billing-app')).toBeInTheDocument()
    expect(screen.getByText('orders')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
  })

  it('renders 404 error inline', async () => {
    vi.mocked(client.getGroupDetail).mockRejectedValue(
      new client.ApiError(404, 'group_not_found', "consumer group 'ghost' does not exist", 'prod', false),
    )
    renderWithQuery(<GroupDetailView cluster="prod" group="ghost" />)
    const errors = await screen.findAllByText(/group_not_found/)
    expect(errors).toHaveLength(2)
  })
})
