import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithRouter } from '../test/utils'
import { CommandPalette, PaletteView } from './CommandPalette'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getClusters: vi.fn(),
  getTopics: vi.fn(),
  getGroups: vi.fn(),
}))

const items = [
  { label: 'orders', kind: 'topic' as const, to: '/c/prod/topics/orders' },
  { label: 'payments', kind: 'topic' as const, to: '/c/prod/topics/payments' },
  { label: 'billing', kind: 'group' as const, to: '/c/prod/groups/billing' },
]

describe('PaletteView', () => {
  it('filters items as the user types', async () => {
    render(<PaletteView items={items} onNavigate={vi.fn()} onClose={vi.fn()} />)
    await userEvent.type(screen.getByRole('combobox'), 'ord')
    expect(screen.getByText('orders')).toBeInTheDocument()
    expect(screen.queryByText('billing')).not.toBeInTheDocument()
  })

  it('Enter navigates to the first match and closes', async () => {
    const onNavigate = vi.fn()
    const onClose = vi.fn()
    render(<PaletteView items={items} onNavigate={onNavigate} onClose={onClose} />)
    await userEvent.type(screen.getByRole('combobox'), 'bill{Enter}')
    expect(onNavigate).toHaveBeenCalledWith('/c/prod/groups/billing')
    expect(onClose).toHaveBeenCalled()
  })

  it('Escape closes without navigating', async () => {
    const onClose = vi.fn()
    render(<PaletteView items={items} onNavigate={vi.fn()} onClose={onClose} />)
    await userEvent.type(screen.getByRole('combobox'), '{Escape}')
    expect(onClose).toHaveBeenCalled()
  })
})

describe('CommandPalette', () => {
  beforeEach(() => {
    vi.mocked(client.getClusters).mockResolvedValue({
      clusters: [{ name: 'prod', status: 'healthy', broker_count: 3, error: null }],
    })
    vi.mocked(client.getTopics).mockResolvedValue({
      topics: [
        { name: 'orders', partitions: 3, replication_factor: 1, isr: 1, internal: false },
        { name: '__consumer_offsets', partitions: 1, replication_factor: 1, isr: 1, internal: true },
      ],
      as_of: Date.now(),
    })
    vi.mocked(client.getGroups).mockResolvedValue({
      groups: [{ group_id: 'billing', state: 'Stable', protocol_type: 'consumer', member_count: 1, group_type: 'Classic' }],
      as_of: Date.now(),
    })
  })

  it('is closed by default and opens on Cmd/Ctrl+K', async () => {
    await renderWithRouter(<CommandPalette cluster="prod" />)
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()

    await userEvent.keyboard('{Meta>}k{/Meta}')
    expect(await screen.findByRole('combobox')).toBeInTheDocument()
  })

  it('Cmd/Ctrl+K toggles the palette closed again', async () => {
    await renderWithRouter(<CommandPalette cluster="prod" />)
    await userEvent.keyboard('{Meta>}k{/Meta}')
    await screen.findByRole('combobox')

    await userEvent.keyboard('{Meta>}k{/Meta}')
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
  })

  it('does not fetch clusters/topics/groups until opened', async () => {
    await renderWithRouter(<CommandPalette cluster="prod" />)
    expect(client.getClusters).not.toHaveBeenCalled()
    expect(client.getTopics).not.toHaveBeenCalled()
    expect(client.getGroups).not.toHaveBeenCalled()

    await userEvent.keyboard('{Meta>}k{/Meta}')
    await waitFor(() => expect(client.getClusters).toHaveBeenCalled())
    expect(client.getTopics).toHaveBeenCalledWith('prod', expect.anything())
    expect(client.getGroups).toHaveBeenCalledWith('prod', expect.anything())
  })

  it('builds items from all three sources, excludes internal topics, and navigates on selection', async () => {
    const { router } = await renderWithRouter(<CommandPalette cluster="prod" />)
    await userEvent.keyboard('{Meta>}k{/Meta}')

    expect(await screen.findByText('prod')).toBeInTheDocument()
    expect(screen.getByText('orders')).toBeInTheDocument()
    expect(screen.getByText('billing')).toBeInTheDocument()
    expect(screen.queryByText('__consumer_offsets')).not.toBeInTheDocument()

    await userEvent.click(screen.getByText('orders'))
    expect(router.state.location.pathname).toBe('/c/prod/topics/orders')
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
  })
})
