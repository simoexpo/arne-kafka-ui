import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { PaletteView } from './CommandPalette'

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
