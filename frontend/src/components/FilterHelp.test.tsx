import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { FilterHelp } from './FilterHelp'

describe('FilterHelp', () => {
  // Content shape from the 2026-08-17 three-lens review (first-time user,
  // accuracy, copy editing): one coherent order-message story, simple to
  // complex, every operator family shown by a typed-out example — never as
  // a bare symbol table — and `[]` taught as "ANY element".
  it('opens the syntax popup on click and teaches every form by example', async () => {
    const user = userEvent.setup()
    render(<FilterHelp />)
    expect(screen.queryByTestId('filter-help-popover')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('filter-help'))
    const pop = screen.getByTestId('filter-help-popover')
    for (const example of [
      'key:order',
      'key=order-42',
      'value.customer.tier=gold',
      'value.total>=99.5',
      'value.items[].sku=abc',
      'value."ship.zone"=EU',
      '"key:order"',
    ]) {
      expect(pop).toHaveTextContent(example)
    }
    expect(pop).toHaveTextContent('!=')
    expect(pop).toHaveTextContent(/any/i)
    expect(pop).toHaveTextContent(/case-insensitive/i)
    expect(pop).toHaveTextContent(/key or value/i)
    expect(pop).toHaveTextContent(/outer quotes/i)
  })

  it('closes on Escape and on outside click', async () => {
    const user = userEvent.setup()
    render(<div><FilterHelp /><button>outside</button></div>)
    await user.click(screen.getByTestId('filter-help'))
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByTestId('filter-help-popover')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('filter-help'))
    await user.click(screen.getByText('outside'))
    expect(screen.queryByTestId('filter-help-popover')).not.toBeInTheDocument()
  })
})
