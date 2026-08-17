import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { FilterHelp } from './FilterHelp'

describe('FilterHelp', () => {
  it('opens the syntax popup on click; compact form still shows every operator and shape', async () => {
    const user = userEvent.setup()
    render(<FilterHelp />)
    expect(screen.queryByTestId('filter-help-popover')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('filter-help'))
    const pop = screen.getByTestId('filter-help-popover')
    // One shape row and one operator row instead of the full target×operator
    // matrix (owner: popup was getting too long).
    for (const shape of ['key:foo', 'value=bar', 'value.path.to.field>42']) {
      expect(pop).toHaveTextContent(shape)
    }
    for (const op of ['!=', '>=', '<=']) {
      expect(pop).toHaveTextContent(op)
    }
    expect(pop).toHaveTextContent('value."field.with.dots"=x')
    expect(pop).toHaveTextContent('"key:foo"')
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
