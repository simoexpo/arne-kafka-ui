import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { FilterHelp } from './FilterHelp'

describe('FilterHelp', () => {
  it('opens the syntax popup on click and lists every filter form', async () => {
    const user = userEvent.setup()
    render(<FilterHelp />)
    expect(screen.queryByTestId('filter-help-popover')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('filter-help'))
    const pop = screen.getByTestId('filter-help-popover')
    for (const syntax of ['key:foo', 'key=foo', 'value:foo', 'value=foo', 'value.path.to.field:foo', 'value.path.to.field=42', 'value.path.to.field!=42', 'value.path.to.field>42', '"key:foo"']) {
      expect(pop).toHaveTextContent(syntax)
    }
    expect(pop).toHaveTextContent(/case-insensitive/i)
    expect(pop).toHaveTextContent(/key or value/i)
    // The quoted-segment syntax and the remaining limitations (design spec
    // 2026-08-17) must be discoverable here, not just true.
    expect(pop).toHaveTextContent('value."field.with.dots"=x')
    expect(pop).toHaveTextContent(/outer quotes/i)
    expect(pop).not.toHaveTextContent(/can't be addressed/i)
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
