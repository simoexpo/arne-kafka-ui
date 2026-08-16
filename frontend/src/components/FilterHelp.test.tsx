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
    expect(pop).toHaveTextContent('key:')
    expect(pop).toHaveTextContent('value:')
    expect(pop).toHaveTextContent('path.to.field=42')
    expect(pop).toHaveTextContent(/key or value/i)
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
