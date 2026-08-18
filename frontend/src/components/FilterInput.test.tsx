import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FilterInput } from './FilterInput'

describe('FilterInput', () => {
  it('shows no clear button when the value is empty', () => {
    render(<FilterInput value="" onChange={vi.fn()} placeholder="filter…" ariaLabel="filter x" />)
    expect(screen.queryByRole('button', { name: 'clear filter' })).not.toBeInTheDocument()
  })

  it('shows a clear button once there is text', () => {
    render(<FilterInput value="abc" onChange={vi.fn()} placeholder="filter…" ariaLabel="filter x" />)
    expect(screen.getByRole('button', { name: 'clear filter' })).toBeInTheDocument()
  })

  it('clicking clear calls onChange with an empty string and refocuses the input', async () => {
    const onChange = vi.fn()
    render(<FilterInput value="abc" onChange={onChange} placeholder="filter…" ariaLabel="filter x" />)
    await userEvent.click(screen.getByRole('button', { name: 'clear filter' }))
    expect(onChange).toHaveBeenCalledWith('')
    expect(screen.getByLabelText('filter x')).toHaveFocus()
  })

  it('wrapper is content-sized, not stretched to fill its row', () => {
    render(<FilterInput value="" onChange={vi.fn()} placeholder="filter…" ariaLabel="filter x" />)
    const wrapper = screen.getByLabelText('filter x').parentElement
    expect(wrapper?.className).toMatch(/\bw-fit\b/)
  })
})

describe('FilterInput combobox', () => {
  const proposals = (text: string) => ('key'.startsWith(text) && text !== '' ? ['key:', 'key='] : [])

  const setup = (value = 'k', onChange = vi.fn()) => {
    render(<FilterInput value={value} onChange={onChange} placeholder="filter…" ariaLabel="filter" proposals={proposals} />)
    const input = screen.getByLabelText('filter')
    fireEvent.focus(input)
    return { input, onChange }
  }

  it('opens with proposals on focus and wires ARIA', () => {
    const { input } = setup()
    expect(screen.getAllByTestId('filter-proposal').map((el) => el.textContent)).toEqual(['key:', 'key='])
    expect(input).toHaveAttribute('role', 'combobox')
    expect(input).toHaveAttribute('aria-expanded', 'true')
    expect(screen.getByRole('listbox')).toBeInTheDocument()
    expect(screen.getAllByRole('option')[0]).toHaveAttribute('aria-selected', 'true')
  })

  it('ArrowDown moves the highlight and Enter accepts it', () => {
    const { input, onChange } = setup()
    fireEvent.keyDown(input, { key: 'ArrowDown' })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChange).toHaveBeenCalledWith('key=')
  })

  it('Tab accepts the highlighted proposal', () => {
    const { input, onChange } = setup()
    fireEvent.keyDown(input, { key: 'Tab' })
    expect(onChange).toHaveBeenCalledWith('key:')
  })

  it('Escape dismisses until the text changes, then the dropdown returns', () => {
    const { input } = setup()
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
    // The value prop stays 'k' (parent-controlled), so the change event's
    // only observable effect here is resetting the dismissal.
    fireEvent.change(input, { target: { value: 'ke' } })
    expect(screen.getByRole('listbox')).toBeInTheDocument()
  })

  it('click accepts a proposal', () => {
    const { onChange } = setup()
    const row = screen.getAllByTestId('filter-proposal')[1]
    fireEvent.mouseDown(row)
    fireEvent.click(row)
    expect(onChange).toHaveBeenCalledWith('key=')
  })

  it('arrow keys step from the clamped highlight after rows shrink under it', () => {
    let rows = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    const onChange = vi.fn()
    render(<FilterInput value="x" onChange={onChange} placeholder="p" ariaLabel="dyn" proposals={() => rows} />)
    const input = screen.getByLabelText('dyn')
    fireEvent.focus(input)
    for (let i = 0; i < 6; i++) fireEvent.keyDown(input, { key: 'ArrowDown' }) // highlight raw = 6
    rows = ['a', 'b', 'c'] // shrink without a text change (e.g. field set changed)
    fireEvent.blur(input)
    fireEvent.focus(input) // re-render with 3 rows; clamped display = row 2
    fireEvent.keyDown(input, { key: 'ArrowDown' }) // must step from the CLAMPED row, wrapping to 0
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChange).toHaveBeenCalledWith('a')
  })

  it('the listbox caps its height and scrolls instead of dropping rows', () => {
    const many = Array.from({ length: 20 }, (_, i) => `key:option-${i}`)
    render(<FilterInput value="k" onChange={vi.fn()} placeholder="p" ariaLabel="many" proposals={() => many} />)
    fireEvent.focus(screen.getByLabelText('many'))
    expect(screen.getAllByTestId('filter-proposal')).toHaveLength(20)
    const list = screen.getByRole('listbox')
    expect(list.className).toMatch(/\bmax-h-56\b/)
    expect(list.className).toMatch(/\boverflow-y-auto\b/)
  })

  it('no proposals prop means no combobox semantics', () => {
    render(<FilterInput value="k" onChange={() => {}} placeholder="p" ariaLabel="plain" />)
    expect(screen.getByLabelText('plain')).not.toHaveAttribute('role')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })
})
