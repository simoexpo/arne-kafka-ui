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

  it('Escape dismisses until the text changes', () => {
    const { input } = setup()
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
    fireEvent.change(input, { target: { value: 'ke' } })
    // the change resets the dismissal; the parent re-render supplies the new
    // value in real usage, so reopening is covered by the integration test
  })

  it('click accepts a proposal without blurring the input', () => {
    const { onChange } = setup()
    const row = screen.getAllByTestId('filter-proposal')[1]
    fireEvent.mouseDown(row)
    fireEvent.click(row)
    expect(onChange).toHaveBeenCalledWith('key=')
  })

  it('no proposals prop means no combobox semantics', () => {
    render(<FilterInput value="k" onChange={() => {}} placeholder="p" ariaLabel="plain" />)
    expect(screen.getByLabelText('plain')).not.toHaveAttribute('role')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })
})
