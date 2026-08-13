import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
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
})
