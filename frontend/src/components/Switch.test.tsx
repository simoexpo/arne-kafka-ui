import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Switch } from './Switch'

// Same dedup shape as the Stat.tsx precedent: TopicsPage's "show internal"
// switch and TopicDetailPage's "show all configs" switch used to each
// define byte-identical Tailwind track/thumb markup — nothing enforced
// they'd keep agreeing. This is a consistency guarantee only; no visual
// change is intended for either call site.
describe('Switch', () => {
  it('renders the visible label text', () => {
    render(<Switch checked={false} label="show internal" onChange={vi.fn()} />)
    expect(screen.getByText('show internal')).toBeInTheDocument()
  })

  it('reflects checked via role=switch and aria-checked', () => {
    render(<Switch checked label="show internal" onChange={vi.fn()} />)
    expect(screen.getByRole('switch', { name: 'show internal' })).toHaveAttribute('aria-checked', 'true')
  })

  it('unchecked reflects aria-checked=false', () => {
    render(<Switch checked={false} label="show internal" onChange={vi.fn()} />)
    expect(screen.getByRole('switch', { name: 'show internal' })).toHaveAttribute('aria-checked', 'false')
  })

  it('calls onChange when clicked', async () => {
    const onChange = vi.fn()
    render(<Switch checked={false} label="show internal" onChange={onChange} />)
    await userEvent.click(screen.getByRole('switch', { name: 'show internal' }))
    expect(onChange).toHaveBeenCalledTimes(1)
  })

  it('supports a separate accessible name via ariaLabel', () => {
    render(<Switch checked={false} label="show all configs" ariaLabel="show all configs" onChange={vi.fn()} />)
    expect(screen.getByRole('switch', { name: 'show all configs' })).toBeInTheDocument()
  })
})
