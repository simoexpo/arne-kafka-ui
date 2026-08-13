import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { LivePill, PlayPauseToggle } from './LivePill'

describe('LivePill', () => {
  it('renders nothing when the buffer is empty', () => {
    const { container } = render(<LivePill count={0} capped={false} onClick={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the buffered count', () => {
    render(<LivePill count={7} capped={false} onClick={vi.fn()} />)
    expect(screen.getByText('▲ 7 new')).toBeInTheDocument()
  })

  it('keeps counting past the buffer cap and appends "· older dropped" once overflowed, rather than freezing at a fixed string', () => {
    render(<LivePill count={612} capped={true} onClick={vi.fn()} />)
    expect(screen.getByText('▲ 612 new · older dropped')).toBeInTheDocument()
    expect(screen.queryByText('▲ 612 new')).not.toBeInTheDocument() // the suffix must be present, not a bare count
  })

  it('calls onClick when clicked', async () => {
    const user = userEvent.setup()
    const onClick = vi.fn()
    render(<LivePill count={3} capped={false} onClick={onClick} />)
    await user.click(screen.getByTestId('live-pill'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})

describe('PlayPauseToggle', () => {
  it('shows aria-pressed=false and a pause icon while live (not paused)', () => {
    render(<PlayPauseToggle paused={false} onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    expect(btn).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByTestId('icon-pause')).toBeInTheDocument()
  })

  it('shows aria-pressed=true and a play icon while paused', () => {
    render(<PlayPauseToggle paused={true} onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    expect(btn).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByTestId('icon-play')).toBeInTheDocument()
  })

  it('calls onClick when clicked', async () => {
    const user = userEvent.setup()
    const onClick = vi.fn()
    render(<PlayPauseToggle paused={false} onClick={onClick} />)
    await user.click(screen.getByTestId('play-pause-toggle'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
