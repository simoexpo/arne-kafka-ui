import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { LivePill, PlayPauseToggle } from './LivePill'

describe('LivePill', () => {
  it('renders nothing when the buffer is empty', () => {
    const { container } = render(<LivePill count={0} capped={false} attached onClick={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the buffered count', () => {
    render(<LivePill count={7} capped={false} attached onClick={vi.fn()} />)
    expect(screen.getByText('▲ 7 new')).toBeInTheDocument()
  })

  it('keeps counting past the buffer cap and appends "· older dropped" once overflowed, rather than freezing at a fixed string', () => {
    render(<LivePill count={612} capped={true} attached onClick={vi.fn()} />)
    expect(screen.getByText('▲ 612 new · older dropped')).toBeInTheDocument()
    expect(screen.queryByText('▲ 612 new')).not.toBeInTheDocument() // the suffix must be present, not a bare count
  })

  it('calls onClick when clicked', async () => {
    const user = userEvent.setup()
    const onClick = vi.fn()
    render(<LivePill count={3} capped={false} attached onClick={onClick} />)
    await user.click(screen.getByTestId('live-pill'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
  it('labels the click by what it will do: flush when attached, jump to now when detached', () => {
    const { rerender } = render(<LivePill count={2} capped={false} attached onClick={vi.fn()} />)
    expect(screen.getByTestId('live-pill')).toHaveAttribute('aria-label', 'flush buffered live messages')
    rerender(<LivePill count={2} capped={false} attached={false} onClick={vi.fn()} />)
    expect(screen.getByTestId('live-pill')).toHaveAttribute('aria-label', 'jump to now and show new messages')
  })

})

describe('PlayPauseToggle', () => {
  it('shows aria-pressed=false while live (not paused)', () => {
    render(<PlayPauseToggle paused={false} onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    expect(btn).toHaveAttribute('aria-pressed', 'false')
  })

  it('shows aria-pressed=true while paused', () => {
    render(<PlayPauseToggle paused={true} onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    expect(btn).toHaveAttribute('aria-pressed', 'true')
  })

  // The lit icon must mirror the CURRENT state, not the click action: a lit
  // pause icon reads as "paused" to a user, so pause can only be lit while
  // actually paused, and play only while actually live.
  it('while live: play icon is lit emerald, pause icon is unlit zinc', () => {
    render(<PlayPauseToggle paused={false} onClick={vi.fn()} />)
    expect(screen.getByTestId('icon-play')).toHaveClass('text-emerald-500')
    expect(screen.getByTestId('icon-pause')).toHaveClass('text-zinc-400')
  })

  it('while paused: pause icon is lit amber, play icon is unlit zinc', () => {
    render(<PlayPauseToggle paused={true} onClick={vi.fn()} />)
    expect(screen.getByTestId('icon-pause')).toHaveClass('text-amber-500')
    expect(screen.getByTestId('icon-play')).toHaveClass('text-zinc-400')
  })

  it('renders the play icon before the pause icon (left-to-right order)', () => {
    render(<PlayPauseToggle paused={false} onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    const icons = btn.querySelectorAll('svg')
    expect(icons[0]).toHaveAttribute('data-testid', 'icon-play')
    expect(icons[1]).toHaveAttribute('data-testid', 'icon-pause')
  })

  it('calls onClick when clicked', async () => {
    const user = userEvent.setup()
    const onClick = vi.fn()
    render(<PlayPauseToggle paused={false} onClick={onClick} />)
    await user.click(screen.getByTestId('play-pause-toggle'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })
})
