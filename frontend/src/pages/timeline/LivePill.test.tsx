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

  it('aria-label states the action: "pause" while live, "resume" while genuinely paused', () => {
    const { rerender } = render(<PlayPauseToggle paused={false} onClick={vi.fn()} />)
    expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-label', 'pause live updates')
    rerender(<PlayPauseToggle paused={true} onClick={vi.fn()} />)
    expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-label', 'resume live updates')
  })

  // M3: `paused` renders lit/pressed while inspecting even when the actual
  // pause reason is 'none' (TimelineHeader passes `paused || inspecting`) —
  // but the click still PAUSES from there (toggleClick's 'none' branch), it
  // does not resume anything. `inspectingOnly` is true for exactly that
  // case, and must override the label so a screen reader is told what the
  // click actually does instead of "resume live updates".
  it('aria-label says the click pauses explicitly (and stays paused after inspection) when lit ONLY because of an open inspection', () => {
    render(<PlayPauseToggle paused={true} inspectingOnly title="paused while inspecting" onClick={vi.fn()} />)
    const btn = screen.getByTestId('play-pause-toggle')
    expect(btn).toHaveAttribute('aria-label', 'pause explicitly — stays paused after inspection')
    expect(btn).not.toHaveAttribute('aria-label', 'resume live updates')
  })
})
