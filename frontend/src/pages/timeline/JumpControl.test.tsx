import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { JumpControl } from './JumpControl'

describe('JumpControl', () => {
  it('jumping to "now" calls onJump immediately with kind now, no expansion', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-now'))
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'now' })
    expect(screen.queryByTestId('jump-offset-input')).not.toBeInTheDocument()
  })

  it('jumping to "beginning" calls onJump immediately with kind beginning', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-beginning'))
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'beginning' })
  })

  it('has aria-labels on every segment', () => {
    render(<JumpControl onJump={vi.fn()} />)
    expect(screen.getByLabelText('jump to now')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to beginning')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to offset')).toBeInTheDocument()
    expect(screen.getByLabelText('jump to timestamp')).toBeInTheDocument()
  })

  it('clicking "offset…" expands partition + offset inputs without calling onJump', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    expect(screen.getByTestId('jump-offset-partition-input')).toBeInTheDocument()
    expect(screen.getByTestId('jump-offset-value-input')).toBeInTheDocument()
    expect(onJump).not.toHaveBeenCalled()
  })

  it('offset apply is disabled until both partition and offset are valid numbers', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    const apply = screen.getByTestId('jump-offset-apply')
    expect(apply).toBeDisabled()

    await user.type(screen.getByTestId('jump-offset-partition-input'), '2')
    expect(apply).toBeDisabled() // offset still empty

    await user.type(screen.getByTestId('jump-offset-value-input'), 'abc')
    expect(apply).toBeDisabled() // non-numeric

    await user.clear(screen.getByTestId('jump-offset-value-input'))
    await user.type(screen.getByTestId('jump-offset-value-input'), '42')
    expect(apply).toBeEnabled()

    await user.click(apply)
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'offset', partition: 2, offset: 42 })
  })

  it('offset input applies on Enter when valid', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    await user.type(screen.getByTestId('jump-offset-partition-input'), '0')
    await user.type(screen.getByTestId('jump-offset-value-input'), '100{Enter}')
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'offset', partition: 0, offset: 100 })
  })

  it('Enter does not apply while offset inputs are invalid', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-offset'))
    await user.type(screen.getByTestId('jump-offset-partition-input'), '0')
    await user.type(screen.getByTestId('jump-offset-value-input'), '-5{Enter}')
    expect(onJump).not.toHaveBeenCalled()
  })

  it('clicking "timestamp…" expands an epoch-ms input, disabled apply until numeric, applies on Enter', async () => {
    const user = userEvent.setup()
    const onJump = vi.fn()
    render(<JumpControl onJump={onJump} />)
    await user.click(screen.getByTestId('jump-timestamp'))
    const apply = screen.getByTestId('jump-timestamp-apply')
    expect(apply).toBeDisabled()

    await user.type(screen.getByTestId('jump-timestamp-input'), 'nope')
    expect(apply).toBeDisabled()

    await user.clear(screen.getByTestId('jump-timestamp-input'))
    await user.type(screen.getByTestId('jump-timestamp-input'), '1700000000000{Enter}')
    expect(onJump).toHaveBeenCalledExactlyOnceWith({ kind: 'timestamp', ts_ms: 1700000000000 })
  })

  it('switching segments resets the previous expansion', async () => {
    const user = userEvent.setup()
    render(<JumpControl onJump={vi.fn()} />)
    await user.click(screen.getByTestId('jump-offset'))
    expect(screen.getByTestId('jump-offset-partition-input')).toBeInTheDocument()
    await user.click(screen.getByTestId('jump-timestamp'))
    expect(screen.queryByTestId('jump-offset-partition-input')).not.toBeInTheDocument()
    expect(screen.getByTestId('jump-timestamp-input')).toBeInTheDocument()
  })
})
