import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { CopyButton } from './CopyButton'

describe('CopyButton', () => {
  it('copies the given text to the clipboard and shows a copied hint', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })
    render(<CopyButton text="orders" label="topic name" />)
    await userEvent.click(screen.getByRole('button', { name: 'copy topic name' }))
    expect(writeText).toHaveBeenCalledWith('orders')
    expect(await screen.findByText(/copied/i)).toBeInTheDocument()
  })
})

// The transient "copied" hint must not occupy layout space — inside a
// table cell it used to widen the column for 1.5s on every click. It
// floats over adjacent content instead (owner request 2026-08-18).
it('the copied hint floats without taking layout space', async () => {
  const user = userEvent.setup()
  Object.defineProperty(navigator, 'clipboard', { value: { writeText: vi.fn().mockResolvedValue(undefined) }, configurable: true })
  render(<CopyButton text="x" label="thing" />)
  const wrapper = screen.getByLabelText('copy thing').parentElement!
  expect(wrapper.className).toMatch(/\brelative\b/)
  await user.click(screen.getByLabelText('copy thing'))
  const hint = screen.getByText('copied')
  expect(hint.className).toMatch(/\babsolute\b/)
  expect(hint.className).toMatch(/\btop-full\b/)
})
