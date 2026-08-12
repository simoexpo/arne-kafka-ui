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
