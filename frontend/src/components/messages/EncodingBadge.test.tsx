import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { EncodingBadge } from './EncodingBadge'

describe('EncodingBadge', () => {
  it('renders the encoding name', () => {
    render(<EncodingBadge encoding="avro" />)
    expect(screen.getByText('avro')).toBeInTheDocument()
  })
  it('styles decode_error as an error', () => {
    render(<EncodingBadge encoding="decode_error" />)
    expect(screen.getByText('decode_error').className).toMatch(/red/)
  })
})
