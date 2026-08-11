import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { JsonView } from './JsonView'

describe('JsonView', () => {
  it('renders nested objects with keys and scalars', () => {
    render(<JsonView value={{ user: { id: 42, name: 'ada' }, tags: ['a'] }} />)
    expect(screen.getByText('user')).toBeInTheDocument()
    expect(screen.getByText('42')).toBeInTheDocument()
    expect(screen.getByText('"ada"')).toBeInTheDocument()
    expect(screen.getByText('[1]')).toBeInTheDocument()
  })
  it('renders scalars directly', () => {
    render(<JsonView value={null} />)
    expect(screen.getByText('null')).toBeInTheDocument()
  })
})
