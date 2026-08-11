import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { PayloadView } from './PayloadView'

describe('PayloadView', () => {
  it('renders json payloads as a tree with schema id', () => {
    render(<PayloadView payload={{ encoding: 'json', text: '{"a":1}', schema_id: 7, error: null }} />)
    expect(screen.getByText('a')).toBeInTheDocument()
    expect(screen.getByText(/schema id 7/)).toBeInTheDocument()
  })
  it('renders decode errors loudly with raw bytes', () => {
    render(<PayloadView payload={{ encoding: 'decode_error', text: 'AAECAw==', schema_id: 9, error: 'schema registry returned 404 for schema id 9' }} />)
    expect(screen.getByText(/schema registry returned 404/)).toBeInTheDocument()
    expect(screen.getByText('decode_error')).toBeInTheDocument()
    expect(screen.getByText('AAECAw==')).toBeInTheDocument()
  })
  it('renders null payloads as null marker', () => {
    render(<PayloadView payload={null} />)
    expect(screen.getByText('∅ null')).toBeInTheDocument()
  })
  it('renders utf8 as preformatted text', () => {
    render(<PayloadView payload={{ encoding: 'utf8', text: 'hello', schema_id: null, error: null }} />)
    expect(screen.getByText('hello')).toBeInTheDocument()
  })
})
