import { describe, expect, it, vi } from 'vitest'
import userEvent from '@testing-library/user-event'
import { render, screen } from '@testing-library/react'
import { PayloadView } from './PayloadView'

describe('PayloadView', () => {
  it('renders json payloads as a tree with schema id', () => {
    render(<PayloadView payload={{ encoding: 'json', text: '{"a":1}', schema_id: 7, error: null }} label="value" />)
    expect(screen.getByText('a')).toBeInTheDocument()
    expect(screen.getByText(/schema id 7/)).toBeInTheDocument()
  })
  it('renders decode errors loudly with raw bytes', () => {
    render(<PayloadView payload={{ encoding: 'decode_error', text: 'AAECAw==', schema_id: 9, error: 'schema registry returned 404 for schema id 9' }} label="value" />)
    expect(screen.getByText(/schema registry returned 404/)).toBeInTheDocument()
    expect(screen.getByText('decode_error')).toBeInTheDocument()
    expect(screen.getByText('AAECAw==')).toBeInTheDocument()
  })
  it('renders null payloads as null marker', () => {
    render(<PayloadView payload={null} label="value" />)
    expect(screen.getByText('∅ null')).toBeInTheDocument()
  })
  it('renders utf8 as preformatted text', () => {
    render(<PayloadView payload={{ encoding: 'utf8', text: 'hello', schema_id: null, error: null }} label="value" />)
    expect(screen.getByText('hello')).toBeInTheDocument()
  })
})

describe('copy affordance', () => {
  it('copies json-able values pretty-printed', async () => {
    const user = userEvent.setup()
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<PayloadView payload={{ encoding: 'json', text: '{"a":{"b":1}}', schema_id: null, error: null }} label="value" />)
    await user.click(screen.getByLabelText('copy value'))
    expect(writeText).toHaveBeenCalledWith('{\n  "a": {\n    "b": 1\n  }\n}')
  })

  it('copies non-json text verbatim', async () => {
    const user = userEvent.setup()
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<PayloadView payload={{ encoding: 'utf8', text: 'order-1', schema_id: null, error: null }} label="key" />)
    await user.click(screen.getByLabelText('copy key'))
    expect(writeText).toHaveBeenCalledWith('order-1')
  })

  // Future-proofing for production payloads of any size (owner ruling
  // 2026-08-17): the body sits in a height-capped scroll container whose
  // scrollbars appear only on overflow — vertical for tall payloads,
  // horizontal for long unwrapped lines (nowrap makes long fields/deep
  // nesting scroll sideways instead of wrapping into porridge).
  it('json body sits in a capped, both-axis scroll container', () => {
    render(<PayloadView payload={{ encoding: 'json', text: '{"a":1}', schema_id: null, error: null }} label="value" />)
    const scroller = screen.getByTestId('payload-scroll')
    expect(scroller.className).toMatch(/\bmax-h-80\b/)
    expect(scroller.className).toMatch(/\boverflow-auto\b/)
    expect(scroller.className).toMatch(/\bwhitespace-nowrap\b/)
  })

  it('raw text keeps its own line structure without soft-wrapping', () => {
    render(<PayloadView payload={{ encoding: 'utf8', text: 'a very long single line', schema_id: null, error: null }} label="key" />)
    const pre = screen.getByTestId('payload-scroll').querySelector('pre')
    expect(pre?.className).toMatch(/\bwhitespace-pre\b/)
    expect(pre?.className).not.toMatch(/pre-wrap|break-all/)
  })
})
