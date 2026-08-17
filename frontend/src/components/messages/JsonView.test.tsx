import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { JsonView } from './JsonView'

describe('JsonView', () => {
  it('renders scalars directly, unchanged', () => {
    render(<JsonView value={null} />)
    expect(screen.getByText('null')).toBeInTheDocument()
  })

  it('quotes string values but leaves numbers and booleans bare', () => {
    const { container } = render(<JsonView value={{ name: 'ada', age: 30, active: true }} />)
    expect(container.textContent).toContain('"ada"')
    expect(container.textContent).toMatch(/"age": 30/)
    expect(container.textContent).toMatch(/"active": true/)
  })

  it('renders an expanded object with opening/closing braces and quoted keys', () => {
    const { container } = render(<JsonView value={{ id: 3, tier: 'gold' }} />)
    const text = container.textContent!
    expect(text.startsWith('{')).toBe(true)
    expect(text.trim().endsWith('}')).toBe(true)
    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getByText('tier')).toBeInTheDocument()
  })

  it('separates sibling members with commas, no trailing comma after the last', () => {
    const { container } = render(<JsonView value={{ a: 1, b: 2 }} />)
    const text = container.textContent!
    expect(text).toContain('"a": 1,')
    expect(text).not.toMatch(/"b": 2,/)
  })

  // Owner ruling 2026-08-17: opening the inspect view shows the WHOLE
  // JSON — every nesting level expanded — with collapsing still available
  // per node.
  it('renders fully expanded at every depth', () => {
    render(<JsonView value={{ a: { b: { c: { d: { e: 'deep-leaf' } } } }, nums: [1, 2, 3] }} />)
    expect(screen.getByText('"deep-leaf"')).toBeInTheDocument()
    expect(screen.getByText('3')).toBeInTheDocument()
  })

  it('a node can still be collapsed by hand', () => {
    // jsdom doesn't implement native <details> click-activation, so this
    // fires the `toggle` event the browser would dispatch after the click.
    const { container } = render(<JsonView value={{ a: { b: 1 } }} />)
    const details = container.querySelectorAll('details')
    const inner = details[details.length - 1]
    inner.open = false
    fireEvent(inner, new Event('toggle'))
    expect(screen.queryByText('1')).not.toBeInTheDocument()
  })

  it('renders arrays with brackets, comma-separated, without index keys', () => {
    const { container } = render(<JsonView value={['a', 'b']} />)
    const text = container.textContent!
    expect(text.startsWith('[')).toBe(true)
    expect(text).toContain('"a",')
    expect(text.trim().endsWith(']')).toBe(true)
    expect(screen.queryByText('0')).not.toBeInTheDocument()
  })

  it('renders an empty object as a bare pair of braces', () => {
    render(<JsonView value={{ empty: {} }} />)
    expect(screen.getByText('{}')).toBeInTheDocument()
  })
})
