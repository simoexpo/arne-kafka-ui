import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
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
    const { container } = render(<JsonView value={{ id: 3, tier: 'gold' }} depth={0} />)
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

  it('collapses a node beyond the default-open depth into a placeholder, hiding its members', () => {
    render(<JsonView value={{ level1: { level2: { deep: 'value' } } }} />)
    expect(screen.queryByText('deep')).not.toBeInTheDocument()
    expect(screen.getByText('level2')).toBeInTheDocument()
    expect(screen.getByText('…')).toBeInTheDocument()
  })

  it('toggles a collapsed node open when its summary/arrow is clicked', async () => {
    const user = userEvent.setup()
    render(<JsonView value={{ level1: { level2: { deep: 'value' } } }} />)
    expect(screen.queryByText('deep')).not.toBeInTheDocument()

    await user.click(screen.getByText('level2'))
    expect(screen.getByText('deep')).toBeInTheDocument()
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
