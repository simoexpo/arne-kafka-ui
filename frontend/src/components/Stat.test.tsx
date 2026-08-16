import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Stat } from './Stat'

// Fonts pass (owner-approved 2026-08-17 analysis, item 6): OverviewPage.tsx
// and TopicDetailPage.tsx each used to define their own local `Stat`
// component with identical classes (text-xs label / text-xl font-semibold
// value) — nothing enforced they'd keep agreeing. This single shared
// component is the guarantee; no visual change is intended.
describe('Stat', () => {
  it('renders a label/value pair with a data-testid keyed by the label', () => {
    render(<Stat label="partitions" value="12" />)
    const stat = screen.getByTestId('stat-partitions')
    expect(stat).toHaveTextContent('partitions')
    expect(stat).toHaveTextContent('12')
  })

  it('label uses text-xs, value uses text-xl font-semibold', () => {
    render(<Stat label="partitions" value="12" />)
    expect(screen.getByText('partitions').className).toMatch(/\btext-xs\b/)
    const value = screen.getByText('12')
    expect(value.className).toMatch(/\btext-xl\b/)
    expect(value.className).toMatch(/\bfont-semibold\b/)
  })

  it('optionally warns the value red, e.g. for a non-zero under-replicated count', () => {
    render(<Stat label="under-replicated" value="3" warn />)
    expect(screen.getByText('3').className).toMatch(/text-red-600/)
  })

  it('does not warn by default', () => {
    render(<Stat label="topics" value="4" />)
    expect(screen.getByText('4').className).not.toMatch(/text-red-600/)
  })
})
