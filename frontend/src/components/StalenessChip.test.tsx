import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StalenessChip } from './StalenessChip'

describe('StalenessChip', () => {
  it('shows relative age', () => {
    render(<StalenessChip asOf={1_000_000} now={1_015_000} />)
    expect(screen.getByText('15s ago')).toBeInTheDocument()
  })
  it('shows placeholder when asOf is null', () => {
    render(<StalenessChip asOf={null} now={1_000} />)
    expect(screen.getByText('no data yet')).toBeInTheDocument()
  })
  it('is "just now" and fresh within a poll cycle', () => {
    render(<StalenessChip asOf={1_000_000} now={1_005_000} />)
    const chip = screen.getByText('just now')
    expect(chip).toHaveAttribute('data-staleness', 'fresh')
  })
  it('turns amber and "aging" past 36s', () => {
    render(<StalenessChip asOf={1_000_000} now={1_040_000} />)
    const chip = screen.getByText('40s ago')
    expect(chip).toHaveAttribute('data-staleness', 'aging')
    expect(chip.className).toMatch(/amber/)
  })
  it('turns red and "stale" past 2m', () => {
    render(<StalenessChip asOf={1_000_000} now={1_180_000} />)
    const chip = screen.getByText('3m ago')
    expect(chip).toHaveAttribute('data-staleness', 'stale')
    expect(chip.className).toMatch(/red/)
  })
  it('stays quiet while refreshing even when the cached data is stale', () => {
    render(<StalenessChip asOf={1_000_000} now={1_180_000} refreshing />)
    const chip = screen.getByText('3m ago')
    expect(chip).toHaveAttribute('data-staleness', 'refreshing')
    expect(chip.className).not.toMatch(/red/)
    expect(chip.className).not.toMatch(/amber/)
  })
})
