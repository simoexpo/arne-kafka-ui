import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StalenessChip } from './StalenessChip'

describe('StalenessChip', () => {
  it('shows relative age', () => {
    render(<StalenessChip asOf={1_000_000} now={1_004_000} />)
    expect(screen.getByText('4s ago')).toBeInTheDocument()
  })
  it('shows placeholder when asOf is null', () => {
    render(<StalenessChip asOf={null} now={1_000} />)
    expect(screen.getByText('no data yet')).toBeInTheDocument()
  })
})
