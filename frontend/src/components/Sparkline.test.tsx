import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Sparkline } from './Sparkline'

describe('Sparkline', () => {
  it('renders an svg polyline for points', () => {
    render(<Sparkline points={[{ x: 0, y: 1 }, { x: 1, y: 3 }, { x: 2, y: 2 }]} />)
    const svg = screen.getByRole('img', { name: /throughput/i })
    expect(svg.querySelector('polyline')).not.toBeNull()
  })
  it('renders placeholder text without points', () => {
    render(<Sparkline points={[]} />)
    expect(screen.getByText('no samples yet')).toBeInTheDocument()
  })
})
