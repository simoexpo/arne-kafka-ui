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

  it('maps x against a fixed domain instead of data min/max when provided', () => {
    render(<Sparkline points={[{ x: 100, y: 1 }, { x: 200, y: 2 }]} domain={{ min: 0, max: 200 }} />)
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // w=240, h=48, pad=2 -> sx(x) = 2 + (x/200)*236 ; sy(y) = 48 - 2 - (y/yMax)*44, yMax=2
    expect(polyline?.getAttribute('points')).toBe('120.0,24.0 238.0,2.0')
  })

  it('does not re-center a single point around a fixed domain (no zoom-out)', () => {
    render(<Sparkline points={[{ x: 190, y: 1 }]} domain={{ min: 0, max: 200 }} />)
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // sx(190) = 2 + (190/200)*236 = 226.2 ; sy(1) = 48-2-(1/1e-9... ) -> yMax = max(1,1e-9) = 1 -> sy = 48-2-44=2.0
    expect(polyline?.getAttribute('points')).toBe('226.2,2.0')
  })
})
