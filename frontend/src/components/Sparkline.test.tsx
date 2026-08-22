import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Sparkline } from './Sparkline'

describe('Sparkline', () => {
  it('renders an svg polyline for points', () => {
    render(<Sparkline points={[{ x: 0, y: 1 }, { x: 1, y: 3 }, { x: 2, y: 2 }]} />)
    const svg = screen.getByRole('img', { name: /throughput/i })
    expect(svg.querySelector('polyline')).not.toBeNull()
  })
  // Owner ruling 2026-08-19: the plot area carries its own background so the
  // chart region reads as a chart instead of floating on the panel surface.
  it('tints the plot area so the chart region is visible', () => {
    render(<Sparkline points={[{ x: 1, y: 1 }, { x: 2, y: 3 }]} />)
    const rect = screen.getByRole('img', { name: /sparkline/i }).querySelector('rect')
    expect(rect?.getAttribute('class')).toMatch(/fill-zinc-100/)
  })

  // Owner ruling 2026-08-22: with no axis labels the line heights read as
  // arbitrary values (the flat bottom was being read as "-1"). The scale is
  // anchored by two labels: 0 at the bottom, the window's peak at the top.
  it('labels the y axis: zero at the bottom, the peak at the top', () => {
    render(<Sparkline points={[{ x: 0, y: 0.2 }, { x: 1, y: 1.4 }]} unit="msg/s" />)
    const svg = screen.getByRole('img', { name: /sparkline/i })
    const labels = [...svg.querySelectorAll('text')].map((t) => t.textContent)
    expect(labels).toContain('0')
    expect(labels).toContain('1.4 msg/s')
  })

  // Owner design 2026-08-19: sampling only happens while someone watches, so
  // a return visit leaves a hole. The line must break there instead of
  // drawing a rate across time nobody measured.
  it('breaks the line at a gap instead of interpolating across it', () => {
    render(
      <Sparkline
        points={[
          { x: 1000, y: 1 },
          { x: 2000, y: 2 },
          { x: 200_000, y: 3, gapBefore: true },
          { x: 201_000, y: 4 },
        ]}
      />,
    )
    const lines = document.querySelectorAll('polyline')
    expect(lines).toHaveLength(2)
    expect(lines[0].getAttribute('points')?.trim().split(' ')).toHaveLength(2)
    expect(lines[1].getAttribute('points')?.trim().split(' ')).toHaveLength(2)
  })

  // A one-sample stretch draws no stroke at all, which used to mean an empty
  // chart for the first sample of a visit — each lone point gets a dot.
  it('marks a lone point with a dot so a single sample is visible', () => {
    const { container } = render(<Sparkline points={[{ x: 1000, y: 1 }, { x: 200_000, y: 3, gapBefore: true }]} />)
    expect(container.querySelectorAll('circle')).toHaveLength(2)
  })

  it('a continuous stretch is drawn as a line, with no dots', () => {
    const { container } = render(<Sparkline points={[{ x: 1000, y: 1 }, { x: 2000, y: 2 }]} />)
    expect(container.querySelectorAll('polyline')).toHaveLength(1)
    expect(container.querySelectorAll('circle')).toHaveLength(0)
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
    expect(polyline?.getAttribute('points')).toBe('146.0,24.0 238.0,2.0')
  })

  it('does not re-center a single point around a fixed domain (no zoom-out)', () => {
    render(<Sparkline points={[{ x: 190, y: 1 }]} domain={{ min: 0, max: 200 }} />)
    const svg = screen.getByRole('img', { name: /throughput/i })
    const polyline = svg.querySelector('polyline')
    // sx(190) = 2 + (190/200)*236 = 226.2 ; sy(1) = 48-2-(1/1e-9... ) -> yMax = max(1,1e-9) = 1 -> sy = 48-2-44=2.0
    expect(polyline?.getAttribute('points')).toBe('228.8,2.0')
  })
})
