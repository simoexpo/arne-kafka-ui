import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ApiError } from '../api/client'
import { Panel } from './Panel'

describe('Panel', () => {
  it('renders children when neither loading nor error', () => {
    render(<Panel title="Brokers">content here</Panel>)
    expect(screen.getByText('Brokers')).toBeInTheDocument()
    expect(screen.getByText('content here')).toBeInTheDocument()
  })
  it('renders skeleton while loading', () => {
    render(<Panel title="Brokers" loading>content</Panel>)
    expect(screen.queryByText('content')).not.toBeInTheDocument()
    expect(screen.getAllByTestId('skeleton').length).toBeGreaterThan(0)
  })
  it('renders structured error inline, error wins over loading', () => {
    const err = new ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true)
    render(<Panel title="Brokers" loading error={err}>content</Panel>)
    expect(screen.getByText(/kafka_timeout/)).toBeInTheDocument()
    expect(screen.getByText(/timed out/)).toBeInTheDocument()
    expect(screen.getByText(/retriable/i)).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })
  it('renders plain errors too', () => {
    render(<Panel title="X" error={new Error('network down')}>c</Panel>)
    expect(screen.getByText(/network down/)).toBeInTheDocument()
  })
})
