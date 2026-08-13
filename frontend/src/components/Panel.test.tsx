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
  it('renders no heading when title is omitted', () => {
    render(<Panel>content here</Panel>)
    expect(screen.getByText('content here')).toBeInTheDocument()
    expect(screen.queryByRole('heading')).not.toBeInTheDocument()
  })
  it('a failed refresh with existing data keeps the data visible and shows a compact banner', () => {
    const err = new ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true)
    render(<Panel title="Brokers" error={err} hasData>content here</Panel>)
    expect(screen.getByText('content here')).toBeInTheDocument() // stale data stays
    const banner = screen.getByTestId('panel-error-banner')
    expect(banner).toHaveTextContent('kafka_timeout')
    expect(banner).toHaveTextContent('fetch metadata timed out')
    expect(screen.queryByText(/retriable/i)).not.toBeInTheDocument() // that's the full block's extra line, not the compact banner's
  })
  it('a cold-load failure (no data yet) still renders the full error block, not the banner', () => {
    const err = new ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'prod', true)
    render(<Panel title="Brokers" error={err}>content here</Panel>)
    expect(screen.queryByTestId('panel-error-banner')).not.toBeInTheDocument()
    expect(screen.queryByText('content here')).not.toBeInTheDocument()
    expect(screen.getByText(/retriable/i)).toBeInTheDocument()
  })
  it('a background refetch with existing data does not skeleton over it', () => {
    render(<Panel title="Brokers" loading hasData>content here</Panel>)
    expect(screen.getByText('content here')).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })
})
