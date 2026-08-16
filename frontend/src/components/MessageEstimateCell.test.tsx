import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MessageEstimateCell } from './MessageEstimateCell'

// Shared by OverviewPage's "Top topics" table and TopicsPage's inventory
// table (n8, queue-review): both used to carry an identical, independently
// maintained 8-line tri-state ternary — a real message count, a dash with an
// estimate-error tooltip, or a plain dash. One component now owns that
// contract.
describe('MessageEstimateCell', () => {
  it('renders the formatted count when the estimate is known', () => {
    render(<MessageEstimateCell estimate={1500} error={null} />)
    expect(screen.getByText('1.5k')).toBeInTheDocument()
  })

  it('renders a plain dash when there is no estimate and no error (e.g. an internal topic)', () => {
    render(<MessageEstimateCell estimate={null} error={null} />)
    const dash = screen.getByText('—')
    expect(dash).not.toHaveAttribute('title')
  })

  it('renders a dash with a Kafka-attributed tooltip when a watermark fetch failed', () => {
    render(<MessageEstimateCell estimate={null} error="counting messages timed out" />)
    const dash = screen.getByText('—')
    expect(dash).toHaveAttribute('title', "Kafka couldn't provide a count — counting messages timed out")
  })

  it('the error dash is keyboard-reachable and screen-reader-labelled, not title= alone', () => {
    render(<MessageEstimateCell estimate={null} error="counting messages timed out" />)
    const dash = screen.getByText('—')
    expect(dash).toHaveAttribute('tabIndex', '0')
    expect(dash).toHaveAttribute('aria-label', "Kafka couldn't provide a count — counting messages timed out")
  })
})
