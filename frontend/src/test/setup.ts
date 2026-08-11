import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'
import '@testing-library/jest-dom/vitest'

afterEach(() => {
  cleanup()
})

// jsdom performs no layout: every element reports offsetWidth/offsetHeight
// of 0. @tanstack/react-virtual reads these to size its scroll viewport, and
// zero collapses its visible range to nothing regardless of `initialRect`.
// Stub a fixed, non-zero viewport so virtualized lists render in tests.
Object.defineProperty(HTMLElement.prototype, 'offsetWidth', { configurable: true, value: 900 })
Object.defineProperty(HTMLElement.prototype, 'offsetHeight', { configurable: true, value: 600 })
