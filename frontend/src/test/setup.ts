import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'
import '@testing-library/jest-dom/vitest'

afterEach(() => {
  cleanup()
  // Several features persist a small preference to localStorage (theme,
  // time-display mode) — jsdom's localStorage otherwise survives across
  // tests in the SAME file, leaking one test's toggle into the next.
  localStorage.clear()
})

// jsdom performs no layout: every element reports offsetWidth/offsetHeight
// of 0. @tanstack/react-virtual reads these to size its scroll viewport, and
// zero collapses its visible range to nothing regardless of `initialRect`.
// Stub a fixed, non-zero viewport so virtualized lists render in tests.
Object.defineProperty(HTMLElement.prototype, 'offsetWidth', { configurable: true, value: 900 })
Object.defineProperty(HTMLElement.prototype, 'offsetHeight', { configurable: true, value: 600 })

// @tanstack/virtual-core schedules
// some of its own scroll/rect bookkeeping via `requestAnimationFrame` on the
// window it was constructed against (see `Virtualizer.scheduleScrollReconcile`'s
// `this.targetWindow.requestAnimationFrame(...)` call). jsdom's default rAF
// runs on a REAL ~16ms timer — one can still be pending when a test's own
// `cleanup()` unmounts the component (or, across test FILES, after that
// file's jsdom `window` has been torn down entirely), surfacing as a bogus
// "window is not defined" unhandled error attributed to whatever test
// happens to be running when the stale timer finally fires — nothing to do
// with that test's own code. Running every rAF callback SYNCHRONOUSLY
// (same tick, immediately) means none is ever left pending across a
// teardown boundary: every scheduled callback has already resolved by the
// time a test's own assertions (and `cleanup()`) run. This does not
// swallow real unhandled errors — it removes exactly one false-positive
// caused by test-environment lifecycle, not application code; anything
// that actually throws still fails the suite loudly, synchronously, in the
// test that triggered it.
let rafId = 0
;(globalThis as unknown as { requestAnimationFrame: typeof requestAnimationFrame }).requestAnimationFrame = ((
  cb: FrameRequestCallback,
) => {
  cb(0)
  return ++rafId
}) as typeof requestAnimationFrame
;(globalThis as unknown as { cancelAnimationFrame: typeof cancelAnimationFrame }).cancelAnimationFrame = (() => {}) as typeof cancelAnimationFrame
