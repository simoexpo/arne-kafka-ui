import { renderHook } from '@testing-library/react'
import { StrictMode } from 'react'
import { describe, expect, it } from 'vitest'
import { useFallingEdge } from './useFallingEdge'

describe('useFallingEdge', () => {
  // Owner-reported regression (2026-08-16): the jump-landing effect never
  // fired in a real browser while every jsdom test stayed green. The app
  // mounts under <StrictMode> (main.tsx), which double-invokes every render
  // in development — a hook that records "what the value was last render" by
  // writing a ref DURING render reads its own just-written value on the
  // second pass and reports no edge at all, so `scrollToEdge('bottom')` never
  // ran. This block renders the hook the way the app actually renders it.
  describe('under StrictMode double-rendering', () => {
    it('still reports the true -> false edge', () => {
      const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
        initialProps: { value: true },
        wrapper: StrictMode,
      })
      expect(result.current).toBe(false)

      rerender({ value: false })
      expect(result.current).toBe(true)
    })

    it('does not invent an edge while the value holds steady', () => {
      const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
        initialProps: { value: false },
        wrapper: StrictMode,
      })
      rerender({ value: true })
      expect(result.current).toBe(false)
      rerender({ value: true })
      expect(result.current).toBe(false)
    })

    it('reports a second edge after the value goes true again', () => {
      const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
        initialProps: { value: true },
        wrapper: StrictMode,
      })
      rerender({ value: false })
      expect(result.current).toBe(true)

      rerender({ value: true })
      expect(result.current).toBe(false)

      rerender({ value: false })
      expect(result.current).toBe(true)
    })
  })

  // A render React throws away (an interrupted concurrent render, a
  // StrictMode second pass) must not consume the edge: the hook's output has
  // to be a function of the last DIFFERENT value it saw, not of how many
  // times it happened to run.
  it('reports the same edge no matter how many times a render repeats', () => {
    const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
      initialProps: { value: true },
    })
    rerender({ value: false })
    expect(result.current).toBe(true)
  })

  it('is false on mount, even when the initial value is true', () => {
    const { result } = renderHook(() => useFallingEdge(true))
    expect(result.current).toBe(false)
  })

  it('is true exactly on the render where the value went true -> false', () => {
    const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
      initialProps: { value: false },
    })
    expect(result.current).toBe(false)

    rerender({ value: true })
    expect(result.current).toBe(false)

    rerender({ value: false })
    expect(result.current).toBe(true)
  })

  // Deliberately a level, not a one-render pulse: see the hook's own doc
  // comment. Re-rendering with the SAME value must not change the answer —
  // that is exactly the property StrictMode's double render broke.
  it('stays true across further renders at the same value, and goes false when it rises again', () => {
    const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
      initialProps: { value: true },
    })
    rerender({ value: false })
    expect(result.current).toBe(true)

    rerender({ value: false })
    expect(result.current).toBe(true)

    rerender({ value: true })
    expect(result.current).toBe(false)
  })

  it('two independent instances do not consume each other\'s edge', () => {
    const { result, rerender } = renderHook(
      ({ a, b }) => ({ a: useFallingEdge(a), b: useFallingEdge(b) }),
      { initialProps: { a: true, b: true } },
    )
    rerender({ a: false, b: true })
    expect(result.current.a).toBe(true)
    expect(result.current.b).toBe(false)
  })
})
