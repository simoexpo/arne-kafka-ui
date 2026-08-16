import { renderHook } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { useFallingEdge } from './useFallingEdge'

describe('useFallingEdge', () => {
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

  it('is false again on the render immediately after the falling edge', () => {
    const { result, rerender } = renderHook(({ value }) => useFallingEdge(value), {
      initialProps: { value: true },
    })
    rerender({ value: false })
    expect(result.current).toBe(true)

    rerender({ value: false })
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
