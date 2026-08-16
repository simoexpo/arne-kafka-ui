import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useShowDelay } from './useShowDelay'

beforeEach(() => vi.useFakeTimers())
afterEach(() => vi.useRealTimers())

describe('useShowDelay', () => {
  it('is false immediately when active turns true, even for one frame', () => {
    const { result } = renderHook(({ active }) => useShowDelay(active, 400), { initialProps: { active: true } })
    expect(result.current).toBe(false)
  })

  it('becomes true once active has held for the full delay', () => {
    const { result, rerender } = renderHook(({ active }) => useShowDelay(active, 400), {
      initialProps: { active: false },
    })
    rerender({ active: true })
    act(() => vi.advanceTimersByTime(399))
    expect(result.current).toBe(false)
    act(() => vi.advanceTimersByTime(1))
    expect(result.current).toBe(true)
  })

  it('never fires if active goes false again before the delay elapses (a fast gesture renders nothing)', () => {
    const { result, rerender } = renderHook(({ active }) => useShowDelay(active, 400), {
      initialProps: { active: false },
    })
    rerender({ active: true })
    act(() => vi.advanceTimersByTime(200))
    rerender({ active: false })
    act(() => vi.advanceTimersByTime(1000))
    expect(result.current).toBe(false)
  })

  it('flips back to false the instant active goes false, even after already showing', () => {
    const { result, rerender } = renderHook(({ active }) => useShowDelay(active, 400), {
      initialProps: { active: true },
    })
    act(() => vi.advanceTimersByTime(400))
    expect(result.current).toBe(true)
    rerender({ active: false })
    expect(result.current).toBe(false)
  })

  it('a second true->false->true cycle re-arms its own fresh delay', () => {
    const { result, rerender } = renderHook(({ active }) => useShowDelay(active, 400), {
      initialProps: { active: true },
    })
    act(() => vi.advanceTimersByTime(400))
    expect(result.current).toBe(true)
    rerender({ active: false })
    rerender({ active: true })
    act(() => vi.advanceTimersByTime(399))
    expect(result.current).toBe(false)
    act(() => vi.advanceTimersByTime(1))
    expect(result.current).toBe(true)
  })
})
