import { renderHook, act } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { getTimeDisplayMode, setTimeDisplayMode, useTimeDisplayMode } from './timeDisplayMode'

describe('timeDisplayMode', () => {
  it('defaults to utc (today\'s behavior) when nothing is stored', () => {
    expect(getTimeDisplayMode()).toBe('utc')
  })

  it('setTimeDisplayMode persists the choice to localStorage', () => {
    setTimeDisplayMode('local')
    expect(localStorage.getItem('timeDisplayMode')).toBe('local')
    expect(getTimeDisplayMode()).toBe('local')
  })

  it('persists across a remount — a fresh subscriber reads the stored mode, not a component-local default', () => {
    setTimeDisplayMode('local')
    const { result, unmount } = renderHook(() => useTimeDisplayMode())
    expect(result.current).toBe('local')
    unmount()
    const remounted = renderHook(() => useTimeDisplayMode())
    expect(remounted.result.current).toBe('local')
  })

  it('useTimeDisplayMode re-renders every subscriber when the mode changes', () => {
    const { result } = renderHook(() => useTimeDisplayMode())
    expect(result.current).toBe('utc')
    act(() => setTimeDisplayMode('local'))
    expect(result.current).toBe('local')
    act(() => setTimeDisplayMode('utc'))
    expect(result.current).toBe('utc')
  })
})
