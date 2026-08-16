import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import * as sse from '../../api/sse'
import type { MessageOut, SseErrorData } from '../../api/types'
import type { PauseReason } from '../../lib/timeline/model'
import { useLiveTail, type UseLiveTailDeps } from './useLiveTail'

vi.mock('../../api/sse', async (importOriginal) => ({
  ...(await importOriginal<typeof sse>()),
  tailTopic: vi.fn(),
}))

interface TailHandlers {
  onMessage: (m: MessageOut) => void
  onError: (e: SseErrorData) => void
  onTransportError: () => void
}

function mockTail() {
  const handles: { handlers: TailHandlers; close: ReturnType<typeof vi.fn> }[] = []
  vi.mocked(sse.tailTopic).mockImplementation((_c, _t, h) => {
    const close = vi.fn()
    handles.push({ handlers: h as unknown as TailHandlers, close })
    return { close }
  })
  return handles
}

const mk = (offset: number): MessageOut => ({
  partition: 0,
  offset,
  timestamp_ms: 1000 + offset,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
})

function deps(overrides: Partial<UseLiveTailDeps> = {}): UseLiveTailDeps {
  return {
    predicateRef: { current: () => true },
    attachedRef: { current: true },
    pauseReasonRef: { current: 'none' as PauseReason },
    onLiveInsert: vi.fn(),
    onBuffer: vi.fn(),
    onChange: vi.fn(),
    ...overrides,
  }
}

beforeEach(() => vi.clearAllMocks())
afterEach(() => vi.restoreAllMocks())

describe('useLiveTail', () => {
  it('opens the tail stream for the given cluster/topic on mount', () => {
    const handles = mockTail()
    renderHook(() => useLiveTail('prod', 'orders', deps()))
    expect(sse.tailTopic).toHaveBeenCalledTimes(1)
    expect(sse.tailTopic).toHaveBeenCalledWith('prod', 'orders', expect.anything())
    expect(handles).toHaveLength(1)
  })

  it('starts alive with no error text', () => {
    mockTail()
    const { result } = renderHook(() => useLiveTail('prod', 'orders', deps()))
    expect(result.current).toEqual({ alive: true, errorText: null })
  })

  it('a message failing the predicate is dropped: neither insert, buffer, nor onChange fire', () => {
    const handles = mockTail()
    const d = deps({ predicateRef: { current: () => false } })
    renderHook(() => useLiveTail('prod', 'orders', d))
    act(() => handles[0].handlers.onMessage(mk(1)))
    expect(d.onLiveInsert).not.toHaveBeenCalled()
    expect(d.onBuffer).not.toHaveBeenCalled()
    expect(d.onChange).not.toHaveBeenCalled()
  })

  it('a matching message merges live when attached and unpaused', () => {
    const handles = mockTail()
    const d = deps({ attachedRef: { current: true }, pauseReasonRef: { current: 'none' } })
    renderHook(() => useLiveTail('prod', 'orders', d))
    const m = mk(1)
    act(() => handles[0].handlers.onMessage(m))
    expect(d.onLiveInsert).toHaveBeenCalledWith(m)
    expect(d.onBuffer).not.toHaveBeenCalled()
    expect(d.onChange).toHaveBeenCalledTimes(1)
  })

  it('a matching message buffers instead of merging while detached', () => {
    const handles = mockTail()
    const d = deps({ attachedRef: { current: false }, pauseReasonRef: { current: 'none' } })
    renderHook(() => useLiveTail('prod', 'orders', d))
    const m = mk(1)
    act(() => handles[0].handlers.onMessage(m))
    expect(d.onBuffer).toHaveBeenCalledWith(m)
    expect(d.onLiveInsert).not.toHaveBeenCalled()
    expect(d.onChange).toHaveBeenCalledTimes(1)
  })

  it('a matching message buffers instead of merging while paused, even if attached', () => {
    const handles = mockTail()
    const d = deps({ attachedRef: { current: true }, pauseReasonRef: { current: 'explicit' } })
    renderHook(() => useLiveTail('prod', 'orders', d))
    const m = mk(1)
    act(() => handles[0].handlers.onMessage(m))
    expect(d.onBuffer).toHaveBeenCalledWith(m)
    expect(d.onLiveInsert).not.toHaveBeenCalled()
  })

  it('a server error stops the stream for good and surfaces its text', () => {
    const handles = mockTail()
    const { result } = renderHook(() => useLiveTail('prod', 'orders', deps()))
    act(() => handles[0].handlers.onError({ code: 'boom', message: 'kafka fell over' }))
    expect(result.current).toEqual({ alive: false, errorText: 'boom: kafka fell over' })
    expect(handles[0].close).toHaveBeenCalledTimes(1)
  })

  it('a transport error stops the stream for good with fixed wording', () => {
    const handles = mockTail()
    const { result } = renderHook(() => useLiveTail('prod', 'orders', deps()))
    act(() => handles[0].handlers.onTransportError())
    expect(result.current).toEqual({ alive: false, errorText: 'connection lost — retrying is manual' })
    expect(handles[0].close).toHaveBeenCalledTimes(1)
  })

  it('closes the stream on unmount', () => {
    const handles = mockTail()
    const { unmount } = renderHook(() => useLiveTail('prod', 'orders', deps()))
    unmount()
    expect(handles[0].close).toHaveBeenCalledTimes(1)
  })

  it('does not reopen the stream when the consumer rerenders with fresh inline callbacks', () => {
    mockTail()
    const { rerender } = renderHook((d: UseLiveTailDeps) => useLiveTail('prod', 'orders', d), {
      initialProps: deps(),
    })
    expect(sse.tailTopic).toHaveBeenCalledTimes(1)
    rerender(deps())
    rerender(deps())
    expect(sse.tailTopic).toHaveBeenCalledTimes(1)
  })

  it('reopens (closing the old handle) when cluster or topic changes', () => {
    const handles = mockTail()
    const { rerender } = renderHook(({ cluster, topic }) => useLiveTail(cluster, topic, deps()), {
      initialProps: { cluster: 'prod', topic: 'orders' },
    })
    expect(sse.tailTopic).toHaveBeenCalledTimes(1)
    rerender({ cluster: 'prod', topic: 'shipments' })
    expect(sse.tailTopic).toHaveBeenCalledTimes(2)
    expect(handles[0].close).toHaveBeenCalledTimes(1)
  })
})
