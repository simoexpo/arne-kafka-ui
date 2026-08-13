import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../../test/fake-event-source'
import { ApiError } from '../../api/client'
import { useTimelinePage } from './useTimelinePage'
import type { MessageOut } from '../../api/types'

beforeEach(() => FakeEventSource.install())
afterEach(() => FakeEventSource.uninstall())

const mk = (offset: number): MessageOut => ({
  partition: 0,
  offset,
  timestamp_ms: 100 + offset,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
})

describe('useTimelinePage', () => {
  it('builds the request url from loadPage params', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
    )
    expect(result.current.state.loading).toBe(true)
  })

  it('delivers matches to onMatches at page_end when under the batch threshold', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    const onMatches = vi.fn()
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, onMatches)
    })
    const es = FakeEventSource.instances[0]
    act(() => {
      es.emit('match', mk(1))
      es.emit('match', mk(2))
    })
    expect(onMatches).not.toHaveBeenCalled()
    act(() => {
      es.emit('page_end', { cursor: 'c', exhausted: false })
    })
    expect(onMatches).toHaveBeenCalledTimes(1)
    expect(onMatches.mock.calls[0][0].map((m: MessageOut) => m.offset)).toEqual([1, 2])
    expect(result.current.state.loading).toBe(false)
  })

  it('flushes automatically at 25 matches, and again at page_end for the remainder', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    const onMatches = vi.fn()
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, onMatches)
    })
    const es = FakeEventSource.instances[0]
    act(() => {
      for (let i = 0; i < 30; i++) es.emit('match', mk(i))
    })
    expect(onMatches).toHaveBeenCalledTimes(1)
    expect(onMatches.mock.calls[0][0]).toHaveLength(25)
    act(() => {
      es.emit('page_end', { cursor: 'c', exhausted: false })
    })
    expect(onMatches).toHaveBeenCalledTimes(2)
    expect(onMatches.mock.calls[1][0]).toHaveLength(5)
  })

  it('page_end updates the direction-matching cursor and exhausted flag', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: 'c-back', exhausted: false })
    })
    expect(result.current.cursors.back).toBe('c-back')
    expect(result.current.cursors.forward).toBeNull()
    expect(result.current.state.exhausted.back).toBe(false)
    expect(result.current.state.exhausted.forward).toBe(false)
  })

  it('a forward page_end never touches the back cursor/exhausted flag', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: 'c-fwd', exhausted: false })
    })
    expect(result.current.cursors.forward).toBe('c-fwd')
    expect(result.current.cursors.back).toBeNull()
    expect(result.current.state.exhausted.forward).toBe(false)
    expect(result.current.state.exhausted.back).toBe(false)
  })

  it('an empty page with a non-null cursor delivers zero matches, no error, and leaves exhausted false', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    const onMatches = vi.fn()
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, cursor: 'c0' }, onMatches)
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: 'c1', exhausted: false })
    })
    expect(result.current.cursors.forward).toBe('c1')
    expect(result.current.state.exhausted.forward).toBe(false)
    expect(result.current.state.error).toBeNull()
    // Never called with matches, but the page resolved cleanly (no crash,
    // no inferred end-of-data from the absence of onMatches calls).
    expect(onMatches).not.toHaveBeenCalled()
  })

  it('exhausted: true with a null cursor is the only end-of-data signal', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: null, exhausted: true })
    })
    expect(result.current.cursors.forward).toBeNull()
    expect(result.current.state.exhausted.forward).toBe(true)
  })

  it('a new loadPage closes the previous in-flight stream', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    const first = FakeEventSource.instances[0]
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
    })
    expect(first.closed).toBe(true)
    expect(FakeEventSource.instances[1].closed).toBe(false)
  })

  it('closes the stream after page_end (terminal)', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: 'c', exhausted: false })
    })
    expect(FakeEventSource.instances.at(-1)!.closed).toBe(true)
  })

  it('closes the stream after error (terminal) and surfaces a structured ApiError', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('error', {
        code: 'kafka_error',
        message: 'boom',
        cluster: 'prod',
        retriable: true,
      })
    })
    expect(FakeEventSource.instances.at(-1)!.closed).toBe(true)
    // A structured ApiError (not a bare string) so consumers like Panel's
    // describeError can classify kafka vs backend vs app errors correctly
    // instead of every page error collapsing into the generic
    // "connection lost" banner.
    expect(result.current.state.error).toBeInstanceOf(ApiError)
    const err = result.current.state.error as ApiError
    expect(err.code).toBe('kafka_error')
    expect(err.message).toBe('boom')
    expect(err.cluster).toBe('prod')
    expect(err.retriable).toBe(true)
    expect(result.current.state.loading).toBe(false)
  })

  it('reports progress updates', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('progress', { scanned: 10, matches: 2, budget: 250000 })
    })
    expect(result.current.state.progress).toEqual({ scanned: 10, matches: 2, budget: 250000 })
  })

  it('cancel() closes the in-flight stream', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      result.current.cancel()
    })
    expect(FakeEventSource.instances[0].closed).toBe(true)
  })

  it('unmount cleans up any in-flight stream', () => {
    const { result, unmount } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    unmount()
    expect(FakeEventSource.instances[0].closed).toBe(true)
  })

  it('uses the established transport-error wording, as a plain Error (legitimately connection-lost)', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].fireTransportError()
    })
    expect(result.current.state.error).not.toBeInstanceOf(ApiError)
    expect(result.current.state.error).toBeInstanceOf(Error)
    expect(result.current.state.error?.message).toBe('connection lost — retrying is manual')
    expect(result.current.state.loading).toBe(false)
  })

  it('loadPage resets exhausted for its own direction at the start (anchor jumps clear stale exhaustion)', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: null, exhausted: true })
    })
    expect(result.current.state.exhausted.forward).toBe(true)

    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'latest' }, vi.fn())
    })
    expect(result.current.state.exhausted.forward).toBe(false)
  })

  it('a loadPage started synchronously from onMatches (during the outer page_end flush) is not stomped by the outer trailing state updates', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, (msgs) => {
        if (msgs.some((m) => m.offset === 1)) {
          result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
        }
      })
    })
    const firstEs = FakeEventSource.instances[0]
    act(() => {
      firstEs.emit('match', mk(1))
      firstEs.emit('page_end', { cursor: 'c-first', exhausted: false })
    })
    // The second loadPage (started inside the first onMatches call) must have
    // opened its own stream, and its loading:true must survive the first
    // page_end handler's own trailing loading:false update.
    const secondEs = FakeEventSource.instances[1]
    expect(secondEs).toBeDefined()
    expect(result.current.state.loading).toBe(true)
    expect(result.current.cursors.back).toBe('c-first')

    act(() => {
      secondEs.emit('page_end', { cursor: 'c-second', exhausted: true })
    })
    expect(result.current.state.loading).toBe(false)
    expect(result.current.cursors.forward).toBe('c-second')
    expect(result.current.state.exhausted.forward).toBe(true)
  })

  it('reset() clears both cursors, exhausted flags, error, progress, loading, and closes any in-flight stream', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    act(() => {
      FakeEventSource.instances[0].emit('page_end', { cursor: 'c-back', exhausted: false })
    })
    expect(result.current.cursors.back).toBe('c-back')

    // Start a second, still in-flight page (forward), then reset mid-flight.
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, cursor: 'c-back' }, vi.fn())
    })
    expect(result.current.state.loading).toBe(true)
    const inFlight = FakeEventSource.instances.at(-1)!

    act(() => {
      result.current.reset()
    })
    expect(result.current.cursors).toEqual({ back: null, forward: null })
    expect(result.current.state.exhausted).toEqual({ back: false, forward: false })
    expect(result.current.state.error).toBeNull()
    expect(result.current.state.progress).toBeNull()
    expect(result.current.state.loading).toBe(false)
    expect(inFlight.closed).toBe(true)
  })

  it('reset() kills the in-flight generation: a stale page_end after reset is ignored', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, vi.fn())
    })
    const es = FakeEventSource.instances[0]
    act(() => {
      result.current.reset()
    })
    const stateBefore = result.current.state
    const cursorsBefore = result.current.cursors
    act(() => {
      es.emit('page_end', { cursor: 'ignored', exhausted: true })
    })
    expect(result.current.state).toBe(stateBefore)
    expect(result.current.cursors).toBe(cursorsBefore)
  })

  it('events on a superseded (closed) stream are ignored, even if the transport still fires them', () => {
    const { result } = renderHook(() => useTimelinePage('prod', 'orders'))
    const onMatchesA = vi.fn()
    act(() => {
      result.current.loadPage({ direction: 'back', limit: 100, anchor: 'latest' }, onMatchesA)
    })
    const esA = FakeEventSource.instances[0]
    act(() => {
      result.current.loadPage({ direction: 'forward', limit: 100, anchor: 'beginning' }, vi.fn())
    })
    expect(esA.closed).toBe(true)

    const stateBefore = result.current.state
    const cursorsBefore = result.current.cursors
    act(() => {
      esA.emit('match', mk(1))
      esA.emit('page_end', { cursor: 'stale-cursor', exhausted: true })
    })
    expect(onMatchesA).not.toHaveBeenCalled()
    expect(result.current.state).toBe(stateBefore)
    expect(result.current.cursors).toBe(cursorsBefore)
  })
})
