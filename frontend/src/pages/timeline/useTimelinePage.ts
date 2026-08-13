import { useCallback, useEffect, useRef, useState } from 'react'
import { ApiError } from '../../api/client'
import { timelinePage } from '../../api/sse'
import type { TimelinePageParams, TimelineProgress } from '../../api/sse'
import type { MessageOut } from '../../api/types'

// Deliver matches in batches rather than one state/callback trip per
// message: a filtered scan (or a deep 'back' page) can emit hundreds of
// matches in a burst, and re-rendering per-message would thrash the
// virtualized list for no visible benefit. We don't need per-animation-frame
// scheduling (overkill for a single page load) — a simple fixed-size
// threshold bounds the worst case (at most BATCH_SIZE - 1 renders "wasted"
// on partial batches) while the final partial batch always flushes at the
// terminal event (page_end/error), so nothing is ever silently held back.
const BATCH_SIZE = 25

interface TimelineState {
  loading: boolean
  progress: TimelineProgress | null
  exhausted: { back: boolean; forward: boolean }
  // A structured error, not a bare string: consumers (Panel's describeError)
  // need `code`/`cluster`/`retriable` to tell a kafka-side failure from a
  // real connection-to-Betrachtung loss. Server-emitted `error` events
  // become an ApiError carrying that structure; a transport failure
  // (EventSource's onerror) legitimately IS connection-lost and is a plain
  // Error with the established wording.
  error: ApiError | Error | null
}

interface TimelineCursors {
  back: string | null
  forward: string | null
}

export interface UseTimelinePage {
  loadPage(params: TimelinePageParams, onMatches: (m: MessageOut[]) => void): void
  state: TimelineState
  cursors: TimelineCursors
  cancel(): void
}

export function useTimelinePage(cluster: string, topic: string): UseTimelinePage {
  const [state, setState] = useState<TimelineState>({
    loading: false,
    progress: null,
    exhausted: { back: false, forward: false },
    error: null,
  })
  const [cursors, setCursors] = useState<TimelineCursors>({ back: null, forward: null })
  const handleRef = useRef<{ close: () => void } | null>(null)
  // Bumped by every loadPage (and by cancel) — each page's handler closures
  // capture the generation they were created under and no-op once it's
  // stale. This is the authoritative "am I still the current stream" check:
  // FakeEventSource (and, in principle, a real transport under a pathological
  // edge case) can still fire events after close(), so closing the handle
  // alone isn't a sufficient guard against a superseded stream's events
  // reaching state.
  const generationRef = useRef(0)

  const cancel = useCallback(() => {
    generationRef.current += 1
    handleRef.current?.close()
    handleRef.current = null
  }, [])

  // Unmount cleanup: never leave a stream open past the component's life.
  useEffect(() => cancel, [cancel])

  const loadPage = useCallback(
    (params: TimelinePageParams, onMatches: (m: MessageOut[]) => void) => {
      // One in-flight page max: starting a new one always closes whatever
      // was running, terminal event or not.
      handleRef.current?.close()
      handleRef.current = null
      const myGen = ++generationRef.current

      let batch: MessageOut[] = []
      const flush = () => {
        if (batch.length === 0) return
        const delivered = batch
        batch = []
        onMatches(delivered)
      }

      // Anchor jumps (and fresh cursor pages) must clear stale exhaustion
      // for the direction being (re)loaded — Task 7's viewport jumps rely
      // on this so a previously-exhausted direction can become
      // not-exhausted again after e.g. "jump to beginning".
      setState((prev) => ({
        ...prev,
        loading: true,
        progress: null,
        error: null,
        exhausted: { ...prev.exhausted, [params.direction]: false },
      }))

      const handle = timelinePage(cluster, topic, params, {
        onMatch: (m) => {
          if (generationRef.current !== myGen) return
          batch.push(m)
          if (batch.length >= BATCH_SIZE) flush()
        },
        onProgress: (p) => {
          if (generationRef.current !== myGen) return
          setState((prev) => ({ ...prev, progress: p }))
        },
        onPageEnd: (cursor, exhausted) => {
          if (generationRef.current !== myGen) return
          // Terminal: close before anything else, so no late transport
          // event (or browser auto-reconnect) can resurrect this stream —
          // the exact reconnect-loop failure mode v1 hit.
          handle.close()
          handleRef.current = null
          // Finalize ALL of this page's state BEFORE flushing matches to
          // the caller: flush() invokes the caller's onMatches synchronously,
          // and a caller that starts another loadPage from inside onMatches
          // (e.g. auto-advancing to the next page) must see its own
          // loading:true/cursors survive — not get overwritten by this
          // handler's own trailing updates running after the fact. The
          // generation guard above additionally protects against the
          // (structurally impossible here, but defensive) case where this
          // handler fires after being superseded.
          setCursors((prev) => ({ ...prev, [params.direction]: cursor }))
          setState((prev) => ({
            ...prev,
            loading: false,
            exhausted: { ...prev.exhausted, [params.direction]: exhausted },
          }))
          flush()
        },
        onError: (e) => {
          if (generationRef.current !== myGen) return
          handle.close()
          handleRef.current = null
          const error = new ApiError(0, e.code, e.message, e.cluster ?? null, e.retriable ?? false)
          setState((prev) => ({ ...prev, loading: false, error }))
          flush()
        },
        onTransportError: () => {
          if (generationRef.current !== myGen) return
          // Not a server-emitted terminal event, but EventSource's default
          // behavior is to auto-reconnect — exactly the v1 reconnect-loop
          // bug. A page load is a one-shot request/response over SSE, so we
          // treat any transport failure as terminal too rather than let the
          // browser retry into a zombie stream. Unlike a server-emitted
          // `error` event, this one is genuinely a lost connection to
          // Betrachtung itself — a plain Error, not an ApiError, so
          // describeError's generic connection-lost banner is exactly right.
          handle.close()
          handleRef.current = null
          setState((prev) => ({ ...prev, loading: false, error: new Error('connection lost — retrying is manual') }))
          flush()
        },
      })
      handleRef.current = handle
    },
    [cluster, topic],
  )

  return { loadPage, state, cursors, cancel }
}
