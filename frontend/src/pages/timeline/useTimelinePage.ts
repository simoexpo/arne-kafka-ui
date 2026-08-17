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
  // real connection-to-Arne loss. Server-emitted `error` events
  // become an ApiError carrying that structure; a transport failure
  // (EventSource's onerror) legitimately IS connection-lost and is a plain
  // Error with the established wording.
  error: ApiError | Error | null
}

export interface UseTimelinePage {
  /**
   * `onPageEnd`, if given, fires SYNCHRONOUSLY inside the very same event
   * handler that processes the SSE `page_end` event — the same tick as the
   * internal `state.loading` update, not a render later —
   * a caller that commits a page's rows to its own store needs that
   * mutation to land in the SAME render as `state.loading` flipping false,
   * or ref-based DOM handles reading the store — e.g. a scroll-position
   * reposition — see stale, pre-commit content for exactly one render).
   * Receives the raw `cursor`/`exhausted` straight off the wire — never
   * called on a page error or transport error (there is no cursor to
   * report; the caller's own error handling takes over instead).
   */
  loadPage(
    params: TimelinePageParams,
    onMatches: (m: MessageOut[]) => void,
    onPageEnd?: (cursor: string | null, exhausted: boolean) => void,
  ): void
  state: TimelineState
  cancel(): void
  // Full viewport reset for a jump: unlike loadPage (which only clears the
  // exhaustion flag for the direction it's (re)loading), a jump invalidates
  // BOTH directions at once — the old exhausted flags describe a window the
  // user is leaving entirely, not one they're paging within. Closing/
  // bumping the generation here (same as cancel()) additionally guarantees a
  // stale in-flight page from before the jump can never resurrect stale
  // state after the jump's own loadPage has started.
  reset(): void
}

export function useTimelinePage(cluster: string, topic: string): UseTimelinePage {
  const [state, setState] = useState<TimelineState>({
    loading: false,
    progress: null,
    exhausted: { back: false, forward: false },
    error: null,
  })
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
    // A bare cancel (unlike reset()) is meant to stop an in-flight page
    // while leaving whatever already loaded intact — exhausted flags and the
    // caller's own store are untouched. But `loading` must still flip back
    // to false here: no further terminal event will ever arrive for a
    // generation that's just been superseded, so nothing else would ever
    // clear it. (Timeline's own Cancel affordance actually gates on
    // `gestureRunning`, not `state.loading` directly — see its own comment
    // — but `loading` staying stuck true would still be a lie about this
    // hook's own state.)
    setState((prev) => (prev.loading ? { ...prev, loading: false } : prev))
  }, [])

  // Unmount cleanup: never leave a stream open past the component's life.
  useEffect(() => cancel, [cancel])

  const reset = useCallback(() => {
    cancel()
    setState({ loading: false, progress: null, exhausted: { back: false, forward: false }, error: null })
  }, [cancel])

  const loadPage = useCallback(
    (
      params: TimelinePageParams,
      onMatches: (m: MessageOut[]) => void,
      onPageEnd?: (cursor: string | null, exhausted: boolean) => void,
    ) => {
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
      // for the direction being (re)loaded — a viewport jump relies
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
          // loading:true survive — not get overwritten by this handler's
          // own trailing updates running after the fact. The
          // generation guard above additionally protects against the
          // (structurally impossible here, but defensive) case where this
          // handler fires after being superseded.
          setState((prev) => ({
            ...prev,
            loading: false,
            exhausted: { ...prev.exhausted, [params.direction]: exhausted },
          }))
          flush()
          // Re-check the generation ONE MORE TIME,
          // immediately before this final call — `flush()` just invoked the
          // caller's `onMatches` synchronously, and a caller that starts a
          // NEW `loadPage` from inside it (e.g. auto-advancing to the next
          // page) has already bumped `generationRef` by the time control
          // returns here. Without this second check, a superseded
          // generation's `onPageEnd` would still fire — handing a caller
          // (e.g. Timeline.tsx's synchronous store commit) a cursor/
          // direction pairing that's no longer the current request.
          if (generationRef.current !== myGen) return
          // After the internal state updates + the final flush (so a
          // trailing partial batch has already reached the caller's
          // `onMatches`) — see this callback's own doc comment for why this
          // must be synchronous rather than observed via `state`.
          onPageEnd?.(cursor, exhausted)
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
          // Arne itself — a plain Error, not an ApiError, so
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

  return { loadPage, state, cancel, reset }
}
