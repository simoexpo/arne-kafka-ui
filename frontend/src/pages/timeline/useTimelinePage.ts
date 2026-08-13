import { useCallback, useEffect, useRef, useState } from 'react'
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
  error: string | null
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

  const cancel = useCallback(() => {
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

      let batch: MessageOut[] = []
      const flush = () => {
        if (batch.length === 0) return
        const delivered = batch
        batch = []
        onMatches(delivered)
      }

      setState((prev) => ({ ...prev, loading: true, progress: null, error: null }))

      const handle = timelinePage(cluster, topic, params, {
        onMatch: (m) => {
          batch.push(m)
          if (batch.length >= BATCH_SIZE) flush()
        },
        onProgress: (p) => setState((prev) => ({ ...prev, progress: p })),
        onPageEnd: (cursor, exhausted) => {
          // Terminal: close before anything else, so no late transport
          // event (or browser auto-reconnect) can resurrect this stream —
          // the exact reconnect-loop failure mode v1 hit.
          handle.close()
          handleRef.current = null
          flush()
          setCursors((prev) => ({ ...prev, [params.direction]: cursor }))
          setState((prev) => ({
            ...prev,
            loading: false,
            exhausted: { ...prev.exhausted, [params.direction]: exhausted },
          }))
        },
        onError: (e) => {
          handle.close()
          handleRef.current = null
          flush()
          setState((prev) => ({ ...prev, loading: false, error: e.message }))
        },
        onTransportError: () => {
          // Not a server-emitted terminal event, but EventSource's default
          // behavior is to auto-reconnect — exactly the v1 reconnect-loop
          // bug. A page load is a one-shot request/response over SSE, so we
          // treat any transport failure as terminal too rather than let the
          // browser retry into a zombie stream.
          handle.close()
          handleRef.current = null
          flush()
          setState((prev) => ({ ...prev, loading: false, error: 'connection lost' }))
        },
      })
      handleRef.current = handle
    },
    [cluster, topic],
  )

  return { loadPage, state, cursors, cancel }
}
