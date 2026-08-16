import { useEffect, useRef, useState } from 'react'
import type { RefObject } from 'react'
import { ApiError } from '../../api/client'
import { tailTopic } from '../../api/sse'
import type { MessageOut } from '../../api/types'
import type { PauseReason } from '../../lib/timeline/model'

export interface UseLiveTailDeps {
  // Refs, never plain values: the mount effect below runs once per
  // [cluster, topic] and must read each message's routing decision as of
  // the moment it arrives, not as of mount.
  predicateRef: RefObject<(m: MessageOut) => boolean>
  attachedRef: RefObject<boolean>
  pauseReasonRef: RefObject<PauseReason>
  // True while at least one message row is expanded (design spec v1.7,
  // "Inspection pause"): forces buffering even in the ordinarily "merge
  // live" case (attached, unpaused) — an open inspection is a stronger
  // "don't move things" signal than either of those.
  inspectingRef: RefObject<boolean>
  onLiveInsert: (m: MessageOut) => void
  onBuffer: (m: MessageOut) => void
  onChange: () => void
}

/**
 * Owns the tail SSE stream for the component's lifetime: opens once per
 * [cluster, topic], closes on unmount or on any error (server or
 * transport), and routes each message to onLiveInsert or onBuffer per
 * attachedRef/pauseReasonRef. Callers must pass stable (memoized)
 * callbacks — the effect captures `deps` at mount and does not re-run on
 * every render, so a fresh inline callback each render is simply never
 * seen.
 */
export function useLiveTail(
  cluster: string,
  topic: string,
  deps: UseLiveTailDeps,
): { alive: boolean; error: ApiError | Error | null } {
  const [alive, setAlive] = useState(true)
  // A structured ApiError (or, for a transport failure, a plain Error)
  // rather than a pre-formatted string — same shape as useTimelinePage's
  // `state.error` — so the consumer renders it through `describeError` for
  // product-voice wording instead of the raw wire code/message.
  const [error, setError] = useState<ApiError | Error | null>(null)
  const tailHandleRef = useRef<{ close: () => void } | null>(null)

  useEffect(() => {
    const handle = tailTopic(cluster, topic, {
      onMessage: (m) => {
        if (!deps.predicateRef.current(m)) return
        if (deps.attachedRef.current && deps.pauseReasonRef.current === 'none' && !deps.inspectingRef.current) {
          deps.onLiveInsert(m)
        } else {
          deps.onBuffer(m)
        }
        deps.onChange()
      },
      onError: (e) => {
        setAlive(false)
        setError(new ApiError(0, e.code, e.message, e.cluster ?? null, e.retriable ?? false))
        tailHandleRef.current?.close()
        tailHandleRef.current = null
      },
      onTransportError: () => {
        setAlive(false)
        // A plain Error always renders under describeError's
        // connection-lost headline, so the reason carries only what that
        // headline doesn't already say — no "connection lost" prefix here,
        // or the composed banner stutters it twice.
        setError(new Error('retrying is manual'))
        tailHandleRef.current?.close()
        tailHandleRef.current = null
      },
    })
    tailHandleRef.current = handle
    return () => {
      tailHandleRef.current?.close()
      tailHandleRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cluster, topic])

  return { alive, error }
}
