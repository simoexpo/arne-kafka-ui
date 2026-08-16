import { useEffect, useRef, useState } from 'react'
import type { RefObject } from 'react'
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
export function useLiveTail(cluster: string, topic: string, deps: UseLiveTailDeps): { alive: boolean; errorText: string | null } {
  const [alive, setAlive] = useState(true)
  const [errorText, setErrorText] = useState<string | null>(null)
  const tailHandleRef = useRef<{ close: () => void } | null>(null)

  useEffect(() => {
    const handle = tailTopic(cluster, topic, {
      onMessage: (m) => {
        if (!deps.predicateRef.current(m)) return
        if (deps.attachedRef.current && deps.pauseReasonRef.current === 'none') {
          deps.onLiveInsert(m)
        } else {
          deps.onBuffer(m)
        }
        deps.onChange()
      },
      onError: (e) => {
        setAlive(false)
        setErrorText(`${e.code}: ${e.message}`)
        tailHandleRef.current?.close()
        tailHandleRef.current = null
      },
      onTransportError: () => {
        setAlive(false)
        setErrorText('connection lost — retrying is manual')
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

  return { alive, errorText }
}
