import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import { tailTopic } from '../../api/sse'
import type { TimelinePageParams } from '../../api/sse'
import type { MessageOut } from '../../api/types'
import { MessageList } from '../../components/messages/MessageList'
import { Panel } from '../../components/Panel'
import { StalenessChip } from '../../components/StalenessChip'
import { parseFilterQuery } from '../../lib/filterQuery'
import { createTimelineStore } from '../../lib/timelineStore'
import { useTimelinePage } from './useTimelinePage'

const PAGE_LIMIT = 100
// Empty-page contract (see useTimelinePage): a page can come back with zero
// matches but a non-null cursor (nothing in this window passed the filter).
// We keep paginating automatically rather than leaving the user staring at
// an unexplained empty screen — but bound the number of *automatic*
// continuations per user gesture so a sparse/mismatched filter over a huge
// topic can't spin forever. ~20 auto-continues (per the design spec) is the
// safety cap; beyond it we stop and hand control back with an affordance.
const ITERATION_CAP = 20

type Direction = 'back' | 'forward'

export function Timeline({ cluster, topic }: { cluster: string; topic: string }) {
  // The store is mutable (merge-native insert/rows), so it lives in a ref;
  // `bump` forces a re-render whenever an insert changes what rows() would
  // return. `Timeline` is remounted (via a key) whenever cluster/topic
  // changes, so a fresh store per mount is correct — no reset-on-prop-change
  // logic needed here.
  const storeRef = useRef(createTimelineStore())
  const [, bump] = useReducer((c: number) => c + 1, 0)

  const { loadPage, state, cursors } = useTimelinePage(cluster, topic)

  // Task 7 wires live-through-predicate with the EMPTY predicate (matches
  // everything) — the real filter box lands in Task 9 and will replace this
  // with a parsed query's predicate.
  const predicateRef = useRef(parseFilterQuery('').predicate)

  const pendingDirectionRef = useRef<Direction | null>(null)
  const matchedRef = useRef(false)
  const iterationRef = useRef(0)
  const wasLoadingRef = useRef(false)
  const [continueDirection, setContinueDirection] = useState<Direction | null>(null)

  const [live, setLive] = useState(true)
  const [tailErrorText, setTailErrorText] = useState<string | null>(null)
  const tailHandleRef = useRef<{ close: () => void } | null>(null)

  const runPage = useCallback(
    (direction: Direction, params: TimelinePageParams, opts: { resetIteration: boolean }) => {
      if (opts.resetIteration) iterationRef.current = 0
      matchedRef.current = false
      pendingDirectionRef.current = direction
      setContinueDirection(null)
      loadPage(params, (msgs: MessageOut[]) => {
        matchedRef.current = true
        storeRef.current.insert(msgs, direction)
        bump()
      })
    },
    [loadPage],
  )

  // Initial page: latest 100, on mount only.
  useEffect(() => {
    runPage('back', { direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }, { resetIteration: true })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Empty-page auto-continue: fires on the loading:true -> false edge of a
  // page we ourselves issued (tracked via pendingDirectionRef). If that page
  // delivered no matches, isn't an error, and the direction isn't exhausted,
  // keep pulling pages in the same direction until something lands, the
  // topic edge is hit, or the iteration cap is reached.
  useEffect(() => {
    const wasLoading = wasLoadingRef.current
    wasLoadingRef.current = state.loading
    if (!wasLoading || state.loading) return
    const direction = pendingDirectionRef.current
    if (direction === null) return
    pendingDirectionRef.current = null
    if (matchedRef.current) return
    if (state.error) return
    if (state.exhausted[direction]) return
    const cursor = cursors[direction]
    if (cursor === null) return
    if (iterationRef.current >= ITERATION_CAP) {
      setContinueDirection(direction)
      return
    }
    iterationRef.current += 1
    runPage(direction, { direction, limit: PAGE_LIMIT, cursor }, { resetIteration: false })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.loading, state.error, state.exhausted.back, state.exhausted.forward, cursors.back, cursors.forward])

  // Live tail: on by default, ON for the lifetime of the component. An
  // error (server-emitted or transport) stops it for good — Task 8 adds the
  // pause/resume affordance; Task 7 only needs the freeze-in-place pattern.
  useEffect(() => {
    const handle = tailTopic(cluster, topic, {
      onMessage: (m) => {
        if (!predicateRef.current(m)) return
        storeRef.current.insert([m], 'live')
        bump()
      },
      onError: (e) => {
        setLive(false)
        setTailErrorText(`${e.code}: ${e.message}`)
        tailHandleRef.current?.close()
        tailHandleRef.current = null
      },
      onTransportError: () => {
        setLive(false)
        setTailErrorText('connection lost — retrying is manual')
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

  const rows = storeRef.current.rows()

  const loadOlder = () => {
    if (cursors.back === null) return
    runPage('back', { direction: 'back', limit: PAGE_LIMIT, cursor: cursors.back }, { resetIteration: true })
  }
  const loadNewer = () => {
    if (cursors.forward === null) return
    runPage('forward', { direction: 'forward', limit: PAGE_LIMIT, cursor: cursors.forward }, { resetIteration: true })
  }
  const continueScan = () => {
    if (continueDirection === null) return
    const cursor = cursors[continueDirection]
    if (cursor === null) return
    runPage(continueDirection, { direction: continueDirection, limit: PAGE_LIMIT, cursor }, { resetIteration: true })
  }

  const showLoadOlder = cursors.back !== null && !state.exhausted.back
  const showLoadNewer = cursors.forward !== null && !state.exhausted.forward

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">{rows.length} messages</h2>
        {live ? (
          <span className="animate-pulse text-emerald-500">● live</span>
        ) : (
          <StalenessChip asOf={rows[0]?.timestamp_ms ?? null} failed={false} />
        )}
      </div>
      {tailErrorText && (
        <p className="text-sm text-amber-600 dark:text-amber-400">{`live stopped — ${tailErrorText}`}</p>
      )}
      {continueDirection === 'forward' ? (
        <button
          data-testid="continue-scan"
          onClick={continueScan}
          className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
        >
          scanned far, nothing found here — continue
        </button>
      ) : (
        showLoadNewer && (
          <button
            data-testid="load-newer"
            onClick={loadNewer}
            className="w-full rounded border border-zinc-300 py-1 text-xs text-zinc-500 dark:border-zinc-700"
          >
            load newer
          </button>
        )
      )}
      <Panel error={state.error} hasData={rows.length > 0} loading={state.loading && rows.length === 0}>
        <MessageList messages={rows} />
      </Panel>
      {continueDirection === 'back' ? (
        <button
          data-testid="continue-scan"
          onClick={continueScan}
          className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
        >
          scanned far, nothing found here — continue
        </button>
      ) : state.exhausted.back ? (
        <p className="py-2 text-center text-xs text-zinc-400">— beginning of topic —</p>
      ) : (
        showLoadOlder && (
          <button
            data-testid="load-older"
            onClick={loadOlder}
            className="w-full rounded border border-zinc-300 py-1 text-xs text-zinc-500 dark:border-zinc-700"
          >
            load older
          </button>
        )
      )}
    </div>
  )
}
