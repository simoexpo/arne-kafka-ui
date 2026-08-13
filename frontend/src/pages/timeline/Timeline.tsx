import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import { tailTopic } from '../../api/sse'
import type { TimelinePageParams } from '../../api/sse'
import type { MessageOut } from '../../api/types'
import { MessageList } from '../../components/messages/MessageList'
import { Panel } from '../../components/Panel'
import { StalenessChip } from '../../components/StalenessChip'
import { parseFilterQuery } from '../../lib/filterQuery'
import { createTimelineStore } from '../../lib/timelineStore'
import { JumpControl, type JumpTarget } from './JumpControl'
import { LivePill, PlayPauseToggle } from './LivePill'
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
// Auto-pause buffer cap (see design spec): once paused (auto or explicit),
// live messages that pass the predicate accumulate here instead of the
// store. Capped at 500 — beyond that we drop the OLDEST buffered entry per
// arrival and switch the pill to an honest "500+ · older dropped" label
// rather than a raw (and increasingly meaningless) count.
const BUFFER_CAP = 500
// Scroll offset below which the viewport counts as "pinned to top" —
// roughly half a row, per the design spec's threshold.
const TOP_PIN_THRESHOLD = 20

type Direction = 'back' | 'forward'
// 'none': live inserts straight into the store. 'auto': paused by scrolling
// off the top — returning to the top resumes. 'explicit': paused via the
// play/pause pill — overrides top-pinning, only the pill itself resumes it.
type PauseReason = 'none' | 'auto' | 'explicit'

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

  // Pause machinery: pauseReasonRef is the source of truth (read at render
  // time, like storeRef), mutated directly and paired with `bump()` so a
  // change is visible next render — same pattern as the store itself,
  // avoiding stale closures in the tail effect (which only runs once, on
  // [cluster, topic]). bufferRef holds live messages held back while paused;
  // bufferOverflowRef flips permanently (until the next flush) once the
  // buffer has dropped an oldest entry, so the pill can show the honest
  // "500+ · older dropped" label instead of a raw, increasingly meaningless
  // count.
  const pauseReasonRef = useRef<PauseReason>('none')
  const bufferRef = useRef<MessageOut[]>([])
  const bufferOverflowRef = useRef(false)

  const flushBuffer = useCallback(() => {
    if (bufferRef.current.length > 0) {
      storeRef.current.insert(bufferRef.current, 'live')
    }
    bufferRef.current = []
    bufferOverflowRef.current = false
  }, [])

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
        if (pauseReasonRef.current === 'none') {
          storeRef.current.insert([m], 'live')
        } else {
          bufferRef.current.push(m)
          if (bufferRef.current.length > BUFFER_CAP) {
            bufferRef.current.shift() // drop the OLDEST buffered entry
            bufferOverflowRef.current = true
          }
        }
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

  // Wired directly onto MessageList's real scroll element (scroll events
  // don't bubble, so this can't live on a wrapper). Scrolling off the top
  // auto-pauses (only from 'none' — 'auto'/'explicit' are already paused).
  // Returning to the top auto-flushes + resumes, but ONLY if the pause was
  // automatic: an explicit pause overrides top-pinning entirely.
  const handleScroll = (scrollTop: number) => {
    const pinnedTop = scrollTop < TOP_PIN_THRESHOLD
    if (pinnedTop) {
      if (pauseReasonRef.current === 'auto') {
        flushBuffer()
        pauseReasonRef.current = 'none'
        bump()
      }
    } else if (pauseReasonRef.current === 'none') {
      pauseReasonRef.current = 'auto'
      bump()
    }
  }

  // Clicking the "▲ n new" pill always flushes; it only resumes when the
  // pause was automatic — an explicit pause stays paused (the pill just
  // clears what had built up so far).
  const handlePillClick = () => {
    flushBuffer()
    if (pauseReasonRef.current === 'auto') pauseReasonRef.current = 'none'
    bump()
  }

  // The explicit play/pause toggle: pausing always forces 'explicit'
  // (overriding whatever auto/none state was in effect); un-pausing always
  // flushes + resumes fully, regardless of how it got paused.
  const handlePlayPauseToggle = () => {
    if (pauseReasonRef.current === 'none') {
      pauseReasonRef.current = 'explicit'
    } else {
      flushBuffer()
      pauseReasonRef.current = 'none'
    }
    bump()
  }

  // A jump repositions the viewport: the current window is no longer
  // meaningful, so the store and any buffered live messages are dropped
  // outright (not flushed — they belong to the OLD viewport). 'now' is the
  // only jump that lands pinned at the true top, so it's the only one that
  // resumes live; the others land mid-topic or at the tail end, where an
  // unpaused live prepend would immediately scroll the user's new anchor
  // out of view — so they enter paused-auto (the pill counts, top-pinning
  // still resumes normally from there on).
  const handleJump = (target: JumpTarget) => {
    storeRef.current.clear()
    bufferRef.current = []
    bufferOverflowRef.current = false
    switch (target.kind) {
      case 'now':
        pauseReasonRef.current = 'none'
        runPage('back', { direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }, { resetIteration: true })
        break
      case 'beginning':
        pauseReasonRef.current = 'auto'
        runPage('forward', { direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }, { resetIteration: true })
        break
      case 'offset':
        pauseReasonRef.current = 'auto'
        runPage(
          'back',
          { direction: 'back', limit: PAGE_LIMIT, anchor: 'offset', partition: target.partition, offset: target.offset },
          { resetIteration: true },
        )
        break
      case 'timestamp':
        pauseReasonRef.current = 'auto'
        runPage('back', { direction: 'back', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: target.ts_ms }, { resetIteration: true })
        break
    }
    bump()
  }

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
  const paused = pauseReasonRef.current !== 'none'

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">{rows.length} messages</h2>
        <div className="flex items-center gap-2">
          <LivePill count={bufferRef.current.length} capped={bufferOverflowRef.current} onClick={handlePillClick} />
          {live ? (
            <>
              {!paused && <span className="animate-pulse text-emerald-500">● live</span>}
              <PlayPauseToggle paused={paused} onClick={handlePlayPauseToggle} />
            </>
          ) : (
            <StalenessChip asOf={rows[0]?.timestamp_ms ?? null} failed={false} />
          )}
        </div>
      </div>
      <JumpControl onJump={handleJump} />
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
        <MessageList messages={rows} onScroll={handleScroll} />
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
