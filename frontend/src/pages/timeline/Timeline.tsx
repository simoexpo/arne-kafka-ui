import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import { tailTopic } from '../../api/sse'
import type { TimelinePageParams } from '../../api/sse'
import type { MessageOut } from '../../api/types'
import { FilterInput } from '../../components/FilterInput'
import { MessageList } from '../../components/messages/MessageList'
import type { MessageListHandle } from '../../components/messages/MessageList'
import { Panel } from '../../components/Panel'
import { StalenessChip } from '../../components/StalenessChip'
import { parseFilterQuery, type FilterQueryApi } from '../../lib/filterQuery'
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

  const { loadPage, state, cursors, reset, cancel } = useTimelinePage(cluster, topic)

  // The live-tail predicate: starts as match-all, replaced immediately (no
  // debounce) whenever the debounced filter box settles — see applyFilter.
  const predicateRef = useRef(parseFilterQuery('').predicate)
  // The currently active filter's server-side params (null = unfiltered).
  // A page's cursor does NOT remember the filter it was scanned under (the
  // backend's Cursor only encodes per-partition positions), so every
  // subsequent page request — load-older/newer, a jump, or an
  // auto-continued page — must resend it via withFilter for the filter to
  // stay in effect until the user changes or clears it.
  const activeFilterApiRef = useRef<FilterQueryApi | null>(null)
  // Which anchor a settled filter change re-reads from. Kept deliberately
  // simple per the brief: 'default' (back/latest) covers every case except
  // right after a jump-to-beginning, which re-anchors forward/beginning so
  // refiltering while viewing the start of the topic doesn't silently snap
  // the user back to the live tail. offset/timestamp jumps fall back to
  // 'default', same as no jump at all.
  const anchorContextRef = useRef<'default' | 'beginning'>('default')

  const pendingDirectionRef = useRef<Direction | null>(null)
  const matchedRef = useRef(false)
  const iterationRef = useRef(0)
  const wasLoadingRef = useRef(false)
  const [continueDirection, setContinueDirection] = useState<Direction | null>(null)
  // Running totals across an entire gesture: a user-issued page plus every
  // auto-continued empty page that follows it. A single page's own
  // `state.progress` resets to null at the start of each new page, so
  // without this ref the progress row / cap affordance would misleadingly
  // shrink back to a tiny number every time an empty page silently
  // auto-continues, instead of showing a true running total. Only ever
  // incremented with a PRIOR (already-ended) page's final numbers — see the
  // auto-continue effect and the render-time formula below, which together
  // keep this from double-counting the page currently in flight.
  const gestureScannedRef = useRef(0)
  const gestureMatchesRef = useRef(0)

  // Viewport repositioning after a jump: 'now'/'offset'/'timestamp' land
  // looking at the top of the new window; 'beginning' lands at the start of
  // history looking forward, so the bottom (oldest-visible) edge is what
  // matters there. Tracked separately from pendingDirectionRef/wasLoadingRef
  // (which drive the empty-page auto-continue) via its own "was loading"
  // edge-detector, since the two concerns are independent and a jump's
  // first page can itself be an empty page that auto-continues further.
  const listRef = useRef<MessageListHandle>(null)
  const pendingScrollEdgeRef = useRef<'top' | 'bottom' | null>(null)
  const scrollWasLoadingRef = useRef(false)

  const [live, setLive] = useState(true)
  const [tailErrorText, setTailErrorText] = useState<string | null>(null)
  const tailHandleRef = useRef<{ close: () => void } | null>(null)

  // Pause machinery: pauseReasonRef is the source of truth (read at render
  // time, like storeRef), mutated directly and paired with `bump()` so a
  // change is visible next render — same pattern as the store itself,
  // avoiding stale closures in the tail effect (which only runs once, on
  // [cluster, topic]). bufferRef holds the actual live messages held back
  // while paused, capped at BUFFER_CAP (oldest dropped first) so flushing
  // never inserts an unbounded backlog. bufferReceivedRef is a SEPARATE,
  // uncapped counter — the pill keeps counting honestly past the cap
  // (`"{n} new"`, n growing past 500) rather than freezing at a fixed
  // string; bufferOverflowRef flips permanently (until the next flush) once
  // the buffer has actually started dropping entries, which only gates the
  // "· older dropped" suffix.
  const pauseReasonRef = useRef<PauseReason>('none')
  const bufferRef = useRef<MessageOut[]>([])
  const bufferReceivedRef = useRef(0)
  const bufferOverflowRef = useRef(false)

  const flushBuffer = useCallback(() => {
    if (bufferRef.current.length > 0) {
      storeRef.current.insert(bufferRef.current, 'live')
    }
    bufferRef.current = []
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
  }, [])

  const runPage = useCallback(
    (
      direction: Direction,
      params: TimelinePageParams,
      // resetIteration: restarts the auto-continue cap counter (every fresh
      // page request needs this — a load-older click, a jump, or the
      // continue-scan button all get a new run at the cap). resetGesture:
      // zeroes the running scanned/matches totals; defaults to
      // resetIteration's value, EXCEPT continueScan explicitly passes
      // `false` — clicking "continue" resumes the SAME gesture the cap
      // interrupted, so the totals the user already saw must keep growing,
      // never snap back to 0. Every other resetIteration:true call site
      // (mount, applyFilter, loadOlder/loadNewer, jumps) is a genuinely NEW
      // gesture and resets both together.
      opts: { resetIteration: boolean; resetGesture?: boolean },
    ) => {
      if (opts.resetIteration) iterationRef.current = 0
      if (opts.resetGesture ?? opts.resetIteration) {
        gestureScannedRef.current = 0
        gestureMatchesRef.current = 0
      }
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

  // Resends the currently active filter's server-side params (a no-op when
  // unfiltered) onto any page request — see activeFilterApiRef.
  const withFilter = useCallback((params: TimelinePageParams): TimelinePageParams => {
    const api = activeFilterApiRef.current
    if (!api) return params
    return { ...params, filter: api.filter, q: api.q, ...(api.path !== undefined ? { path: api.path } : {}) }
  }, [])

  // The anchor a fresh (non-cursor) page request starts from, given the
  // current anchor context — see anchorContextRef.
  const baseAnchorParams = useCallback(
    (): TimelinePageParams =>
      anchorContextRef.current === 'beginning'
        ? { direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }
        : { direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' },
    [],
  )

  // A settled filter change (see the debounce effect below): switches the
  // live-tail predicate immediately, drops the current viewport (store +
  // any buffered live messages — same as a jump, since the loaded window no
  // longer reflects the new filter), resets both pagination directions, and
  // reloads the first page from the current anchor context with the parsed
  // filter attached.
  const applyFilter = useCallback(
    (text: string) => {
      const parsed = parseFilterQuery(text)
      predicateRef.current = parsed.predicate
      activeFilterApiRef.current = parsed.api
      storeRef.current.clear()
      bufferRef.current = []
      bufferReceivedRef.current = 0
      bufferOverflowRef.current = false
      reset()
      const base = baseAnchorParams()
      runPage(base.direction, withFilter(base), { resetIteration: true })
      bump()
    },
    [reset, runPage, baseAnchorParams, withFilter],
  )

  const [filterText, setFilterText] = useState('')
  // Skip the very first run: the mount effect below already loads the
  // initial (unfiltered) page — without this guard, filterText's initial
  // '' value would debounce into a redundant clear+reload 500ms after every
  // mount.
  const isFirstFilterEffectRef = useRef(true)
  useEffect(() => {
    if (isFirstFilterEffectRef.current) {
      isFirstFilterEffectRef.current = false
      return
    }
    const id = setTimeout(() => applyFilter(filterText), 500)
    return () => clearTimeout(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterText])

  // Initial page: latest 100, on mount only.
  useEffect(() => {
    runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }), { resetIteration: true })
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
    // Fold this just-ended page's final progress into the gesture-wide
    // running total BEFORE deciding whether to loop or cap — a filtered
    // scan's progress resets per page, so without this the cap
    // affordance/progress row would understate how much was actually
    // scanned across every auto-continued page in this gesture. This is the
    // ONLY place gestureScannedRef/MatchesRef are incremented, and only
    // ever with a page that has already ended — see the render-time
    // formula, which adds the CURRENT in-flight page's progress
    // separately (and only while still loading) to avoid double-counting.
    gestureScannedRef.current += state.progress?.scanned ?? 0
    gestureMatchesRef.current += state.progress?.matches ?? 0
    if (iterationRef.current >= ITERATION_CAP) {
      setContinueDirection(direction)
      return
    }
    iterationRef.current += 1
    runPage(direction, withFilter({ direction, limit: PAGE_LIMIT, cursor }), { resetIteration: false })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.loading, state.error, state.exhausted.back, state.exhausted.forward, cursors.back, cursors.forward])

  // Jump viewport repositioning: fires on the very first loading:true ->
  // false edge after a jump set pendingScrollEdgeRef (handleJump), whether
  // or not that first page matched anything — a jump's job is to reposition
  // the viewport for the new anchor, which doesn't depend on rows already
  // being present. Cleared after firing once, so a subsequent empty-page
  // auto-continue for the same jump doesn't re-trigger it.
  useEffect(() => {
    const wasLoading = scrollWasLoadingRef.current
    scrollWasLoadingRef.current = state.loading
    if (!wasLoading || state.loading) return
    const edge = pendingScrollEdgeRef.current
    if (edge === null) return
    pendingScrollEdgeRef.current = null
    listRef.current?.scrollToEdge(edge)
  }, [state.loading])

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
          bufferReceivedRef.current += 1
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
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
    // A jump invalidates BOTH pagination directions, not just the one being
    // (re)loaded: the old cursors describe a window the user is leaving
    // entirely. reset() clears both cursors/exhausted flags (and kills any
    // in-flight page) synchronously, BEFORE runPage starts the new one —
    // without this, a stale opposite-direction cursor from before the jump
    // could leave load-older/load-newer visible and pointing at the wrong
    // window.
    reset()
    // 'beginning' lands at the start of history looking forward — the
    // bottom (oldest-visible) edge is the meaningful anchor there. Every
    // other jump lands looking at the top of its new window.
    pendingScrollEdgeRef.current = target.kind === 'beginning' ? 'bottom' : 'top'
    // A jump re-anchors where a subsequent filter settle will re-read from
    // (see anchorContextRef/applyFilter): only 'beginning' is tracked as
    // its own anchor context, every other jump falls back to 'default'.
    anchorContextRef.current = target.kind === 'beginning' ? 'beginning' : 'default'
    switch (target.kind) {
      case 'now':
        pauseReasonRef.current = 'none'
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }), { resetIteration: true })
        break
      case 'beginning':
        pauseReasonRef.current = 'auto'
        runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }), { resetIteration: true })
        break
      case 'offset':
        pauseReasonRef.current = 'auto'
        runPage(
          'back',
          withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'offset', partition: target.partition, offset: target.offset }),
          { resetIteration: true },
        )
        break
      case 'timestamp':
        pauseReasonRef.current = 'auto'
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: target.ts_ms }), { resetIteration: true })
        break
    }
    bump()
  }

  const loadOlder = () => {
    if (cursors.back === null) return
    runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, cursor: cursors.back }), { resetIteration: true })
  }
  const loadNewer = () => {
    if (cursors.forward === null) return
    runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, cursor: cursors.forward }), { resetIteration: true })
  }
  // Cancels the in-flight filtered page (leaving already-loaded rows
  // intact) without letting the empty-page auto-continue effect
  // immediately relaunch another page: clearing pendingDirectionRef BEFORE
  // cancel() makes that effect's "direction === null" guard bail out on the
  // loading:true -> false edge cancel() itself triggers.
  const handleCancelScan = () => {
    pendingDirectionRef.current = null
    cancel()
  }

  const continueScan = () => {
    if (continueDirection === null) return
    const cursor = cursors[continueDirection]
    if (cursor === null) return
    // resetGesture: false — continuing past the cap is the SAME gesture,
    // not a new one; only the cap counter itself restarts.
    runPage(continueDirection, withFilter({ direction: continueDirection, limit: PAGE_LIMIT, cursor }), {
      resetIteration: true,
      resetGesture: false,
    })
  }

  const showLoadOlder = cursors.back !== null && !state.exhausted.back
  const showLoadNewer = cursors.forward !== null && !state.exhausted.forward
  const paused = pauseReasonRef.current !== 'none'
  const filterActive = activeFilterApiRef.current !== null
  // The current page's own progress is only added while it's actually in
  // flight — once a page ends (whether it looped again or hit the cap), its
  // final numbers have already been folded into the gesture refs above, so
  // adding it again here would double-count it.
  const progressScanned = gestureScannedRef.current + (state.loading ? (state.progress?.scanned ?? 0) : 0)
  const progressMatches = gestureMatchesRef.current + (state.loading ? (state.progress?.matches ?? 0) : 0)
  const continueScanLabel = filterActive
    ? `scanned ${progressScanned} records · ${progressMatches} matches — continue`
    : 'scanned far, nothing found here — continue'

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">{rows.length} messages</h2>
        <div className="flex items-center gap-2">
          <LivePill count={bufferReceivedRef.current} capped={bufferOverflowRef.current} onClick={handlePillClick} />
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
      <div className="flex flex-wrap items-center gap-3">
        <FilterInput value={filterText} onChange={setFilterText} placeholder="filter messages…" ariaLabel="filter messages" />
        {filterActive && state.loading && (
          <div data-testid="filter-progress" className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
            <span>{`scanned ${progressScanned} · ${progressMatches} matches`}</span>
            <button
              type="button"
              data-testid="cancel-scan"
              onClick={handleCancelScan}
              className="rounded border border-zinc-300 px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
            >
              Cancel
            </button>
          </div>
        )}
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
          {continueScanLabel}
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
        <MessageList ref={listRef} messages={rows} onScroll={handleScroll} />
      </Panel>
      {continueDirection === 'back' ? (
        <button
          data-testid="continue-scan"
          onClick={continueScan}
          className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
        >
          {continueScanLabel}
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
