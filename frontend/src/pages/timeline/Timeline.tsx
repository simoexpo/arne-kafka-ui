import { useCallback, useEffect, useLayoutEffect, useReducer, useRef, useState } from 'react'
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
// How close to the bottom (in px of unscrolled remaining distance) counts as
// "reached the last row" for the bottom-sentinel scroll-triggered
// pagination (spec: "scroll down -> next 100 older"). Same order of
// magnitude as TOP_PIN_THRESHOLD, kept as its own named constant since it
// guards a different feature (load-older, not live-pause).
const BOTTOM_PIN_THRESHOLD = 20

type Direction = 'back' | 'forward'
// 'none': live inserts straight into the store (only takes effect while
// ATTACHED — see `attached` below). 'auto': paused by scrolling off the top
// — returning to the top resumes, but only while attached. 'explicit':
// paused via the play/pause pill — overrides top-pinning, only the pill
// itself resumes it.
type PauseReason = 'none' | 'auto' | 'explicit'

// Window cap honesty (design spec v1.4): the timestamp a reposition anchors
// on, read from whichever edge row (top or bottom) the sentinel just found
// invalidated. Most rows carry a real timestamp, but it's nullable in
// principle (see MessageOut) — falling back to the nearest neighbor toward
// the OTHER edge keeps the anchor close to the true edge instead of
// guessing from an arbitrary row. Returns null only in the pathological
// case where every row in the window lacks a timestamp, telling the caller
// to leave the sentinel inert rather than fabricate an anchor.
function findAnchorTs(rows: readonly MessageOut[], edge: 'top' | 'bottom'): number | null {
  if (edge === 'bottom') {
    for (let i = rows.length - 1; i >= 0; i--) {
      const ts = rows[i].timestamp_ms
      if (ts !== null) return ts
    }
  } else {
    for (let i = 0; i < rows.length; i++) {
      const ts = rows[i].timestamp_ms
      if (ts !== null) return ts
    }
  }
  return null
}

export function Timeline({
  cluster,
  topic,
  windowCap,
}: {
  cluster: string
  topic: string
  // Test-only override of the store's default 2000-row cap: production
  // code never passes this. It exists purely so the window-cap-honesty
  // tests (design spec v1.4) can force top/bottom drops with a handful of
  // messages instead of needing to insert thousands of rows.
  windowCap?: number
}) {
  // The store is mutable (merge-native insert/rows), so it lives in a ref;
  // `bump` forces a re-render whenever an insert changes what rows() would
  // return. `Timeline` is remounted (via a key) whenever cluster/topic
  // changes, so a fresh store per mount is correct — no reset-on-prop-change
  // logic needed here.
  const storeRef = useRef(createTimelineStore(windowCap))
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
  // Count of matches delivered by the CURRENT page only (reset at the start
  // of every runPage call, incremented as batches flush) — used to detect a
  // filtered page that stopped short of a full page (budget spent before
  // `PAGE_LIMIT` matches were found). Deliberately independent of
  // `state.progress.matches`: that field only updates when a `progress`
  // event actually arrives, which a short/fast page need not send before
  // `page_end`, whereas every delivered match always reaches this ref.
  const pageMatchesRef = useRef(0)
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

  // Scroll anchoring (design spec v1.3 "Scroll anchoring", owner feedback
  // 2026-08-15): a forward page's matches rank newer, so they land near the
  // top of the newest-first merge — i.e. they prepend above whatever the
  // reader was looking at. Left alone, scrollTop stays numerically
  // unchanged after the prepend, which silently relocates the reader to
  // the top of the NEWLY loaded page instead of keeping them at the
  // junction they were reading. runPage's onMatches captures pre-insert
  // metrics here (see below) for EVERY forward insert — the top sentinel
  // fires precisely at the top, so there is no "pinned means show me the
  // new" case for forward pages (that logic belongs to 'live' inserts,
  // which never pass through here). Not used for 'back' (appends below,
  // doesn't move the viewport).
  const pendingAnchorRef = useRef<{ top: number; height: number } | null>(null)

  const [live, setLive] = useState(true)
  const [tailErrorText, setTailErrorText] = useState<string | null>(null)
  const tailHandleRef = useRef<{ close: () => void } | null>(null)

  // Attached vs detached windows (design spec v1.3): the loaded window is
  // either ATTACHED to now (opened at latest, or forward-paginated until the
  // topic reported exhausted) or DETACHED (any historical jump — beginning,
  // offset, timestamp — or a filter settle that re-reads from the
  // 'beginning' anchor context). While detached, live tail messages never
  // merge into the list — they only ever buffer — so the rendered list can
  // never show live rows adjacent to a historical window with unloaded pages
  // between them. `attached` is real React state (the header renders from
  // it); `attachedRef` mirrors it for the tail SSE callback closure (that
  // effect only runs once, on [cluster, topic], same reason pauseReasonRef
  // is a ref rather than state).
  const [attached, setAttachedState] = useState(true)
  const attachedRef = useRef(true)
  const setAttached = useCallback((value: boolean) => {
    attachedRef.current = value
    setAttachedState(value)
  }, [])

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

  // Window cap honesty (design spec v1.4, owner ruling 2026-08-15): the
  // store's cap (see timelineStore's enforceCap) silently drops rows at the
  // end opposite the insert origin once the window is full. These two refs
  // track whether THIS window has ever had a drop at that edge since it was
  // last (re)loaded — reset alongside pendingAnchorRef everywhere the window
  // is cleared (handleJump, applyFilter, reposition below). A top drop means
  // the window no longer includes the tail (see topDroppedRef's use at the
  // insert call sites below, which also detaches immediately); a bottom
  // drop means the back cursor now points at a range the window slid past —
  // both make their respective cursor unsafe to follow — checked by
  // repositionIfDropped below, which every cursor-follower (loadOlder,
  // loadNewer, continueScan, the auto-continue effect) calls before
  // touching cursors.back/cursors.forward (review round 1, F1: the check
  // must live on the followers themselves, not just the scroll sentinels
  // that usually trigger them — a drop can land while a page is already in
  // flight, after the sentinel that started it has already run).
  const topDroppedRef = useRef(false)
  const bottomDroppedRef = useRef(false)

  // Called after every store insert (live, buffer flush, page match) with
  // that insert's OWN drop delta (never the cumulative total — see
  // InsertResult). A top drop detaches the window right away: "attached"
  // means the window includes the tail, and a top drop just made that
  // false. A bottom drop only marks the back cursor unsafe; it doesn't
  // change attachment on its own (the reposition, when the reader actually
  // pushes past it, is what detaches — see `reposition` below).
  const noteDrops = useCallback(
    (result: { droppedTop: number; droppedBottom: number }) => {
      if (result.droppedTop > 0) {
        topDroppedRef.current = true
        if (attachedRef.current) setAttached(false)
      }
      if (result.droppedBottom > 0) {
        bottomDroppedRef.current = true
      }
    },
    [setAttached],
  )

  const flushBuffer = useCallback(() => {
    if (bufferRef.current.length > 0) {
      noteDrops(storeRef.current.insert(bufferRef.current, 'live'))
    }
    bufferRef.current = []
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
  }, [noteDrops])

  const runPage = useCallback(
    (
      direction: Direction,
      params: TimelinePageParams,
      // resetIteration: restarts the auto-continue cap counter (every fresh
      // page request needs this — a load-older scroll, a jump, or the
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
      pageMatchesRef.current = 0
      pendingDirectionRef.current = direction
      setContinueDirection(null)
      loadPage(params, (msgs: MessageOut[]) => {
        matchedRef.current = true
        pageMatchesRef.current += msgs.length
        // Scroll anchoring: capture "before" metrics for a forward-origin
        // prepend, but only when the reader isn't pinned to the very top
        // right now — see pendingAnchorRef's comment. A page's matches can
        // flush in several batches (useTimelinePage's BATCH_SIZE), so this
        // runs per flush; the effect below consumes and clears it after
        // each one.
        // Capture unconditionally for forward inserts: the top sentinel
        // fires precisely when the reader is at/near the top, so a
        // "skip when pinned to top" exclusion (first attempt, owner-bounced)
        // skipped the exact case anchoring exists for. Live inserts are
        // direction 'live' and never pass here — the attached pinned-top
        // prepend behavior is untouched.
        if (direction === 'forward') {
          const metrics = listRef.current?.scrollMetrics() ?? null
          if (metrics) {
            pendingAnchorRef.current = metrics
          }
        }
        noteDrops(storeRef.current.insert(msgs, direction))
        bump()
      })
    },
    [loadPage, noteDrops],
  )

  // Resends the currently active filter's server-side params (a no-op when
  // unfiltered) onto any page request — see activeFilterApiRef.
  const withFilter = useCallback((params: TimelinePageParams): TimelinePageParams => {
    const api = activeFilterApiRef.current
    if (!api) return params
    return { ...params, filter: api.filter, q: api.q, ...(api.path !== undefined ? { path: api.path } : {}) }
  }, [])

  // Window cap honesty (design spec v1.4, owner ruling 2026-08-15): pushing
  // past a dropped edge repositions instead of accreting against a stale
  // cursor — a reposition IS a jump (same clear/reset/detach machinery as
  // handleJump below), just anchored at the edge row's own timestamp
  // instead of a user-picked target. `direction` is the SAME direction the
  // caller was already pursuing ('back' for the bottom edge, 'forward' for
  // the top one) — landing edge mirrors handleJump's own rule (forward
  // lands looking from the bottom, i.e. the 'beginning' jump's edge; every
  // other direction lands from the top).
  const reposition = useCallback(
    (direction: Direction, tsMs: number) => {
      storeRef.current.clear()
      pendingAnchorRef.current = null
      bufferRef.current = []
      bufferReceivedRef.current = 0
      bufferOverflowRef.current = false
      // A fresh window can't have a stale drop anymore — a seam is
      // unconstructible in a window that was just cleared and reloaded.
      topDroppedRef.current = false
      bottomDroppedRef.current = false
      reset()
      pendingScrollEdgeRef.current = direction === 'forward' ? 'bottom' : 'top'
      anchorContextRef.current = 'default'
      pauseReasonRef.current = 'auto'
      setAttached(false)
      runPage(direction, withFilter({ direction, limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: tsMs }), {
        resetIteration: true,
      })
      bump()
    },
    [reset, runPage, withFilter, setAttached],
  )

  // Review round 1 (F1, High): the drop-flag gate belongs on every
  // CURSOR-FOLLOWER, not just the scroll sentinels — loadOlder/loadNewer,
  // continueScan (both the sentinel-driven and the button-driven call), and
  // the empty-page auto-continue effect all follow cursors.back/
  // cursors.forward directly, and each one is an independent path that can
  // reach a drop-invalidated cursor (e.g. a live drop landing mid-scan,
  // or a pill-flush drop landing while a page is already in flight — the
  // sentinel that kicked off that in-flight page has no way to gate a
  // decision that hasn't happened yet). Centralizing the check+reposition
  // here means every follower gets it by construction rather than by
  // remembering to duplicate it. Returns true when it took the reposition
  // branch (caller must treat this as "handled, do not also follow the
  // cursor"); false means the edge is clean and the caller should proceed
  // normally. Reads the store directly (not the render-time `rows` const)
  // so it's safe to call from the auto-continue effect too, whose closure
  // may be older than the current render.
  const repositionIfDropped = useCallback(
    (direction: Direction): boolean => {
      const dropped = direction === 'back' ? bottomDroppedRef.current : topDroppedRef.current
      if (!dropped) return false
      const ts = findAnchorTs(storeRef.current.rows(), direction === 'back' ? 'bottom' : 'top')
      if (ts !== null) reposition(direction, ts)
      // Pathological (no row in the window carries a timestamp): stay
      // inert rather than guess an anchor OR fall back to the stale
      // cursor — either would be dishonest, unlike doing nothing.
      return true
    },
    [reposition],
  )

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
      // A pending scroll-anchor capture belongs to the window being
      // discarded — applying it to the fresh one would yank the viewport.
      pendingAnchorRef.current = null
      bufferRef.current = []
      bufferReceivedRef.current = 0
      bufferOverflowRef.current = false
      // Same reasoning: a drop recorded against the window being replaced
      // says nothing about the freshly re-read one.
      topDroppedRef.current = false
      bottomDroppedRef.current = false
      reset()
      const base = baseAnchorParams()
      // The settled window's anchor decides attached/detached exactly like a
      // jump would: re-reading from 'beginning' is a historical window
      // (detached); re-reading from 'default' (back/latest) is attached.
      setAttached(anchorContextRef.current !== 'beginning')
      runPage(base.direction, withFilter(base), { resetIteration: true })
      bump()
    },
    [reset, runPage, baseAnchorParams, withFilter, setAttached],
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
    if (state.error) return
    if (state.exhausted[direction]) return
    const cursor = cursors[direction]
    if (cursor === null) return
    // Fold this just-ended page's final progress into the gesture-wide
    // running total BEFORE deciding whether to loop or cap — a filtered
    // scan's progress resets per page, so without this the cap
    // affordance/progress row would understate how much was actually
    // scanned across every auto-continued page in this gesture. Folded
    // unconditionally (not just on a zero-match page): a page that found
    // *some* matches but stopped short of a full page (budget spent) must
    // not have its scanned/matches total silently dropped either — see the
    // matchedRef branch below. This is the ONLY place gestureScannedRef/
    // MatchesRef are incremented, and only ever with a page that has already
    // ended — see the render-time formula, which adds the CURRENT in-flight
    // page's progress separately (and only while still loading) to avoid
    // double-counting.
    gestureScannedRef.current += state.progress?.scanned ?? 0
    gestureMatchesRef.current += state.progress?.matches ?? 0
    if (matchedRef.current) {
      // A page that filled all the way to PAGE_LIMIT is a normal, complete
      // page — the scroll sentinels already cover continuing from there.
      // One that matched *something* but stopped
      // short of a full page (the scan budget ran out before finding
      // PAGE_LIMIT matches, cursor non-null, not exhausted) must say so and
      // offer to continue rather than end quietly (spec: no silent stops) —
      // but must NOT auto-continue on its own, since the user already has
      // real matches to look at.
      if (activeFilterApiRef.current !== null && pageMatchesRef.current < PAGE_LIMIT) {
        setContinueDirection(direction)
      }
      return
    }
    // F1 (review round 1): this loop follows cursors[direction] directly,
    // same hazard as loadOlder/loadNewer/continueScan — a drop can land
    // WHILE this page was in flight (e.g. a pill flush mid-scroll-load; see
    // the auto-continue drop test), invalidating the very cursor this page
    // just returned. Chosen variant: trigger the reposition directly rather
    // than merely stopping and waiting for a later user gesture — it fixes
    // the seam proactively, and gesture totals stay honest for free: a
    // reposition's own runPage call always passes resetIteration:true with
    // no resetGesture override, so gestureScanned/MatchesRef reset to 0
    // (same as every other genuinely-new-gesture call site) instead of
    // carrying a stale total into a window that didn't earn it.
    if (repositionIfDropped(direction)) return
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

  // Scroll anchoring: consumes whatever runPage's onMatches just captured
  // (see pendingAnchorRef) after the corresponding rows have rendered, and
  // nudges scrollTop by the height delta so the viewport stays at the
  // junction instead of drifting with the newly prepended content. No
  // dependency array — a forward page's matches can flush in several
  // batches, each its own bump()/render/capture, and this must run after
  // every one of them; the ref-guard (cleared immediately) makes every
  // other render a no-op.
  useLayoutEffect(() => {
    const anchor = pendingAnchorRef.current
    if (!anchor) return
    pendingAnchorRef.current = null
    const metrics = listRef.current?.scrollMetrics()
    if (!metrics) return
    listRef.current?.adjustScrollTop(metrics.height - anchor.height)
  })

  // Re-attach: fires on the false -> true edge of `state.exhausted.forward`
  // while detached — the reader forward-paginated a historical window all
  // the way to the topic's current edge, i.e. caught the tail. Buffered live
  // messages flush in (store dedup makes the overlap with anything already
  // loaded safe) and live merging resumes, unless the pause was explicit.
  // Guarded to only fire on that specific edge (not on every render where
  // forward happens to already be exhausted) and only while detached (an
  // attached window reaching forward-exhausted, e.g. mount, is a no-op).
  // `reset()` (called by every jump and by applyFilter) clears exhausted
  // back to false first, so a subsequent jump re-arms this edge naturally.
  const prevExhaustedForwardRef = useRef(false)
  useEffect(() => {
    const wasExhausted = prevExhaustedForwardRef.current
    prevExhaustedForwardRef.current = state.exhausted.forward
    if (wasExhausted || !state.exhausted.forward) return
    if (attachedRef.current) return
    setAttached(true)
    flushBuffer()
    pauseReasonRef.current = pauseReasonRef.current === 'explicit' ? 'explicit' : 'none'
    bump()
  }, [state.exhausted.forward, flushBuffer, setAttached])

  // Live tail: on by default, ON for the lifetime of the component. An
  // error (server-emitted or transport) stops it for good — Task 8 adds the
  // pause/resume affordance; Task 7 only needs the freeze-in-place pattern.
  useEffect(() => {
    const handle = tailTopic(cluster, topic, {
      onMessage: (m) => {
        if (!predicateRef.current(m)) return
        // While detached, live messages ALWAYS buffer — merging them would
        // recreate the false seam a historical window exists to avoid.
        // pauseReasonRef CAN legitimately read 'none' while detached (e.g.
        // re-attach set it, then a filter settle from the 'beginning'
        // context detached again): attached is the authoritative gate.
        if (attachedRef.current && pauseReasonRef.current === 'none') {
          noteDrops(storeRef.current.insert([m], 'live'))
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

  // Clicking the "▲ n new" pill: while DETACHED, flushing in place would
  // recreate the false seam the historical window exists to avoid — so
  // instead the pill jumps to now (fresh latest page, scrolled to top, live
  // resumed), abandoning the historical reading position by design (an
  // explicit click). While ATTACHED, it flushes and scrolls to the top —
  // the click means "show me the new messages", and flushing without
  // moving would just shift content invisibly above the viewport. It only
  // resumes when the pause was automatic — an explicit pause stays paused
  // (the pill clears what had built up so far).
  const handlePillClick = () => {
    if (!attachedRef.current) {
      handleJump({ kind: 'now' })
      return
    }
    flushBuffer()
    listRef.current?.scrollToEdge('top')
    if (pauseReasonRef.current === 'auto') pauseReasonRef.current = 'none'
    bump()
  }

  // The explicit play/pause toggle: while DETACHED the toggle is only ever
  // shown lit paused (see the header render below — live rendering is off
  // by definition there), so the only honest click is the same jump-to-now
  // the pill offers (design spec v1.3, owner ruling 2026-08-15) — flushing
  // in place would recreate the false seam a historical window exists to
  // avoid, same reasoning as handlePillClick. While ATTACHED: pausing
  // always forces 'explicit' (overriding whatever auto/none state was in
  // effect); un-pausing always flushes + resumes fully, regardless of how
  // it got paused.
  const handlePlayPauseToggle = () => {
    if (!attachedRef.current) {
      handleJump({ kind: 'now' })
      return
    }
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
  // outright (not flushed — they belong to the OLD viewport). 'now' attaches
  // (live tail merges straight in again); every other jump — beginning,
  // offset, timestamp — DETACHES: it lands mid-topic or at the tail end,
  // where merging live rows in would create a false seam with the unloaded
  // pages between the historical window and now. Detached windows enter
  // paused-auto (the pill counts; while detached, top-pinning does NOT
  // resume live — only re-attaching, via catching the tail or jumping to
  // now, does).
  const handleJump = (target: JumpTarget) => {
    storeRef.current.clear()
    // A pending scroll-anchor capture belongs to the window being left —
    // never let it adjust the post-jump viewport (defense-in-depth: today
    // capture and consumption share one commit, but that's implicit).
    pendingAnchorRef.current = null
    bufferRef.current = []
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
    // Same reasoning as pendingAnchorRef: a drop recorded against the OLD
    // window says nothing about the fresh one a jump is about to load.
    topDroppedRef.current = false
    bottomDroppedRef.current = false
    // A jump invalidates BOTH pagination directions, not just the one being
    // (re)loaded: the old cursors describe a window the user is leaving
    // entirely. reset() clears both cursors/exhausted flags (and kills any
    // in-flight page) synchronously, BEFORE runPage starts the new one —
    // without this, a stale opposite-direction cursor from before the jump
    // could let a scroll sentinel fire a request pointed at the wrong
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
        setAttached(true)
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }), { resetIteration: true })
        break
      case 'beginning':
        pauseReasonRef.current = 'auto'
        setAttached(false)
        runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }), { resetIteration: true })
        break
      case 'offset':
        pauseReasonRef.current = 'auto'
        setAttached(false)
        runPage(
          'back',
          withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'offset', partition: target.partition, offset: target.offset }),
          { resetIteration: true },
        )
        break
      case 'timestamp':
        pauseReasonRef.current = 'auto'
        setAttached(false)
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: target.ts_ms }), { resetIteration: true })
        break
    }
    bump()
  }

  // Scroll is the only pagination affordance (no load-older/load-newer
  // buttons): each checks repositionIfDropped FIRST (F1, review round 1) —
  // a drop at this edge means the cursor below is stale, and following it
  // would recreate exactly the false seam the drop-detach exists to avoid.
  const loadOlder = () => {
    if (repositionIfDropped('back')) return
    if (cursors.back === null || state.exhausted.back) return
    runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, cursor: cursors.back }), { resetIteration: true })
  }
  const loadNewer = () => {
    if (repositionIfDropped('forward')) return
    if (cursors.forward === null || state.exhausted.forward) return
    runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, cursor: cursors.forward }), { resetIteration: true })
  }

  // Wired directly onto MessageList's real scroll element (scroll events
  // don't bubble, so this can't live on a wrapper). Scrolling off the top
  // auto-pauses (only from 'none' — 'auto'/'explicit' are already paused).
  // Returning to the top auto-flushes + resumes, but ONLY if the pause was
  // automatic (an explicit pause overrides top-pinning entirely) AND the
  // window is attached — while detached, top-of-window ≠ now, so being
  // pinned to the top of a historical window does nothing; re-attaching only
  // happens via catching the tail (forward-exhausted effect) or jumping to
  // now (jump / pill click).
  //
  // Also the scroll-triggered pagination sentinels (spec: "scroll down ->
  // next 100 older") — scroll is the ONLY pagination affordance, there is no
  // load-older/load-newer button: a bottom-sentinel reaching the last row
  // loads the next older page automatically, guarded by not already loading
  // (`loadOlder`/`loadNewer` themselves no-op on a null cursor, an exhausted
  // direction, or — window cap honesty, F1 — a dropped edge, reposition-ing
  // instead; the loading guard is the only one that needs restating here).
  // The symmetric top edge only ever matters once a beginning-jump has
  // opened a forward cursor — reusing the same top-pin check that already
  // drives live-pause auto-resume.
  //
  // When the continue affordance (I2's partial-match budget stop, or the
  // iteration-cap stop) is showing for a direction, the sentinel must drive
  // `continueScan()` instead of `loadOlder`/`loadNewer` directly — those
  // reset the gesture (resetGesture defaults to resetIteration:true), which
  // would silently drop the running scanned/matches totals the continue
  // affordance is displaying, exactly the "silent stop" I2 exists to avoid.
  const handleScroll = (scrollTop: number, scrollHeight: number, clientHeight: number) => {
    const pinnedTop = scrollTop < TOP_PIN_THRESHOLD
    if (pinnedTop) {
      if (attachedRef.current && pauseReasonRef.current === 'auto') {
        flushBuffer()
        pauseReasonRef.current = 'none'
        bump()
      }
      // Window cap honesty: the drop-flag gate lives in loadNewer/
      // continueScan themselves now (see repositionIfDropped) — the
      // sentinel just decides WHICH follower to call, same as before.
      if (!state.loading) {
        if (continueDirection === 'forward') continueScan()
        else loadNewer()
      }
    } else if (pauseReasonRef.current === 'none') {
      pauseReasonRef.current = 'auto'
      bump()
    }
    const nearBottom = scrollHeight - (scrollTop + clientHeight) < BOTTOM_PIN_THRESHOLD
    if (nearBottom && !state.loading) {
      if (continueDirection === 'back') continueScan()
      else loadOlder()
    }
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
    // F1 (review round 1): reachable from BOTH the scroll sentinel above
    // and the standalone continue-scan button in the JSX below — a drop can
    // land while the button is showing (the reviewer's exact repro: a
    // filtered scan stops, live traffic drops the edge, THEN the button is
    // clicked), so the gate has to live here, not just on the sentinel path.
    if (repositionIfDropped(continueDirection)) return
    const cursor = cursors[continueDirection]
    if (cursor === null) return
    // resetGesture: false — continuing past the cap is the SAME gesture,
    // not a new one; only the cap counter itself restarts.
    runPage(continueDirection, withFilter({ direction: continueDirection, limit: PAGE_LIMIT, cursor }), {
      resetIteration: true,
      resetGesture: false,
    })
  }

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
          <LivePill count={bufferReceivedRef.current} capped={bufferOverflowRef.current} attached={attached} onClick={handlePillClick} />
          {live && attached ? (
            // Attached: the normal live indicator/toggle cluster.
            <>
              {!paused && <span className="animate-pulse text-emerald-500">● live</span>}
              <PlayPauseToggle paused={paused} onClick={handlePlayPauseToggle} />
            </>
          ) : !attached ? (
            // Detached (design spec v1.3, owner ruling 2026-08-15): the
            // toggle IS the mode signal — always shown lit paused (live
            // rendering is off by definition while detached, regardless of
            // pauseReason, which keeps driving buffering underneath), no
            // pulsing "● live", and no staleness chip — a historical page is
            // immutable, there is nothing to be stale about. Clicking the
            // toggle here jumps to now (handlePlayPauseToggle), same as the
            // pill.
            <PlayPauseToggle paused={!attached || paused} onClick={handlePlayPauseToggle} />
          ) : (
            // Attached but live has died: this alarm is honest (new data
            // is genuinely missing) — keep today's aging/alarm tiers.
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
      {continueDirection === 'forward' && (
        <button
          data-testid="continue-scan"
          onClick={continueScan}
          className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
        >
          {continueScanLabel}
        </button>
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
      ) : (
        // F2 (review round 1): the caption claims the window's oldest row
        // IS the topic's first message — a bottom drop makes that false
        // even while state.exhausted.back is still (stale) true from
        // before the drop, so it must not render until a reposition
        // (which resets bottomDroppedRef) genuinely re-earns exhaustion.
        state.exhausted.back && !bottomDroppedRef.current && (
          <p className="py-2 text-center text-xs text-zinc-400">— beginning of topic —</p>
        )
      )}
    </div>
  )
}
