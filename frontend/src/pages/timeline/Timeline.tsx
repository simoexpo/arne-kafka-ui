import { useCallback, useEffect, useLayoutEffect, useReducer, useRef, useState } from 'react'
import { tailTopic } from '../../api/sse'
import type { TimelineDirection, TimelinePageParams } from '../../api/sse'
import type { MessageOut } from '../../api/types'
import { FilterInput } from '../../components/FilterInput'
import { FilterHelp } from '../../components/FilterHelp'
import { MessageList } from '../../components/messages/MessageList'
import type { MessageListHandle } from '../../components/messages/MessageList'
import { Panel } from '../../components/Panel'
import { StalenessChip } from '../../components/StalenessChip'
import { TimeZoneToggle } from '../../components/TimeZoneToggle'
import { parseFilterQuery, type FilterQueryApi } from '../../lib/filterQuery'
import { formatWindowRange } from '../../lib/format'
import { decodeCursor } from '../../lib/timelineCursor'
import { createSlidingWindowStore, type InsertOutcome } from '../../lib/timelineStore'
import { useTimeDisplayMode } from '../../lib/timeDisplayMode'
import { createLiveBuffer } from '../../lib/timeline/liveBuffer'
import { planJump } from '../../lib/timeline/jumpPlan'
import type { AnchorContext, PauseReason } from '../../lib/timeline/model'
import { classifyScroll } from '../../lib/timeline/scrollZones'
import { stepSettling, type SettlingState } from '../../lib/timeline/settling'
import { useFallingEdge } from './useFallingEdge'
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
// Live-buffer cap while paused/detached (design spec).
const BUFFER_CAP = 500
// A real-browser probe observed settling resolve within 2-3 events; this caps a pathological non-settling case.
const MAX_SETTLE_ATTEMPTS = 10
// Owner ruling 2026-08-16 (sliding-window followups, "NEXT STEP"): a
// filtered scan that resolves in milliseconds (e.g. scroll-up after a
// timestamp jump with a filter active, re-scanning a range already mostly
// cached) flashed the progress row + Cancel button showing "scanned 0 · 0
// matches" — the intended machinery (real progress, cancellable) surfacing
// with zero information content, gone before a human could read or act on
// it. Show-delaying the row this long means only a scan a human could
// plausibly WATCH ever renders it; totals/cancel behavior for a genuinely
// slow scan are unaffected — this only gates whether the row appears at
// all, never what it shows once it does.
const PROGRESS_SHOW_DELAY_MS = 400

type Direction = TimelineDirection

export function Timeline({
  cluster,
  topic,
  windowCap,
  partitionIds,
}: {
  cluster: string
  topic: string
  // Test-only override of the store's default 2000-row cap: production
  // code never passes this. It exists purely so the sliding-window property
  // walk and the window-cap-honesty tests can force top/bottom trims with a
  // handful of messages instead of needing to insert thousands of rows.
  windowCap?: number
  partitionIds?: number[]
}) {
  // The store is mutable (edge-map-tracking insert/rows), so it lives in a
  // ref; `bump` forces a re-render whenever a store mutation changes what
  // rows()/edges() would return. `Timeline` is remounted (via a key)
  // whenever cluster/topic changes, so a fresh store per mount is correct —
  // no reset-on-prop-change logic needed here.
  const storeRef = useRef(createSlidingWindowStore(windowCap))
  const [, bump] = useReducer((c: number) => c + 1, 0)
  // The store's edge cursor a given direction reads FROM next: 'back'
  // reads older rows, which live at the store's 'bottom' edge; 'forward'
  // reads newer rows, from the 'top' edge.
  const edgeCursorFor = (d: Direction) => storeRef.current.edges()[d === 'back' ? 'bottom' : 'top']

  // `state.exhausted`/`state.loading`/`state.progress`/`state.error` are the
  // source of truth for the auto-continue effect below; pagination itself
  // (loadOlder/loadNewer/continueScan) reads the store's own `edges()`
  // exclusively — the just-landed page's own cursor reaches the store
  // directly via runPage's synchronous `onPageEnd` callback (see its
  // comment).
  const { loadPage, state, reset, cancel } = useTimelinePage(cluster, topic)

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
  const anchorContextRef = useRef<AnchorContext>('default')

  const pendingDirectionRef = useRef<Direction | null>(null)
  // Task 3: each in-flight page's own rows, accumulated per-generation
  // (cleared at the start of every runPage call, appended to as batches
  // flush from useTimelinePage) so the page-end effect below can commit the
  // WHOLE page to the store in one `insertPage` call — the store's own
  // contract (see its interface doc comment) takes one page's rows plus
  // that SAME page's own start/continuation cursor pair; it isn't designed
  // to be fed a page's rows piecemeal across several calls (an anchor
  // bootstrap's opposite-side seed, in particular, needs the FULL page's
  // row offsets, not just whatever happened to be in one batch).
  const pageRowsRef = useRef<MessageOut[]>([])
  // The cursor's decoded positions the in-flight page was ISSUED with (null
  // for an anchor page — no request cursor exists at all). Set by runPage
  // at issue time; consumed once, by the page-end effect, as `insertPage`'s
  // `startPositions` argument.
  const pendingStartPositionsRef = useRef<Record<number, number> | null>(null)
  // Only meaningful for a back-direction anchor bootstrap (the store's
  // M-new anchor-awareness fix — see createSlidingWindowStore's `insertPage`
  // doc comment): whether THIS bootstrap, if it turns out to be one, should
  // claim the window is attached to the tail. Every anchor call site below
  // passes this explicitly (never relies on the store's own default, which
  // exists only for that module's unit-test ergonomics).
  const pendingAttachRef = useRef(false)
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
  const [continueDirection, setContinueDirection] = useState<Direction | null>(null)
  // Running totals across an entire gesture: a user-issued page plus every
  // auto-continued empty page that follows it. A single page's own
  // `state.progress` resets to null at the start of each new page, so
  // without this ref the progress row / cap affordance would misleadingly
  // shrink back to a tiny number every time an empty page silently
  // auto-continues, instead of showing a true running total. Only ever
  // incremented with a PRIOR (already-ended) page's final numbers — see the
  // page-end effect and the render-time formula below, which together keep
  // this from double-counting the page currently in flight.
  const gestureScannedRef = useRef(0)
  const gestureMatchesRef = useRef(0)
  // B-2 fix: whether a gesture (a user-issued page plus every
  // auto-continued empty page that follows it) is still running — as
  // opposed to `state.loading`, which is only true for the CURRENT page and
  // flips false/true at every page boundary. The show-delayed progress row
  // below (`scanRunning`) must gate on THIS, not on `state.loading`: a
  // multi-page scan whose individual pages each resolve in well under
  // PROGRESS_SHOW_DELAY_MS previously re-armed a fresh 400ms timer at every
  // page boundary and could run indefinitely without ever rendering
  // anything or offering Cancel. Set `true` at the top of every `runPage`
  // call; set `false` on every TERMINAL branch of the auto-continue effect
  // below (error, exhausted, matched-with-affordance, no next cursor,
  // iteration-cap) and in `handleCancelScan` — never on the relaunch
  // branch, which is the same gesture continuing.
  const [gestureRunning, setGestureRunning] = useState(false)

  // Viewport repositioning after a jump: 'now' lands looking at the top of
  // its new window. 'beginning'/'offset'/'timestamp' (owner ruling
  // 2026-08-15: the latter two were 'back'-anchored and top-landing before —
  // they now read forward from the target, same as 'beginning') land at the
  // start of their loaded window looking forward, so the bottom
  // (oldest-visible) edge — where the target itself sits — is what matters
  // there. Tracked separately from pendingDirectionRef (which drives the
  // empty-page auto-continue) via its own `useFallingEdge` instance, since
  // the two concerns are independent and a jump's first page can itself be
  // an empty page that auto-continues further.
  const listRef = useRef<MessageListHandle>(null)
  const pendingScrollEdgeRef = useRef<'top' | 'bottom' | null>(null)
  // See docs/superpowers/specs/timeline-design-decisions.md ("MessageList.scrollToEdge's return value") for why re-snapping is needed at all.
  const settlingRef = useRef<SettlingState | null>(null)
  const settleAttemptsRef = useRef(0)

  // Scroll anchoring: capture the row at index 0 before a forward batch (or
  // the last committed row before a back commit — see runPage's `onPageEnd`
  // for the capture site); after render, find that same row's new index and
  // adjust scrollTop by its offset delta. Robust to a same-commit trim below
  // the reader, which a total-height delta is not — see
  // docs/superpowers/specs/timeline-design-decisions.md ("Scroll anchoring:
  // rejected total-height-delta approach") for why.
  const pendingAnchorRef = useRef<{ partition: number; offset: number; priorTop: number } | null>(null)

  // Owner-requested (2026-08-15): the row landed on by an offset jump gets a
  // highlighted marker (MessageRow's `isJumpTarget`) so the reader can find
  // it at a glance in the loaded window. Real state (not a ref): it only
  // ever changes on a jump or a filter settle — both already re-render via
  // other state/bump — and MessageList needs it on every render, not just
  // via an imperative escape hatch. Cleared (set back to null) by every
  // jump (including non-offset ones — see handleJump) and by a settled
  // filter change (see applyFilter), both of which already clear the store
  // itself: the highlight only ever makes sense for the window it was set
  // in, never carried forward into a different one.
  const [jumpTarget, setJumpTarget] = useState<{ partition: number; offset: number } | null>(null)

  // UTC/local display toggle (owner ruling 2026-08-15): purely re-renders
  // the header's zone label/time from the SAME loaded rows — see
  // `lib/timeDisplayMode`'s own comment. No refetch: nothing below reads
  // this to decide what to request, only how to format what's already here.
  //
  // Owner ruling (moved 2026-08-16): the rendered `TimeZoneToggle` control
  // itself now lives in THIS header (see the render below), next to the
  // window-range display it rewrites and the live/pause controls — it used
  // to sit in the sidebar (`layout/AppShell.tsx`), where its effect was
  // never actually visible. The global store this reads from is unchanged.
  const timeDisplayMode = useTimeDisplayMode()

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
  //
  // Task 3: starts FALSE (not true) and is ONLY ever flipped true by the
  // page-end effect below, once that page's own `insertPage` call has
  // CONFIRMED the store itself is attached (`edges().top === null`) — never
  // optimistically ahead of it. This is what makes the sliding store's
  // `insertLive` throw-on-detached precondition provably unreachable from
  // this component's own call pattern (see the tail effect's onMessage
  // handler below, and the "insertLive never throws" test in
  // Timeline.test.tsx): a live message can only ever reach `insertLive`
  // while `attachedRef.current` is true, and `attachedRef.current` can only
  // ever become true in the same synchronous update where the store's own
  // attachment was just verified. Every jump (including 'now') therefore
  // sets `attached` false immediately (matching the store's own immediate
  // `clear()`) and lets this same confirmed-catch-up path bring it back to
  // true once the fresh anchor page actually lands — a live message arriving
  // in that gap simply buffers (safe), never throws.
  const [attached, setAttachedState] = useState(false)
  const attachedRef = useRef(false)
  const setAttached = useCallback((value: boolean) => {
    attachedRef.current = value
    setAttachedState(value)
  }, [])

  // pauseReasonRef is a ref (not state), mutated directly and paired with
  // `bump()`: the tail effect only runs once, on [cluster, topic], so state
  // would close over a stale value there.
  const pauseReasonRef = useRef<PauseReason>('none')
  const liveBufferRef = useRef(createLiveBuffer(BUFFER_CAP))

  // `state.exhausted.back`/`.forward` (React state from useTimelinePage) is
  // only refreshed by a NEW page landing in that direction — these two refs
  // override a STALE exhausted flag when a trim (page or live insert) has
  // touched that side since. Cleared the instant a fresh page for that
  // direction lands; set the instant a trim touches that side. See
  // `noteOutcome` below and its two call sites. (Predecessor machinery this
  // replaced: docs/superpowers/specs/timeline-design-decisions.md.)
  const bottomTrimmedSinceRef = useRef(false)
  const topTrimmedSinceRef = useRef(false)

  // Called after every store mutation (a page commit or a live insert) with
  // that mutation's OWN outcome (never a cumulative total — see
  // InsertOutcome). A top trim detaches the UI right away: "attached" means
  // the window includes the tail, and a top trim just made that false. Both
  // trim kinds also update the "since a fresh page landed" tracking used to
  // override a stale exhausted flag (see the refs' own comment above).
  const noteOutcome = useCallback(
    (outcome: InsertOutcome) => {
      if (outcome.trimmedTop > 0) {
        topTrimmedSinceRef.current = true
        if (attachedRef.current) setAttached(false)
      }
      if (outcome.trimmedBottom > 0) bottomTrimmedSinceRef.current = true
    },
    [setAttached],
  )

  const flushBuffer = useCallback(() => {
    const drained = liveBufferRef.current.drain()
    if (drained.length > 0) {
      noteOutcome(storeRef.current.insertLive(drained))
    }
  }, [noteOutcome])

  // Resends the currently active filter's server-side params (a no-op when
  // unfiltered) onto any page request — see activeFilterApiRef. Moved above
  // `runPage` (fix round 1, C3): `runPage`'s own `onPageEnd` callback now
  // needs it directly, to re-issue a rejected-stale page with the filter
  // still attached — and a `useCallback`'s dependency array is evaluated
  // immediately, so it can only ever list something already declared by
  // this point in the render.
  const withFilter = useCallback((params: TimelinePageParams): TimelinePageParams => {
    const api = activeFilterApiRef.current
    if (!api) return params
    return { ...params, filter: api.filter, q: api.q, ...(api.path !== undefined ? { path: api.path } : {}) }
  }, [])

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
      // gesture and resets both together. `attach`: see pendingAttachRef's
      // own comment — required (not optional) so every call site states its
      // intent explicitly.
      opts: { resetIteration: boolean; resetGesture?: boolean; attach: boolean },
    ) => {
      setGestureRunning(true)
      if (opts.resetIteration) iterationRef.current = 0
      if (opts.resetGesture ?? opts.resetIteration) {
        gestureScannedRef.current = 0
        gestureMatchesRef.current = 0
      }
      matchedRef.current = false
      pageMatchesRef.current = 0
      pendingDirectionRef.current = direction
      // Each page request records its own start (task 3): a cursor page
      // decodes the positions it was issued with; an anchor page (no
      // request cursor at all) has none — exactly `insertPage`'s own
      // `startPositions` contract.
      pendingStartPositionsRef.current = params.cursor !== undefined ? decodeCursor(params.cursor) : null
      pendingAttachRef.current = opts.attach
      pageRowsRef.current = []
      setContinueDirection(null)
      loadPage(
        params,
        (msgs: MessageOut[]) => {
          matchedRef.current = true
          pageMatchesRef.current += msgs.length
          // Scroll anchoring, per BATCH (fix round 2, N5 — review of
          // 000fd9f): a forward scan's overlay (see pendingAnchorRef's own
          // comment, and M2 below) prepends each new batch above whatever
          // the reader is currently looking at, exactly like the final
          // commit does — without this, an intermediate overlay update
          // would silently relocate the reader the same way M1 originally
          // fixed for the commit itself. Capture the row CURRENTLY at the
          // top of the overlay (index 0 — see pendingAnchorRef's own
          // comment on why that index specifically) before adding this
          // batch; the layout effect below finds that same row's new index
          // once the batch has rendered and adjusts by the difference.
          // This ALSO covers the final trailing batch: `flush()` (see
          // useTimelinePage's own doc comment) always calls this callback
          // BEFORE `onPageEnd` fires, so there is no separate capture
          // needed at commit time — each batch's own capture chains into
          // the next, and the last one chains straight into the commit
          // (summing to the same total adjustment as anchoring once to the
          // original pre-scan top row would have, just incrementally).
          if (direction === 'forward') {
            const beforeRows = storeRef.current.previewWithOverlay(pageRowsRef.current)
            if (beforeRows.length > 0) {
              const priorTop = listRef.current?.rowOffsetAt(0) ?? null
              if (priorTop !== null) {
                pendingAnchorRef.current = { partition: beforeRows[0].partition, offset: beforeRows[0].offset, priorTop }
              }
            }
          }
          pageRowsRef.current.push(...msgs)
          // No store COMMIT here (task 3, unlike the old per-batch
          // `store.insert` calls): a page's rows are committed to the store
          // ATOMICALLY, once, by `onPageEnd` below via `insertPage` — the
          // store's own contract takes the full page's rows plus a single
          // start/continuation cursor pair, not a per-batch trickle (an
          // anchor bootstrap's opposite-side seed in particular needs every
          // row this page ever delivers, not just one batch's worth).
          //
          // Fix round 1, M2: matches must still STREAM to the reader as
          // they arrive (product charter: "stream results"), even though
          // the store commit itself waits for page-end — `bump()` here
          // re-renders with `pageRowsRef.current` now longer, and the
          // render-time `rows` below merges it over the committed store
          // via `previewWithOverlay` (display-only, no store mutation).
          bump()
        },
        // Fires SYNCHRONOUSLY with `page_end` (see useTimelinePage's own doc
        // comment on why: a render-later commit, via a `useEffect` keyed on
        // `state.loading`, leaves ref-based DOM handles — e.g. the scroll
        // viewport reposition below — reading the store from BEFORE this
        // page's rows landed, for exactly one render. Committing here
        // instead means `storeRef.current.rows()` already reflects this
        // page by the time ANY effect for this commit runs.) `exhausted`
        // itself isn't needed here — per the empty-page contract `cursor`
        // is null exactly when `exhausted` is true, and `insertPage` only
        // ever wants the cursor.
        (cursor) => {
          const rows = pageRowsRef.current
          pageRowsRef.current = []
          // Scroll anchoring is captured per-BATCH now (fix round 2, N5 —
          // see the `onMatches` callback above, which always runs, via
          // `flush()`, before this callback does) — there is deliberately
          // no separate capture here: `flush()`'s own trailing call already
          // set `pendingAnchorRef` for this exact commit, and capturing
          // again here (against the now-about-to-be-cleared `pageRowsRef`)
          // would just re-derive the SAME "before" state a second time —
          // or, worse, the WRONG one, since the row this batch's own
          // capture chained from may no longer be at index 0 by the time
          // this callback runs (see `pendingAnchorRef`'s own comment).
          //
          // A 'back' page needs its own capture, HERE rather than per-batch
          // (see the back-anchoring comment below `pendingAnchorRef` for
          // why it is needed at all): a back page's rows rank OLDER, so both
          // its overlay batches and its commit append BELOW everything
          // already rendered — appending never moves an existing row, so
          // only the commit's own top TRIM can move the reader, and only the
          // commit knows whether one happened. The junction row is the LAST
          // committed row (the reader is at the bottom — that is what fired
          // the bottom sentinel — and a back commit only ever trims the TOP,
          // so this row always survives). Its rendered offset is read BEFORE
          // `insertPage` runs, since `rowOffsetAt` reports the LAST RENDER's
          // measurements and this page's commit has not rendered yet. Safe
          // against an already-rendered overlay for the same reason: those
          // rows sit below this one, so its own offset is identical either
          // way.
          let backAnchor: { partition: number; offset: number; priorTop: number } | null = null
          if (direction === 'back') {
            const committed = storeRef.current.rows()
            const lastIndex = committed.length - 1
            const priorTop = lastIndex >= 0 ? listRef.current?.rowOffsetAt(lastIndex) ?? null : null
            if (priorTop !== null) {
              backAnchor = { partition: committed[lastIndex].partition, offset: committed[lastIndex].offset, priorTop }
            }
          }
          const outcome = storeRef.current.insertPage(rows, direction, pendingStartPositionsRef.current, cursor, {
            attach: pendingAttachRef.current,
          })
          // Fix round 1, C3: a CONCURRENT trim (typically a live insert)
          // can advance the same-side edge map PAST what this now-landing
          // page assumed when it was issued — the store detects this and
          // rejects the page wholesale (see its own doc comment) rather
          // than risk an interior hole. Nothing here landed: don't touch
          // detach/reattach — an ACCEPTED page is what re-earns those,
          // never a rejected one.
          if (outcome.rejectedStale) {
            // Fix round 2, N6: this REJECTED page's own SSE response may
            // still have reported `exhausted: true` to `useTimelinePage`'s
            // internal state (e.g. a genuinely-reached topic start that
            // arrived just as a concurrent trim invalidated it) — mark
            // this direction's exhausted flag untrustworthy UNCONDITIONALLY
            // here, not contingent on whether the retry below actually
            // fires, so that even a skipped retry (no fresh cursor to
            // offer) can't leave that stale `true` unguarded: it would
            // otherwise show a false "beginning of topic" caption or wrongly
            // gate further pagination. bottomTrimmedSinceRef/
            // topTrimmedSinceRef (see their own comment) are exactly the
            // existing mechanism for discounting a stale exhausted flag —
            // no need for a separate one.
            if (direction === 'back') bottomTrimmedSinceRef.current = true
            else topTrimmedSinceRef.current = true
            const freshCursor = edgeCursorFor(direction)
            // Fix round 2, N3 (charter: no zombie scans): a retry consumes
            // an iteration-cap slot just like an empty-page auto-continue
            // does — a pathological storm of concurrent trims invalidating
            // one retry after another must eventually land on the SAME
            // continue-scan affordance, never loop silently forever.
            // `resetIteration` stays `false` (this is a continuation of the
            // SAME gesture, not a new one) but the counter itself still
            // advances.
            if (freshCursor !== null && iterationRef.current < ITERATION_CAP) {
              iterationRef.current += 1
              runPage(direction, withFilter({ direction, limit: PAGE_LIMIT, cursor: freshCursor }), {
                resetIteration: false,
                attach: false,
              })
            } else if (freshCursor !== null) {
              setContinueDirection(direction)
            }
            return
          }
          noteOutcome(outcome)
          // Arm the back-direction anchor only if this commit actually
          // trimmed above the reader — below the cap a back page moves
          // nothing (delta would be exactly 0 anyway) and the viewport must
          // be left strictly alone.
          if (backAnchor !== null && outcome.trimmedTop > 0) pendingAnchorRef.current = backAnchor
          // "Just became store-attached" catch-up: covers BOTH a detached
          // window's forward page catching the tail (v1.3's reattach) AND
          // an anchor bootstrap ('now', or mount) whose own attach:true just
          // landed — see `attached`'s own comment above for why UI-attached
          // is only ever flipped true here, in direct response to the
          // store's OWN confirmed attachment, never ahead of it.
          //
          // Fix round 1, C1: gates on `outcome.attached` (the store's REAL
          // attachment truth), never on `edges().top === null` — that reads
          // null for a SECOND, unrelated reason too (an empty or otherwise
          // incomplete top map, e.g. a zero-row anchor page under the
          // empty-page contract), which would otherwise make the UI believe
          // a genuinely-detached historical window was attached, and go on
          // to call `insertLive` on it — throwing (see the store's own
          // precondition doc comment).
          if (!attachedRef.current && outcome.attached) {
            setAttached(true)
            flushBuffer()
            pauseReasonRef.current = pauseReasonRef.current === 'explicit' ? 'explicit' : 'none'
          }
          bump()
          // Direction-based staleness refresh (see bottomTrimmedSinceRef/
          // topTrimmedSinceRef's own comment): a fresh page landing for this
          // direction re-earns that side's exhausted truth. Placed after
          // `noteOutcome` so a same-page trim on the OPPOSITE side (the only
          // side a 'back'/'forward' page can ever trim — see enforceCap) is
          // never immediately un-set by this line.
          if (direction === 'back') bottomTrimmedSinceRef.current = false
          else topTrimmedSinceRef.current = false
        },
      )
    },
    [loadPage, noteOutcome, flushBuffer, setAttached, withFilter],
  )

  // The anchor a fresh (non-cursor) page request starts from, given the
  // current anchor context — see anchorContextRef. Deliberately simple:
  // 'beginning' is the only context that re-reads forward/beginning; every
  // other context (default, or a settled-past offset/timestamp jump)
  // re-reads back/latest, same as no jump at all — unchanged from v1.3.
  const baseAnchorParams = useCallback(
    (): TimelinePageParams =>
      anchorContextRef.current === 'beginning'
        ? { direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }
        : { direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' },
    [],
  )

  // Drops the loaded window wholesale: store, pending scroll anchor, jump
  // highlight, live buffer, stale-exhausted flags, both pagination
  // directions (reset() also kills any in-flight page), and UI attachment.
  // Both call sites — a settled filter change (applyFilter) and any jump
  // (handleJump) — discard the whole viewport, so they must stay in
  // lockstep. `nextJumpTarget` is the new window's jump highlight (only an
  // offset jump sets one; every other caller passes null).
  const resetWindow = useCallback(
    (nextJumpTarget: { partition: number; offset: number } | null) => {
      storeRef.current.clear()
      pendingAnchorRef.current = null
      setJumpTarget(nextJumpTarget)
      liveBufferRef.current.clear()
      bottomTrimmedSinceRef.current = false
      topTrimmedSinceRef.current = false
      reset()
      setAttached(false)
    },
    [reset, setAttached],
  )

  // A settled filter change (see the debounce effect below): switches the
  // live-tail predicate immediately, drops the current viewport (same as a
  // jump, since the loaded window no longer reflects the new filter), and
  // reloads the first page from the current anchor context with the parsed
  // filter attached.
  const applyFilter = useCallback(
    (text: string) => {
      const parsed = parseFilterQuery(text)
      predicateRef.current = parsed.predicate
      activeFilterApiRef.current = parsed.api
      resetWindow(null)
      const base = baseAnchorParams()
      // A re-read from anything other than 'beginning' collapses to the
      // 'default' context (matches v1.3: refiltering after an offset/
      // timestamp jump reads from latest, not from the old jump target) —
      // clears any stale offset/timestamp memory a later loadNewer
      // fallback (see its own comment) might otherwise wrongly reuse.
      if (anchorContextRef.current !== 'beginning') anchorContextRef.current = 'default'
      // The settled window's anchor decides attached/detached exactly like a
      // jump would: re-reading from 'beginning' is a historical window
      // (detached, and the store's own bootstrap opt below matches); every
      // other context is attached (back/latest).
      const attach = anchorContextRef.current !== 'beginning'
      runPage(base.direction, withFilter(base), { resetIteration: true, attach })
      bump()
    },
    [resetWindow, runPage, baseAnchorParams, withFilter],
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
    runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }), {
      resetIteration: true,
      attach: true,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Auto-continue / iteration-cap / partial-match-affordance decision (task
  // 3): fires on the loading:true -> false edge of a page we ourselves
  // issued (tracked via pendingDirectionRef). Unlike v1.3/v1.4, this effect
  // no longer commits anything to the store itself — that already happened
  // SYNCHRONOUSLY, in `runPage`'s `onPageEnd` callback (see its own comment
  // for why: this effect runs a render later, which is fine for a decision
  // that only ever starts ANOTHER async request, but was NOT fine for the
  // store commit itself — a ref-based DOM handle, e.g. the scroll-viewport
  // reposition effect below, would otherwise read stale content for exactly
  // one render). This effect purely decides whether an empty (or partial,
  // filtered) page should auto-continue, hit the iteration cap, or land
  // normally — reading `state.exhausted`/`state.progress` fresh (unlike the
  // synchronous callback, which must avoid stale-closure reads of `state`)
  // and `storeRef.current.edges()` for the next cursor (already up to date,
  // since the synchronous commit ran before this effect ever could).
  const autoContinueFallingEdge = useFallingEdge(state.loading)
  useEffect(() => {
    if (!autoContinueFallingEdge) return
    const direction = pendingDirectionRef.current
    if (direction === null) return
    pendingDirectionRef.current = null
    if (state.error) {
      // Fix round 1, M2: the page errored — nothing landed, `onPageEnd`
      // never ran, and whatever it had accumulated so far was never
      // committed. Drop the display overlay too (honest: the store's own
      // edges never advanced for this page, so leaving its uncommitted
      // matches on screen would misrepresent what's actually loaded).
      if (pageRowsRef.current.length > 0) {
        pageRowsRef.current = []
        bump()
      }
      setGestureRunning(false)
      return
    }

    if (state.exhausted[direction]) {
      setGestureRunning(false)
      return
    }
    gestureScannedRef.current += state.progress?.scanned ?? 0
    gestureMatchesRef.current += state.progress?.matches ?? 0
    if (matchedRef.current) {
      // A page that filled all the way to PAGE_LIMIT is a normal, complete
      // page — the scroll sentinels already cover continuing from there.
      // One that matched *something* but stopped short of a full page (the
      // scan budget ran out before finding PAGE_LIMIT matches, cursor
      // non-null, not exhausted) must say so and offer to continue rather
      // than end quietly (spec: no silent stops) — but must NOT
      // auto-continue on its own, since the user already has real matches
      // to look at.
      if (activeFilterApiRef.current !== null && pageMatchesRef.current < PAGE_LIMIT) {
        setContinueDirection(direction)
      }
      setGestureRunning(false)
      return
    }
    const nextCursor = edgeCursorFor(direction)
    if (nextCursor === null) {
      setGestureRunning(false)
      return
    }
    if (iterationRef.current >= ITERATION_CAP) {
      setContinueDirection(direction)
      setGestureRunning(false)
      return
    }
    iterationRef.current += 1
    runPage(direction, withFilter({ direction, limit: PAGE_LIMIT, cursor: nextCursor }), {
      resetIteration: false,
      attach: false,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoContinueFallingEdge, state.error, state.exhausted.back, state.exhausted.forward])

  // Jump viewport repositioning: fires on the very first loading:true ->
  // false edge after a jump set pendingScrollEdgeRef (handleJump), whether
  // or not that first page matched anything — a jump's job is to reposition
  // the viewport for the new anchor, which doesn't depend on rows already
  // being present. Cleared after firing once, so a subsequent empty-page
  // auto-continue for the same jump doesn't re-trigger it.
  const jumpLandingFallingEdge = useFallingEdge(state.loading)
  useEffect(() => {
    if (!jumpLandingFallingEdge) return
    const edge = pendingScrollEdgeRef.current
    if (edge === null) return
    pendingScrollEdgeRef.current = null
    const landed = listRef.current?.scrollToEdge(edge)
    // A zero-row anchor page renders Panel's loading skeleton instead of the scroller — nothing to arm settling on.
    if (landed !== null && landed !== undefined) {
      settlingRef.current = { edge, lastScrollHeight: null }
      settleAttemptsRef.current = 0
    }
  }, [jumpLandingFallingEdge])

  // Scroll anchoring (row-identity rewrite, fix round 1 M1; per-batch, fix
  // round 2 N5): consumes whatever the LAST `onMatches` call captured (see
  // pendingAnchorRef's own comment — every batch captures, including the
  // final one, via `flush()` always running before `onPageEnd`) after the
  // corresponding rows have rendered, finds the SAME junction row's NEW
  // index, and nudges scrollTop by the difference between its rendered
  // offset now and before — robust to a trim that ALSO happened below it
  // in the same commit (unlike a total-height delta, which that trim would
  // corrupt). Searches the SAME view the render actually used: the overlay
  // (`previewWithOverlay`) if a page is still mid-flight (another batch of
  // the SAME page still pending, or a DIFFERENT page already started before
  // this render committed — either way `pageRowsRef` is non-empty), or the
  // plain committed `rows()` once a commit has actually run and cleared it.
  // No dependency array — deliberately runs after every render (the
  // ref-guard, cleared immediately, makes any render without a pending
  // anchor a no-op).
  useLayoutEffect(() => {
    const anchor = pendingAnchorRef.current
    if (!anchor) return
    pendingAnchorRef.current = null
    const currentRows =
      pageRowsRef.current.length > 0 ? storeRef.current.previewWithOverlay(pageRowsRef.current) : storeRef.current.rows()
    const newIndex = currentRows.findIndex((r) => r.partition === anchor.partition && r.offset === anchor.offset)
    if (newIndex === -1) return // the junction row itself is gone — nothing sane to anchor to (shouldn't happen; see the comment on why a forward insert can't trim its own top)
    const newTop = listRef.current?.rowOffsetAt(newIndex) ?? null
    if (newTop === null) return
    listRef.current?.adjustScrollTop(newTop - anchor.priorTop)
  })

  // Live tail: on by default, ON for the lifetime of the component. An
  // error (server-emitted or transport) stops it for good.
  useEffect(() => {
    const handle = tailTopic(cluster, topic, {
      onMessage: (m) => {
        if (!predicateRef.current(m)) return
        // While detached, live messages ALWAYS buffer — merging them would
        // recreate the false seam a historical window exists to avoid.
        // `attachedRef.current` is the gate (never `pauseReasonRef` alone):
        // it can only be true once the store's own attachment has been
        // confirmed (see its own comment above), so this branch can never
        // reach a detached store — `insertLive`'s throw precondition is
        // structurally unreachable from this call site (see
        // "insertLive never throws" in Timeline.test.tsx).
        if (attachedRef.current && pauseReasonRef.current === 'none') {
          noteOutcome(storeRef.current.insertLive([m]))
        } else {
          liveBufferRef.current.push(m)
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

  // Fix round 1, M2: display the committed store rows merged with whatever
  // the CURRENT in-flight page has accumulated so far (a display-only
  // overlay — see `previewWithOverlay`'s own doc comment). The fast path
  // (no accumulated rows: nothing in flight, or an in-flight page that
  // just hasn't matched anything yet) skips the merge entirely.
  const rows = pageRowsRef.current.length > 0 ? storeRef.current.previewWithOverlay(pageRowsRef.current) : storeRef.current.rows()

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

  // resetWindow drops buffered live messages outright (they belong to the OLD viewport, never flushed); `attached`
  // is set false immediately here (matching the store's own clear()) and only re-confirmed true by the page-end
  // effect once the fresh anchor page actually lands — see `attached`'s own comment for why never optimistically.
  const handleJump = (target: JumpTarget) => {
    const plan = planJump(target, PAGE_LIMIT)
    resetWindow(plan.highlight)
    pendingScrollEdgeRef.current = plan.scrollEdge
    anchorContextRef.current = plan.anchorContext
    pauseReasonRef.current = plan.pauseReason
    runPage(plan.params.direction, withFilter(plan.params), { resetIteration: true, attach: plan.attach })
    bump()
  }

  // Scroll is the only pagination affordance (no load-older/load-newer
  // buttons). Task 3: both read the store's OWN minted edge cursor directly
  // — no drop-flag/reposition machinery needed anymore, since every trim
  // leaves behind a real, followable cursor by construction (the whole
  // point of the sliding-window redesign). `bottomTrimmedSinceRef`/
  // `topTrimmedSinceRef` only override a STALE exhausted flag (see their own
  // comment) — the underlying edge cursor itself is always current.
  const loadOlder = () => {
    if (state.exhausted.back && !bottomTrimmedSinceRef.current) return
    const cursor = storeRef.current.edges().bottom
    if (cursor === null) return
    runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, cursor }), { resetIteration: true, attach: false })
  }
  // `edges().top === null` here means nothing more to load; `state.exhausted
  // .forward` already covers that case above. See docs/superpowers/specs/
  // timeline-design-decisions.md ("loadNewer's deleted forward-anchor
  // fallback") for why no fallback is needed for a null top edge.
  const loadNewer = () => {
    if (state.exhausted.forward && !topTrimmedSinceRef.current) return
    const cursor = storeRef.current.edges().top
    if (cursor === null) return
    runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, cursor }), { resetIteration: true, attach: false })
  }

  // Wired directly onto MessageList's real scroll element (scroll events
  // don't bubble, so this can't live on a wrapper). Scrolling off the top
  // auto-pauses (only from 'none' — 'auto'/'explicit' are already paused).
  // Returning to the top auto-flushes + resumes, but ONLY if the pause was
  // automatic (an explicit pause overrides top-pinning entirely) AND the
  // window is attached — while detached, top-of-window ≠ now, so being
  // pinned to the top of a historical window does nothing; re-attaching only
  // happens via catching the tail (forward-exhausted, in the page-end
  // effect) or jumping to now (jump / pill click).
  //
  // Also the scroll-triggered pagination sentinels (spec: "scroll down ->
  // next 100 older") — scroll is the ONLY pagination affordance, there is no
  // load-older/load-newer button: a bottom-sentinel reaching the last row
  // loads the next older page automatically, guarded by not already loading
  // (`loadOlder`/`loadNewer` themselves no-op on a null cursor or an
  // exhausted, never-trimmed direction). The symmetric top edge only ever
  // matters once a beginning/offset/timestamp jump has opened a forward
  // cursor — reusing the same top-pin check that already drives
  // live-pause auto-resume.
  //
  // When the continue affordance (I2's partial-match budget stop, or the
  // iteration-cap stop) is showing for a direction, the sentinel must drive
  // `continueScan()` instead of `loadOlder`/`loadNewer` directly — those
  // reset the gesture (resetGesture defaults to resetIteration:true), which
  // would silently drop the running scanned/matches totals the continue
  // affordance is displaying, exactly the "silent stop" I2 exists to avoid.
  const handleScroll = (scrollTop: number, scrollHeight: number, clientHeight: number) => {
    // The event that confirms stability is itself consumed here, never falling through to pagination below.
    if (settlingRef.current !== null) {
      settleAttemptsRef.current += 1
      const step = stepSettling(settlingRef.current, scrollHeight, settleAttemptsRef.current, MAX_SETTLE_ATTEMPTS)
      if (step.action === 'resnap') {
        settlingRef.current = step.next
        listRef.current?.scrollToEdge(step.next.edge)
      } else {
        settlingRef.current = null
      }
      return
    }
    const { pinnedTop, nearBottom } = classifyScroll({ scrollTop, scrollHeight, clientHeight })
    if (pinnedTop) {
      if (attachedRef.current && pauseReasonRef.current === 'auto') {
        flushBuffer()
        pauseReasonRef.current = 'none'
        bump()
      }
      if (!state.loading) {
        if (continueDirection === 'forward') continueScan()
        else loadNewer()
      }
    } else if (pauseReasonRef.current === 'none') {
      pauseReasonRef.current = 'auto'
      bump()
    }
    if (nearBottom && !state.loading) {
      if (continueDirection === 'back') continueScan()
      else loadOlder()
    }
  }

  // Cancels the in-flight filtered page (leaving already-loaded — i.e.
  // COMMITTED — rows intact) without letting the empty-page auto-continue
  // effect immediately relaunch another page: clearing pendingDirectionRef
  // BEFORE cancel() makes that effect's "direction === null" guard bail out
  // on the loading:true -> false edge cancel() itself triggers.
  //
  // Fix round 1, M2: also drops this page's uncommitted display overlay —
  // `onPageEnd` never got to run (the page never reached page_end), so
  // nothing here was ever going to be committed; leaving its matches
  // visible after an explicit cancel would be dishonest (the store's own
  // edges never advanced past them, and a later re-scroll re-scans this
  // same range from scratch).
  const handleCancelScan = () => {
    pendingDirectionRef.current = null
    if (pageRowsRef.current.length > 0) {
      pageRowsRef.current = []
      bump()
    }
    setGestureRunning(false)
    cancel()
  }

  const continueScan = () => {
    if (continueDirection === null) return
    const cursor = edgeCursorFor(continueDirection)
    if (cursor === null) return
    // resetGesture: false — continuing past the cap is the SAME gesture,
    // not a new one; only the cap counter itself restarts.
    runPage(continueDirection, withFilter({ direction: continueDirection, limit: PAGE_LIMIT, cursor }), {
      resetIteration: true,
      resetGesture: false,
      attach: false,
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

  // Show-delay the progress row (see PROGRESS_SHOW_DELAY_MS above): starts a
  // timer the moment a filtered GESTURE begins, cleared (and the flag
  // reset) the instant it stops — whether that's a genuine page_end, an
  // error, or an explicit cancel (see `gestureRunning`'s own comment). B-2
  // fix: this must gate on the GESTURE, not on any single page's own
  // `state.loading` — an auto-continued page flips `state.loading` false
  // then true again at every page boundary, which previously reset this
  // timer on every boundary too, so a scan made of quick pages could run
  // indefinitely without ever rendering anything. A gesture that never runs
  // this long never flips the flag at all, so a fast scan renders nothing
  // extra — not even for one frame.
  const scanRunning = filterActive && gestureRunning
  const [progressVisible, setProgressVisible] = useState(false)
  useEffect(() => {
    if (!scanRunning) {
      setProgressVisible(false)
      return
    }
    const id = setTimeout(() => setProgressVisible(true), PROGRESS_SHOW_DELAY_MS)
    return () => clearTimeout(id)
  }, [scanRunning])

  // The forward-continue and back-continue affordances are the same button
  // (S-9) — the two render sites (above/below MessageList, one per
  // direction) are mutually exclusive on `continueDirection`, so the shared
  // `data-testid` is never ambiguous.
  const continueScanButton = (
    <button
      data-testid="continue-scan"
      onClick={continueScan}
      className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
    >
      {continueScanLabel}
    </button>
  )

  return (
    // Flex column filling whatever height TopicDetailPage's tab-body slot
    // hands it (owner feedback 2026-08-15): every row of chrome here (header,
    // filter, jump control, captions) is fixed-size; only the Panel/
    // MessageList slot below grows (flex-1 min-h-0), so MessageList's own
    // internal scroller ends up the ONE scroller on this tab — see its own
    // comment on the className it's given.
    <div className="flex h-full flex-col gap-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400" data-testid="window-range">
          {rows.length === 0
            ? 'no messages loaded'
            : formatWindowRange(rows.at(-1)!.timestamp_ms, rows[0].timestamp_ms, timeDisplayMode)}
        </h2>
        {/* Owner feedback (stability): the pill/live-dot are the VOLATILE
            members of this cluster — they pop in and out with network
            activity, which used to shove the clickable controls (play/pause,
            the zone toggle) sideways underneath the reader's cursor. Fixed
            by ANCHORING from the right rather than relying on ordering
            alone: this row's parent is `justify-between` with exactly two
            children (the `h2` and this div), so this div's own right edge
            sits flush against the header line's right edge regardless of
            its content width — and within it, plain flex-row order means
            the LAST child is flush against THAT edge. Placing the zone
            toggle last (rightmost/outermost) and the play/pause-or-staleness
            slot immediately before it (second-to-last) pins both at a fixed
            distance from that right edge no matter what the volatile
            elements to their left do; the volatile pill/dot come FIRST
            (closest to the free space between the two `h2`/controls
            children), so their own appearing/disappearing only ever grows
            or shrinks that free space, never the position of anything to
            their right. See Timeline.test.tsx's "stable header controls"
            suite for the pixel-stability assertion. */}
        <div className="flex items-center gap-2">
          <LivePill count={liveBufferRef.current.received} capped={liveBufferRef.current.overflowed} attached={attached} onClick={handlePillClick} />
          {live && attached && !paused && <span className="animate-pulse text-emerald-500">● live</span>}
          {attached ? (
            live ? (
              // Attached: the normal live play/pause toggle.
              <PlayPauseToggle paused={paused} onClick={handlePlayPauseToggle} />
            ) : (
              // Attached but live has died: this alarm is honest (new data
              // is genuinely missing) — keep today's aging/alarm tiers.
              <StalenessChip asOf={rows[0]?.timestamp_ms ?? null} failed={false} />
            )
          ) : (
            // Detached (design spec v1.3, owner ruling 2026-08-15): the
            // toggle IS the mode signal — always shown lit paused (live
            // rendering is off by definition while detached, regardless of
            // pauseReason, which keeps driving buffering underneath), no
            // pulsing "● live", and no staleness chip — a historical page is
            // immutable, there is nothing to be stale about. Clicking the
            // toggle here jumps to now (handlePlayPauseToggle), same as the
            // pill.
            <PlayPauseToggle paused onClick={handlePlayPauseToggle} />
          )}
          <TimeZoneToggle />
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        <div className="min-w-0 flex-1">
          <FilterInput value={filterText} onChange={setFilterText} placeholder="filter messages…" ariaLabel="filter messages" className="w-full" fullWidth />
        </div>
        <FilterHelp />
        {progressVisible && (
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
      <JumpControl onJump={handleJump} partitionIds={partitionIds} />
      {tailErrorText && (
        <p className="text-sm text-amber-600 dark:text-amber-400">{`live stopped — ${tailErrorText}`}</p>
      )}
      {continueDirection === 'forward' && continueScanButton}
      <Panel
        className="flex min-h-0 flex-1 flex-col overflow-hidden"
        error={state.error}
        hasData={rows.length > 0}
        loading={state.loading && rows.length === 0}
      >
        <MessageList ref={listRef} messages={rows} onScroll={handleScroll} jumpTarget={jumpTarget} />
      </Panel>
      {continueDirection === 'back' ? (
        continueScanButton
      ) : (
        // Captions (design spec v1.6): "beginning of topic" only when the
        // bottom edge is genuinely the topic start — exhausted AND not
        // stale (see bottomTrimmedSinceRef's own comment: a live/forward
        // trim after a genuine exhausted:true response can slide the
        // window's actual bottom edge past the true start again, even
        // though `state.exhausted.back` itself hasn't been refreshed by a
        // new page yet).
        state.exhausted.back && !bottomTrimmedSinceRef.current && (
          <p className="py-2 text-center text-xs text-zinc-400">— beginning of topic —</p>
        )
      )}
    </div>
  )
}
