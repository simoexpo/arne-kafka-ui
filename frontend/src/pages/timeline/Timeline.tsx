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
import { formatWindowRange } from '../../lib/format'
import { decodeCursor } from '../../lib/timelineCursor'
import { createSlidingWindowStore, type InsertOutcome } from '../../lib/timelineStore'
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

// Which anchor the currently loaded window was bootstrapped from (task 3;
// supersedes the v1.3-era 'default' | 'beginning' pair). Carries the FULL
// anchor params, not just a kind tag, for two reasons: (1) a settled filter
// change re-reads from this same context (unchanged from v1.3 — only
// 'beginning' gets its own re-anchor, everything else falls back to
// back/latest); (2) NEW in task 3 — loadNewer's forward-anchor fallback
// (see its own comment below) needs to re-issue the EXACT same anchor with
// direction=forward when the store's top edge was never seeded at all (a
// zero-row anchor bootstrap page never seeds the opposite-side map — see
// createSlidingWindowStore's own doc comment on that one gap).
type AnchorContext =
  | { kind: 'default' }
  | { kind: 'beginning' }
  | { kind: 'offset'; partition: number; offset: number }
  | { kind: 'timestamp'; ts_ms: number }

export function Timeline({
  cluster,
  topic,
  windowCap,
}: {
  cluster: string
  topic: string
  // Test-only override of the store's default 2000-row cap: production
  // code never passes this. It exists purely so the sliding-window property
  // walk and the window-cap-honesty tests can force top/bottom trims with a
  // handful of messages instead of needing to insert thousands of rows.
  windowCap?: number
}) {
  // The store is mutable (edge-map-tracking insert/rows), so it lives in a
  // ref; `bump` forces a re-render whenever a store mutation changes what
  // rows()/edges() would return. `Timeline` is remounted (via a key)
  // whenever cluster/topic changes, so a fresh store per mount is correct —
  // no reset-on-prop-change logic needed here.
  const storeRef = useRef(createSlidingWindowStore(windowCap))
  const [, bump] = useReducer((c: number) => c + 1, 0)

  // `cursors` (per-direction raw page_end cursors) is deliberately NOT
  // destructured here: task 3 moved the store commit into runPage's own
  // synchronous `onPageEnd` callback (see its comment), which receives the
  // just-landed page's cursor directly as an argument — reading it via
  // `cursors[direction]` a render later would reintroduce the exact
  // staleness problem that synchronous callback exists to avoid.
  // `state.exhausted`/`state.loading`/`state.progress`/`state.error` remain
  // the source of truth for the auto-continue effect below; pagination
  // itself (loadOlder/loadNewer/continueScan) now reads the store's own
  // `edges()` exclusively.
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
  const anchorContextRef = useRef<AnchorContext>({ kind: 'default' })

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
  const wasLoadingRef = useRef(false)
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
  // 2026-08-15; ROW-IDENTITY rewrite, fix round 1, M1 — review of 079f30f):
  // a forward page's matches rank newer, so they land near the top of the
  // newest-first merge — i.e. they prepend above whatever the reader was
  // looking at. Left alone, scrollTop stays numerically unchanged after the
  // prepend, which silently relocates the reader to the top of the NEWLY
  // loaded page instead of keeping them at the junction they were reading.
  //
  // The FIRST version of this fix (task 3) captured total scrollHeight
  // before/after and adjusted by "added − removed". That's wrong the
  // instant an insert ALSO trims rows BELOW the viewport in the same
  // commit (routine at a small cap: recovering rows via a forward page can
  // simultaneously overflow-trim the bottom) — the trim shrinks the total
  // height without moving anything the reader can see above them, so
  // "added − removed" can cancel toward ~0: exactly the forbidden
  // relocation this mechanism exists to prevent.
  //
  // Fixed by anchoring to a SPECIFIC row's IDENTITY instead of a total
  // delta: captured in runPage's `onMatches` callback (see below, one
  // capture per BATCH as of fix round 2, N5 — including the final one,
  // since `flush()` always runs before `onPageEnd`), right before that
  // batch's rows get added — the "junction" row is whatever is CURRENTLY
  // at the top of the merged (possibly overlaid) list, since a forward
  // insert can only ever prepend ABOVE it, never move it out from under
  // itself (a top trim is impossible on a forward-direction insert — see
  // `enforceCap`: only `back`-direction overflow ever trims the top).
  // Consumed by the layout effect below, which finds that SAME row's NEW
  // index after the batch/commit renders and adjusts scrollTop by the
  // difference in `MessageList#rowOffsetAt` — robust to a simultaneous
  // trim (which only ever happens BELOW this row) and to estimate-vs-
  // measured drift (both reads go through the same virtualizer API).
  //
  // 'back' pages need the SAME treatment, for the mirror-image reason
  // (real-browser stall found in the 2026-08-15 rollout drill; the original
  // "a back page appends below, it doesn't move the viewport" was only true
  // BELOW the cap). Once the window is full, every back page trims exactly
  // as many rows off the TOP as it appends at the bottom: total height stops
  // changing, and every remaining row shifts UP by the trimmed height while
  // scrollTop stays numerically unchanged. The reader — sitting at the
  // bottom, which is what fired the sentinel in the first place — is
  // therefore left pinned at scrollTop === max: the whole window slides
  // underneath them (they never see the page they just asked for), and the
  // browser, having no position change to report on an unchanged
  // scrollHeight, never fires another scroll event. Since scroll is the ONLY
  // pagination affordance, that killed the down-walk permanently at the cap.
  // The capture site differs from 'forward' (commit-time, not per-batch —
  // see runPage's `onPageEnd`), the consumption is identical.
  //
  // `priorTop` is ALWAYS `0` here (fix round 2, N4 — flagged as "correct
  // by accident" otherwise): the junction row captured is, by construction,
  // ALWAYS the row currently at index 0 (`rowOffsetAt(0)`), and index 0's
  // own cumulative offset from the top of the list is trivially 0 no
  // matter WHICH row occupies that slot. `rowOffsetAt(0)` is called anyway
  // (rather than hard-coding `priorTop: 0`) for two honest reasons, not
  // because the call result is in doubt: (1) it doubles as the "is the
  // list actually mounted" check — returns `null` before the virtualizer
  // exists (e.g. mid-jump, while `Panel` shows a loading skeleton), which a
  // hardcoded `0` would silently paper over; (2) it keeps the invariant
  // explicit and self-enforcing — if a future change ever captured some
  // OTHER index instead of 0, this call would immediately start returning
  // that row's real (non-zero) offset rather than silently inheriting a
  // now-wrong hardcoded assumption.
  const pendingAnchorRef = useRef<{ partition: number; offset: number; priorTop: number } | null>(null)

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

  // Task 3 (replaces the v1.4 windowCap-honesty drop flags + repositionIf
  // Dropped/reposition/findAnchorTs machinery entirely — the sliding store's
  // own edge maps make every trim followable by a REAL minted cursor, so
  // there is nothing left to "reposition": loadOlder/loadNewer simply read
  // `edges()` directly, trim or no trim). What's left is a narrower,
  // caption/gate-only concern: `state.exhausted.back`/`.forward` (React
  // state from useTimelinePage) is only ever refreshed by a NEW page
  // actually landing in that direction — so if a trim happens on a side
  // whose exhausted flag is currently (stale) true, that flag would
  // otherwise wrongly veto both the "beginning of topic" caption and further
  // pagination on that side, even though the store's own edge for that side
  // is now a real, followable, non-exhausted cursor again. These two refs
  // track "has a trim happened on this side since the last time a page for
  // that direction actually landed" — cleared the instant a fresh page for
  // that direction lands (that page's own exhausted flag is authoritative
  // again), set the instant a trim touches that side (from a page OR a live
  // insert). See noteOutcome below and its two call sites.
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
    if (bufferRef.current.length > 0) {
      noteOutcome(storeRef.current.insertLive(bufferRef.current))
    }
    bufferRef.current = []
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
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
            const freshCursor = storeRef.current.edges()[direction === 'back' ? 'bottom' : 'top']
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
      anchorContextRef.current.kind === 'beginning'
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
      bottomTrimmedSinceRef.current = false
      topTrimmedSinceRef.current = false
      reset()
      const base = baseAnchorParams()
      // A re-read from anything other than 'beginning' collapses to the
      // 'default' context (matches v1.3: refiltering after an offset/
      // timestamp jump reads from latest, not from the old jump target) —
      // clears any stale offset/timestamp memory a later loadNewer
      // fallback (see its own comment) might otherwise wrongly reuse.
      if (anchorContextRef.current.kind !== 'beginning') anchorContextRef.current = { kind: 'default' }
      // The settled window's anchor decides attached/detached exactly like a
      // jump would: re-reading from 'beginning' is a historical window
      // (detached, and the store's own bootstrap opt below matches); every
      // other context is attached (back/latest).
      const attach = anchorContextRef.current.kind !== 'beginning'
      setAttached(false)
      runPage(base.direction, withFilter(base), { resetIteration: true, attach })
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
  useEffect(() => {
    const wasLoading = wasLoadingRef.current
    wasLoadingRef.current = state.loading
    if (!wasLoading || state.loading) return
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
      return
    }

    if (state.exhausted[direction]) return
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
      return
    }
    const nextCursor = storeRef.current.edges()[direction === 'back' ? 'bottom' : 'top']
    if (nextCursor === null) return
    if (iterationRef.current >= ITERATION_CAP) {
      setContinueDirection(direction)
      return
    }
    iterationRef.current += 1
    runPage(direction, withFilter({ direction, limit: PAGE_LIMIT, cursor: nextCursor }), {
      resetIteration: false,
      attach: false,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.loading, state.error, state.exhausted.back, state.exhausted.forward])

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

  // A jump repositions the viewport: the current window is no longer
  // meaningful, so the store and any buffered live messages are dropped
  // outright (not flushed — they belong to the OLD viewport). Every jump
  // sets `attached` false immediately here (matching the store's own
  // immediate `clear()` — see `attached`'s own comment for why even 'now'
  // does this, deferring the actual re-attach to the page-end effect once
  // its fresh anchor page confirms the store itself is attached). Detached
  // windows enter paused-auto (the pill counts; while detached, top-pinning
  // does NOT resume live — only re-attaching does).
  const handleJump = (target: JumpTarget) => {
    storeRef.current.clear()
    // A pending scroll-anchor capture belongs to the window being left —
    // never let it adjust the post-jump viewport (defense-in-depth: today
    // capture and consumption share one commit, but that's implicit).
    pendingAnchorRef.current = null
    bufferRef.current = []
    bufferReceivedRef.current = 0
    bufferOverflowRef.current = false
    bottomTrimmedSinceRef.current = false
    topTrimmedSinceRef.current = false
    // A jump invalidates BOTH pagination directions, not just the one being
    // (re)loaded: the old cursors describe a window the user is leaving
    // entirely. reset() clears both cursors/exhausted flags (and kills any
    // in-flight page) synchronously, BEFORE runPage starts the new one.
    reset()
    setAttached(false)
    // 'beginning' lands at the start of history looking forward — the
    // bottom (oldest-visible) edge is the meaningful anchor there. Every
    // other jump lands looking at the top of its new window.
    pendingScrollEdgeRef.current = target.kind === 'beginning' ? 'bottom' : 'top'
    switch (target.kind) {
      case 'now':
        anchorContextRef.current = { kind: 'default' }
        // Forced to 'none' regardless of any prior explicit pause — jumping
        // to now is an intentional resume-live action (v1.3). The store
        // isn't confirmed attached yet (fresh clear()), so this alone can't
        // cause a live insert — see `attached`'s own comment.
        pauseReasonRef.current = 'none'
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'latest' }), {
          resetIteration: true,
          attach: true,
        })
        break
      case 'beginning':
        anchorContextRef.current = { kind: 'beginning' }
        pauseReasonRef.current = 'auto'
        runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, anchor: 'beginning' }), {
          resetIteration: true,
          attach: false,
        })
        break
      case 'offset':
        anchorContextRef.current = { kind: 'offset', partition: target.partition, offset: target.offset }
        pauseReasonRef.current = 'auto'
        runPage(
          'back',
          withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'offset', partition: target.partition, offset: target.offset }),
          { resetIteration: true, attach: false },
        )
        break
      case 'timestamp':
        anchorContextRef.current = { kind: 'timestamp', ts_ms: target.ts_ms }
        pauseReasonRef.current = 'auto'
        runPage('back', withFilter({ direction: 'back', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: target.ts_ms }), {
          resetIteration: true,
          attach: false,
        })
        break
    }
    bump()
  }

  // loadNewer's forward-anchor fallback (task 3): the store's top edge is
  // only ever seeded from a page's OWN rows (same-direction continuation
  // cursor, or — for an anchor bootstrap — the opposite-side row-offset
  // seed; see createSlidingWindowStore's doc comment). A historical
  // (offset/timestamp) anchor bootstrap that happens to return ZERO rows for
  // every partition (a heavily filtered jump landing on a gap, say) never
  // seeds the top map at all, so `edges().top` reads null — not because the
  // window is attached, but because nothing above it has ever been
  // recorded. The only way to read "above" such a jump is to re-issue the
  // EXACT SAME anchor with direction=forward (anchors are caller
  // parameters, not part of the cursor — the anchor partition property
  // guarantees back(anchor) and forward(anchor) split the topic disjointly
  // and completely, so this is exact, not a guess). 'beginning' never needs
  // this (it's already forward-anchored — its own same-direction
  // continuation cursor always advances via rule 1 regardless of row
  // count) and neither does 'default' (always attached, edges().top is
  // masked null for a different, correct reason).
  const forwardAnchorFallback = (): TimelinePageParams | null => {
    const ctx = anchorContextRef.current
    if (ctx.kind === 'offset') return { direction: 'forward', limit: PAGE_LIMIT, anchor: 'offset', partition: ctx.partition, offset: ctx.offset }
    if (ctx.kind === 'timestamp') return { direction: 'forward', limit: PAGE_LIMIT, anchor: 'timestamp', ts_ms: ctx.ts_ms }
    return null
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
  const loadNewer = () => {
    if (state.exhausted.forward && !topTrimmedSinceRef.current) return
    const cursor = storeRef.current.edges().top
    if (cursor !== null) {
      runPage('forward', withFilter({ direction: 'forward', limit: PAGE_LIMIT, cursor }), { resetIteration: true, attach: false })
      return
    }
    const fallback = forwardAnchorFallback()
    if (fallback) runPage('forward', withFilter(fallback), { resetIteration: true, attach: false })
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
  // matters once a beginning-jump (or an offset/timestamp jump — see the
  // forward-anchor fallback) has opened a forward cursor — reusing the same
  // top-pin check that already drives live-pause auto-resume.
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
    cancel()
  }

  const continueScan = () => {
    if (continueDirection === null) return
    const cursor = storeRef.current.edges()[continueDirection === 'back' ? 'bottom' : 'top']
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
          {rows.length === 0 ? 'no messages loaded' : formatWindowRange(rows.at(-1)!.timestamp_ms, rows[0].timestamp_ms)}
        </h2>
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
      <Panel
        className="flex min-h-0 flex-1 flex-col overflow-hidden"
        error={state.error}
        hasData={rows.length > 0}
        loading={state.loading && rows.length === 0}
      >
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
