import type { TimelineDirection } from '../api/sse'
import type { MessageOut } from '../api/types'
import { decodeCursor, encodeCursor } from './timelineCursor'

function partitionKey(partition: number, offset: number): string {
  return `${partition}:${offset}`
}

/**
 * Insert `msg` into `arr` keeping it sorted ascending by offset. `arr` holds
 * one partition's messages — offset order is Kafka's own truth for that
 * partition, so this is the only ordering ever used within a partition.
 */
function insertByOffsetAscending(arr: MessageOut[], msg: MessageOut): void {
  let lo = 0
  let hi = arr.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (arr[mid].offset < msg.offset) lo = mid + 1
    else hi = mid
  }
  arr.splice(lo, 0, msg)
}

/** Binary-searches `arr` (offset-ascending) for `offset`, returning its index or -1. */
function findOffsetIndex(arr: MessageOut[], offset: number): number {
  let lo = 0
  let hi = arr.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (arr[mid].offset < offset) lo = mid + 1
    else hi = mid
  }
  return lo < arr.length && arr[lo].offset === offset ? lo : -1
}

// ===========================================================================
// v1.6 sliding window store (spec: docs/superpowers/specs/2026-08-13-
// messages-timeline-design.md, "Sliding window (v1.6 — owner ruling)").
// `Timeline.tsx` drives `createSlidingWindowStore` exclusively — see
// docs/superpowers/specs/timeline-design-decisions.md for this file's
// predecessor.
//
// Design summary (see the doc comments below for the exact per-rule
// reasoning): the store tracks two per-partition position maps — `bottomMap`
// (exclusive upper bound for reading older) and `topMap` (inclusive lower
// bound for reading newer) — updated on every page insert and on every
// row-exact trim, so a cursor minted from either map, followed in the
// opposite direction, reconstructs exactly what's missing on that side: no
// gap, no duplicate. `attached` tracks whether the top edge is genuinely the
// live tail (in which case `edges().top` is null, per the documented
// interface) or a real, followable boundary left behind by a top trim.
// ===========================================================================

export interface InsertOutcome {
  trimmedTop: number
  trimmedBottom: number
  /**
   * The store's REAL attachment state as of this call (fix round 1, C1).
   * NOT the same thing as `edges().top === null` — that also reads `null`
   * whenever `topMap` is simply empty (e.g. a zero-row anchor page, legal
   * per the empty-page contract), which has nothing to do with whether the
   * window is genuinely attached to the tail. A caller deciding whether it
   * may now call `insertLive` (or otherwise treat the window as attached)
   * must read THIS field (or call `isAttached()`), never infer it from
   * `edges().top`.
   */
  attached: boolean
  /**
   * `true` when this `insertPage` call was REJECTED wholesale as stale
   * (fix round 1, C3) — see `insertPage`'s own doc comment. No rows were
   * inserted and no map was mutated; `trimmedTop`/`trimmedBottom` are both
   * `0` and `attached` reflects whatever it already was before this call.
   * The caller should re-issue the same request from the store's OWN fresh
   * `edges()` instead of trusting anything about this outcome. Always
   * `false` for `insertLive` (which has no start-position/staleness concept
   * at all).
   */
  rejectedStale: boolean
}

interface SlidingWindowStore {
  /**
   * Inserts one page's rows.
   *
   * `startPositions` is the cursor's positions the page was REQUESTED with,
   * or `null` for an anchor-fetched page (Latest/Beginning/Offset/Timestamp
   * — none of those start from a request cursor). `contCursor` is the
   * response's own continuation cursor, or `null` when the backend reported
   * `exhausted: true` (nothing more in `direction` from these positions).
   *
   * STALENESS PRECONDITION (fix round 1, C3 — enforced, not left to caller
   * discipline): before touching anything, compares `startPositions` (per
   * partition) against the CURRENT same-side map (`bottomMap` for `back`,
   * `topMap` for `forward`). If the current value has already moved PAST
   * what this page assumed — `back`: current `bottomMap[p]` is strictly
   * GREATER than `startPositions[p]`; `forward`: current `topMap[p]` is
   * strictly LESS than `startPositions[p]` — a CONCURRENT trim (a live
   * insert, or another page) recovered exactly the range this in-flight
   * page never asked about, and blindly merging its own row-derived
   * tightening would regress the map back past that just-recovered range:
   * an interior hole at the exact seam the trim was supposed to keep
   * whole. Such a page is REJECTED WHOLESALE — no rows inserted, no map
   * touched, `{ rejectedStale: true }` returned — rather than trying to
   * merge only the "safe" part of it (there isn't a locally-safe partial
   * merge: the page's own row-derived tightening has no way to know which
   * of its rows fall inside the newly-recovered range and which don't,
   * since `insertRows`' dedup only tells it about OFFSETS, not this map's
   * timeline). The caller should re-issue the same logical request from
   * the store's own fresh `edges()` instead.
   */
  insertPage(
    rows: MessageOut[],
    direction: TimelineDirection,
    startPositions: Record<number, number> | null,
    contCursor: string | null,
    /**
     * `attach`: only meaningful for a BACK-direction anchor bootstrap
     * (`startPositions === null`) — controls whether this bootstrap claims
     * the window covers the live tail (M-new, task-3 review carry-over).
     * Latest genuinely reads from `hi`, so it attaches; Offset/Timestamp
     * anchors land mid-topic and must NOT attach even though they, too, are
     * back-direction anchor bootstraps with no request cursor. A forward
     * anchor bootstrap (Beginning) ignores this entirely — its own
     * attached transition only ever comes from a later forward page
     * reporting `exhausted`, never from its own bootstrap. Defaults to
     * `true` for the unit-test suite's ergonomics (most anchor-bootstrap
     * tests here model Latest); production callers (Timeline.tsx) must
     * always pass this explicitly, on every anchor call, and never lean on
     * the default.
     */
    opts?: { attach?: boolean },
  ): InsertOutcome
  /**
   * Live-tail rows: always extend the newest (top) end.
   *
   * PRECONDITION (enforced — throws if violated): only call this while
   * attached (`isAttached()`/`InsertOutcome.attached` — NOT `edges().top
   * === null`, which is not a safe proxy; see `InsertOutcome.attached`'s own
   * doc comment). This is not an arbitrary rule: the
   * per-partition ceiling this method advances `topMap` with is only sound
   * while attached, because attached means every partition is genuinely
   * caught up to the tail — the live feed then delivers each partition's
   * real records in strictly increasing offset order, so any apparent
   * "jump" between what the window already holds and a new live row can
   * only be a span of legitimate holes (compaction tombstones, transaction
   * markers), never a skipped real record. Advancing past a hole loses
   * nothing. While DETACHED, a partition can be mid-recovery (some of its
   * offsets evicted and still owed to a future forward page) and a live
   * row arriving anyway could sit above that gap — advancing the ceiling
   * would then abandon the gap forever, which is exactly why v1.3's design
   * buffers live rows instead of applying them while detached: this store
   * enforces that contract at the boundary rather than trying to make the
   * update rule safe for a case that should never reach it.
   */
  insertLive(rows: MessageOut[]): InsertOutcome
  /** Merged rows, newest-first (partition offset order within a partition, timestamp merge across partitions). */
  rows(): readonly MessageOut[]
  /**
   * Fix round 1, M2: a READ-ONLY preview of what `rows()` would look like
   * with `extraRows` also merged in — WITHOUT mutating anything (no `seen`
   * update, no edge-map update, no cap enforcement). Lets a caller display
   * an in-flight page's own accumulated matches progressively (product
   * charter: "stream results") while the store itself still only ever
   * commits once, atomically, at page-end (`insertPage`'s own contract:
   * an anchor bootstrap's opposite-side seed needs the FULL page's rows,
   * not a per-batch trickle, so the store can't sanely accept a page in
   * pieces). Rows already committed (already in `seen`) are silently
   * excluded from the overlay — they're already in `rows()` itself, and
   * double-counting them would be a visible duplicate. Cheap: bounded by
   * `extraRows`' own size (a page's matches, capped at the page limit),
   * not the whole window.
   */
  previewWithOverlay(extraRows: readonly MessageOut[]): readonly MessageOut[]
  /**
   * Cursors minted from the current edge maps. `top` is `null` while the top
   * edge is genuinely the live tail (`isAttached()`), OR while `topMap` is
   * empty or INCOMPLETE — see the "one gap" section above and fix round 1's
   * C2: an anchor bootstrap's opposite-side seed only ever covers
   * partitions that returned ≥1 row this page, so a partition contributing
   * zero rows would otherwise be silently omitted from a minted cursor,
   * worse than null (nothing would ever trigger recovery for it). `bottom`
   * is `null` only before the store has ever learned a bottom position at
   * all (an empty, freshly created store) — no equivalent completeness
   * check on this side yet (see the test suite's own note on the
   * symmetric, currently-unaddressed gap).
   */
  edges(): { top: string | null; bottom: string | null }
  /**
   * The store's real, ungasked attachment state (fix round 1, C1) — same
   * value as `InsertOutcome.attached`; `edges().top === null` is NOT a safe
   * proxy for it (see `InsertOutcome.attached`'s own doc comment).
   *
   * Test/introspection surface only: every production call site already has
   * an `InsertOutcome` in hand and reads `.attached` off that instead — see
   * `Timeline.tsx`'s own commit path. Kept on the interface for the test
   * suite's convenience (asserting attachment without threading an outcome
   * through).
   */
  isAttached(): boolean
  clear(): void
  /** Test/introspection surface only — no production caller (Timeline.tsx reads `rows().length`). */
  totalCount(): number
}

/**
 * Row-exact sliding window over the merged timeline, with client-owned edge
 * maps that make a trimmed-away (or never-yet-loaded) region re-readable
 * gap-free and duplicate-free, per partition, no matter how the trim
 * happened or which side of the window a partition currently has zero rows
 * in.
 *
 * ## Edge-map maintenance invariant (read this before touching a map-update
 * site)
 *
 * **Revision history, so the reasoning that got REJECTED is on record and
 * doesn't get reinvented:** an earlier version of this comment claimed "a
 * partition not touched by a trim already correctly describes its
 * boundary" — true only once `insertLive` also touched `topMap`, which it
 * originally didn't (fix round 1, C1: a live-delivered row could sit above
 * a stale or absent `topMap` entry forever). The FIRST fix for that added a
 * live-insert ceiling GUARDED by "only advance if this insert's own lowest
 * offset is at or below the existing boundary." Round 2 review proved that
 * guard's protection was illusory: given a window holding partition P's
 * offsets 90-99 with `topMap[P] = 100`, a live insert of `P:150` is
 * discontinuous with the boundary either way — guarding leaves `topMap[P]`
 * at 100 (offset 150 sits in the window UNRECORDED by either map, silently
 * unaccounted, not merely "stale") while removing the guard advances to 151
 * and stray `[100,150)` is stranded. Neither choice is safe **as a general
 * rule** — which is the tell that the right fix isn't a smarter update
 * rule, it's a precondition on when `insertLive` may be called at all. See
 * below.
 *
 * **The resolution:** `insertLive` throws unless the store is attached (the
 * internal `attached` flag — NOT `edges().top === null`, which this same
 * comment's earlier revision already showed is a different condition) —
 * enforced at the top of the method, not left to caller discipline. This
 * isn't an arbitrary restriction; it's the exact
 * condition that makes an UNGUARDED, unconditional per-partition ceiling
 * correct:
 *
 * - While **attached**, every partition is, by definition, genuinely caught
 *   up to the tail (nothing anywhere is mid-recovery — see `attached`'s own
 *   paragraph below for why that's true store-wide, not just per
 *   partition). The live feed (a real Kafka tail) delivers each partition's
 *   records in strictly increasing offset order. So ANY live batch for a
 *   partition already represented in the window is either contiguous with
 *   what's held, or separated from it only by a span of offsets that are
 *   real Kafka holes (compaction tombstones, transaction control records —
 *   offsets that exist but were never going to produce a row). Advancing
 *   `topMap` across a hole span loses nothing, because there was never a
 *   real record there to lose. A partition with NO prior window presence
 *   (first-ever appearance, live-delivered) has no boundary to be
 *   discontinuous with in the first place. Either way, the plain
 *   `topMap.set(p, max(topMap.get(p) ?? -Infinity, max(inserted offsets for
 *   p) + 1))` ceiling is exact — no guard needed, none present.
 * - While **detached**, a partition CAN be mid-recovery (rule 3's trim
 *   bookkeeping is exactly what tracks that), and nothing about a live
 *   feed's offset-ordering guarantee helps: a row arriving "live" while
 *   detached could easily sit above a real, unrecovered gap. This is
 *   precisely why v1.3 (and Task 3, which owns the live subscription)
 *   buffers live rows instead of applying them while detached — the same
 *   "auto-pause" behavior the design doc describes for the live pill. This
 *   store enforces that contract at its own boundary (throws) rather than
 *   trying to make the update rule safe for a case that should never reach
 *   it: a caller bug here is a programming error, not a data condition to
 *   quietly tolerate.
 *
 * Both maps are updated by exactly three kinds of event — plus one narrow,
 * explicitly-scoped fourth (the anchor opposite-side seed, below) — and
 * never by any other ad hoc path. This is what keeps a trimmed (or
 * never-yet-loaded) region's re-read exact and duplicate-free:
 *
 * 1. **Same-direction page continuation** (authoritative, hole-safe): every
 *    `insertPage` call updates the map matching its own `direction`
 *    (`back` → `bottomMap`, `forward` → `topMap`) straight from the
 *    response's `contCursor`, when non-null. This mirrors the backend's own
 *    `cur_positions` exactly (see `Cursor`'s doc comment and `run_page` in
 *    `backend/src/message/timeline.rs`): it already accounts for holes,
 *    already covers every partition the request touched (including ones
 *    that contributed zero rows this page but still have a position), and
 *    needs no row-offset math on the frontend side at all. Just before this,
 *    the SAME map is also tightened from this call's own inserted row
 *    offsets (`min` for `bottomMap`, `max + 1` for `topMap`) — a row-derived
 *    floor/ceiling, never a regression, that matters only when `contCursor`
 *    is null: a page can legitimately deliver rows and *still* report
 *    `exhausted` (it simply hit the true watermark on this very call), and
 *    without this the map would sit one page short of what's actually now in
 *    the window, letting a later follow-cursor re-fetch rows already held —
 *    a duplicate. Whenever `contCursor` is non-null, rule 1's authoritative
 *    value immediately overrides this estimate anyway.
 * 2. **Live insert while attached** (`insertLive`, its own ceiling):
 *    unconditionally `topMap.set(p, max(topMap.get(p) ?? -Infinity,
 *    max(inserted offsets for p) + 1))`, per partition the batch touched —
 *    the ONLY thing that ever advances `topMap` for a live-delivered
 *    partition, since live rows carry no cursor at all. Sound only because
 *    of the attached precondition above; the method throws otherwise, so
 *    this rule and its precondition must be read together, never one
 *    without the other.
 *
 *    The BOTTOM map needs no equivalent live-side mechanism, and this is a
 *    completeness argument, not an omission: while attached, every live (or
 *    re-attach-flush — see below) row's offset is, per partition, always
 *    ≥ that partition's high watermark at the moment of attachment, which
 *    is itself ≥ every offset already in the window for that partition,
 *    which is itself ≥ `bottomMap[p]` (the exclusive upper bound for
 *    reading older — always at or below the window's own oldest-held
 *    offset by construction of rule 3 below). A NEWLY-arriving live row can
 *    therefore never legally sit below `bottomMap[p]` — but `insertLive`
 *    still filters against it anyway; see its own comment for why that
 *    filter is load-bearing for the re-attach buffer flush, not merely
 *    defensive.
 *
 *    **Re-attach buffer flush** (v1.3 semantics, modeled here explicitly):
 *    when a detached window re-attaches (a forward page reports
 *    `exhausted`), any live rows Task 3 buffered while detached get flushed
 *    through `insertLive` immediately after — now legal, since the store is
 *    attached again. Every one of those rows lands in one of three ways: (a)
 *    strictly above the window's current top (a genuine extension — the
 *    ceiling advances normally); (b) an exact duplicate of an offset the
 *    just-exhausted forward page already fetched — `insertRows`' `seen`
 *    dedup silently drops it, same as any other overlapping delivery; or (c)
 *    at or below `bottomMap[p]` — `insertLive`'s own `safeRows` filter drops
 *    it (see its own comment; this is the real-usage case that filter
 *    exists for, not a hypothetical). None of the three is a gap risk.
 * 3. **Row-exact trim** (`enforceCap`): when a trim removes rows from a
 *    side, the map on THAT side is updated, per partition, from the EXACT
 *    offsets just trimmed for that partition — never from a broader "what's
 *    left" computation. Top trim (bottom-insert overflow): for each
 *    partition with ≥1 row trimmed, `topMap.set(p, min(trimmed offsets for
 *    p))` — the smallest trimmed offset is exactly the inclusive lower bound
 *    that recovers the trimmed set and nothing already retained (rows below
 *    it are still in the window; nothing above it existed). Bottom trim
 *    (top/live-insert overflow): for each partition with ≥1 row trimmed,
 *    `bottomMap.set(p, max(trimmed offsets for p) + 1)` — one past the
 *    largest trimmed offset is exactly the exclusive upper bound that
 *    recovers the trimmed set. This is also what makes "a partition fully
 *    trimmed out of the window" safe: its rows vanish from `partitions`, but
 *    its map entry — planted by rule 1, 2, or an earlier rule-3 event — is
 *    untouched by that disappearance and remains exactly correct.
 *
 *    **Completeness backfill, top trim only:** a partition NOT touched by
 *    a given top trim is not automatically safe the way the paragraph
 *    above assumes for the side that trim didn't touch — it could hold
 *    rows that were never seeded by rule 1 or rule 2 at all (e.g. an
 *    anchor page that returned zero rows for it, discovered only later by
 *    a plain `insertPage('back', ...)` call, which never touches `topMap`).
 *    So every top trim ALSO backfills, for every partition still holding
 *    rows that this trim did NOT touch, `topMap.set(p, max(remaining
 *    offsets for p) + 1)` — computed directly from `partitions`' actual
 *    post-trim contents, not from any prior map state. This makes
 *    `topMap`, immediately after any top trim (the exact moment `attached`
 *    flips `false` and the map stops being masked), complete over every
 *    partition the window currently holds — regardless of which of rules
 *    1/2/3 last touched any given partition, or whether any of them ever
 *    did. (This is also the mutation-tested "only load-bearing path" case:
 *    a partition holding rows the CURRENT trim doesn't touch, whose
 *    `topMap` entry has no other source — see `timelineStore.test.ts`.)
 *
 * The one gap none of the three rules fill on their own: an **anchor page**
 * (no request cursor exists at all, `startPositions === null`) has no
 * continuation on its OPPOSITE side to draw from (rule 1 only updates the
 * SAME-direction map) and nothing has ever been trimmed there yet (rule 3
 * hasn't fired). For that one case, and ONLY that case, the opposite-side
 * map is seeded directly from the just-fetched rows' own offsets — per
 * partition, `max(offset) + 1` (back anchor's opposite/top side) or
 * `min(offset)` (forward anchor's opposite/bottom side) — but ONLY when
 * that partition's entry is still ABSENT (fix round 2, N2): this same
 * window can already hold a genuinely-established value for the opposite
 * side from an EARLIER insert with no `clear()` in between, and an
 * unrelated page's own row offsets are never grounds to clobber it — see
 * `insertPage`'s own doc comment on this seeding step. N2's motivating call
 * site (a `loadNewer` forward-anchor fallback that re-issued an anchor
 * without an intervening `clear()`) was deleted from `Timeline.tsx` and is
 * now production-unreachable — every anchor re-issue there goes through
 * `resetWindow`, which always `clear()`s first — so this guard is currently
 * defensive-only, not exercised by any live call site. This is the
 * documented exception ("the edge map seeds from the response continuation
 * + row offsets instead" — see the design doc and this store's own brief);
 * it's exact as long as the anchor's own starting offset has no leading hole
 * immediately at the watermark (a known, accepted limitation shared with the
 * caption logic — see the design doc's "beginning of topic" caption note —
 * not something this store can fix without knowing the true watermark, which
 * `insertPage`'s signature never receives).
 *
 * **When an incomplete opposite-seed actually matters (fix round 2, N1):**
 * the C2 completeness check (`edges()`'s own doc comment) that masks
 * `edges().top` when `topMap` is missing a partition `bottomMap` knows
 * about is only CONSULTED when `topCompletenessMatters` — set true by a
 * HISTORICAL (`attach: false`) back-anchor bootstrap, never by a Latest
 * (`attach: true`) one. A cold partition a Latest bootstrap never saw a row
 * for hides nothing (everything above a Latest anchor is, by construction,
 * the live tail, for every partition); treating it as an incompleteness
 * risk anyway left `edges().top` masked `null` FOREVER once such a window
 * detached — a dead scroll-up with no cursor and no anchor-forward fallback
 * to fall back to for a 'default' context, trapping the reader.
 *
 * `attached` (top-edge liveness) changes on exactly two events, independent
 * of the maps themselves: it becomes `true` on a back-direction anchor
 * bootstrap (Latest reads from `hi` by construction — genuinely the tail —
 * and Offset/Timestamp anchors read backward too, for which this is a
 * harmless over-approximation given the design doc's own accepted
 * limitation that those anchors never open a forward cursor in practice) or
 * when a forward page reports `exhausted` (`contCursor === null` — caught
 * back up to the tail); it becomes `false` on any top trim. `edges().top`
 * reports `null` while `attached` (and also, separately, while `topMap` is
 * empty or incomplete — see `edges()`'s own doc comment for the C2
 * completeness check) — the map keeps tracking underneath regardless, so the
 * moment a top trim detaches the window, a correct boundary is already
 * sitting there from whichever rule last touched it. Crucially, a forward
 * page only reports
 * `exhausted` once EVERY partition in that request's own cursor has reached
 * its true high watermark — so `attached === true` really does mean every
 * tracked partition is caught up store-wide, not just the partitions that
 * page happened to mention; this is what rule 2's soundness argument above
 * leans on.
 */
export function createSlidingWindowStore(cap = 2000): SlidingWindowStore {
  const partitions = new Map<number, MessageOut[]>()
  const seen = new Set<string>()
  const bottomMap = new Map<number, number>()
  const topMap = new Map<number, number>()
  let attached = false
  // Fix round 2, N1 (the blocker on the C2 completeness check): whether an
  // incomplete `topMap` (relative to `bottomMap`) is a real risk worth
  // masking `edges().top` for at all. Set true ONLY by a HISTORICAL
  // (`attach: false`) back-anchor bootstrap — a cold/empty partition
  // omitted from `topMap`'s opposite-side seed there genuinely hides a
  // forward gap (the C2 scenario). A LATEST (`attach: true`) bootstrap
  // never sets this: everything above a Latest anchor is, by construction,
  // the live tail — a cold partition contributing zero rows there has
  // nothing above it to hide (nothing has been produced yet, for ANY
  // partition). Left false by default (and reset by `clear()`) so a
  // Latest-mounted window's `edges().top` is trusted the instant a trim
  // detaches it, same as before the C2 fix ever existed.
  let topCompletenessMatters = false
  let cachedRows: readonly MessageOut[] | null = null

  function totalCount(): number {
    let n = 0
    for (const arr of partitions.values()) n += arr.length
    return n
  }

  // Parameterized over WHICH partitions map to merge (fix round 1, M2):
  // `mergeRows()` below merges the real, committed `partitions`;
  // `previewWithOverlay` merges a throwaway temporary map instead, so the
  // two never duplicate this k-way merge logic (and can't drift apart).
  function mergeRowsFrom(parts: ReadonlyMap<number, readonly MessageOut[]>): MessageOut[] {
    const entries = [...parts.entries()].sort((a, b) => a[0] - b[0])
    const ptrs = entries.map(([, arr]) => arr.length - 1)
    const result: MessageOut[] = []
    for (;;) {
      let bestIdx = -1
      let bestTs = -Infinity
      for (let i = 0; i < entries.length; i++) {
        const ptr = ptrs[i]
        if (ptr < 0) continue
        const ts = entries[i][1][ptr].timestamp_ms ?? -Infinity
        if (bestIdx === -1 || ts > bestTs) {
          bestIdx = i
          bestTs = ts
        }
      }
      if (bestIdx === -1) break
      const ptr = ptrs[bestIdx]
      result.push(entries[bestIdx][1][ptr])
      ptrs[bestIdx] = ptr - 1
    }
    return result
  }

  function mergeRows(): MessageOut[] {
    return mergeRowsFrom(partitions)
  }

  function removeMessage(msg: MessageOut): void {
    const arr = partitions.get(msg.partition)
    if (!arr) return
    const idx = findOffsetIndex(arr, msg.offset)
    if (idx >= 0) arr.splice(idx, 1)
    if (arr.length === 0) partitions.delete(msg.partition)
    seen.delete(partitionKey(msg.partition, msg.offset))
  }

  /**
   * Inserts (deduped) rows, returning the min/max NEWLY inserted offset per
   * partition — used for anchor opposite-side seeding, the same-direction
   * row-derived floor/ceiling, and `insertLive`'s ceiling.
   */
  function insertRows(rows: MessageOut[]): { insertedMin: Map<number, number>; insertedMax: Map<number, number> } {
    const insertedMin = new Map<number, number>()
    const insertedMax = new Map<number, number>()
    let changed = false
    for (const msg of rows) {
      const k = partitionKey(msg.partition, msg.offset)
      if (seen.has(k)) continue
      seen.add(k)
      let arr = partitions.get(msg.partition)
      if (!arr) {
        arr = []
        partitions.set(msg.partition, arr)
      }
      insertByOffsetAscending(arr, msg)
      changed = true
      insertedMin.set(msg.partition, Math.min(insertedMin.get(msg.partition) ?? Infinity, msg.offset))
      insertedMax.set(msg.partition, Math.max(insertedMax.get(msg.partition) ?? -Infinity, msg.offset))
    }
    if (changed) cachedRows = null
    return { insertedMin, insertedMax }
  }

  function mapToPositions(map: Map<number, number>): Record<number, number> {
    return Object.fromEntries(map)
  }

  // Loop-based min/max — NOT a spread into `Math.min`/`Math.max` (`Math.min(...xs)`),
  // which blows the call stack (`RangeError`) somewhere past ~65k arguments.
  // The only call sites pass a single trim's per-partition offsets, bounded
  // by `excess` (i.e. by `cap`) — safely under that limit today — but the
  // loop form costs nothing and stays correct even if a future caller feeds
  // it something larger.
  function minOf(xs: number[]): number {
    let m = Infinity
    for (const x of xs) if (x < m) m = x
    return m
  }
  function maxOf(xs: number[]): number {
    let m = -Infinity
    for (const x of xs) if (x > m) m = x
    return m
  }

  function mergeCursorInto(map: Map<number, number>, cursor: string): void {
    const decoded = decodeCursor(cursor)
    for (const [p, v] of Object.entries(decoded)) map.set(Number(p), v)
  }

  /**
   * Row-exact cap enforcement: `kind` is which side just grew
   * ('bottom-insert' for a `back` page — extends older rows — 'top-insert'
   * for a `forward` page or a live insert — extends newer rows). Overflow
   * always trims from the OPPOSITE end, exactly `excess` rows, updating that
   * side's edge map from the exact trimmed offsets (rule 3, plus its
   * completeness backfill on a top trim — see this function's home doc
   * comment above). A top trim also detaches the window.
   */
  function enforceCap(kind: 'bottom-insert' | 'top-insert'): InsertOutcome {
    const excess = totalCount() - cap
    if (excess <= 0) return { trimmedTop: 0, trimmedBottom: 0, attached, rejectedStale: false }
    const merged = mergeRows() // newest-first
    const trimmedByPartition = new Map<number, number[]>()
    const recordTrim = (msg: MessageOut) => {
      let offsets = trimmedByPartition.get(msg.partition)
      if (!offsets) {
        offsets = []
        trimmedByPartition.set(msg.partition, offsets)
      }
      offsets.push(msg.offset)
      removeMessage(msg)
    }
    let result: InsertOutcome
    if (kind === 'bottom-insert') {
      // A back-page insert grows the bottom; overflow trims the TOP
      // (newest) rows exactly — the window slides down and detaches.
      for (let i = 0; i < excess; i++) recordTrim(merged[i])
      for (const [p, offsets] of trimmedByPartition) topMap.set(p, minOf(offsets))

      // Completeness backfill (fix round 1, C1b; mutation-verified — see
      // timelineStore.test.ts's N1 tests): a partition can be
      // holding rows in the window RIGHT NOW without ever having had its
      // own top boundary planted — e.g. it contributed zero rows to the
      // anchor page (so the anchor opposite-side seed skipped it), or was
      // first discovered by a plain `back` page (rule 1 only ever touches
      // `bottomMap` for that direction) — and hasn't been top-trimmed
      // before either. Any partition still holding rows that THIS trim did
      // NOT touch gets its boundary set fresh from what's actually still
      // there — `max(remaining offsets) + 1` — so the minted top edge is
      // complete over every partition the window holds the instant
      // `attached` flips `false` and `edges().top` stops being masked,
      // regardless of which of rules 1/2/3 last touched any given
      // partition, or whether any of them ever did.
      for (const [p, arr] of partitions) {
        if (trimmedByPartition.has(p) || arr.length === 0) continue
        topMap.set(p, arr[arr.length - 1].offset + 1) // `arr` is offset-ascending: last entry is the max
      }
      attached = false
      result = { trimmedTop: excess, trimmedBottom: 0, attached, rejectedStale: false }
    } else {
      // A forward-page or live insert grows the top; overflow trims the
      // BOTTOM (oldest) rows exactly.
      for (let i = 0; i < excess; i++) recordTrim(merged[merged.length - 1 - i])
      for (const [p, offsets] of trimmedByPartition) bottomMap.set(p, maxOf(offsets) + 1)
      result = { trimmedTop: 0, trimmedBottom: excess, attached, rejectedStale: false }
    }
    cachedRows = null
    return result
  }

  /**
   * Fix round 1, C3: is `startPositions` (this page's own request-time
   * positions) still safe to merge, or has the current same-side map
   * already moved PAST it (a concurrent trim recovered exactly the range
   * this page never asked about)? `null` startPositions (an anchor page)
   * is never stale — it has no prior request to be stale relative to.
   */
  function isStale(direction: TimelineDirection, startPositions: Record<number, number> | null): boolean {
    if (startPositions === null) return false
    const map = direction === 'back' ? bottomMap : topMap
    for (const [pStr, startPos] of Object.entries(startPositions)) {
      const current = map.get(Number(pStr))
      if (current === undefined) continue // nothing to compare against yet — trust the page
      if (direction === 'back' ? current > startPos : current < startPos) return true
    }
    return false
  }

  return {
    insertPage(rows, direction, startPositions, contCursor, opts) {
      // Staleness precondition (fix round 1, C3) — see this method's own
      // doc comment. Checked BEFORE any mutation: a rejected page must
      // leave the store byte-for-byte as it was.
      if (isStale(direction, startPositions)) {
        return { trimmedTop: 0, trimmedBottom: 0, attached, rejectedStale: true }
      }
      const { insertedMin, insertedMax } = insertRows(rows)

      if (startPositions === null) {
        // Anchor bootstrap: seed the OPPOSITE-direction edge from row
        // offsets — the one case with no continuation to draw from on that
        // side at all (see the home doc comment's "one gap none of the three
        // rules fill" section).
        //
        // Fix round 2, N2: only fill entries that are still ABSENT
        // outright — never unconditionally `.set()`. An opposite-side seed
        // is an ESTIMATE for a side this request didn't actually focus on
        // (it only reflects THIS page's own row offsets, nothing about the
        // map's true history), and this same window can already hold a
        // genuinely-established, independently-verified value for it from
        // an EARLIER insert (no `clear()` in between — e.g. Timeline's
        // loadNewer forward-anchor fallback re-issuing an anchor while
        // `bottomMap` already holds the real edge from the window's own
        // first bootstrap). Deferring entirely to whatever's already
        // there — rather than trying to "merge" the estimate in via some
        // min/max rule — is the simplest rule that can only ever leave the
        // map correct: an already-present entry was necessarily planted by
        // a verified source (rule 1, the live ceiling, a trim, or an
        // earlier opposite-seed of its own), which this unrelated page's
        // own row offsets have no way to improve on or safely combine with.
        if (direction === 'back') {
          topCompletenessMatters ||= !(opts?.attach ?? true)
          for (const [p, max] of insertedMax) if (!topMap.has(p)) topMap.set(p, max + 1)
        } else {
          for (const [p, min] of insertedMin) if (!bottomMap.has(p)) bottomMap.set(p, min)
        }
      }

      // Same-direction edge, row-derived floor/ceiling: keeps the map
      // advancing even when `contCursor` is null (exhausted) — a page can
      // legitimately deliver rows and *still* report exhausted (it just hit
      // the true watermark on this call), and without this the map would be
      // stuck one page short of what's actually now in the window, letting
      // a later follow-cursor re-fetch rows already held (a duplicate). Safe
      // because a same-direction page's own rows are always adjacent to
      // (never past) the existing boundary, so this can only tighten it —
      // rule 1 below then overrides with the exact, hole-safe value whenever
      // one is available.
      if (direction === 'back') {
        for (const [p, min] of insertedMin) bottomMap.set(p, Math.min(bottomMap.get(p) ?? Infinity, min))
      } else {
        for (const [p, max] of insertedMax) topMap.set(p, Math.max(topMap.get(p) ?? -Infinity, max + 1))
      }

      // Same-direction edge: always authoritative from the response's own
      // continuation cursor when present (rule 1) — overrides the row-derived
      // value above with the exact, hole-safe backend-confirmed position.
      if (contCursor !== null) {
        mergeCursorInto(direction === 'back' ? bottomMap : topMap, contCursor)
      }

      // Attached transitions (see home doc comment): a back-anchor bootstrap
      // starts at the tail — but ONLY when the caller confirms this
      // particular bootstrap is Latest-style (M-new: Offset/Timestamp
      // anchors are also back-direction, startPositions === null bootstraps,
      // but they land mid-topic, not at the tail, and must stay detached —
      // see `opts.attach`'s own doc comment above). A forward page reporting
      // `exhausted` caught back up to the tail regardless of how the window
      // got here.
      if (direction === 'back' && startPositions === null && (opts?.attach ?? true)) attached = true
      if (direction === 'forward' && contCursor === null) attached = true

      return enforceCap(direction === 'back' ? 'bottom-insert' : 'top-insert')
    },
    insertLive(rows) {
      // Precondition, enforced (fix round 2 — see the class doc comment's
      // "The resolution" section): only sound while attached. A caller
      // violating this is a programming error (Task 3 must buffer live rows
      // while detached, per v1.3), not a data condition to tolerate quietly.
      if (!attached) {
        throw new Error(
          'insertLive called while detached: live rows must be buffered by the caller until the window re-attaches (isAttached() / InsertOutcome.attached becomes true)',
        )
      }
      // N2 defensive drop — filter out anything at or below `bottomMap[p]`
      // before inserting. This is NOT purely hypothetical belt-and-
      // suspenders against some future caller bug (L-new, task-3 review
      // carry-over): it IS reached in real usage by Task 3's re-attach
      // buffer flush. A live row buffered while detached carries an offset
      // that was genuinely above the window's top at the moment it was
      // buffered — but forward paging can keep running for a while after
      // that (advancing `bottomMap` via rule 1's row-derived tightening, or
      // via a bottom trim) before the buffer is actually flushed through
      // this method, once the store re-attaches. A buffered row can
      // therefore legitimately arrive here already covered by what forward
      // recovery has since fetched (or already trimmed back out) — this
      // filter is what makes that safe, not just defensive. One
      // consequence worth flagging rather than silently accepting: a row
      // this filter drops is counted NOWHERE (no top/bottom trim counter,
      // no caller-visible signal at all) — an accepted limitation (the row
      // was never displayed and never will be, so there is nothing
      // meaningful to surface), not a bug, but deliberately noted here so
      // it isn't mistaken for telemetry this method doesn't provide.
      const safeRows = rows.filter((r) => r.offset >= (bottomMap.get(r.partition) ?? -Infinity))
      const { insertedMax } = insertRows(safeRows)
      // Unconditional ceiling (fix round 2, restored — see the class doc
      // comment's rule 2): sound only because of the attached check above.
      for (const [p, max] of insertedMax) topMap.set(p, Math.max(topMap.get(p) ?? -Infinity, max + 1))
      return enforceCap('top-insert')
    },
    rows() {
      if (cachedRows === null) cachedRows = Object.freeze(mergeRows())
      return cachedRows
    },
    previewWithOverlay(extraRows) {
      if (extraRows.length === 0) {
        if (cachedRows === null) cachedRows = Object.freeze(mergeRows())
        return cachedRows
      }
      // Shallow-clone the partitions map: untouched partitions share the
      // SAME underlying array (safe — read-only from here), only the
      // partitions `extraRows` actually touches get their own cloned copy
      // before insertion, so `partitions` itself is never mutated.
      const temp = new Map<number, MessageOut[]>(partitions)
      const tempSeen = new Set<string>()
      const cloned = new Set<number>()
      for (const msg of extraRows) {
        const k = partitionKey(msg.partition, msg.offset)
        if (seen.has(k) || tempSeen.has(k)) continue // already committed, or already added to this overlay
        tempSeen.add(k)
        if (!cloned.has(msg.partition)) {
          temp.set(msg.partition, [...(temp.get(msg.partition) ?? [])])
          cloned.add(msg.partition)
        }
        insertByOffsetAscending(temp.get(msg.partition)!, msg)
      }
      return Object.freeze(mergeRowsFrom(temp))
    },
    edges() {
      const bottom = bottomMap.size === 0 ? null : encodeCursor(mapToPositions(bottomMap))
      // C2 completeness check (see this method's own doc comment above):
      // `topMap` is trustworthy as a minted cursor only if it has an entry
      // for every partition `bottomMap` knows about — `bottomMap`, right
      // after any back-direction request, is authoritatively complete over
      // every partition that request touched (rule 1), which for an ANCHOR
      // request is every partition of the topic. `topMap`'s own anchor
      // opposite-seed has no equivalent guarantee (only partitions with
      // ≥1 row this page get seeded) — this is the ONLY event that can
      // create the discrepancy this check catches.
      //
      // Fix round 2, N1: this check is only CONSULTED when
      // `topCompletenessMatters` — i.e. when the window's provenance
      // includes a historical anchor bootstrap. A Latest-only window never
      // sets that flag, so an omitted cold partition (harmless there — see
      // the flag's own doc comment) never masks `edges().top`.
      const topComplete = !topCompletenessMatters || [...bottomMap.keys()].every((p) => topMap.has(p))
      const top = attached || topMap.size === 0 || !topComplete ? null : encodeCursor(mapToPositions(topMap))
      return { top, bottom }
    },
    isAttached() {
      return attached
    },
    clear() {
      partitions.clear()
      seen.clear()
      bottomMap.clear()
      topMap.clear()
      attached = false
      topCompletenessMatters = false
      cachedRows = null
    },
    totalCount,
  }
}
