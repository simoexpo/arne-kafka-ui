import type { MessageOut } from '../api/types'
import { decodeCursor, encodeCursor } from './timelineCursor'

export type InsertOrigin = 'live' | 'back' | 'forward'

export interface Dropped {
  top: number
  bottom: number
}

// What THIS insert call dropped (a delta), as opposed to `dropped()`'s
// running cumulative total — the Timeline needs the delta to know whether
// *this* insert invalidated an edge (see the v1.4 window-cap-honesty
// ruling), which a cumulative counter can't answer on its own.
export interface InsertResult {
  droppedTop: number
  droppedBottom: number
}

export interface TimelineStore {
  insert(msgs: MessageOut[], origin: InsertOrigin): InsertResult
  rows(): readonly MessageOut[]
  dropped(): Dropped
  clear(): void
}

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

/**
 * Merge-native read path: rows() is a k-way merge over per-partition,
 * offset-ordered streams. Within a partition, order is ALWAYS offset order
 * (never timestamp — producer clocks lie, offsets don't). Across partitions,
 * we interleave by timestamp, repeatedly taking whichever partition's current
 * head has the greatest timestamp; ties go to the smaller partition id.
 *
 * // TODO(task-3): delete — superseded by `createSlidingWindowStore` below
 * (spec v1.6). Kept only because `Timeline.tsx` still drives this v1.4-era
 * API (`insert`/`origin`/`InsertResult`); task 3 rewires `Timeline.tsx` onto
 * the new store and removes this one.
 */
export function createTimelineStore(cap = 2000): TimelineStore {
  const partitions = new Map<number, MessageOut[]>()
  const seen = new Set<string>()
  const dropCounts: Dropped = { top: 0, bottom: 0 }
  let cachedRows: readonly MessageOut[] | null = null

  function totalCount(): number {
    let n = 0
    for (const arr of partitions.values()) n += arr.length
    return n
  }

  function mergeRows(): MessageOut[] {
    const parts = [...partitions.entries()].sort((a, b) => a[0] - b[0])
    // Each partition array is offset-ascending; walk it from the end so the
    // "head" of each stream is its highest (newest-by-offset) remaining entry.
    const ptrs = parts.map(([, arr]) => arr.length - 1)
    const result: MessageOut[] = []
    for (;;) {
      let bestIdx = -1
      let bestTs = -Infinity
      for (let i = 0; i < parts.length; i++) {
        const ptr = ptrs[i]
        if (ptr < 0) continue
        const ts = parts[i][1][ptr].timestamp_ms ?? -Infinity
        // Strict `>` only: on a tie the first candidate found wins, and we
        // iterate partitions in ascending id order, so smaller partition id
        // wins ties as required.
        if (bestIdx === -1 || ts > bestTs) {
          bestIdx = i
          bestTs = ts
        }
      }
      if (bestIdx === -1) break
      const ptr = ptrs[bestIdx]
      result.push(parts[bestIdx][1][ptr])
      ptrs[bestIdx] = ptr - 1
    }
    return result
  }

  function removeMessage(msg: MessageOut): void {
    const arr = partitions.get(msg.partition)
    if (!arr) return
    const idx = arr.findIndex((m) => m.offset === msg.offset)
    if (idx >= 0) arr.splice(idx, 1)
    if (arr.length === 0) partitions.delete(msg.partition)
    seen.delete(partitionKey(msg.partition, msg.offset))
  }

  function enforceCap(origin: InsertOrigin): InsertResult {
    const excess = totalCount() - cap
    if (excess <= 0) return { droppedTop: 0, droppedBottom: 0 }
    const merged = mergeRows() // newest-first
    let result: InsertResult
    if (origin === 'back') {
      // Backward-fill overflow drops from the top (newest end) — the
      // opposite end from where 'back' inserts (older messages).
      for (let i = 0; i < excess; i++) removeMessage(merged[i])
      dropCounts.top += excess
      result = { droppedTop: excess, droppedBottom: 0 }
    } else {
      // live/forward overflow drops from the bottom (oldest end).
      for (let i = 0; i < excess; i++) removeMessage(merged[merged.length - 1 - i])
      dropCounts.bottom += excess
      result = { droppedTop: 0, droppedBottom: excess }
    }
    cachedRows = null
    return result
  }

  return {
    insert(msgs, origin) {
      let changed = false
      for (const msg of msgs) {
        const k = partitionKey(msg.partition, msg.offset)
        // Dedup on partition:offset: first-inserted record wins and later
        // duplicates are silently dropped. This matches the server's
        // re-emission overlap semantics (e.g. a 'back' page and a 'live'
        // tail can both deliver the same offset; only the first copy seen
        // is kept, subsequent identical offsets are no-ops).
        if (seen.has(k)) continue
        seen.add(k)
        let arr = partitions.get(msg.partition)
        if (!arr) {
          arr = []
          partitions.set(msg.partition, arr)
        }
        insertByOffsetAscending(arr, msg)
        changed = true
      }
      if (changed) cachedRows = null
      return enforceCap(origin)
    },
    rows() {
      // Freeze the memoized array so a consumer's in-place sort/splice can't
      // silently corrupt every subsequent read of the shared cache — it's
      // zero-copy (no clone) and mutation attempts throw in strict mode.
      if (cachedRows === null) cachedRows = Object.freeze(mergeRows())
      return cachedRows
    },
    dropped() {
      return { top: dropCounts.top, bottom: dropCounts.bottom }
    },
    clear() {
      partitions.clear()
      seen.clear()
      dropCounts.top = 0
      dropCounts.bottom = 0
      cachedRows = null
    },
  }
}

// ===========================================================================
// v1.6 sliding window store (spec: docs/superpowers/specs/2026-08-13-
// messages-timeline-design.md, "Sliding window (v1.6 — owner ruling)").
//
// This is a DELIBERATELY SEPARATE store, side by side with the v1.4-era
// `createTimelineStore` above rather than a replacement of it: `Timeline.tsx`
// (task 3's file, not touched here) still imports and drives the old store
// (`insert`/`origin`/`InsertResult`-with-booleans), and the frontend suite
// must keep compiling and passing until task 3 rewires it. `createTimelineStore`
// above is therefore frozen as-is — task 3 is expected to switch
// `Timeline.tsx` over to `createSlidingWindowStore` below and then delete the
// old one (marked `// TODO(task-3): delete` at its definition above).
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
}

export interface SlidingWindowStore {
  /**
   * Inserts one page's rows.
   *
   * `startPositions` is the cursor's positions the page was REQUESTED with,
   * or `null` for an anchor-fetched page (Latest/Beginning/Offset/Timestamp
   * — none of those start from a request cursor). `contCursor` is the
   * response's own continuation cursor, or `null` when the backend reported
   * `exhausted: true` (nothing more in `direction` from these positions).
   */
  insertPage(
    rows: MessageOut[],
    direction: 'back' | 'forward',
    startPositions: Record<number, number> | null,
    contCursor: string | null,
  ): InsertOutcome
  /**
   * Live-tail rows: always extend the newest (top) end.
   *
   * PRECONDITION (enforced — throws if violated): only call this while
   * attached (`edges().top === null`). This is not an arbitrary rule: the
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
  /** Merged rows, newest-first (partition offset order within a partition, timestamp merge across partitions — same rule as the v1.4 store above). */
  rows(): readonly MessageOut[]
  /**
   * Cursors minted from the current edge maps. `top` is `null` while the top
   * edge is genuinely the live tail (nothing has ever been trimmed off the
   * top since the last `clear()`, or a forward page has since caught back up
   * — see `attached` below); `bottom` is `null` only before the store has
   * ever learned a bottom position at all (an empty, freshly created store).
   */
  edges(): { top: string | null; bottom: string | null }
  clear(): void
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
 * **The resolution:** `insertLive` throws unless the store is attached
 * (`edges().top === null`) — enforced at the top of the method, not left
 * to caller discipline. This isn't an arbitrary restriction; it's the exact
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
 * Both maps are updated by exactly three kinds of event, and NEVER by a
 * fourth ad hoc path — this is what keeps a trimmed (or never-yet-loaded)
 * region's re-read exact and duplicate-free:
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
 *    offset by construction of rule 3 below). A live row can therefore
 *    never legally arrive below `bottomMap[p]`; there is no lower boundary
 *    for it to invalidate. (One cheap defensive line remains anyway — see
 *    `insertLive`'s own comment — purely as a belt-and-suspenders against a
 *    future caller bug, not because this invariant is expected to need it.)
 *
 *    **Re-attach buffer flush** (v1.3 semantics, modeled here explicitly):
 *    when a detached window re-attaches (a forward page reports
 *    `exhausted`), any live rows Task 3 buffered while detached get flushed
 *    through `insertLive` immediately after — now legal, since the store is
 *    attached again. Every one of those rows is, by the same offset-
 *    monotonicity argument, either strictly above the window's current top
 *    (a genuine extension — the ceiling advances normally) or at-or-below
 *    an offset the just-exhausted forward page already fetched (an exact
 *    duplicate — `insertRows`' `seen` dedup silently drops it, same as any
 *    other overlapping delivery). Neither case is a gap risk.
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
 *    `topMap` entry has no other source — see `timelineSlidingStore.test.ts`.)
 *
 * The one gap none of the three rules fill on their own: an **anchor page**
 * (no request cursor exists at all, `startPositions === null`) has no
 * continuation on its OPPOSITE side to draw from (rule 1 only updates the
 * SAME-direction map) and nothing has ever been trimmed there yet (rule 3
 * hasn't fired). For that one case, and ONLY that case, the opposite-side
 * map is seeded directly from the just-fetched rows' own offsets — per
 * partition, `max(offset) + 1` (back anchor's opposite/top side) or
 * `min(offset)` (forward anchor's opposite/bottom side). This is the
 * documented exception ("the edge map seeds from the response continuation
 * + row offsets instead" — see the design doc and this store's own brief);
 * it's exact as long as the anchor's own starting offset has no leading hole
 * immediately at the watermark (a known, accepted limitation shared with the
 * caption logic — see the design doc's "beginning of topic" caption note —
 * not something this store can fix without knowing the true watermark, which
 * `insertPage`'s signature never receives).
 *
 * `attached` (top-edge liveness) changes on exactly two events, independent
 * of the maps themselves: it becomes `true` on a back-direction anchor
 * bootstrap (Latest reads from `hi` by construction — genuinely the tail —
 * and Offset/Timestamp anchors read backward too, for which this is a
 * harmless over-approximation given the design doc's own accepted
 * limitation that those anchors never open a forward cursor in practice) or
 * when a forward page reports `exhausted` (`contCursor === null` — caught
 * back up to the tail); it becomes `false` on any top trim. `edges().top`
 * reports `null` exactly while `attached`, regardless of `topMap`'s stored
 * content — the map keeps tracking underneath so the moment a top trim
 * detaches the window, a correct boundary is already sitting there from
 * whichever rule last touched it. Crucially, a forward page only reports
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
  let cachedRows: readonly MessageOut[] | null = null

  function totalCount(): number {
    let n = 0
    for (const arr of partitions.values()) n += arr.length
    return n
  }

  function mergeRows(): MessageOut[] {
    const parts = [...partitions.entries()].sort((a, b) => a[0] - b[0])
    const ptrs = parts.map(([, arr]) => arr.length - 1)
    const result: MessageOut[] = []
    for (;;) {
      let bestIdx = -1
      let bestTs = -Infinity
      for (let i = 0; i < parts.length; i++) {
        const ptr = ptrs[i]
        if (ptr < 0) continue
        const ts = parts[i][1][ptr].timestamp_ms ?? -Infinity
        if (bestIdx === -1 || ts > bestTs) {
          bestIdx = i
          bestTs = ts
        }
      }
      if (bestIdx === -1) break
      const ptr = ptrs[bestIdx]
      result.push(parts[bestIdx][1][ptr])
      ptrs[bestIdx] = ptr - 1
    }
    return result
  }

  function removeMessage(msg: MessageOut): void {
    const arr = partitions.get(msg.partition)
    if (!arr) return
    const idx = arr.findIndex((m) => m.offset === msg.offset)
    if (idx >= 0) arr.splice(idx, 1)
    if (arr.length === 0) partitions.delete(msg.partition)
    seen.delete(partitionKey(msg.partition, msg.offset))
  }

  /** Inserts (deduped) rows, returning the min/max NEWLY inserted offset per partition — used only for anchor opposite-side seeding. */
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
  // `xs` here is bounded by a single trim/insert's size, which has no
  // built-in ceiling, so this must hold for arbitrarily large inputs.
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
    if (excess <= 0) return { trimmedTop: 0, trimmedBottom: 0 }
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
      // timelineSlidingStore.test.ts's N1 tests): a partition can be
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
      result = { trimmedTop: excess, trimmedBottom: 0 }
    } else {
      // A forward-page or live insert grows the top; overflow trims the
      // BOTTOM (oldest) rows exactly.
      for (let i = 0; i < excess; i++) recordTrim(merged[merged.length - 1 - i])
      for (const [p, offsets] of trimmedByPartition) bottomMap.set(p, maxOf(offsets) + 1)
      result = { trimmedTop: 0, trimmedBottom: excess }
    }
    cachedRows = null
    return result
  }

  return {
    insertPage(rows, direction, startPositions, contCursor) {
      const { insertedMin, insertedMax } = insertRows(rows)

      if (startPositions === null) {
        // Anchor bootstrap: seed the OPPOSITE-direction edge from row
        // offsets — the one case with no continuation to draw from on that
        // side at all (see the home doc comment's "one gap none of the three
        // rules fill" section).
        if (direction === 'back') {
          for (const [p, max] of insertedMax) topMap.set(p, max + 1)
        } else {
          for (const [p, min] of insertedMin) bottomMap.set(p, min)
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
      // starts at the tail; a forward page reporting exhausted caught back
      // up to it.
      if (direction === 'back' && startPositions === null) attached = true
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
          'insertLive called while detached: live rows must be buffered by the caller until the window re-attaches (edges().top === null)',
        )
      }
      // N2 defensive drop (belt-and-suspenders, NOT load-bearing — see the
      // class doc comment's bottom-map completeness argument for why a
      // legal attached live row can never legitimately sit below
      // `bottomMap[p]`): silently ignore one anyway, counted nowhere, purely
      // against a future caller bug.
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
    edges() {
      const bottom = bottomMap.size === 0 ? null : encodeCursor(mapToPositions(bottomMap))
      const top = attached || topMap.size === 0 ? null : encodeCursor(mapToPositions(topMap))
      return { top, bottom }
    },
    clear() {
      partitions.clear()
      seen.clear()
      bottomMap.clear()
      topMap.clear()
      attached = false
      cachedRows = null
    },
    totalCount,
  }
}
