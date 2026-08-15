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
  /** Live-tail rows: always extend the newest (top) end. */
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
 * Both maps are updated by exactly two kinds of event, and NEVER by a third
 * ad hoc path — this is what keeps a trimmed region's re-read exact:
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
 * 2. **Row-exact trim** (`enforceCap`): when a trim removes rows from a
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
 *    recovers the trimmed set. A partition NOT touched by a given trim (no
 *    rows of its own were among the N trimmed) is left alone — its existing
 *    entry, planted by rule 1 or an earlier rule-2 event, already correctly
 *    describes its boundary and trimming a DIFFERENT partition's rows tells
 *    us nothing new about it. This is also what makes "a partition fully
 *    trimmed out of the window" safe: its rows vanish from `partitions`, but
 *    its map entry — planted the last time rule 1 or rule 2 touched it — is
 *    untouched by that disappearance and remains exactly correct.
 *
 * The one gap neither rule can fill on its own: an **anchor page** (no
 * request cursor exists at all, `startPositions === null`) has no
 * continuation on its OPPOSITE side to draw from (rule 1 only updates the
 * SAME-direction map) and nothing has ever been trimmed there yet (rule 2
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
 * whichever rule last touched it.
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

  function mergeCursorInto(map: Map<number, number>, cursor: string): void {
    const decoded = decodeCursor(cursor)
    for (const [p, v] of Object.entries(decoded)) map.set(Number(p), v)
  }

  /**
   * Row-exact cap enforcement: `kind` is which side just grew
   * ('bottom-insert' for a `back` page — extends older rows — 'top-insert'
   * for a `forward` page or a live insert — extends newer rows). Overflow
   * always trims from the OPPOSITE end, exactly `excess` rows, updating that
   * side's edge map from the exact trimmed offsets (rule 2 — see this
   * function's home doc comment above). A top trim also detaches the window.
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
      for (const [p, offsets] of trimmedByPartition) topMap.set(p, Math.min(...offsets))
      attached = false
      result = { trimmedTop: excess, trimmedBottom: 0 }
    } else {
      // A forward-page or live insert grows the top; overflow trims the
      // BOTTOM (oldest) rows exactly.
      for (let i = 0; i < excess; i++) recordTrim(merged[merged.length - 1 - i])
      for (const [p, offsets] of trimmedByPartition) bottomMap.set(p, Math.max(...offsets) + 1)
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
        // side at all (see the home doc comment's "one gap neither rule can
        // fill" section).
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
      insertRows(rows)
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
