import { describe, expect, it } from 'vitest'
import { createSlidingWindowStore } from './timelineStore'
import { decodeCursor, encodeCursor } from './timelineCursor'
import type { MessageOut } from '../api/types'

const mk = (partition: number, offset: number, ts: number, text = `v${partition}.${offset}`): MessageOut => ({
  partition,
  offset,
  timestamp_ms: ts,
  key: { encoding: 'utf8', text: `k${partition}.${offset}`, schema_id: null, error: null },
  value: { encoding: 'utf8', text, schema_id: null, error: null },
  headers: [],
})

const key = (m: MessageOut) => `${m.partition}:${m.offset}`

// ---------------------------------------------------------------------------
// Fixture + backend-faithful chunk simulator.
//
// The store must never be told more than rows/direction/startPositions/
// contCursor — exactly what a real page response gives it. To test the
// property walk honestly we need a "pretend backend" that behaves like
// `run_page` (see backend/src/message/timeline.rs): per-partition adjacent-
// first streams, a k-way merge preferring the best timestamp for the
// direction (ties: smaller partition wins), and exact offset-based cursor
// advance (jump to the window boundary only when a partition's window is
// fully drained; otherwise advance only to the taken min/max).
//
// Our fixture is unfiltered, so every simulated page is equivalent to
// backend's own "chunk_index == 0, filter: None" case, where `base_span` is
// exactly the requested span/limit — no separate multi-chunk looping is
// needed to match backend's real behavior faithfully here. It DOES include
// holes (an offset that exists but has no message) — `simulateChunk`'s
// "drained window jumps to the window boundary" rule already handles those
// exactly like backend's own completeness tracking does.
// ---------------------------------------------------------------------------

interface Watermarks {
  [partition: number]: readonly [lo: number, hi: number]
}

/**
 * `holes` are real Kafka holes: the offset exists (it still counts toward
 * watermarks/windows exactly like a normal offset — e.g. a transaction
 * control record or a compacted tombstone) but has no user-visible message.
 * They don't shift any other offset's position or timestamp.
 */
function buildFixture(
  numPartitions: number,
  perPartition: number,
  holes: ReadonlyArray<readonly [partition: number, offset: number]> = [],
): { all: MessageOut[]; byPartition: Map<number, MessageOut[]> } {
  const holeSet = new Set(holes.map(([p, o]) => `${p}:${o}`))
  const all: MessageOut[] = []
  const total = numPartitions * perPartition
  for (let i = 0; i < total; i++) {
    const partition = i % numPartitions
    const offset = Math.floor(i / numPartitions)
    if (holeSet.has(`${partition}:${offset}`)) continue
    all.push(mk(partition, offset, i * 10, `v${i}`))
  }
  const byPartition = new Map<number, MessageOut[]>()
  for (const row of all) {
    let arr = byPartition.get(row.partition)
    if (!arr) {
      arr = []
      byPartition.set(row.partition, arr)
    }
    arr.push(row)
  }
  for (const arr of byPartition.values()) arr.sort((a, b) => a.offset - b.offset)
  return { all, byPartition }
}

interface ChunkResult {
  taken: MessageOut[]
  newPositions: Record<number, number>
  exhausted: boolean
}

function simulateChunk(
  byPartition: Map<number, MessageOut[]>,
  watermarks: Watermarks,
  positions: Record<number, number>,
  direction: 'back' | 'forward',
  span: number,
): ChunkResult {
  const partitions = Object.keys(positions).map(Number)
  const windows: Record<number, { start: number; end: number }> = {}
  const streamOffsets: Record<number, number[]> = {}
  for (const p of partitions) {
    const [lo, hi] = watermarks[p]
    const pos = Math.min(Math.max(positions[p], lo), hi)
    const { start, end } = direction === 'back' ? { start: Math.max(pos - span, lo), end: pos } : { start: pos, end: Math.min(pos + span, hi) }
    windows[p] = { start, end }
    const inWindow = (byPartition.get(p) ?? []).filter((r) => r.offset >= start && r.offset < end).map((r) => r.offset)
    streamOffsets[p] = direction === 'back' ? inWindow.slice().sort((a, b) => b - a) : inWindow.slice().sort((a, b) => a - b)
  }
  const ptr: Record<number, number> = Object.fromEntries(partitions.map((p) => [p, 0]))
  const takenMin: Record<number, number> = {}
  const takenMax: Record<number, number> = {}
  const taken: MessageOut[] = []
  const rowOf = (p: number, offset: number) => (byPartition.get(p) ?? []).find((r) => r.offset === offset)!

  for (;;) {
    if (taken.length >= span) break
    let bestP: number | null = null
    let bestOffset: number | null = null
    let bestTs = 0
    for (const p of partitions) {
      const idx = ptr[p]
      const stream = streamOffsets[p]
      if (idx >= stream.length) continue
      const offset = stream[idx]
      const ts = rowOf(p, offset).timestamp_ms ?? Number.NEGATIVE_INFINITY
      const better = bestP === null || (direction === 'back' ? ts > bestTs : ts < bestTs) || (ts === bestTs && p < bestP)
      if (better) {
        bestP = p
        bestOffset = offset
        bestTs = ts
      }
    }
    if (bestP === null) break
    taken.push(rowOf(bestP, bestOffset!))
    takenMin[bestP] = takenMin[bestP] === undefined ? bestOffset! : Math.min(takenMin[bestP], bestOffset!)
    takenMax[bestP] = takenMax[bestP] === undefined ? bestOffset! : Math.max(takenMax[bestP], bestOffset!)
    ptr[bestP]++
  }

  const newPositions: Record<number, number> = {}
  for (const p of partitions) {
    const w = windows[p]
    if (w.end <= w.start) {
      newPositions[p] = positions[p]
      continue
    }
    const drained = ptr[p] >= streamOffsets[p].length
    if (drained) {
      newPositions[p] = direction === 'back' ? w.start : w.end
    } else if (direction === 'back') {
      newPositions[p] = takenMin[p] !== undefined ? takenMin[p] : positions[p]
    } else {
      newPositions[p] = takenMax[p] !== undefined ? takenMax[p] + 1 : positions[p]
    }
  }
  const exhausted = partitions.every((p) => {
    const [lo, hi] = watermarks[p]
    return direction === 'back' ? newPositions[p] === lo : newPositions[p] === hi
  })
  return { taken, newPositions, exhausted }
}

/** Simulates the FULL adjacent region in one shot (span = whole fixture) — used to validate a minted edge cursor "follows" to exactly the complement of the window. */
function follow(byPartition: Map<number, MessageOut[]>, watermarks: Watermarks, positions: Record<number, number>, direction: 'back' | 'forward'): MessageOut[] {
  const hugeSpan = 10_000
  return simulateChunk(byPartition, watermarks, positions, direction, hugeSpan).taken
}

/**
 * The core correctness invariant this store must uphold at every step,
 * mirroring the backend's "anchor partition property": per partition, the
 * window's own rows + what `edges().bottom` follows (back) + what
 * `edges().top` follows (forward, only when detached) must partition that
 * partition's full fixture disjointly and completely. When `edges().top` is
 * null (attached), the window's own newest row for that partition must
 * already be the fixture's true newest row (nothing left above, by
 * definition of "attached to the live tail").
 */
function assertGaplessComplement(
  store: ReturnType<typeof createSlidingWindowStore>,
  fixture: { all: MessageOut[]; byPartition: Map<number, MessageOut[]> },
  watermarks: Watermarks,
) {
  const edges = store.edges()
  const windowByPartition = new Map<number, Set<number>>()
  for (const row of store.rows()) {
    let s = windowByPartition.get(row.partition)
    if (!s) {
      s = new Set()
      windowByPartition.set(row.partition, s)
    }
    s.add(row.offset)
  }
  const bottomPositions = edges.bottom ? decodeCursor(edges.bottom) : {}
  const topPositions = edges.top ? decodeCursor(edges.top) : {}
  const bottomFollowed = edges.bottom ? follow(fixture.byPartition, watermarks, bottomPositions, 'back') : []
  const topFollowed = edges.top ? follow(fixture.byPartition, watermarks, topPositions, 'forward') : []

  for (const [partition, allRowsForP] of fixture.byPartition) {
    const windowSet = windowByPartition.get(partition) ?? new Set<number>()
    const bottomSet = new Set(bottomFollowed.filter((r) => r.partition === partition).map((r) => r.offset))
    const topSet = new Set(topFollowed.filter((r) => r.partition === partition).map((r) => r.offset))

    // Disjointness: pairwise, no offset counted twice.
    for (const o of windowSet) {
      expect(bottomSet.has(o), `partition ${partition} offset ${o} in both window and bottom-follow`).toBe(false)
      expect(topSet.has(o), `partition ${partition} offset ${o} in both window and top-follow`).toBe(false)
    }
    for (const o of bottomSet) {
      expect(topSet.has(o), `partition ${partition} offset ${o} in both bottom-follow and top-follow`).toBe(false)
    }

    // Completeness: union covers every offset the fixture has for this partition.
    const union = new Set<number>([...windowSet, ...bottomSet, ...topSet])
    const allOffsets = new Set(allRowsForP.map((r) => r.offset))
    expect(union).toEqual(allOffsets)

    // Attached invariant: if top is null, this partition's window must
    // already hold its true newest offset (nothing newer left unseen).
    if (!edges.top && windowSet.size > 0) {
      const maxKnown = Math.max(...windowSet)
      const maxTrue = Math.max(...[...allOffsets])
      expect(maxKnown, `partition ${partition} attached but not caught up`).toBe(maxTrue)
    }
  }
}

describe('createSlidingWindowStore — property walk', () => {
  it('slides row-exactly down past the cap and back up to the tail, gap-free and duplicate-free throughout — adversarial fixture (M1: holes, a timestamp tie, a live-only partition, live inserts interleaved into both legs)', () => {
    const numPartitions = 3
    const perPartition = 10
    const cap = 8
    const span = 4

    // Holes: (0,4) and (1,7) exist as offsets (watermarks unaffected) but
    // have no message — must not break paging or the invariant.
    const pagedFixture = buildFixture(numPartitions, perPartition, [
      [0, 4],
      [1, 7],
    ])
    // A genuine cross-partition timestamp tie: (0,3) and (1,3) share a
    // timestamp. Tie-break (smaller partition wins) must hold wherever both
    // are simultaneously in the window.
    const row03 = pagedFixture.byPartition.get(0)!.find((r) => r.offset === 3)!
    const row13 = pagedFixture.byPartition.get(1)!.find((r) => r.offset === 3)!
    row13.timestamp_ms = row03.timestamp_ms

    // Partition 3 is discovered ONLY via live insert (never in the anchor
    // page's own request, which only ever asks for partitions 0-2) — but,
    // matching real Kafka, its records are genuinely part of the backend's
    // data (`pagedFixture` below) the whole time, just not yet known to this
    // client. So once `topMap` picks it up (via the live-insert ceiling),
    // an ordinary forward page CAN legitimately recover it — no second live
    // insert needed to "fill the gap." `insertLive` itself is only ever
    // called here while attached (its own enforced precondition — see
    // timelineStore.ts): once for its first-ever appearance, right after
    // the anchor page (still attached, before any trim), and once more
    // after full re-attachment at the end of the walk (the "re-attach
    // buffer flush" case from the store's own doc comment) — genuinely NEW
    // rows at that point, contiguous with what's by then fully recovered.
    const partition3Batch1 = [mk(3, 0, 295), mk(3, 1, 296)] // pre-existing backend data, discovered live
    const partition3Batch2 = [mk(3, 2, 297), mk(3, 3, 298)] // genuinely new, arrives after re-attachment
    pagedFixture.byPartition.set(3, [...partition3Batch1])
    pagedFixture.all.push(...partition3Batch1)

    // `deliveredByPartition` is what the invariant checker is told "exists
    // so far": partitions 0-2 (and partition 3's batch 1, once delivered)
    // are real backend data recoverable by paging at will; partition 3's
    // batch 2 only becomes "real" once actually live-delivered, mirroring
    // genuinely-live production the backend didn't have a moment ago.
    const deliveredByPartition = new Map<number, MessageOut[]>(pagedFixture.byPartition)
    deliveredByPartition.set(3, [])
    const deliveredFixture = { all: pagedFixture.all, byPartition: deliveredByPartition }

    const watermarks: Watermarks = {
      0: [0, perPartition],
      1: [0, perPartition],
      2: [0, perPartition],
      3: [0, 1_000], // generous — covers batch 2's offsets too, never clamps
    }

    const store = createSlidingWindowStore(cap)
    const everRendered = new Set<string>()

    function recordRendered() {
      for (const r of store.rows()) everRendered.add(key(r))
    }

    function assertOrderedDedupedCapped() {
      const rows = store.rows()
      const seen = new Set<string>()
      for (const r of rows) {
        const k = key(r)
        expect(seen.has(k), `duplicate row ${k}`).toBe(false)
        seen.add(k)
      }
      // L1: non-increasing (the fixture now has a genuine tie), with the
      // documented tie-break (smaller partition wins) asserted explicitly.
      for (let i = 1; i < rows.length; i++) {
        const prevTs = rows[i - 1].timestamp_ms!
        const curTs = rows[i].timestamp_ms!
        expect(prevTs, `not non-increasing at ${i}`).toBeGreaterThanOrEqual(curTs)
        if (prevTs === curTs) {
          expect(rows[i - 1].partition, `tie at ts ${prevTs}: smaller partition must win`).toBeLessThan(rows[i].partition)
        }
      }
      expect(store.totalCount()).toBeLessThanOrEqual(cap)
      expect(store.totalCount()).toBe(rows.length)
    }

    function deliverLive(batch: MessageOut[]) {
      const beforeCount = store.totalCount()
      const outcome = store.insertLive(batch)
      const afterCount = store.totalCount()
      expect(beforeCount - (outcome.trimmedTop + outcome.trimmedBottom) + batch.length).toBe(afterCount)
      for (const r of batch) deliveredByPartition.get(r.partition)!.push(r)
      assertOrderedDedupedCapped()
      assertGaplessComplement(store, deliveredFixture, watermarks)
      recordRendered()
    }

    // --- Walk down: latest page, then page by page past the cap. ---
    // L2: `isFirstBackCall` follows the continuation cursor like a real
    // client would, independent of whether any particular page happened to
    // be empty (the old `beforeSet.size === 0` check re-triggered anchor
    // semantics on an empty page, which isn't what a real client does).
    //
    // Positions are re-derived from `store.edges().bottom` at the START of
    // every iteration (mirroring the up-walk below), rather than carried
    // forward in a local variable from the previous chunk's own
    // `newPositions`. This matters once live inserts are interleaved (M1):
    // a live insert can bottom-trim rows that a back-page had *already*
    // paginated past (e.g. offset 7 gets fetched by page N, then a live
    // insert between page N and N+1 evicts it again as now-oldest) — the
    // store's `bottomMap` correctly reverts to recover it, but a client-side
    // cursor variable that ignores the store and just carries the previous
    // page's own continuation forward would skip right past it, never
    // re-fetching it: a real, silent gap. A real client always mints its
    // next request from the store's current edge, precisely so an
    // interleaved trim like this is never missed.
    let isFirstBackCall = true
    let downIter = 0

    for (;;) {
      const positions: Record<number, number> = isFirstBackCall
        ? { 0: perPartition, 1: perPartition, 2: perPartition }
        : decodeCursor(store.edges().bottom!)
      const chunk = simulateChunk(pagedFixture.byPartition, watermarks, positions, 'back', span)
      const beforeCount = store.totalCount()
      const outcome = store.insertPage(chunk.taken, 'back', isFirstBackCall ? null : positions, chunk.exhausted ? null : encodeCursor(chunk.newPositions))
      isFirstBackCall = false
      const afterCount = store.totalCount()

      expect(beforeCount - (outcome.trimmedTop + outcome.trimmedBottom) + chunk.taken.length).toBe(afterCount)
      assertOrderedDedupedCapped()
      assertGaplessComplement(store, deliveredFixture, watermarks)
      recordRendered()
      downIter++

      // M1: partition 3's first-ever appearance, live-delivered right after
      // the anchor page — still attached at this point (cap 8 isn't
      // exceeded until the down-walk is well underway), which is exactly
      // `insertLive`'s enforced precondition. `bottomMap` never picks up
      // partition 3 here (the anchor's own request excluded it, and no
      // back-page ever re-mentions a key it was never given), so the
      // down-walk's own positions naturally stay confined to 0-2 without
      // needing to filter anything out.
      if (downIter === 1) deliverLive(partition3Batch1)

      if (chunk.exhausted || chunk.taken.length === 0) break
    }

    // Fully walked down: bottom edge must be the genuine topic start for
    // every paged partition (holes included — they don't move the true lo).
    const bottomAtFloor = decodeCursor(store.edges().bottom!)
    for (const p of [0, 1, 2]) expect(bottomAtFloor[p]).toBe(0)
    // Cap forced trims well before we reached the floor, so we must be detached.
    expect(store.edges().top).not.toBeNull()

    // --- Walk back up via edges().top until re-attached at the tail. ---
    // Partition 3 is NOT filtered out here (unlike an earlier iteration of
    // this fixture): once top-trimmed like any other partition, it's fully
    // recoverable via ORDINARY forward paging — `pagedFixture` has its real
    // data (see setup above) precisely so this works, matching a real
    // backend that has always had it regardless of how this client first
    // discovered it. No second live insert is needed (or attempted) while
    // detached — that would violate `insertLive`'s own precondition.
    for (;;) {
      const topPositions = decodeCursor(store.edges().top!)
      const chunk = simulateChunk(pagedFixture.byPartition, watermarks, topPositions, 'forward', span)
      const beforeCount = store.totalCount()
      const outcome = store.insertPage(chunk.taken, 'forward', topPositions, chunk.exhausted ? null : encodeCursor(chunk.newPositions))
      const afterCount = store.totalCount()

      expect(beforeCount - (outcome.trimmedTop + outcome.trimmedBottom) + chunk.taken.length).toBe(afterCount)
      assertOrderedDedupedCapped()
      assertGaplessComplement(store, deliveredFixture, watermarks)
      recordRendered()

      if (chunk.exhausted) break
    }

    // Re-attached: top edge is null again.
    expect(store.edges().top).toBeNull()

    // M1: the "re-attach buffer flush" — genuinely NEW live rows (never part
    // of `pagedFixture`) delivered now that we're attached again, exactly
    // the case the store's own doc comment models: contiguous with what's
    // now fully recovered, so the ceiling advances cleanly, no gap risk.
    deliverLive(partition3Batch2)
    expect(store.edges().top).toBeNull()

    const rows = store.rows()
    // `pagedFixture.all` already includes partition 3's batch 1 (pushed in
    // during setup — it's real backend data from the start); only batch 2
    // (genuinely new) needs adding here.
    const everything = [...pagedFixture.all, ...partition3Batch2]
    const maxTs = Math.max(...everything.map((r) => r.timestamp_ms!))
    expect(rows[0].timestamp_ms).toBe(maxTs)

    // Union of everything ever rendered reconstructs the whole fixture
    // (paged partitions + both live-delivered batches) exactly.
    expect(everRendered.size).toBe(everything.length)
    for (const row of everything) expect(everRendered.has(key(row))).toBe(true)
  })
})

describe('createSlidingWindowStore — merge, dedup, ordering', () => {
  it('keeps rows sorted newest-first and dedups across calls', () => {
    const s = createSlidingWindowStore()
    s.insertPage([mk(0, 1, 100), mk(0, 3, 300)], 'back', null, null)
    s.insertLive([mk(1, 1, 200), mk(0, 3, 300)])
    expect(s.rows().map((m) => m.timestamp_ms)).toEqual([300, 200, 100])
    expect(s.rows()).toHaveLength(3)
  })

  it('ties break by partition then offset', () => {
    const s = createSlidingWindowStore()
    s.insertPage([mk(1, 5, 100), mk(0, 9, 100), mk(0, 7, 100)], 'back', null, null)
    expect(s.rows().map((m) => `${m.partition}:${m.offset}`)).toEqual(['0:9', '0:7', '1:5'])
  })

  it('within-partition offset order beats timestamp', () => {
    const s = createSlidingWindowStore()
    s.insertPage([mk(0, 1, 400), mk(0, 2, 100)], 'back', null, null)
    expect(s.rows().map((m) => m.offset)).toEqual([2, 1])
  })

  it('rows() result is frozen', () => {
    const s = createSlidingWindowStore()
    s.insertPage([mk(0, 1, 100)], 'back', null, null)
    expect(Object.isFrozen(s.rows())).toBe(true)
  })
})

describe('createSlidingWindowStore — row-exact trims and edge-map exactness', () => {
  it('a forward/live overflow trims exactly the oldest N rows from the bottom and advances the bottom map by exactly those offsets', () => {
    const s = createSlidingWindowStore(3)
    s.insertPage([mk(0, 1, 100), mk(0, 2, 200), mk(0, 3, 300)], 'back', null, encodeCursor({ 0: 1 }))
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 1 })

    const outcome = s.insertLive([mk(0, 4, 400), mk(0, 5, 500)])
    expect(outcome).toEqual({ trimmedTop: 0, trimmedBottom: 2, attached: true, rejectedStale: false })
    expect(s.rows().map((m) => m.offset)).toEqual([5, 4, 3])
    // Trimmed offsets {1,2}: bottom map must advance to max(trimmed)+1 = 3,
    // recovering exactly those two rows on a back-follow, no more no less.
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 3 })
  })

  it('a forward anchor bootstrap seeds bottom from row offsets and top from the continuation, then a back-page overflow trims the top row-exactly', () => {
    const s = createSlidingWindowStore(3)
    // Forward anchor (Beginning-like): startPositions null, direction forward.
    // Same-direction (top) edge comes from contCursor; opposite (bottom)
    // edge has no request cursor to draw from, so it seeds from the actual
    // row offsets fetched (min offset == the true start here, no leading holes).
    s.insertPage([mk(0, 5, 500), mk(0, 4, 400), mk(0, 3, 300)], 'forward', null, encodeCursor({ 0: 6 }))
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 6 })
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 3 })

    const outcome = s.insertPage([mk(0, 2, 200), mk(0, 1, 100)], 'back', { 0: 3 }, encodeCursor({ 0: 0 }))
    expect(outcome).toEqual({ trimmedTop: 2, trimmedBottom: 0, attached: false, rejectedStale: false })
    expect(s.rows().map((m) => m.offset)).toEqual([3, 2, 1])
    // Trimmed offsets {5,4}: top map must advance to min(trimmed) = 4,
    // recovering exactly those two rows on a forward-follow.
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 4 })
  })

  it('a partition fully trimmed out of the window is re-entered gap-free via the top map', () => {
    const s = createSlidingWindowStore(4)
    // Two partitions; partition 1's rows are the newest overall, so they'll
    // be entirely squeezed out by the next top trim.
    s.insertPage(
      [mk(1, 9, 1000), mk(0, 9, 500), mk(1, 8, 990), mk(0, 8, 490)],
      'back',
      null,
      encodeCursor({ 0: 8, 1: 8 }),
    )
    expect(s.totalCount()).toBe(4)

    // Insert two older partition-0 rows: cap forces exactly 2 top trims (the
    // newest 2 rows overall: partition 1's offsets 9 and 8).
    const outcome = s.insertPage([mk(0, 7, 480), mk(0, 6, 470)], 'back', { 0: 8, 1: 8 }, encodeCursor({ 0: 6, 1: 8 }))
    expect(outcome).toEqual({ trimmedTop: 2, trimmedBottom: 0, attached: false, rejectedStale: false })
    expect(s.rows().map((m) => `${m.partition}:${m.offset}`)).toEqual(['0:9', '0:8', '0:7', '0:6'])
    // Partition 1 is fully absent from the window now, but its edge must
    // survive with the exact recovering value (partition 0's own edge entry,
    // seeded earlier and never touched by this trim, may still be present
    // too — harmless, since it correctly reports "nothing more" for a
    // partition that was never trimmed — so we only assert what this trim
    // must guarantee: partition 1's exact recovery bound).
    expect(decodeCursor(s.edges().top!)[1]).toBe(8)

    // Re-enter partition 1 by following the top map forward.
    const reentered = s.insertPage([mk(1, 8, 800), mk(1, 9, 900)], 'forward', { 1: 8 }, null)
    expect(reentered.trimmedBottom).toBeGreaterThanOrEqual(0)
    const offsets = s.rows().map((m) => `${m.partition}:${m.offset}`)
    expect(new Set(offsets).has('1:8')).toBe(true)
    expect(new Set(offsets).has('1:9')).toBe(true)
  })

  it('re-attaches when a forward page reports exhausted (contCursor null)', () => {
    const s = createSlidingWindowStore(2)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))
    s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 })) // trims top(0,9) -> detached
    expect(s.edges().top).not.toBeNull()

    const outcome = s.insertPage([mk(0, 9, 900)], 'forward', { 0: 9 }, null) // exhausted forward page
    expect(outcome.trimmedBottom).toBe(1)
    expect(s.edges().top).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// C1 regression tests (fix round 1, review verdict on 2c17f57): `insertLive`
// never advanced `topMap`, so a partition whose rows arrived only via live
// insert could hold rows above a stale (or entirely missing) top boundary.
// Both counterexamples below are the reviewer's own, reproduced verbatim.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — C1 regression: live inserts and the top edge map', () => {
  it('(A) a live-delivered row spared by an unrelated top trim must not leave a stale topMap entry that re-delivers it', () => {
    const s = createSlidingWindowStore(6)
    // Anchor page (back-direction: attached=true, so edges().top reads null
    // for now regardless) seeds topMap = {0:14, 1:7, 2:22} internally
    // (opposite-side row-offset seed) — not observable yet, but tracked.
    s.insertPage(
      [mk(0, 13, 500), mk(0, 12, 400), mk(1, 6, 1000), mk(1, 5, 900), mk(2, 21, 1300), mk(2, 20, 1200)],
      'back',
      null,
      encodeCursor({ 0: 12, 1: 5, 2: 20 }),
    )
    expect(s.edges().top).toBeNull() // attached: Latest-style anchor bootstrap

    // Live insert extends partition 1 to offset 7 (exactly the old topMap
    // boundary) and partition 2 to 22/23. Cap 6 forces a bottom trim of the
    // 3 oldest rows — (0,12),(0,13),(1,5) — which does NOT touch (1,7): it
    // survives, newly held, in the window.
    s.insertLive([mk(1, 7, 1100), mk(2, 22, 2000), mk(2, 23, 2100)])
    expect(s.rows().some((r) => r.partition === 1 && r.offset === 7)).toBe(true)

    // A later back-page overflow trims the top — but only partition 2's
    // newest row (23) is among the globally newest, so this trim corrects
    // partition 2 and never touches partition 1 at all.
    const bottomNow = decodeCursor(s.edges().bottom!)
    s.insertPage([mk(0, 11, 100)], 'back', bottomNow, encodeCursor({ ...bottomNow, 0: 11 }))

    const top = decodeCursor(s.edges().top!)
    const maxHeld1 = Math.max(...s.rows().filter((r) => r.partition === 1).map((r) => r.offset))
    // The bug: without the fix, top[1] stays 7 — exactly where partition 1's
    // row 7 already sits — so following it would re-deliver an already-held
    // row. Fixed: top[1] must sit strictly above everything already held.
    expect(top[1]).toBeGreaterThan(maxHeld1)
  })

  it('(B) a partition introduced only via live insert (never in the anchor page) must not be silently omitted from a later top edge', () => {
    const s = createSlidingWindowStore(4)
    // Anchor page: partition 0 only. Partition 1 contributes zero rows here.
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))

    // Partition 1's very first rows ever arrive live, while still attached.
    s.insertLive([mk(1, 50, 500), mk(1, 51, 600)])
    expect(s.rows().some((r) => r.partition === 1)).toBe(true)

    // A later back-page overflow trims the top — partition 0's rows are the
    // newest, so this trim touches only partition 0; partition 1's live rows
    // survive, untouched by it.
    const outcome = s.insertPage([mk(0, 7, 100), mk(0, 6, 50)], 'back', { 0: 8 }, encodeCursor({ 0: 6 }))
    expect(outcome.trimmedTop).toBeGreaterThan(0)
    expect(s.edges().top).not.toBeNull() // detached now

    const top = decodeCursor(s.edges().top!)
    // The bug: without the fix, partition 1 was never seeded into topMap at
    // all (it had zero rows in the anchor page, and insertLive didn't touch
    // topMap), so it's silently missing from the minted cursor's key set —
    // a real backend request built from it would never read partition 1
    // again. Fixed: partition 1 must be present and its bound must sit
    // strictly above everything already held for it.
    expect(top[1]).toBeDefined()
    const maxHeld1 = Math.max(...s.rows().filter((r) => r.partition === 1).map((r) => r.offset))
    expect(top[1]).toBeGreaterThan(maxHeld1)
  })
})

// ---------------------------------------------------------------------------
// M-new (task-3 review carry-over, hard requirement): the store previously
// set `attached = true` on ANY back-direction anchor bootstrap
// (`startPositions === null`), which is only correct for a Latest-style
// anchor (genuinely reads from the tail). Offset/Timestamp anchors are
// historical jumps — landing mid-topic must NOT claim the window already
// covers the live tail. `insertPage` gains a caller-supplied opt so only a
// caller that KNOWS it's bootstrapping from Latest can attach; every other
// anchor bootstrap (offset/timestamp/beginning) must pass `attach: false`
// (or omit it for a forward anchor, where it's a no-op either way) and stay
// detached. Default (opt omitted) is `true` — deliberately: it keeps every
// pre-existing back-anchor-bootstrap call above (all of which model a
// Latest-style read) passing unchanged; production code (Timeline.tsx) is
// required to pass the flag explicitly on every anchor call, never rely on
// this default.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — anchor-awareness (M-new)', () => {
  it('an offset/timestamp-style anchor bootstrap (attach: false) leaves the store detached: edges().top is non-null and insertLive throws', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }), { attach: false })
    // The opposite-side (top) edge is still seeded from row offsets exactly
    // as any back-anchor bootstrap does — only `attached` itself must differ.
    expect(s.edges().top).not.toBeNull()
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 10 })
    expect(() => s.insertLive([mk(0, 10, 1000)])).toThrow()
  })

  it('a Latest-style anchor bootstrap (default, attach omitted) still attaches: edges().top is null and insertLive does not throw', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))
    expect(s.edges().top).toBeNull()
    expect(() => s.insertLive([mk(0, 10, 1000)])).not.toThrow()
  })

  it('a Latest-style anchor bootstrap with attach explicitly true attaches, same as the default', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }), { attach: true })
    expect(s.edges().top).toBeNull()
  })
})

describe('createSlidingWindowStore — codec integration', () => {
  it('accepts a real backend-shaped continuation cursor (carrying a direction field)', () => {
    // base64(JSON.stringify({"direction":"back","positions":[[0,60],[1,5]]}))
    const backendCursor = 'eyJkaXJlY3Rpb24iOiJiYWNrIiwicG9zaXRpb25zIjpbWzAsNjBdLFsxLDVdXX0='
    const s = createSlidingWindowStore()
    s.insertPage([mk(0, 61, 100), mk(1, 6, 90)], 'back', null, backendCursor)
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 60, 1: 5 })
  })
})

describe('createSlidingWindowStore — clear', () => {
  it('resets rows, counts, and edges; never resets otherwise', () => {
    const s = createSlidingWindowStore(4)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))
    s.insertLive([mk(0, 10, 1000)])
    expect(s.totalCount()).toBeGreaterThan(0)

    s.clear()
    expect(s.rows()).toEqual([])
    expect(s.totalCount()).toBe(0)
    expect(s.edges()).toEqual({ top: null, bottom: null })
  })
})

// ---------------------------------------------------------------------------
// Fix round 2 (review verdict on 49dca27): the guarded ceiling was itself
// wrong (N4's counterexample: window p:90-99, topMap 100, insertLive p:150
// — neither guarding nor advancing is safe as a general rule). The actual
// fix is a precondition, enforced at the interface: `insertLive` throws
// unless attached. These tests pin that enforcement directly.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — insertLive attached precondition (fix round 2, N6)', () => {
  it('throws on a fresh store that has never attached', () => {
    const s = createSlidingWindowStore()
    expect(() => s.insertLive([mk(0, 0, 100)])).toThrow()
  })

  it('throws once detached by a top trim', () => {
    const s = createSlidingWindowStore(2)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))
    s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 })) // trims top(0,9) -> detached
    expect(s.edges().top).not.toBeNull()
    expect(() => s.insertLive([mk(0, 10, 1000)])).toThrow()
  })

  it('does not throw while attached, and stops throwing again once re-attached', () => {
    const s = createSlidingWindowStore(2)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))
    expect(() => s.insertLive([mk(0, 10, 1000)])).not.toThrow()

    s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 })) // detach
    s.insertPage([mk(0, 11, 1100)], 'forward', { 0: 11 }, null) // exhausted -> re-attach
    expect(s.edges().top).toBeNull()
    expect(() => s.insertLive([mk(0, 12, 1200)])).not.toThrow()
  })
})

// ---------------------------------------------------------------------------
// N1 (mutation-verified): the property walk's own invariant checks stayed
// green even with the live-insert ceiling deleted outright (nothing in that
// giant walk isolates the ONE case where the ceiling is the sole source of
// a `topMap` entry). These two tests pin that case directly, per side (the
// live-insert ceiling itself, and the top-trim completeness backfill) —
// each was run red-by-deletion, then green-by-restoration, against the
// actual `timelineStore.ts` code (not just reasoned about).
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — mutation-verified load-bearing paths (N1)', () => {
  it('the live-insert ceiling is the ONLY source of topMap for a partition holding zero window rows at top-trim time', () => {
    const s = createSlidingWindowStore(5)
    // Anchor: partition 0 only — partition 1 contributes zero rows.
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))

    // Partition 1's only-ever row, live-delivered while attached: sets
    // `topMap[1]` via the ceiling — nothing else has ever touched it.
    s.insertLive([mk(1, 50, 500)])

    // Push enough newer partition-0 rows to bottom-trim partition 1's row
    // back OUT of the window (it's globally the oldest of the lot).
    const evictOutcome = s.insertLive([mk(0, 20, 2000), mk(0, 21, 2100), mk(0, 22, 2200)])
    expect(evictOutcome.trimmedBottom).toBe(1)
    expect(s.rows().some((r) => r.partition === 1)).toBe(false) // zero rows now

    // A later, unrelated top trim (partition 0's own newest row) — the
    // completeness backfill SKIPS partition 1 here (it holds zero rows), so
    // this trim cannot be what sets `topMap[1]`.
    const topTrimOutcome = s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 }))
    expect(topTrimOutcome.trimmedTop).toBeGreaterThan(0)

    // `topMap[1]` must still be exactly what the ceiling set (51) — mutation
    // check: deleting the ceiling line in `insertLive` turns this `toBe(51)`
    // into `toBe(undefined)` failing red; restoring it goes green.
    expect(decodeCursor(s.edges().top!)[1]).toBe(51)
  })

  it('the top-trim completeness backfill is the ONLY source of topMap for a partition first discovered via a plain back page', () => {
    const s = createSlidingWindowStore(4)
    // Anchor: partition 0 only.
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }))

    // Partition 1 discovered via a plain (non-anchor) back page: rule 1
    // only ever updates `bottomMap` for a `back`-direction call, and rule 2
    // (the live ceiling) never ran for it at all — `topMap` has no entry
    // for partition 1 after this call.
    s.insertPage([mk(1, 20, 500), mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7, 1: 19 }))
    expect(s.rows().some((r) => r.partition === 1 && r.offset === 20)).toBe(true)

    // A later top trim that does NOT touch partition 1 (its timestamp is
    // low enough to survive) — only the completeness backfill can plant a
    // `topMap` entry for it now.
    const outcome = s.insertPage([mk(0, 6, 600)], 'back', { 0: 7, 1: 19 }, encodeCursor({ 0: 6, 1: 19 }))
    expect(outcome.trimmedTop).toBeGreaterThan(0)

    // Mutation check: deleting the backfill loop in `enforceCap` turns this
    // `toBe(21)` into `toBe(undefined)` failing red; restoring it goes green.
    expect(decodeCursor(s.edges().top!)[1]).toBe(21)
  })
})

// ---------------------------------------------------------------------------
// Fix round 1 (review of 079f30f) — C1: the store's real `attached` state
// was never directly observable. `edges().top === null` is NOT a safe
// proxy for it — it's ALSO null whenever `topMap` is simply empty (a
// zero-row anchor page, legal per the empty-page contract), which has
// nothing to do with attachment. Callers that gated on that (Timeline.tsx)
// could therefore treat a genuinely-detached, historical window as
// attached, and go on to call `insertLive` on it — hitting the throw.
// Exposed two ways per the review: a real `isAttached()` getter, and an
// `attached` field on every `InsertOutcome` (so a caller reacting to a
// JUST-LANDED page's own outcome doesn't need a second store call).
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — isAttached() and InsertOutcome.attached (C1, fix round 1)', () => {
  it('a zero-row historical (attach:false) anchor bootstrap is NOT attached, even though edges().top also reads null — for an unrelated reason (an empty topMap, not attachment)', () => {
    const s = createSlidingWindowStore(10)
    // Empty-page contract: an anchor page can legitimately deliver ZERO
    // rows (nothing in this window) with a non-null, not-exhausted cursor.
    const outcome = s.insertPage([], 'back', null, encodeCursor({ 0: 8 }), { attach: false })
    expect(s.edges().top).toBeNull() // masked by an EMPTY topMap (no rows ever seeded it)
    expect(s.isAttached()).toBe(false) // the actual truth: this bootstrap never claimed to attach
    expect(outcome.attached).toBe(false)
  })

  it('a Latest-style (attach:true) bootstrap IS attached, reflected identically by isAttached() and the outcome', () => {
    const s = createSlidingWindowStore(10)
    const outcome = s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8 }), { attach: true })
    expect(s.isAttached()).toBe(true)
    expect(outcome.attached).toBe(true)
  })

  it('isAttached() flips false the instant a top trim detaches, matching the outcome from that same call', () => {
    const s = createSlidingWindowStore(2)
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }), { attach: true })
    expect(s.isAttached()).toBe(true)
    const outcome = s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 })) // trims top -> detach
    expect(outcome.trimmedTop).toBeGreaterThan(0)
    expect(s.isAttached()).toBe(false)
    expect(outcome.attached).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// Fix round 1 — C2: an anchor bootstrap's opposite-side seed only ever
// touches partitions that returned ≥1 row THIS PAGE (see the class doc
// comment's "one gap" section) — a partition contributing zero rows to a
// multi-partition anchor page (a real, unremarkable scenario, not just an
// adversarial one) is silently OMITTED from that seed. Left unaddressed,
// `edges().top` would mint a cursor missing that partition's key entirely —
// worse than null, since a partial cursor doesn't trigger any recovery path
// (Timeline's forward-anchor fallback only ever fires on a NULL top edge):
// the backend would never be asked about that partition again, a permanent,
// silent gap.
//
// Fix: `edges().top` treats the map as complete only when every partition
// `bottomMap` currently knows about ALSO has a `topMap` entry. `bottomMap`
// is the reliable "full partition list" reference specifically because,
// right after a back-direction request (anchor OR continuation), rule 1
// populates it authoritatively from the backend's own `cur_positions` —
// which (per the design doc) already covers every partition the request
// touched, including zero-row ones; an ANCHOR request's `cur_positions`
// touches every partition of the topic, not just the ones with rows this
// page. `topMap`'s own row-derived seed has no equivalent guarantee, which
// is exactly the asymmetry this check exists to catch.
//
// This is deliberately a COMPUTED check (topMap keys ⊇ bottomMap keys) at
// `edges()` call time, not a separately maintained/reset boolean flag: it
// requires no extra state to keep correct across `clear()`, and it's
// PRECISE rather than pessimistic — a single-partition topic (or any anchor
// page that happens to cover every partition, e.g. this store's own
// property walk below) is correctly read as complete immediately, while a
// genuinely-incomplete multi-partition case is still caught. Every other
// map-update rule (same-direction rule 1, the live-insert ceiling, the trim
// backfill) is already proven complete over its own relevant partition set
// (see their own doc comments) — the anchor opposite-seed is the ONLY event
// that can ever create this specific discrepancy, so comparing against
// `bottomMap` catches exactly (and only) that.
//
// NOTE (documented, not fixed this round — flagged in the task report): the
// SYMMETRIC gap exists for `bottomMap` after a FORWARD anchor bootstrap
// (`beginning`) whose opposite-side (bottom) row-seed also only covers
// partitions with ≥1 row. Out of scope for this review round (which asked
// for `topMap`/`edges().top` specifically); `edges().bottom` does not yet
// get an equivalent completeness check.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — edges().top completeness (C2, fix round 1)', () => {
  it('a multi-partition anchor bootstrap where one partition contributes zero rows leaves edges().top null — not a partial cursor that would silently omit it', () => {
    const s = createSlidingWindowStore(10)
    // Anchor page: the response's cur_positions (rule 1, authoritative)
    // covers BOTH partitions 0 and 1 — an anchor request always touches
    // every partition of the topic — but only partition 0 returned a row
    // this page; partition 1 genuinely had none in this window.
    s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8, 1: 5 }), { attach: false })
    // bottomMap (rule 1) now knows about {0, 1}; topMap (opposite-seed) was
    // only ever seeded for partition 0 — INCOMPLETE.
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 8, 1: 5 })
    expect(s.edges().top).toBeNull()
  })

  it('a single-partition (or every-partition-returned-a-row) anchor bootstrap is immediately complete — edges().top is a real cursor, not null', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8 }), { attach: false })
    // bottomMap and topMap both only ever know about partition 0 here —
    // trivially complete (topMap keys ⊇ bottomMap keys).
    expect(s.edges().top).not.toBeNull()
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 10 })
  })

  it('completeness is restored once a real forward page (anchor or continuation) reports a partition topMap was missing', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8, 1: 5 }), { attach: false })
    expect(s.edges().top).toBeNull() // incomplete (partition 1 missing from topMap)

    // A forward page's own rule-1 contCursor covers every partition ITS
    // OWN request touched — here, both, closing the gap.
    s.insertPage([mk(0, 9, 900), mk(1, 5, 500)], 'forward', { 0: 9, 1: 5 }, encodeCursor({ 0: 10, 1: 6 }))
    expect(s.edges().top).not.toBeNull()
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 10, 1: 6 })
  })
})

// ---------------------------------------------------------------------------
// Fix round 2 (re-review of 000fd9f) — N1 (High, THE BLOCKER): the C2
// completeness check above applied unconditionally, regardless of WHY the
// window is incomplete. For a Latest-anchored bootstrap (`attach: true`),
// a cold/empty partition that never contributed a row (but that the back
// request's own `cur_positions` still enumerates in `bottomMap`) makes
// `topMap` permanently "incomplete" relative to `bottomMap` — but this
// omission hides NOTHING: for a Latest bootstrap, genuinely EVERYTHING
// above the anchor point is the live tail (nothing has been produced yet
// that a forward read could ever find, for ANY partition, cold or not).
// Applying the C2 check here anyway meant `edges().top` stayed `null`
// FOREVER once such a window detached — `loadNewer` finds no cursor,
// `forwardAnchorFallback` returns null for the 'default' anchor context
// (Latest has no "same anchor, forward" replay to fall back to — see
// Timeline.tsx's own comment), so NO request ever fires: a dead scroll-up,
// the window can never re-attach. Reachable on any over-partitioned topic
// (any partition quiet enough to contribute zero rows to one Latest page).
//
// Fix: the C2 completeness check now applies ONLY when the CURRENT
// incompleteness traces back to a HISTORICAL anchor bootstrap (`attach:
// false` — Offset/Timestamp; Beginning does not touch `topMap`'s opposite
// side at all, see the class doc comment). A Latest bootstrap (`attach:
// true`, or the default) never sets this flag — an omitted partition there
// is provably harmless, so `edges().top` is trusted at face value the
// instant a trim detaches, exactly as it was before the C2 fix existed.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — edges().top completeness only matters for historical bootstraps (N1, fix round 2)', () => {
  it("the reviewer's exact probe: a Latest mount with a cold partition (rows from p0/p1 only, contCursor enumerating p0/p1/p2) still exposes a real top edge after detaching, and the up-walk works", () => {
    const s = createSlidingWindowStore(4)
    // Latest anchor bootstrap: rows only from partitions 0 and 1 — the
    // response's own cur_positions (rule 1, bottomMap) enumerates p0, p1,
    // AND the cold p2 (an anchor request touches every partition of the
    // topic, including ones that never produced a row this page).
    const mountOutcome = s.insertPage(
      [mk(0, 9, 900), mk(1, 9, 950), mk(0, 8, 800), mk(1, 8, 850)],
      'back',
      null,
      encodeCursor({ 0: 8, 1: 8, 2: 0 }),
      { attach: true },
    )
    expect(mountOutcome.attached).toBe(true)
    // topMap never learned about p2 (no rows) — the OLD, unconditional C2
    // check would read this as incomplete forever. p2 hides nothing for a
    // Latest bootstrap, so it must not matter.
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 8, 1: 8, 2: 0 })

    // Detach via an over-cap back page (cap 4, already holding 4 rows —
    // this trims the newest 2 off the top).
    const detachOutcome = s.insertPage(
      [mk(0, 7, 700), mk(1, 7, 750)],
      'back',
      { 0: 8, 1: 8, 2: 0 },
      encodeCursor({ 0: 7, 1: 7, 2: 0 }),
    )
    expect(detachOutcome.trimmedTop).toBeGreaterThan(0)
    expect(s.isAttached()).toBe(false)

    // The blocker: edges().top must be a REAL cursor, not null — a dead
    // scroll-up (no cursor, no fallback for a Latest-style context) would
    // otherwise trap the reader in a detached window forever.
    expect(s.edges().top).not.toBeNull()

    // The up-walk must actually work: following edges().top forward
    // recovers the trimmed rows gap-free.
    const topPositions = decodeCursor(s.edges().top!)
    const outcome = s.insertPage(
      [mk(0, 9, 900), mk(1, 9, 950)],
      'forward',
      topPositions,
      null, // caught the tail
    )
    expect(outcome.rejectedStale).toBe(false)
    expect(s.rows().some((r) => r.partition === 0 && r.offset === 9)).toBe(true)
    expect(s.rows().some((r) => r.partition === 1 && r.offset === 9)).toBe(true)
    expect(s.isAttached()).toBe(true) // re-attached
  })

  it('a HISTORICAL (attach: false) bootstrap with the same cold-partition shape still reads edges().top as null — the C2 protection is unchanged for the case it exists for', () => {
    const s = createSlidingWindowStore(4)
    s.insertPage(
      [mk(0, 9, 900), mk(1, 9, 950), mk(0, 8, 800), mk(1, 8, 850)],
      'back',
      null,
      encodeCursor({ 0: 8, 1: 8, 2: 0 }),
      { attach: false },
    )
    expect(s.isAttached()).toBe(false)
    // Still masked null (isAttached() false, but ALSO genuinely incomplete
    // per C2 — a historical jump's cold partition IS a real risk).
    expect(s.edges().top).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// Fix round 1 — C3: an in-flight page's response can no longer be trusted
// to safely narrow its own same-direction edge map once a CONCURRENT event
// (a live insert's trim, in the reviewer's own probe) has already advanced
// that map PAST what the page assumed when it was issued. Naively merging
// it anyway (the old `Math.min`/`Math.max` row-derived tightening, keyed
// only off the page's OWN rows) silently regresses the map back to a
// position that skips over the very range the concurrent trim just made
// newly-recoverable — an interior hole at exactly the seam the trim was
// supposed to keep whole.
//
// Fix: `insertPage` compares its own `startPositions` against the CURRENT
// same-side map, per partition, before touching anything else. For a
// `back` page, `startPositions[p]` must equal (or be numerically at-or-
// below) the current `bottomMap[p]` — if the current value is strictly
// GREATER (the map has advanced past what this page assumed, meaning a
// trim recovered that range in the interim), the page is REJECTED
// wholesale: no rows inserted, no map mutated, `{ rejectedStale: true }`
// returned. The caller re-issues from the store's own fresh edge instead —
// one wasted round trip in a rare race, correctness by construction.
// Symmetric for `forward`/`topMap` (current value strictly LOWER than
// assumed = stale).
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — stale in-flight page rejection (C3, fix round 1)', () => {
  it("the reviewer's exact probe: a live trim recovers an evicted offset while an older back-page is in flight — landing that page must not regress the bottom map past the recovered range (no interior hole)", () => {
    const s = createSlidingWindowStore(4)
    // Window [12,11,10,9], bottom={0:9} (mount, Latest-style).
    s.insertPage(
      [mk(0, 12, 1200), mk(0, 11, 1100), mk(0, 10, 1000), mk(0, 9, 900)],
      'back',
      null,
      encodeCursor({ 0: 9 }),
      { attach: true },
    )
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 9 })

    // An in-flight back-page is issued FROM bottom={0:9} (captured now, but
    // its response won't land until later, below).
    const inFlightStart = { 0: 9 }

    // Meanwhile: live 13 arrives, growing the top past cap(4) and trimming
    // the oldest (9) off the bottom — bottom map advances to {0:10},
    // recovering exactly offset 9 on a future back-read.
    const liveOutcome = s.insertLive([mk(0, 13, 1300)])
    expect(liveOutcome.trimmedBottom).toBe(1)
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 10 })

    // NOW the stale in-flight page lands: rows [8,7], contCursor {0:7}.
    // Its own startPositions (9) is BELOW the current bottom boundary (10)
    // — a trim recovered [9,10) in the interim — so this page must be
    // rejected wholesale, not merged.
    const outcome = s.insertPage([mk(0, 8, 800), mk(0, 7, 700)], 'back', inFlightStart, encodeCursor({ 0: 7 }))
    expect(outcome.rejectedStale).toBe(true)
    expect(outcome.trimmedTop).toBe(0)
    expect(outcome.trimmedBottom).toBe(0)
    // Nothing from the rejected page was inserted.
    expect(s.rows().some((r) => r.offset === 8)).toBe(false)
    expect(s.rows().some((r) => r.offset === 7)).toBe(false)
    // The bottom map is UNCHANGED by the rejected page — still exactly
    // where the trim left it, so offset 9 remains reachable (no interior
    // hole): a fresh back-read from {0:10} would recover it.
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 10 })

    // The caller re-issues from the FRESH edge and it works normally.
    const retryOutcome = s.insertPage([mk(0, 9, 900)], 'back', { 0: 10 }, encodeCursor({ 0: 9 }))
    expect(retryOutcome.rejectedStale).toBe(false)
    expect(s.rows().some((r) => r.offset === 9)).toBe(true) // recovered, no gap
  })

  it('a page whose startPositions exactly matches the current map (nothing moved in between) is accepted normally', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8 }), { attach: true })
    const outcome = s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 }))
    expect(outcome.rejectedStale).toBe(false)
    expect(s.rows().some((r) => r.offset === 7)).toBe(true)
  })

  it('symmetric: a forward page started from a topMap position a subsequent back trim has since moved past (recovering more below it) is rejected too', () => {
    const s = createSlidingWindowStore(2)
    // Mount at cap: [9,8], bottom={0:8}, top={0:10} (opposite-seed).
    s.insertPage([mk(0, 9, 900), mk(0, 8, 800)], 'back', null, encodeCursor({ 0: 8 }), { attach: true })
    // Overflow trims top(9) -> detach, top map recovers to exactly 9.
    const detachOutcome = s.insertPage([mk(0, 7, 700)], 'back', { 0: 8 }, encodeCursor({ 0: 7 }))
    expect(detachOutcome.trimmedTop).toBe(1)
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 9 })

    // An in-flight forward page is issued FROM top={0:9} (captured now).
    const inFlightStart = { 0: 9 }

    // Meanwhile another back-page overflow trims the top AGAIN (evicting
    // 8) — top map advances FURTHER DOWN to 8, recovering MORE than the
    // in-flight forward page (which only knows to read from 9 upward) was
    // ever told about.
    s.insertPage([mk(0, 6, 600)], 'back', { 0: 7 }, encodeCursor({ 0: 6 }))
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 8 })

    // The stale in-flight page lands: its own startPositions (9) sits
    // ABOVE the current top boundary (8) — the interim trim recovered
    // [8,9) that this page never asked about — so it must be rejected.
    const outcome = s.insertPage([mk(0, 9, 900), mk(0, 10, 1000)], 'forward', inFlightStart, encodeCursor({ 0: 11 }))
    expect(outcome.rejectedStale).toBe(true)
    expect(s.rows().some((r) => r.offset === 10)).toBe(false)
    // Top map unchanged by the rejected page — still exactly where the
    // trim left it.
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 8 })
  })
})

// ---------------------------------------------------------------------------
// Fix round 1 — M2: matches must stream during a scan (product charter),
// not wait for the page's one atomic `insertPage` commit at page-end.
// `previewWithOverlay` merges extra (not-yet-committed) rows in for DISPLAY
// only — no mutation of `partitions`/`seen`/the edge maps/the cap.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — previewWithOverlay (M2, fix round 1)', () => {
  it('merges extra rows into the display view without mutating anything committed', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 5, 500)], 'back', null, encodeCursor({ 0: 5 }), { attach: true })
    const preview = s.previewWithOverlay([mk(0, 6, 600)])
    expect(preview.map((m) => m.offset)).toEqual([6, 5])
    // Nothing committed: the real rows()/edges() are untouched.
    expect(s.rows().map((m) => m.offset)).toEqual([5])
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 5 })
    expect(s.totalCount()).toBe(1)
  })

  it('excludes rows already committed (already `seen`) from the overlay — no visible duplicate', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 5, 500)], 'back', null, encodeCursor({ 0: 5 }), { attach: true })
    const preview = s.previewWithOverlay([mk(0, 5, 500), mk(0, 6, 600)])
    expect(preview.map((m) => m.offset)).toEqual([6, 5]) // offset 5 not duplicated
  })

  it('an empty overlay is just the current committed rows', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 5, 500)], 'back', null, encodeCursor({ 0: 5 }), { attach: true })
    expect(s.previewWithOverlay([])).toEqual(s.rows())
  })

  it('two consecutive previews with disjoint extra rows are each computed fresh (no leakage between calls)', () => {
    const s = createSlidingWindowStore(10)
    s.insertPage([mk(0, 5, 500)], 'back', null, encodeCursor({ 0: 5 }), { attach: true })
    const preview1 = s.previewWithOverlay([mk(0, 6, 600)])
    expect(preview1.map((m) => m.offset)).toEqual([6, 5])
    const preview2 = s.previewWithOverlay([mk(0, 7, 700)])
    // preview2 does NOT include offset 6 (from the FIRST preview call,
    // never committed) — each call is a fresh, independent overlay.
    expect(preview2.map((m) => m.offset)).toEqual([7, 5])
  })
})

// ---------------------------------------------------------------------------
// Fix round 2 — N2 (Medium): the anchor opposite-side seed used to `.set()`
// the opposite map unconditionally, clobbering an already-established,
// more trustworthy value from an EARLIER insert in the SAME window (no
// `clear()` in between) — reachable via Timeline's own loadNewer
// forward-anchor fallback re-issuing an anchor while `bottomMap` already
// holds the real edge from the window's first (back-anchor) bootstrap.
// ---------------------------------------------------------------------------
describe('createSlidingWindowStore — opposite-side anchor seed merges, never clobbers (N2, fix round 2)', () => {
  it("the reviewer's exact probe: a forward-anchor re-issue (loadNewer's fallback) must not clobber an established bottom edge with its own (much higher) row minima", () => {
    const s = createSlidingWindowStore(100)
    // An earlier back-anchor bootstrap already established a real,
    // trustworthy bottom edge.
    s.insertPage([mk(0, 25, 2500), mk(1, 15, 1500)], 'back', null, encodeCursor({ 0: 20, 1: 10 }), { attach: false })
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 20, 1: 10 })

    // Timeline's loadNewer fallback re-issues the SAME anchor forward (no
    // clear() in between) — this page's own rows are 73-79, nowhere near
    // the true bottom edge. The opposite-side seed must not abandon it.
    s.insertPage(
      [mk(0, 79, 7900), mk(0, 78, 7800), mk(1, 41, 4100)],
      'forward',
      null,
      encodeCursor({ 0: 80, 1: 42 }),
    )
    // Bottom edge is UNCHANGED — never regressed to {0:78, 1:41}.
    expect(decodeCursor(s.edges().bottom!)).toEqual({ 0: 20, 1: 10 })
  })

  it('symmetric: a back-anchor re-issue must not clobber an established top edge with its own row maxima', () => {
    const s = createSlidingWindowStore(100)
    s.insertPage([mk(0, 3, 300), mk(1, 2, 200)], 'forward', null, encodeCursor({ 0: 100, 1: 90 }))
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 100, 1: 90 })

    // A back-anchor re-issue (no clear()) whose own rows are far below the
    // established top edge — must not regress it upward.
    s.insertPage([mk(0, 5, 500), mk(1, 4, 400)], 'back', null, encodeCursor({ 0: 4, 1: 3 }), { attach: false })
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 100, 1: 90 })
  })

  it('still fills a genuinely ABSENT partition entry normally (no regression for the common, single-bootstrap case)', () => {
    const s = createSlidingWindowStore(100)
    s.insertPage([mk(0, 9, 900)], 'back', null, encodeCursor({ 0: 8 }), { attach: false })
    expect(decodeCursor(s.edges().top!)).toEqual({ 0: 10 })
  })
})
