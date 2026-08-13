import type { MessageOut } from '../api/types'

export type InsertOrigin = 'live' | 'back' | 'forward'

export interface Dropped {
  top: number
  bottom: number
}

export interface TimelineStore {
  insert(msgs: MessageOut[], origin: InsertOrigin): void
  rows(): MessageOut[]
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
 */
export function createTimelineStore(cap = 2000): TimelineStore {
  const partitions = new Map<number, MessageOut[]>()
  const seen = new Set<string>()
  const dropCounts: Dropped = { top: 0, bottom: 0 }
  let cachedRows: MessageOut[] | null = null

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

  function enforceCap(origin: InsertOrigin): void {
    const excess = totalCount() - cap
    if (excess <= 0) return
    const merged = mergeRows() // newest-first
    if (origin === 'back') {
      // Backward-fill overflow drops from the top (newest end) — the
      // opposite end from where 'back' inserts (older messages).
      for (let i = 0; i < excess; i++) removeMessage(merged[i])
      dropCounts.top += excess
    } else {
      // live/forward overflow drops from the bottom (oldest end).
      for (let i = 0; i < excess; i++) removeMessage(merged[merged.length - 1 - i])
      dropCounts.bottom += excess
    }
    cachedRows = null
  }

  return {
    insert(msgs, origin) {
      let changed = false
      for (const msg of msgs) {
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
      }
      if (changed) cachedRows = null
      enforceCap(origin)
    },
    rows() {
      if (cachedRows === null) cachedRows = mergeRows()
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
