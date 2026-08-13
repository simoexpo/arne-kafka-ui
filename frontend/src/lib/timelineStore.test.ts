import { describe, expect, it } from 'vitest'
import { createTimelineStore } from './timelineStore'
import type { MessageOut } from '../api/types'

const mk = (partition: number, offset: number, ts: number, text = `v${offset}`): MessageOut => ({
  partition, offset, timestamp_ms: ts, key: { encoding: 'utf8', text: `k${offset}`, schema_id: null, error: null },
  value: { encoding: 'utf8', text, schema_id: null, error: null }, headers: [],
})

describe('timelineStore', () => {
  it('keeps rows sorted newest-first and dedups', () => {
    const s = createTimelineStore()
    s.insert([mk(0, 1, 100), mk(0, 3, 300)], 'back')
    s.insert([mk(1, 1, 200), mk(0, 3, 300)], 'live')
    expect(s.rows().map((m) => m.timestamp_ms)).toEqual([300, 200, 100])
    expect(s.rows()).toHaveLength(3)
  })
  it('ties break by partition then offset', () => {
    const s = createTimelineStore()
    s.insert([mk(1, 5, 100), mk(0, 9, 100), mk(0, 7, 100)], 'back')
    expect(s.rows().map((m) => `${m.partition}:${m.offset}`)).toEqual(['0:9', '0:7', '1:5'])
  })
  it('cap drops opposite the origin and counts', () => {
    const s = createTimelineStore(3)
    s.insert([mk(0, 1, 100), mk(0, 2, 200), mk(0, 3, 300)], 'back')
    s.insert([mk(0, 4, 400)], 'live')
    expect(s.rows().map((m) => m.offset)).toEqual([4, 3, 2])
    expect(s.dropped().bottom).toBe(1)
    s.insert([mk(0, 0, 50)], 'back')
    expect(s.rows().map((m) => m.offset)).toEqual([3, 2, 0])
    expect(s.dropped().top).toBe(1)
  })
  it('within_partition_offset_order_beats_timestamp', () => {
    const s = createTimelineStore()
    s.insert([mk(0, 1, 400), mk(0, 2, 100)], 'back')
    expect(s.rows().map((m) => m.offset)).toEqual([2, 1])
  })
})
