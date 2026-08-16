import { describe, expect, it } from 'vitest'
import type { MessageOut } from '../../api/types'
import { createLiveBuffer } from './liveBuffer'

const mk = (offset: number): MessageOut => ({
  partition: 0,
  offset,
  timestamp_ms: 1000 + offset,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
})

describe('createLiveBuffer', () => {
  it('starts empty: nothing received, not overflowed, drains nothing', () => {
    const buf = createLiveBuffer(3)
    expect(buf.received).toBe(0)
    expect(buf.overflowed).toBe(false)
    expect(buf.drain()).toEqual([])
  })

  it('push accumulates messages in arrival order and counts them, uncapped', () => {
    const buf = createLiveBuffer(3)
    buf.push(mk(1))
    buf.push(mk(2))
    expect(buf.received).toBe(2)
    expect(buf.overflowed).toBe(false)
    expect(buf.drain()).toEqual([mk(1), mk(2)])
  })

  it('drops the OLDEST entry once push count exceeds cap, and flags overflow permanently', () => {
    const buf = createLiveBuffer(2)
    buf.push(mk(1))
    buf.push(mk(2))
    buf.push(mk(3)) // over cap: drop offset 1
    expect(buf.overflowed).toBe(true)
    expect(buf.drain()).toEqual([mk(2), mk(3)])
  })

  it('received keeps counting honestly past the cap (never freezes at the cap)', () => {
    const buf = createLiveBuffer(2)
    buf.push(mk(1))
    buf.push(mk(2))
    buf.push(mk(3))
    buf.push(mk(4))
    expect(buf.received).toBe(4)
  })

  it('drain empties the buffer and resets received/overflowed to their initial state', () => {
    const buf = createLiveBuffer(2)
    buf.push(mk(1))
    buf.push(mk(2))
    buf.push(mk(3))
    buf.drain()
    expect(buf.received).toBe(0)
    expect(buf.overflowed).toBe(false)
    expect(buf.drain()).toEqual([])
  })

  it('clear discards buffered messages and resets received/overflowed without returning anything', () => {
    const buf = createLiveBuffer(2)
    buf.push(mk(1))
    buf.push(mk(2))
    buf.push(mk(3))
    buf.clear()
    expect(buf.received).toBe(0)
    expect(buf.overflowed).toBe(false)
    expect(buf.drain()).toEqual([])
  })
})
