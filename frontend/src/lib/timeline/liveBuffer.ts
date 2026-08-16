import type { MessageOut } from '../../api/types'

export interface LiveBuffer {
  push(m: MessageOut): void
  /** Returns the buffered messages and resets received/overflowed. */
  drain(): MessageOut[]
  /** Discards buffered messages and resets received/overflowed. */
  clear(): void
  readonly received: number
  readonly overflowed: boolean
}

export function createLiveBuffer(cap: number): LiveBuffer {
  let items: MessageOut[] = []
  let received = 0
  let overflowed = false
  return {
    push(m) {
      items.push(m)
      received += 1
      if (items.length > cap) {
        items.shift()
        overflowed = true
      }
    },
    drain() {
      const drained = items
      items = []
      received = 0
      overflowed = false
      return drained
    },
    clear() {
      items = []
      received = 0
      overflowed = false
    },
    get received() {
      return received
    },
    get overflowed() {
      return overflowed
    },
  }
}
