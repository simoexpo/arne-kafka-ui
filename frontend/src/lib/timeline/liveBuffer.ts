// Auto-pause buffer policy (see design spec): while paused (auto or
// explicit) or detached, live messages that pass the predicate accumulate
// here instead of the store. Capped — beyond that we drop the OLDEST
// buffered entry per arrival and let the caller show an honest "n+ · older
// dropped" label rather than a raw (and increasingly meaningless) count.
// `received` is a SEPARATE, uncapped counter: it keeps counting honestly
// past the cap rather than freezing at a fixed number; `overflowed` flips
// permanently (until the next drain/clear) once the buffer has actually
// started dropping entries.
import type { MessageOut } from '../../api/types'

export interface LiveBuffer {
  push(m: MessageOut): void
  /** Returns the buffered messages (arrival order) and resets to the initial state. */
  drain(): MessageOut[]
  /** Discards whatever is buffered and resets to the initial state. */
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
        items.shift() // drop the OLDEST buffered entry
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
