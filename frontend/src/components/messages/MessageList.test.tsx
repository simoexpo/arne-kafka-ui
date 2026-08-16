import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MessageList, type MessageListHandle } from './MessageList'
import type { MessageOut } from '../../api/types'

const mk = (offset: number): MessageOut => ({
  partition: 0, offset, timestamp_ms: 1,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
})

const msg = (overrides: Partial<MessageOut> = {}): MessageOut => ({
  partition: 0, offset: 0, timestamp_ms: 1,
  key: null,
  value: { encoding: 'utf8', text: 'v', schema_id: null, error: null },
  headers: [],
  ...overrides,
})

describe('MessageList', () => {
  it('renders visible rows through the virtualizer', () => {
    render(<MessageList messages={Array.from({ length: 200 }, (_, i) => mk(i))} />)
    expect(screen.getByText('p0·0')).toBeInTheDocument()
    expect(screen.getAllByTestId('message-row').length).toBeLessThan(60) // virtualized, not 200
  })
  it('shows an empty state', () => {
    render(<MessageList messages={[]} />)
    expect(screen.getByText('no messages')).toBeInTheDocument()
  })
  it('flags exactly the rows whose timestamp is newer than the row above', () => {
    // newest-first merged order with a same-partition inversion:
    // [p1o5 ts=80, p0o2 ts=50, p0o1 ts=100] → only the ts=100 row is marked
    const rows = [
      msg({ partition: 1, offset: 5, timestamp_ms: 80 }),
      msg({ partition: 0, offset: 2, timestamp_ms: 50 }),
      msg({ partition: 0, offset: 1, timestamp_ms: 100 }),
    ]
    render(<MessageList messages={rows} />)
    expect(screen.getAllByTestId('ts-inversion')).toHaveLength(1)
    const marked = screen.getAllByTestId('message-row')[2]
    expect(marked.querySelector('[data-testid="ts-inversion"]')).toBeInTheDocument()
  })
  it('null timestamps neither get marked nor mark their neighbors', () => {
    const rows = [
      msg({ partition: 0, offset: 3, timestamp_ms: null }),
      msg({ partition: 0, offset: 2, timestamp_ms: 100 }),
      msg({ partition: 0, offset: 1, timestamp_ms: null }),
    ]
    render(<MessageList messages={rows} />)
    expect(screen.queryAllByTestId('ts-inversion')).toHaveLength(0)
  })
  it('marks exactly the row matching jumpTarget partition+offset', () => {
    const rows = [
      msg({ partition: 1, offset: 5 }),
      msg({ partition: 0, offset: 2 }),
      msg({ partition: 0, offset: 5 }), // same offset, different partition — must not match
    ]
    render(<MessageList messages={rows} jumpTarget={{ partition: 0, offset: 2 }} />)
    expect(screen.getAllByTestId('jump-target')).toHaveLength(1)
    const marked = screen.getAllByTestId('message-row')[1]
    expect(marked.querySelector('[data-testid="jump-target"]')).toBeInTheDocument()
  })
  it('marks no row when jumpTarget is null', () => {
    const rows = [msg({ partition: 0, offset: 2 })]
    render(<MessageList messages={rows} jumpTarget={null} />)
    expect(screen.queryAllByTestId('jump-target')).toHaveLength(0)
  })
  it('a prepended forward page does not reset an already-open row (row identity survives an index shift)', async () => {
    const user = userEvent.setup()
    const rowA = msg({ partition: 0, offset: 5 })
    const rowB = msg({
      partition: 0,
      offset: 3,
      headers: [{ key: 'trace-id', value: 'abc' }],
    })
    const { rerender } = render(<MessageList messages={[rowA, rowB]} />)
    // Open rowB (currently at index 1) — its expanded headers become visible.
    await user.click(screen.getAllByTestId('message-row')[1])
    expect(screen.getByText('trace-id')).toBeInTheDocument()

    // A forward page prepends a newer row — rowB shifts from index 1 to
    // index 2, same (partition, offset) identity throughout.
    const rowC = msg({ partition: 0, offset: 10 })
    rerender(<MessageList messages={[rowC, rowA, rowB]} />)
    expect(screen.getByText('trace-id')).toBeInTheDocument()
  })

  // Owner-reported regression (2026-08-16): expanding a row and then
  // receiving a live message drew the list jumbled — rows overlapping each
  // other, with a hole where the expanded one used to be. The virtualizer
  // caches each row's MEASURED height under `getItemKey(index)`, which
  // defaults to the index itself; a live message prepends, every index shifts
  // by one, and each row inherits its neighbour's cached height. Normally
  // that is a few pixels of drift; with one row expanded to ~5-10x the
  // others, the misattribution is the corruption the owner saw. Keying the
  // cache by (partition, offset) — the same identity the React key and the
  // store's dedupe already use — makes a measurement travel with its message.
  describe('measured row heights (identity-keyed cache)', () => {
    // The virtualizer measures via `element.offsetHeight`, which jsdom always
    // reports as the flat stub from test/setup.ts. Deriving it from the
    // element's own text gives rows of genuinely different heights, which is
    // the only condition under which mis-keyed measurements are visible at
    // all. ResizeObserver has to exist for MessageList to attach
    // `measureElement` as a row ref in the first place (see the guard there).
    function withMeasuredRows(tallText: string, tall: number, short: number) {
      const originalOffsetHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight')!
      const originalRo = globalThis.ResizeObserver
      Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
        configurable: true,
        get(this: HTMLElement) {
          if (this.dataset.index === undefined) return 600
          return this.textContent?.includes(tallText) ? tall : short
        },
      })
      globalThis.ResizeObserver = class {
        observe() {}
        unobserve() {}
        disconnect() {}
      } as unknown as typeof ResizeObserver
      return () => {
        Object.defineProperty(HTMLElement.prototype, 'offsetHeight', originalOffsetHeight)
        globalThis.ResizeObserver = originalRo
      }
    }

    const topOf = (row: HTMLElement) => row.parentElement!.style.transform

    it('a prepended row does not hand an existing row its neighbour’s measured height', () => {
      const restore = withMeasuredRows('tall', 200, 40)
      try {
        const short = msg({ partition: 0, offset: 5 })
        const tall = msg({
          partition: 0,
          offset: 3,
          value: { encoding: 'utf8', text: 'tall', schema_id: null, error: null },
        })
        const { rerender } = render(<MessageList messages={[short, tall]} />)
        expect(topOf(screen.getByText('p0·3').closest('[data-testid="message-row"]')!)).toBe('translateY(40px)')

        // A live message prepends: every index shifts by one, no measured
        // height changes. The tall row must still start right below the two
        // 40px rows above it — not be pushed down by inheriting index 2's
        // (nonexistent) measurement while the short row above it inherits the
        // tall one's 200px.
        const live = msg({ partition: 0, offset: 10 })
        rerender(<MessageList messages={[live, short, tall]} />)
        expect(topOf(screen.getByText('p0·3').closest('[data-testid="message-row"]')!)).toBe('translateY(80px)')
      } finally {
        restore()
      }
    })

    it('rowOffsetAt still reports the offset of the row at a given index', () => {
      const restore = withMeasuredRows('tall', 200, 40)
      try {
        const handle = { current: null } as { current: MessageListHandle | null }
        const rows = [
          msg({ partition: 0, offset: 5 }),
          msg({ partition: 0, offset: 3, value: { encoding: 'utf8', text: 'tall', schema_id: null, error: null } }),
          msg({ partition: 0, offset: 1 }),
        ]
        render(<MessageList ref={handle} messages={rows} />)
        expect(handle.current!.rowOffsetAt(0)).toBe(0)
        expect(handle.current!.rowOffsetAt(1)).toBe(40)
        expect(handle.current!.rowOffsetAt(2)).toBe(240)
        expect(handle.current!.rowOffsetAt(3)).toBeNull()
      } finally {
        restore()
      }
    })
  })
})
