import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../../test/fake-event-source'
import * as sse from '../../api/sse'
import type { MessageOut, SseErrorData } from '../../api/types'
import { Timeline } from './Timeline'

vi.mock('../../api/sse', async (importOriginal) => ({
  ...(await importOriginal<typeof sse>()),
  tailTopic: vi.fn(),
}))

beforeEach(() => {
  FakeEventSource.install()
  vi.useFakeTimers()
})
afterEach(() => {
  vi.useRealTimers()
  FakeEventSource.uninstall()
})

interface TailHandlers {
  onMessage: (m: MessageOut) => void
  onError: (e: SseErrorData) => void
  onTransportError: () => void
}

function mockTail() {
  let handlers: TailHandlers | null = null
  const close = vi.fn()
  vi.mocked(sse.tailTopic).mockImplementation((_c, _t, h) => {
    handlers = h as unknown as TailHandlers
    return { close }
  })
  return { handlers: () => handlers!, close }
}

const mk = (offset: number, overrides: Partial<MessageOut> = {}): MessageOut => ({
  partition: 0,
  offset,
  timestamp_ms: 1000 + offset,
  key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null },
  headers: [],
  ...overrides,
})

async function emit(index: number, name: string, data: unknown) {
  await act(async () => {
    FakeEventSource.instances[index].emit(name, data)
  })
}

function typeFilter(text: string) {
  fireEvent.change(screen.getByLabelText('filter messages'), { target: { value: text } })
}

// jsdom reports scrollHeight/clientHeight as 0 (no real layout) — stubbing
// both lets the scroll-triggered-continue test below construct a "near the
// bottom" scroll position. Mirrors the identical helpers in Timeline.test.tsx.
function stubScrollHeight(value: number) {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollHeight')
  Object.defineProperty(Element.prototype, 'scrollHeight', { configurable: true, get: () => value })
  return () => {
    if (original) Object.defineProperty(Element.prototype, 'scrollHeight', original)
    else delete (Element.prototype as unknown as Record<string, unknown>).scrollHeight
  }
}
function stubClientHeight(value: number) {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'clientHeight')
  Object.defineProperty(Element.prototype, 'clientHeight', { configurable: true, get: () => value })
  return () => {
    if (original) Object.defineProperty(Element.prototype, 'clientHeight', original)
    else delete (Element.prototype as unknown as Record<string, unknown>).clientHeight
  }
}

// Scroll is the ONLY pagination affordance (no load-older/load-newer
// buttons) — mirrors the identical helper in Timeline.test.tsx.
function scrollToBottom() {
  const restoreScrollHeight = stubScrollHeight(810)
  const restoreClientHeight = stubClientHeight(600)
  try {
    fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 200 } })
  } finally {
    restoreScrollHeight()
    restoreClientHeight()
  }
}

async function settle(ms = 500) {
  await act(async () => {
    vi.advanceTimersByTime(ms)
  })
}

async function mountAndSettleInitial() {
  render(<Timeline cluster="prod" topic="orders" />)
  await emit(0, 'page_end', { cursor: null, exhausted: true })
}

describe('Timeline filter box', () => {
  it('renders the filter input with the expected placeholder and aria-label', async () => {
    mockTail()
    await mountAndSettleInitial()
    expect(screen.getByLabelText('filter messages')).toHaveAttribute('placeholder', 'filter messages…')
  })

  it('debounces: no request is issued until 500ms of no further edits pass', async () => {
    mockTail()
    await mountAndSettleInitial()
    const before = FakeEventSource.instances.length

    typeFilter('abc')
    await settle(499)
    expect(FakeEventSource.instances).toHaveLength(before)

    await settle(1)
    expect(FakeEventSource.instances).toHaveLength(before + 1)
  })

  it('rapid edits collapse into a single request for the final value', async () => {
    mockTail()
    await mountAndSettleInitial()
    const before = FakeEventSource.instances.length

    typeFilter('a')
    await settle(200)
    typeFilter('ab')
    await settle(200)
    typeFilter('abc')
    await settle(500)

    expect(FakeEventSource.instances).toHaveLength(before + 1)
    expect(FakeEventSource.instances.at(-1)!.url).toContain('q=abc')
  })

  it('a settled filter reloads back/latest with the parsed api params', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('key:foo')
    await settle()
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest&filter=key_contains&q=foo',
    )
  })

  it('live messages are filtered client-side by the parsed predicate immediately after settling', async () => {
    const tail = mockTail()
    await mountAndSettleInitial()

    typeFilter('key:foo')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'page_end', { cursor: null, exhausted: true })

    await act(async () => {
      tail.handlers().onMessage(mk(5, { key: { encoding: 'utf8', text: 'bar', schema_id: null, error: null } }))
    })
    expect(screen.queryByText('p0·5')).not.toBeInTheDocument()

    await act(async () => {
      tail.handlers().onMessage(mk(6, { key: { encoding: 'utf8', text: 'foobar', schema_id: null, error: null } }))
    })
    expect(screen.getByText('p0·6')).toBeInTheDocument()
  })

  it('shows an inline progress row with a Cancel button while a filtered scan runs, and Cancel closes the stream without auto-continuing', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:zzz')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'progress', { scanned: 40, matches: 0, budget: 250000 })

    expect(screen.getByText('scanned 40 · 0 matches')).toBeInTheDocument()
    const cancelBtn = screen.getByTestId('cancel-scan')

    fireEvent.click(cancelBtn)
    expect(FakeEventSource.instances[idx].closed).toBe(true)
    expect(screen.queryByTestId('filter-progress')).not.toBeInTheDocument()

    const countAfterCancel = FakeEventSource.instances.length
    await act(async () => {
      await Promise.resolve()
    })
    expect(FakeEventSource.instances).toHaveLength(countAfterCancel)
  })

  it('editing the filter mid-scan cancels the in-flight page', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:aaa')
    await settle()
    const firstIdx = FakeEventSource.instances.length - 1
    expect(FakeEventSource.instances[firstIdx].closed).toBe(false)

    typeFilter('value:bbb')
    await settle()

    expect(FakeEventSource.instances[firstIdx].closed).toBe(true)
    expect(FakeEventSource.instances.at(-1)!.url).toContain('q=bbb')
  })

  it('hitting the iteration cap with a filter active shows accumulated scanned/matches totals in the continue affordance', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()

    let totalScanned = 0
    for (let i = 0; i < 25; i++) {
      const idx = FakeEventSource.instances.length - 1
      totalScanned += 50
      await emit(idx, 'progress', { scanned: 50, matches: 0, budget: 250000 })
      await emit(idx, 'page_end', { cursor: `c${i + 1}`, exhausted: false })
      if (screen.queryByTestId('continue-scan')) break
    }

    const btn = screen.getByTestId('continue-scan')
    expect(btn).toHaveTextContent(`scanned ${totalScanned} records · 0 matches — continue`)
  })

  it('a filtered page that finds some matches but stops short of a full page (budget spent) shows the continue affordance, not a silent load-older', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'match', mk(1))
    await emit(idx, 'match', mk(2))
    await emit(idx, 'progress', { scanned: 5000, matches: 2, budget: 5000 })
    // Budget spent partway through, not the true topic edge: non-null
    // cursor, exhausted:false — same contract as the zero-match case, just
    // with 1..99 matches this time.
    await emit(idx, 'page_end', { cursor: 'c1', exhausted: false })

    // The real matches must still render — the affordance is additive, not
    // a replacement for the page's own results.
    expect(screen.getByText('p0·1')).toBeInTheDocument()
    expect(screen.getByText('p0·2')).toBeInTheDocument()
    const btn = screen.getByTestId('continue-scan')
    expect(btn).toHaveTextContent('scanned 5000 records · 2 matches — continue')
  })

  // F1 regression (window-cap-honesty review round 1, trace a): a filtered
  // scan can stop with the continue-scan button showing, then live traffic
  // (which still passes the filter predicate) drops the store's oldest row
  // off the bottom before the button is ever clicked — the button's cursor
  // now points at a range the window slid past. Clicking it must reposition
  // by timestamp, never follow that stale cursor into an invisible gap.
  it('a bottom drop while the continue-scan button is showing repositions on click instead of following the stale cursor', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'match', mk(1))
    await emit(idx, 'match', mk(2))
    await emit(idx, 'progress', { scanned: 5000, matches: 2, budget: 5000 })
    await emit(idx, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByTestId('continue-scan')).toBeInTheDocument()

    // Live traffic (must pass the "value:needle" predicate to merge at all)
    // pushes the store past cap(3): the oldest matched row (p0·1) drops off
    // the bottom — the continue button's cursor ('c1') now points past it.
    await act(async () => {
      tail.handlers().onMessage(mk(3, { value: { encoding: 'utf8', text: 'needle-3', schema_id: null, error: null } }))
    })
    await act(async () => {
      tail.handlers().onMessage(mk(4, { value: { encoding: 'utf8', text: 'needle-4', schema_id: null, error: null } }))
    })
    expect(screen.queryByText('p0·1')).not.toBeInTheDocument()

    fireEvent.click(screen.getByTestId('continue-scan'))
    const req = FakeEventSource.instances.at(-1)!.url
    expect(req).toContain('anchor=timestamp')
    expect(req).not.toContain('cursor=')
  })

  it('clearing the filter (×) reloads unfiltered', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('key:foo')
    await settle()
    const filteredIdx = FakeEventSource.instances.length - 1
    await emit(filteredIdx, 'page_end', { cursor: null, exhausted: true })

    fireEvent.click(screen.getByRole('button', { name: 'clear filter' }))
    await settle()

    const last = FakeEventSource.instances.at(-1)!
    expect(last.url).toBe('/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest')
  })

  it('clicking continue after the cap preserves prior gesture totals and adds to them, never restarting from zero', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()

    let totalScanned = 0
    for (let i = 0; i < 25; i++) {
      const idx = FakeEventSource.instances.length - 1
      totalScanned += 50
      await emit(idx, 'progress', { scanned: 50, matches: 0, budget: 250000 })
      await emit(idx, 'page_end', { cursor: `c${i + 1}`, exhausted: false })
      if (screen.queryByTestId('continue-scan')) break
    }
    const btn = screen.getByTestId('continue-scan')
    expect(btn).toHaveTextContent(`scanned ${totalScanned} records · 0 matches — continue`)

    fireEvent.click(btn)
    // Immediately after clicking continue (before any new progress arrives)
    // the running total must still reflect everything scanned before the
    // click — it must NOT have been reset back to 0.
    expect(screen.getByText(`scanned ${totalScanned} · 0 matches`)).toBeInTheDocument()

    const idx2 = FakeEventSource.instances.length - 1
    await emit(idx2, 'progress', { scanned: 30, matches: 0, budget: 250000 })
    expect(screen.getByText(`scanned ${totalScanned + 30} · 0 matches`)).toBeInTheDocument()
  })

  it('scrolling near the bottom while the continue affordance is showing continues the gesture (preserves totals), not a silent reset', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'match', mk(9))
    await emit(idx, 'progress', { scanned: 5000, matches: 1, budget: 5000 })
    // I2's partial-match budget stop: 1 match, cursor non-null, not
    // exhausted -> continue-scan shows instead of load-older.
    await emit(idx, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByTestId('continue-scan')).toHaveTextContent('scanned 5000 records · 1 matches — continue')

    const restoreScrollHeight = stubScrollHeight(810)
    const restoreClientHeight = stubClientHeight(600)
    try {
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 200 } })
    } finally {
      restoreScrollHeight()
      restoreClientHeight()
    }

    // The scroll-triggered continuation must be the SAME gesture — totals
    // must still reflect what was scanned before it, exactly like clicking
    // the button does ("clicking continue after the cap preserves prior
    // gesture totals"). A naive scroll-triggered `loadOlder()` would instead
    // start a fresh gesture and reset the totals to 0.
    expect(screen.getByText('scanned 5000 · 1 matches')).toBeInTheDocument()
  })

  it('withFilter merges the active filter onto scroll-triggered pagination and jumps, and drops it after clearing', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('user.id=42')
    await settle()
    const filteredIdx = FakeEventSource.instances.length - 1
    await emit(filteredIdx, 'match', mk(9))
    await emit(filteredIdx, 'page_end', { cursor: 'c1', exhausted: false })

    // A filtered page with 1 match (fewer than a full page) and a non-null,
    // non-exhausted cursor is the I2 partial-match budget-stop case: the
    // continue affordance shows instead of plain scroll-triggered paging,
    // but clicking it drives the exact same withFilter-merged request.
    fireEvent.click(screen.getByTestId('continue-scan'))
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&cursor=c1&filter=json_eq&q=42&path=user.id',
    )
    const olderIdx = FakeEventSource.instances.length - 1
    await emit(olderIdx, 'page_end', { cursor: 'c2', exhausted: false })

    fireEvent.click(screen.getByTestId('jump-now'))
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest&filter=json_eq&q=42&path=user.id',
    )
    const jumpIdx = FakeEventSource.instances.length - 1
    await emit(jumpIdx, 'page_end', { cursor: 'c3', exhausted: false })

    // Clearing the filter drops it from every subsequent request too.
    typeFilter('')
    await settle()
    const clearedIdx = FakeEventSource.instances.length - 1
    await emit(clearedIdx, 'match', mk(1))
    await emit(clearedIdx, 'page_end', { cursor: 'c4', exhausted: false })

    scrollToBottom()
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&cursor=c4',
    )
  })

  it('after jumping to beginning, a settled filter change re-anchors forward/beginning', async () => {
    mockTail()
    await mountAndSettleInitial()

    fireEvent.click(screen.getByTestId('jump-beginning'))
    const beginIdx = FakeEventSource.instances.length - 1
    await emit(beginIdx, 'page_end', { cursor: 'c-fwd', exhausted: false })

    typeFilter('value:zzz')
    await settle()

    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&anchor=beginning&filter=value_contains&q=zzz',
    )
  })

  it('unmounting mid-debounce issues no request once timers advance', async () => {
    mockTail()
    const { unmount } = render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })

    const before = FakeEventSource.instances.length
    typeFilter('abc')
    unmount()
    await settle()
    expect(FakeEventSource.instances).toHaveLength(before)
  })
})
