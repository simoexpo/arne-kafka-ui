import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../../test/fake-event-source'
import * as sse from '../../api/sse'
import type { MessageOut, SseErrorData } from '../../api/types'
import { encodeCursor } from '../../lib/timelineCursor'
import { Timeline } from './Timeline'

// Task 3: see the identical helper in Timeline.test.tsx — the sliding-window
// store decodes every non-null cursor as a real per-partition position map,
// so an opaque placeholder string like 'c1' is no longer valid here.
const cur = (positions: Record<number, number>) => encodeCursor(positions)
function url(params: Record<string, string>) {
  return `/api/clusters/prod/topics/orders/timeline?${new URLSearchParams(params).toString()}`
}

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

  // Fix round 1 (review of 079f30f), M2: matches must STREAM during a scan
  // (product charter: "stream results"), not wait for the page's single
  // atomic store commit at page_end. Rendered via a DISPLAY-ONLY overlay
  // (the in-flight page's own accumulated matches merged over the
  // committed `rows()`, recomputed fresh each render) — the store's own
  // edge maps still only ever advance once, atomically, at page_end (see
  // `insertPage`'s own contract for why: an anchor bootstrap's opposite-
  // side seed needs the FULL page's rows, not a per-batch trickle).
  it('matches render progressively DURING a filtered scan, before page_end — not held back for the one atomic store commit', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    // useTimelinePage batches matches in groups of 25 (BATCH_SIZE) — a
    // flush fires mid-scan the instant that threshold is crossed,
    // independent of page_end (see its own doc comment). 25 matches here
    // flush automatically, well before page_end fires below.
    for (let i = 0; i < 25; i++) {
      await emit(idx, 'match', mk(100 + i))
    }
    // Still mid-scan — no page_end has fired yet — but the whole batch is
    // already visible via the display overlay, not held back for one
    // atomic store commit at the end. (Row count via the header, not
    // individual row text: the virtualized list only renders rows near the
    // top of a 25+-row window, so the newest (p0·124, always index 0) is a
    // safe row-level check, but the OLDEST wouldn't be — see the message
    // count instead for the full-batch claim.)
    expect(screen.getByText('25 messages')).toBeInTheDocument()
    expect(screen.getByText('p0·124')).toBeInTheDocument()

    await emit(idx, 'match', mk(125))
    await emit(idx, 'page_end', { cursor: null, exhausted: true })
    expect(screen.getByText('26 messages')).toBeInTheDocument()
    expect(screen.getByText('p0·125')).toBeInTheDocument()
  })

  // M2 (continued): cancelling drops the overlay (honest — nothing was
  // ever committed) and leaves the store's own edge maps genuinely
  // untouched, not just visually hidden — proven here by a SECOND,
  // uncancelled scan afterward landing with exactly its own matches, no
  // contamination from the first scan's dropped, never-committed rows.
  it('cancelling a filtered scan mid-flight drops the overlay, and the store is left untouched by it (a later completed scan is not contaminated)', async () => {
    mockTail()
    await mountAndSettleInitial()

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    // Cross the BATCH_SIZE threshold so the overlay is genuinely populated
    // (mid-scan, pre-page_end) before cancelling.
    for (let i = 0; i < 25; i++) {
      await emit(idx, 'match', mk(100 + i))
    }
    expect(screen.getByText('25 messages')).toBeInTheDocument()

    fireEvent.click(screen.getByTestId('cancel-scan'))
    expect(screen.queryByText('25 messages')).not.toBeInTheDocument() // uncommitted, dropped with the overlay
    expect(screen.getByText('0 messages')).toBeInTheDocument()

    // A fresh scan (re-typing the filter re-triggers a settle) lands for
    // real this time — its own match, and ONLY its own match, is what ends
    // up committed. If the cancelled scan's p0·100 had somehow leaked into
    // the store, it would still be here even though a different row
    // (p0·7) is all this second scan ever delivered.
    typeFilter('value:needle2')
    await settle()
    const idx2 = FakeEventSource.instances.length - 1
    await emit(idx2, 'match', mk(7, { value: { encoding: 'utf8', text: 'needle2-7', schema_id: null, error: null } }))
    await emit(idx2, 'page_end', { cursor: null, exhausted: true })
    expect(screen.getByText('p0·7')).toBeInTheDocument()
    expect(screen.queryByText('p0·100')).not.toBeInTheDocument()
    expect(screen.getByText('1 messages')).toBeInTheDocument()
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
      await emit(idx, 'page_end', { cursor: cur({ 0: i + 1 }), exhausted: false })
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
    await emit(idx, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })

    // The real matches must still render — the affordance is additive, not
    // a replacement for the page's own results.
    expect(screen.getByText('p0·1')).toBeInTheDocument()
    expect(screen.getByText('p0·2')).toBeInTheDocument()
    const btn = screen.getByTestId('continue-scan')
    expect(btn).toHaveTextContent('scanned 5000 records · 2 matches — continue')
  })

  // Task 3 (supersedes the v1.4-era "reposition by timestamp" regression
  // test): the sliding-window store's own bottom map always advances to a
  // real, exact, followable cursor when a trim happens — there is no
  // reposition anymore. A filtered scan can stop with the continue-scan
  // button showing, then live traffic (which still passes the filter
  // predicate) trims the store's oldest row off the bottom before the
  // button is ever clicked — clicking it must use the store's fresh bottom
  // edge directly (which the trim already advanced past the evicted row),
  // never anything captured earlier.
  it('a bottom trim while the continue-scan button is showing uses the store\'s fresh edge on click, never a stale value', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })

    typeFilter('value:needle')
    await settle()
    const idx = FakeEventSource.instances.length - 1
    await emit(idx, 'match', mk(1))
    await emit(idx, 'match', mk(2))
    await emit(idx, 'progress', { scanned: 5000, matches: 2, budget: 5000 })
    await emit(idx, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })
    expect(screen.getByTestId('continue-scan')).toBeInTheDocument()

    // Live traffic (must pass the "value:needle" predicate to merge at all)
    // pushes the store past cap(3): the oldest matched row (p0·1) trims off
    // the bottom, advancing the store's own bottom edge past it.
    await act(async () => {
      tail.handlers().onMessage(mk(3, { value: { encoding: 'utf8', text: 'needle-3', schema_id: null, error: null } }))
    })
    await act(async () => {
      tail.handlers().onMessage(mk(4, { value: { encoding: 'utf8', text: 'needle-4', schema_id: null, error: null } }))
    })
    expect(screen.queryByText('p0·1')).not.toBeInTheDocument()

    fireEvent.click(screen.getByTestId('continue-scan'))
    const req = FakeEventSource.instances.at(-1)!.url
    // No reposition: the store's own bottom edge (advanced to recover
    // exactly the trimmed row, offset 1) is used directly, with the filter
    // still merged onto it via withFilter.
    expect(req).toBe(
      url({ direction: 'back', limit: '100', cursor: cur({ 0: 2 }), filter: 'value_contains', q: 'needle' }),
    )
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
      await emit(idx, 'page_end', { cursor: cur({ 0: i + 1 }), exhausted: false })
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
    await emit(idx, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
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
    await emit(filteredIdx, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

    // A filtered page with 1 match (fewer than a full page) and a non-null,
    // non-exhausted cursor is the I2 partial-match budget-stop case: the
    // continue affordance shows instead of plain scroll-triggered paging,
    // but clicking it drives the exact same withFilter-merged request.
    fireEvent.click(screen.getByTestId('continue-scan'))
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      url({ direction: 'back', limit: '100', cursor: cur({ 0: 9 }), filter: 'json_eq', q: '42', path: 'user.id' }),
    )
    const olderIdx = FakeEventSource.instances.length - 1
    // Zero matches this time — the empty-page contract auto-continues on
    // its own (unaffected by the assertions below, which all use `.at(-1)`).
    await emit(olderIdx, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })

    fireEvent.click(screen.getByTestId('jump-now'))
    expect(FakeEventSource.instances.at(-1)!.url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest&filter=json_eq&q=42&path=user.id',
    )
    const jumpIdx = FakeEventSource.instances.length - 1
    await emit(jumpIdx, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })

    // Clearing the filter drops it from every subsequent request too.
    typeFilter('')
    await settle()
    const clearedIdx = FakeEventSource.instances.length - 1
    await emit(clearedIdx, 'match', mk(1))
    await emit(clearedIdx, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })

    scrollToBottom()
    expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 1 }) }))
  })

  it('after jumping to beginning, a settled filter change re-anchors forward/beginning', async () => {
    mockTail()
    await mountAndSettleInitial()

    fireEvent.click(screen.getByTestId('jump-beginning'))
    const beginIdx = FakeEventSource.instances.length - 1
    await emit(beginIdx, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })

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
