import { act, fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../../test/fake-event-source'
import { withFixedTZ } from '../../test/timezone'
import * as sse from '../../api/sse'
import type { MessageOut, SseErrorData } from '../../api/types'
import { encodeCursor } from '../../lib/timelineCursor'
import { setTimeDisplayMode } from '../../lib/timeDisplayMode'
import { Timeline } from './Timeline'

// Task 3: the sliding-window store decodes every non-null cursor as a real
// per-partition position map (see timelineCursor.ts) — unlike the old v1.4
// store, an opaque placeholder string like 'c1' is no longer a valid
// `page_end` cursor in these tests (`decodeCursor` throws on it). `cur`
// mints a real one; `url` builds the exact expected request URL the same
// way `sse.ts` does (URLSearchParams, so cursor bytes are percent-encoded
// consistently) rather than hand-encoding base64 in assertions.
const cur = (positions: Record<number, number>) => encodeCursor(positions)
function url(params: Record<string, string>) {
  return `/api/clusters/prod/topics/orders/timeline?${new URLSearchParams(params).toString()}`
}

vi.mock('../../api/sse', async (importOriginal) => ({
  ...(await importOriginal<typeof sse>()),
  tailTopic: vi.fn(),
}))

beforeEach(() => FakeEventSource.install())
afterEach(() => FakeEventSource.uninstall())

interface TailHandlers {
  onMessage: (m: MessageOut) => void
  onError: (e: SseErrorData) => void
  onTransportError: () => void
}

// tailTopic is mocked wholesale (rather than driven through FakeEventSource
// like the timeline page SSE) so tests can trigger tail events directly
// without disambiguating which EventSource instance belongs to which
// stream.
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

async function settleWithOneRow(index = 0) {
  await emit(index, 'match', mk(1))
  await emit(index, 'page_end', { cursor: null, exhausted: true })
}

// Spies on every `el.scrollTop = ...` assignment across the DOM (not just
// one element) — necessary because a jump always unmounts/remounts
// MessageList in between (see the pause-machinery/jump-control describe
// blocks below): the store is cleared synchronously on click, so the
// eventual scrollToEdge call lands on a BRAND NEW container whose default
// scrollTop is already 0, making "did it get set to 0" indistinguishable
// from "nothing happened" if we only read the final DOM value. Spying on
// the assignment itself proves the call happened, regardless of which
// physical node it landed on.
function spyOnScrollTop() {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollTop')!
  const setter = vi.fn()
  Object.defineProperty(Element.prototype, 'scrollTop', {
    configurable: true,
    get() {
      return original.get!.call(this)
    },
    set(v: number) {
      setter(v)
      original.set!.call(this, v)
    },
  })
  return { setter, restore: () => Object.defineProperty(Element.prototype, 'scrollTop', original) }
}

// Same idea for `scrollHeight`, which jsdom always reports as 0 (no real
// layout) — stubbing it to a distinct value lets the "scroll to bottom"
// (scrollTop = scrollHeight) case prove it actually read scrollHeight,
// rather than coincidentally landing on 0 same as the "scroll to top" case.
function stubScrollHeight(value: number) {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollHeight')
  Object.defineProperty(Element.prototype, 'scrollHeight', { configurable: true, get: () => value })
  return () => {
    if (original) Object.defineProperty(Element.prototype, 'scrollHeight', original)
    else delete (Element.prototype as unknown as Record<string, unknown>).scrollHeight
  }
}

// Same idea for `clientHeight` (also always 0 in jsdom, no real layout) —
// needed alongside `stubScrollHeight` to construct a "near the bottom"
// scroll position for the bottom-sentinel (scroll-triggered pagination)
// tests below.
function stubClientHeight(value: number) {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'clientHeight')
  Object.defineProperty(Element.prototype, 'clientHeight', { configurable: true, get: () => value })
  return () => {
    if (original) Object.defineProperty(Element.prototype, 'clientHeight', original)
    else delete (Element.prototype as unknown as Record<string, unknown>).clientHeight
  }
}

// Scroll is the ONLY pagination affordance (no load-older/load-newer
// buttons) — these two helpers drive the bottom/top sentinels the same way
// a real scroll would. `scrollToBottom` stubs scrollHeight/clientHeight so
// the "near the bottom" arithmetic (see Timeline's BOTTOM_PIN_THRESHOLD)
// reads a real gap rather than jsdom's default 0/0; `scrollToTop` just needs
// scrollTop back under TOP_PIN_THRESHOLD.
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
// A "scroll to top" gesture with scrollHeight/clientHeight stubbed far
// apart so the BOTTOM sentinel's own arithmetic reads a real, non-zero gap
// (jsdom's default 0/0 would otherwise make `nearBottom` true too — see
// BOTTOM_PIN_THRESHOLD — firing an unrelated bottom-sentinel request in the
// same scroll event). Needed whenever the back edge is non-null and not
// exhausted at the moment of the top gesture — task 3: this is now the
// COMMON case (any anchor bootstrap seeds a real opposite-side edge — see
// createSlidingWindowStore's own doc comment), so a bare `scrollTop: 0`
// scroll event is used directly ONLY in the handful of places where the
// back edge is genuinely null/exhausted at that point.
// Total row count, read off the virtualizer's own total-size div (uniform
// 40px rows in jsdom, no ResizeObserver) — used where a test's real point is
// "did every row actually land in the store", now that the header no longer
// renders a raw "{n} messages" count to assert on directly (see
// formatWindowRange's own owner-feedback comment in Timeline.tsx).
function totalRowsHeight(): number {
  const scroller = screen.getByTestId('timeline-scroll')
  const inner = scroller.firstElementChild as HTMLElement
  return Number(inner.style.height.replace('px', ''))
}

function scrollToTopFarFromBottom() {
  const restoreScrollHeight = stubScrollHeight(2000)
  const restoreClientHeight = stubClientHeight(600)
  try {
    fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
  } finally {
    restoreScrollHeight()
    restoreClientHeight()
  }
}

// Real browsers (unlike jsdom) fire genuine native 'scroll' events while a
// jump's landing settles — the virtualizer keeps remeasuring real row
// heights via `ResizeObserver` (jsdom has none) for a couple of cycles
// after `scrollToEdge`, moving the true edge each time. Timeline treats
// this as SETTLING (see `settlingRef`'s own comment on `Timeline`), never a
// pagination trigger, until `scrollHeight` reports the SAME value on two
// CONSECUTIVE events. jsdom never produces that cascade on its own, so any
// test that jumps and then wants to prove a GENUINE subsequent scroll still
// triggers pagination must close settling out first — exactly mirroring
// what a real browser's own cascade already resolved automatically — or
// its own real scroll would itself be read as still-settling and
// swallowed.
//
// Fires the SAME stubbed scrollHeight/clientHeight twice in a row (closing
// settling on the second call — nothing else changed `scrollHeight` in
// between), at a scrollTop that is DELIBERATELY neither pinned-top nor
// near-bottom: closing settling still runs the SECOND call through normal
// `handleScroll` (that's what "closing" means — settling stops swallowing
// as of this exact event), so a value that would itself trivially satisfy
// a sentinel (jsdom's unstubbed default is 0/0/0, which satisfies BOTH)
// would fire an unwanted pagination request as a side effect of merely
// closing out the helper — defeating its own purpose of being a neutral,
// nothing-happens close.
function consumeLandingEcho() {
  const el = screen.getByTestId('timeline-scroll')
  const restoreScrollHeight = stubScrollHeight(2000)
  const restoreClientHeight = stubClientHeight(600)
  try {
    fireEvent.scroll(el, { target: { scrollTop: 500 } })
    fireEvent.scroll(el, { target: { scrollTop: 500 } })
  } finally {
    restoreScrollHeight()
    restoreClientHeight()
  }
}

describe('Timeline', () => {
  it('loads the latest page on mount and renders rows in store order', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
    )
    await emit(0, 'match', mk(2))
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })

    const rows = screen.getAllByTestId('message-row')
    expect(rows[0]).toHaveTextContent('p0·2') // newest (highest ts) first
    expect(rows[1]).toHaveTextContent('p0·1')
  })

  // Owner feedback 2026-08-15: the header used to show the loaded COUNT,
  // which saturates (and lies) at the store's 2000-row cap. It now shows the
  // loaded WINDOW's own time range instead — oldest (bottom row) to newest
  // (top row), straight from the store's rows(), updating as the window
  // slides. Follow-up owner feedback (same day): the date must never be
  // omitted, even within one day ("having just the time could be
  // misleading") — shown once, shared, when both ends fall on the same day.
  it('header shows the loaded window as a compact oldest -> newest range with its date, not a raw count', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(2, { timestamp_ms: 1_704_067_265_000 })) // newest (top): 2024-01-01T00:01:05Z
    await emit(0, 'match', mk(1, { timestamp_ms: 1_704_067_205_000 })) // oldest (bottom): 2024-01-01T00:00:05Z
    await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })

    expect(screen.getByTestId('window-range')).toHaveTextContent('2024-01-01 00:00 → 00:01 UTC')
    expect(screen.queryByText(/^\d+ messages$/)).not.toBeInTheDocument()
  })

  // UTC/local display toggle (owner ruling 2026-08-15): the header's own
  // zone label follows the global toggle, re-rendering the SAME loaded rows
  // — no refetch. Fixed TZ so a mode that silently fell back to UTC math
  // would fail this.
  describe('UTC/local display toggle (fixed TZ=America/New_York)', () => {
    withFixedTZ('America/New_York')

    it('window-range header switches to the local zone label and time when toggled, without any new request', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(2, { timestamp_ms: 1_704_067_265_000 })) // 2024-01-01T00:01:05Z
      await emit(0, 'match', mk(1, { timestamp_ms: 1_704_067_205_000 })) // 2024-01-01T00:00:05Z
      await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })
      expect(screen.getByTestId('window-range')).toHaveTextContent('2024-01-01 00:00 → 00:01 UTC')

      const requestsBefore = FakeEventSource.instances.length
      act(() => setTimeDisplayMode('local'))

      // 2024-01-01 00:00:05/00:01:05 Z == 2023-12-31 19:00/19:01 America/New_York (EST, UTC-5)
      expect(screen.getByTestId('window-range')).toHaveTextContent('2023-12-31 19:00 → 19:01 UTC-5')
      expect(screen.getAllByTestId('message-row')[0]).toHaveTextContent('2023-12-31 19:01:05.000 UTC-5')
      expect(FakeEventSource.instances.length).toBe(requestsBefore)
    })

    // Owner ruling (moved 2026-08-16): the toggle itself moved from the
    // sidebar into this header — this proves it's actually wired here (a
    // real click on the rendered control, not just a programmatic
    // `setTimeDisplayMode` call), driving the exact same zero-network
    // re-render as the test above.
    it('clicking the rendered header toggle flips the window-range and rows live, without any new request', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(2, { timestamp_ms: 1_704_067_265_000 })) // 2024-01-01T00:01:05Z
      await emit(0, 'match', mk(1, { timestamp_ms: 1_704_067_205_000 })) // 2024-01-01T00:00:05Z
      await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })
      expect(screen.getByTestId('window-range')).toHaveTextContent('2024-01-01 00:00 → 00:01 UTC')

      const requestsBefore = FakeEventSource.instances.length
      fireEvent.click(screen.getByRole('button', { name: /time zone/i }))

      expect(screen.getByTestId('window-range')).toHaveTextContent('2023-12-31 19:00 → 19:01 UTC-5')
      expect(screen.getAllByTestId('message-row')[0]).toHaveTextContent('2023-12-31 19:01:05.000 UTC-5')
      expect(FakeEventSource.instances.length).toBe(requestsBefore)
    })

    // The toggle's own stable handle. Everything else in this block reaches
    // it by aria-label, which pins the accessible name but leaves nothing
    // pinning the test-id that out-of-process checks (scripts/smoke.mjs, the
    // browser probes) address it by — the id went missing during a refactor
    // round with no test noticing, precisely because no test named it.
    it('exposes the zone toggle under a stable test-id, and flipping it re-renders the loaded rows', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(2, { timestamp_ms: 1_704_067_265_000 })) // 2024-01-01T00:01:05Z
      await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })

      const toggle = screen.getByTestId('timezone-toggle')
      expect(toggle).toHaveAttribute('data-mode', 'utc')
      expect(screen.getAllByTestId('message-row')[0]).toHaveTextContent('2024-01-01 00:01:05.000 UTC')

      fireEvent.click(toggle)

      expect(screen.getByTestId('timezone-toggle')).toHaveAttribute('data-mode', 'local')
      expect(screen.getAllByTestId('message-row')[0]).toHaveTextContent('2023-12-31 19:01:05.000 UTC-5')
    })

    // Persistence is the global store's job (`lib/timeDisplayMode`,
    // untouched by this move) — this proves the moved call site still reads
    // it correctly on a fresh mount, without needing to click again.
    it('persists the choice across a remount of the Timeline', () => {
      mockTail()
      const { unmount } = render(<Timeline cluster="prod" topic="orders" />)
      fireEvent.click(screen.getByRole('button', { name: /time zone/i }))
      expect(screen.getByRole('button', { name: /time zone/i })).toHaveAttribute('data-mode', 'local')
      unmount()

      render(<Timeline cluster="prod" topic="orders" />)
      expect(screen.getByRole('button', { name: /time zone/i })).toHaveAttribute('data-mode', 'local')
    })
  })

  // Owner ruling (moved 2026-08-16): the UTC/local toggle now lives in this
  // header cluster (see TimeZoneToggle.tsx) — it used to sit in the sidebar
  // (AppShell.test.tsx now proves the reverse: absent there).
  it('renders the time zone toggle in the header, defaulting to UTC', () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    expect(screen.getByRole('button', { name: /time zone/i })).toHaveAttribute('data-mode', 'utc')
  })

  // Owner feedback 2026-08-16: the pill/live-dot are VOLATILE (they pop in
  // and out with network activity) — they must never shove the STABLE,
  // clickable controls (play/pause, the zone toggle) sideways. Fixed by
  // anchoring those two rightmost/outermost in DOM order (see the render's
  // own comment for why plain DOM order is enough, given the parent row's
  // `justify-between`). jsdom does no real layout, so pixel positions can't
  // be asserted here (see the Playwright rect-stability check run against
  // the real app) — this suite instead pins the one thing jsdom CAN prove:
  // that appearing/disappearing volatile elements never change the relative
  // DOM order of play/pause and the zone toggle.
  describe('stable header controls (anchored right of the volatile ones)', () => {
    it('orders play/pause immediately before the zone toggle, both after the pill/live-dot slot', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      const playPause = screen.getByTestId('play-pause-toggle')
      const zoneToggle = screen.getByRole('button', { name: /time zone/i })
      // eslint-disable-next-line no-bitwise
      expect(playPause.compareDocumentPosition(zoneToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('the pill appearing does not reorder play/pause relative to the zone toggle', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      const zoneToggle = screen.getByRole('button', { name: /time zone/i })
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()

      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 100 } }) // auto-pause
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByTestId('live-pill')).toBeInTheDocument()

      const pill = screen.getByTestId('live-pill')
      const playPause = screen.getByTestId('play-pause-toggle')
      // eslint-disable-next-line no-bitwise
      expect(pill.compareDocumentPosition(playPause) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
      // eslint-disable-next-line no-bitwise
      expect(playPause.compareDocumentPosition(zoneToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })
  })

  it('header reads gracefully before any row has loaded', () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    expect(screen.getByTestId('window-range')).toHaveTextContent('no messages loaded')
    expect(screen.queryByText(/^\d+ messages$/)).not.toBeInTheDocument()
  })

  it('starts live tail on mount and prepends a message that passes the (empty) predicate', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    // Settle the initial page with nothing to load and no cursor to chase.
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    expect(screen.getByText('● live')).toBeInTheDocument()

    await act(async () => {
      tail.handlers().onMessage(mk(5))
    })
    expect(screen.getByText('p0·5')).toBeInTheDocument()
  })

  it('scrolling to the bottom requests the next back page by cursor and appends rows', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(9))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
    expect(screen.getByText('p0·9')).toBeInTheDocument()
    // Scroll is the only pagination affordance — no button, even though a
    // back cursor exists and the direction isn't exhausted.
    expect(screen.queryByRole('button', { name: /load older/i })).not.toBeInTheDocument()

    scrollToBottom()
    expect(FakeEventSource.instances[1].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 9 }) }))
    await emit(1, 'match', mk(3))
    await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })

    expect(screen.getByText('p0·9')).toBeInTheDocument()
    expect(screen.getByText('p0·3')).toBeInTheDocument()
  })

  it('an empty page with a non-null cursor auto-continues without a user click', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    // No matches, but a cursor + not-exhausted: the empty-page contract.
    await emit(0, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })
    expect(FakeEventSource.instances).toHaveLength(2)
    expect(FakeEventSource.instances[1].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 5 }) }))
  })

  it('stops auto-continuing at the iteration cap and shows the continue affordance', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    // Keep answering with empty pages; each should provoke exactly one more
    // automatic request, up to the safety cap (~20), after which the loop
    // must stop and hand back a "continue" affordance instead of looping
    // forever.
    for (let i = 0; i < 25; i++) {
      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'page_end', { cursor: cur({ 0: i + 1 }), exhausted: false })
      if (screen.queryByTestId('continue-scan')) break
    }
    expect(screen.getByTestId('continue-scan')).toBeInTheDocument()
    const countAtCap = FakeEventSource.instances.length
    // No further auto-continue happens once capped.
    await act(async () => {})
    expect(FakeEventSource.instances).toHaveLength(countAtCap)

    // Clicking continue resumes the loop from the last known cursor.
    const user = userEvent.setup()
    await user.click(screen.getByTestId('continue-scan'))
    expect(FakeEventSource.instances).toHaveLength(countAtCap + 1)
    expect(screen.queryByTestId('continue-scan')).not.toBeInTheDocument()
  })

  it('shows the beginning-of-topic caption once exhausted, and a further bottom-scroll fires no request', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    expect(screen.queryByRole('button', { name: /load older/i })).not.toBeInTheDocument()
    expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()

    scrollToBottom()
    expect(FakeEventSource.instances).toHaveLength(1) // no request: back is exhausted, cursor is null
  })

  // M5: the tail-error banner is routed through the same `describeError`
  // path as every other error surface (Panel, the sidebar's cluster list) —
  // product-voice wording with kafka attribution, never the raw wire code —
  // and lives in an aria-live region so a screen reader announces the live
  // stream dying.
  it('a tail error stops live and shows a describeError-routed, product-voice headline PLUS the specific reason, in an aria-live region', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    await act(async () => {
      tail.handlers().onError({ code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })
    })
    expect(screen.queryByText('● live')).not.toBeInTheDocument()
    expect(screen.queryByText(/kafka_error/)).not.toBeInTheDocument()
    // The headline alone can't distinguish an ACL denial from a deleted
    // topic from a genuinely dead broker — the reason must render too, not
    // be replaced by the headline.
    const banner = screen.getByText("live stopped — Kafka unreachable — cluster 'prod' — broker gone")
    expect(banner).toBeInTheDocument()
    expect(banner).toHaveAttribute('aria-live', 'polite')
  })

  it('a tail transport error keeps its own specific reason ("retrying is manual") visible alongside the shared connection-lost headline', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    await act(async () => {
      tail.handlers().onTransportError()
    })
    // Regression: a headline-only banner made this string unreachable even
    // though useLiveTail sets it, and it's the opposite of errors.ts's
    // generic "retrying automatically" hint — reconnecting the tail really
    // is manual (no auto-reconnect exists), so it must still be visible.
    expect(
      screen.getByText('live stopped — Connection to Betrachtung lost — connection lost — retrying is manual'),
    ).toBeInTheDocument()
  })

  it('a page error renders as a banner while keeping already-loaded rows visible', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
    expect(screen.getByText('p0·1')).toBeInTheDocument()

    scrollToBottom()
    await emit(1, 'app_error', { code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })

    expect(screen.getByTestId('panel-error-banner')).toBeInTheDocument()
    expect(screen.getByText('p0·1')).toBeInTheDocument() // rows stay visible
  })

  it('classifies a kafka page error as Kafka-unreachable, not a generic connection-lost banner', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

    scrollToBottom()
    await emit(1, 'app_error', {
      code: 'kafka_timeout',
      message: 'fetch metadata timed out',
      cluster: 'prod',
      retriable: true,
    })

    expect(screen.getByText(/Kafka unreachable/)).toBeInTheDocument()
    expect(screen.queryByText(/Connection to Betrachtung lost/)).not.toBeInTheDocument()
  })

  it('a page transport error still shows the generic connection-lost banner', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

    scrollToBottom()
    await act(async () => {
      FakeEventSource.instances[1].fireTransportError()
    })

    expect(screen.getByText(/Connection to Betrachtung lost/)).toBeInTheDocument()
  })

  // Review fix (M1+M2, 2026-08-15): a bad offset jump (no message at the
  // target) used to 400 BEFORE the SSE stream opened — EventSource discards
  // that non-200 body wholesale, so the frontend fell back to the generic
  // transport-error path and showed "connection lost" instead of the real,
  // honest reason. The backend now emits this as an in-stream `app_error`
  // event (see `backend/src/api/messages.rs` and `TimelineEvent::name`'s
  // own comment on why it's "app_error", not "error" — the latter collides
  // with EventSource's own reserved error type in a real browser, which
  // FakeEventSource here doesn't replicate; see `api/sse.test.ts`'s own
  // regression guard for that specific collision), which reaches Timeline
  // exactly like any other server-emitted page error — this pins that the
  // REAL message renders, and that the generic connection-lost wording
  // does NOT.
  it('a bad offset jump renders its real server message, not generic connection-lost wording', async () => {
    mockTail()
    const user = userEvent.setup()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(9))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

    await user.click(screen.getByTestId('jump-offset'))
    await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
    await user.type(screen.getByTestId('jump-offset-value-input'), '9999')
    await user.click(screen.getByTestId('jump-offset-apply'))
    await emit(1, 'app_error', {
      code: 'bad_request',
      message: 'no message with a timestamp at partition 1 offset 9999',
      cluster: null,
      retriable: false,
    })

    expect(screen.getByText('no message with a timestamp at partition 1 offset 9999')).toBeInTheDocument()
    expect(screen.queryByText(/Connection to Betrachtung lost/)).not.toBeInTheDocument()
  })

  it('renders a decode-error row loudly instead of skipping it', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1, {
      value: { encoding: 'decode_error', text: 'AAECAw==', schema_id: 9, error: 'schema registry returned 404 for schema id 9' },
    }))
    await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
    expect(screen.getByText('schema registry returned 404 for schema id 9')).toBeInTheDocument()
    expect(screen.getByText('decode_error')).toBeInTheDocument()
  })

  describe('pause machinery', () => {
    function scrollTo(scrollTop: number) {
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop } })
    }

    it('scrolling off the top auto-pauses: live messages buffer instead of prepending, and the pill counts them', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100) // off the top -> auto-pause
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(6)))
      expect(screen.getByText('▲ 2 new')).toBeInTheDocument()
      expect(screen.queryByText('p0·6')).not.toBeInTheDocument()
    })

    it('clicking the pill flushes the buffer into the store and resumes (auto-pause)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100)
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await user.click(screen.getByTestId('live-pill'))
      expect(screen.getByText('p0·5')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()

      // Resumed: a further live message inserts directly, no more buffering.
      await act(async () => tail.handlers().onMessage(mk(7)))
      expect(screen.getByText('p0·7')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
    })

    it('clicking the pill while attached scrolls back to the top — the click means "show me the new"', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100)
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('live-pill'))
        // Flushing without repositioning would shift content invisibly
        // above the viewport; the pill must land the reader at the top.
        expect(scroll.setter).toHaveBeenCalledWith(0)
      } finally {
        scroll.restore()
      }
    })

    it('scrolling back to the top auto-flushes and resumes without a click', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100)
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      scrollTo(0) // back to top
      expect(screen.getByText('p0·5')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(8)))
      expect(screen.getByText('p0·8')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
    })

    it('explicit pause overrides top-pinning: scrolling to top does NOT auto-resume', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      const toggle = screen.getByTestId('play-pause-toggle')
      expect(toggle).toHaveAttribute('aria-pressed', 'false')
      await user.click(toggle) // explicit pause, while pinned at top
      expect(toggle).toHaveAttribute('aria-pressed', 'true')

      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      scrollTo(0) // still "at the top" but pause is explicit -> no auto-resume
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      expect(toggle).toHaveAttribute('aria-pressed', 'true')

      await user.click(toggle) // explicit play -> resumes + flushes
      expect(screen.getByText('p0·5')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      expect(toggle).toHaveAttribute('aria-pressed', 'false')
    })

    it('keeps counting past the 500 buffer cap, drops the oldest buffered entries, and labels honestly', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100)
      await act(async () => {
        for (let i = 0; i < 501; i++) tail.handlers().onMessage(mk(100 + i))
      })
      expect(screen.getByText('▲ 501 new · older dropped')).toBeInTheDocument()

      // The count keeps growing past the cap rather than freezing at a
      // fixed string once overflowed.
      await act(async () => {
        for (let i = 501; i < 612; i++) tail.handlers().onMessage(mk(100 + i))
      })
      expect(screen.getByText('▲ 612 new · older dropped')).toBeInTheDocument()

      await user.click(screen.getByTestId('live-pill'))
      expect(screen.getByText('p0·711')).toBeInTheDocument() // newest kept
      expect(screen.queryByText('p0·211')).not.toBeInTheDocument() // dropped
      // 1 initial row + exactly 500 buffered (the cap holds regardless of
      // how many arrived while paused).
      expect(totalRowsHeight()).toBe(501 * 40)
    })
  })

  // Design spec v1.7 "Inspection pause": expanding a row to inspect it is a
  // stronger "don't move things" signal than scroll position — while ANY row
  // is expanded, live messages buffer to the pill regardless of scroll
  // position (even pinned at top), and the top-pin auto-resume rule is
  // dominated by the open inspection. Live only resumes automatically once
  // the LAST inspection closes while pinned at top (mirroring auto-pause);
  // the pill/toggle remain available as explicit overrides throughout, and
  // never close an inspection themselves.
  describe('inspection pause', () => {
    it('header shows effective state while inspecting: no live pulse, toggle amber with explanation', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(2))
      await emit(0, 'page_end', { cursor: null, exhausted: true })
      expect(screen.getByText('● live')).toBeInTheDocument()

      await user.click(screen.getAllByTestId('message-row')[0])
      expect(screen.queryByText('● live')).not.toBeInTheDocument()
      const toggle = screen.getByTestId('play-pause-toggle')
      expect(toggle).toHaveAttribute('aria-pressed', 'true')
      expect(toggle).toHaveAttribute('title', expect.stringMatching(/inspecting/))
      // M3: the click actually PAUSES from here (pauseReason was 'none' —
      // only the open inspection makes the toggle render lit/pressed) — the
      // accessible name must say that, not "resume live updates".
      expect(toggle).toHaveAttribute('aria-label', expect.not.stringMatching(/resume/))

      await user.click(screen.getAllByTestId('message-row')[0])
      expect(screen.getByText('● live')).toBeInTheDocument()
      expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-pressed', 'false')
      void tail
    })

    function scrollTo(scrollTop: number) {
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop } })
    }

    it('expanding a row buffers live messages and counts them in the pill, even pinned at top', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      await user.click(screen.getByTestId('message-row'))
      expect(screen.getByText('no headers')).toBeInTheDocument() // expanded

      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(6)))
      expect(screen.getByText('▲ 2 new')).toBeInTheDocument()
      expect(screen.queryByText('p0·6')).not.toBeInTheDocument()
    })

    it('a second, independent expansion keeps buffering after the first closes; closing the last while pinned resumes', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(2))
      await emit(0, 'match', mk(1))
      await emit(0, 'page_end', { cursor: null, exhausted: true })

      const rows = screen.getAllByTestId('message-row')
      await user.click(rows[0])
      await user.click(rows[1])

      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      // Collapsing the FIRST still leaves the SECOND open — still paused.
      await user.click(rows[0])
      await act(async () => tail.handlers().onMessage(mk(6)))
      expect(screen.getByText('▲ 2 new')).toBeInTheDocument()
      expect(screen.queryByText('p0·6')).not.toBeInTheDocument()

      // Collapsing the SECOND (now the LAST) while pinned at top resumes.
      await user.click(rows[1])
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      expect(screen.getByText('p0·5')).toBeInTheDocument()
      expect(screen.getByText('p0·6')).toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(7)))
      expect(screen.getByText('p0·7')).toBeInTheDocument() // resumed: merges directly
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
    })

    it('collapsing the last inspection while scrolled away from top stays paused (auto rules take over)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100) // off top -> auto-pause, independent of any inspection
      await user.click(screen.getByTestId('message-row'))
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await user.click(screen.getByTestId('message-row')) // collapse the last, still scrolled away
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument() // not flushed
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(6)))
      expect(screen.getByText('▲ 2 new')).toBeInTheDocument() // still auto-paused by the scroll rule
    })

    it('scrolling to a pinned top with inspections open does not resume live and does not close inspections', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100) // off top -> auto-pause
      await user.click(screen.getByTestId('message-row')) // expand while auto-paused
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      scrollTo(0) // back to a pinned top, WHILE still inspecting
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument() // no auto-resume
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      expect(screen.getByText('no headers')).toBeInTheDocument() // inspection still open
    })

    it('clicking the pill while inspecting flushes the buffer but keeps inspections open', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      await user.click(screen.getByTestId('message-row')) // expand while pinned at top
      await act(async () => tail.handlers().onMessage(mk(5)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await user.click(screen.getByTestId('live-pill'))
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      expect(screen.getByText('p0·5')).toBeInTheDocument() // flushed into the store
      expect(screen.getByText('no headers')).toBeInTheDocument() // inspection intact

      // Still inspecting: further live messages still buffer, not merge.
      await act(async () => tail.handlers().onMessage(mk(9)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
    })

    // Fix (follow-up round on ada2ad1): MessageRow's expansion used to be
    // local `useState`, which a virtualized row unmounting (scrolled far
    // enough away — routine, not exotic) silently loses. Expansion is now
    // owned by Timeline's `expandedKeysRef`, keyed by (partition, offset)
    // identity, so a remounted row simply re-derives the same answer.
    describe('expansion survives virtualization (identity-owned, not local component state)', () => {
      async function loadManyRows(count: number) {
        for (let i = count; i >= 1; i--) await emit(0, 'match', mk(i))
        await emit(0, 'page_end', { cursor: null, exhausted: true })
      }

      it('an expanded row scrolled far away and back into view stays expanded', async () => {
        mockTail()
        const user = userEvent.setup()
        render(<Timeline cluster="prod" topic="orders" />)
        await loadManyRows(60) // newest-first: p0·60 (top) .. p0·1 (bottom)

        await user.click(screen.getByText('p0·60')) // expand the topmost row
        expect(screen.getByText('no headers')).toBeInTheDocument()

        // Scroll far down: p0·60 scrolls out of the virtualizer's rendered
        // range entirely (its DOM unmounts).
        const restoreA = [stubScrollHeight(60 * 40), stubClientHeight(600)]
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 60 * 40 - 600 } })
        restoreA.forEach((r) => r())
        expect(screen.queryByText('p0·60')).not.toBeInTheDocument() // unmounted

        // Scroll back to the top — the row remounts. With the OLD local-
        // state design this would come back collapsed; expansion owned by
        // identity survives with no re-click.
        const restoreB = [stubScrollHeight(60 * 40), stubClientHeight(600)]
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
        restoreB.forEach((r) => r())
        expect(screen.getByText('p0·60')).toBeInTheDocument()
        expect(screen.getByText('no headers')).toBeInTheDocument()
      })

      it('scrolling an expanded row out of view and back does not change the inspection count (no leak on remount)', async () => {
        const tail = mockTail()
        const user = userEvent.setup()
        render(<Timeline cluster="prod" topic="orders" />)
        await loadManyRows(60)

        await user.click(screen.getByText('p0·60')) // expand the topmost row

        // Scroll away and back — the row's DOM unmounts and remounts.
        const restoreA = [stubScrollHeight(60 * 40), stubClientHeight(600)]
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 60 * 40 - 600 } })
        restoreA.forEach((r) => r())
        const restoreB = [stubScrollHeight(60 * 40), stubClientHeight(600)]
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
        restoreB.forEach((r) => r())

        // A live message still buffers (still inspecting) — the count is
        // genuinely 1, not leaked to 2+ by the remount.
        await act(async () => tail.handlers().onMessage(mk(9999)))
        expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

        // Collapsing the SAME (only) inspection while pinned at top resumes
        // immediately — a leaked/stuck count from the remount would never
        // reach zero here, and the pill would stay stuck forever.
        await user.click(screen.getByText('p0·60'))
        expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      })

      it('a top-eviction (loadOlder overflow) prunes the expanded row\'s key — it genuinely detaches, per existing rules, rather than leaving a leaked count', async () => {
        const user = userEvent.setup()
        render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
        await emit(0, 'match', mk(3))
        await emit(0, 'match', mk(2))
        await emit(0, 'match', mk(1))
        await emit(0, 'page_end', { cursor: cur({ 0: 1 }), exhausted: false })
        // Loaded, newest-first: p0·3 (top), p0·2, p0·1 (bottom) — exactly at cap(3).

        await user.click(screen.getByText('p0·3')) // expand the TOP (newest) row
        expect(screen.getByText('no headers')).toBeInTheDocument()

        // Scroll to the bottom to trigger loadOlder — a 'back' page appends
        // an older row; over cap(3), this trims exactly one row off the TOP:
        // p0·3, the expanded one. A top trim always detaches the window
        // (existing v1.3 rule, unrelated to inspection) — pinned-top-resume
        // is not reachable from here in the same gesture, so this test only
        // proves the key was pruned (the row is gone from `rows()`), not a
        // resume.
        scrollToBottom()
        const idx = FakeEventSource.instances.length - 1
        await emit(idx, 'match', mk(0))
        await emit(idx, 'page_end', { cursor: cur({ 0: 0 }), exhausted: false })
        expect(screen.queryByText('p0·3')).not.toBeInTheDocument() // evicted — pruned, not just off-screen
      })

      it('a forward-page trim of an expanded bottom row (evicted while genuinely pinned at top, no detach) prunes and resumes immediately', async () => {
        const tail = mockTail()
        render(<Timeline cluster="prod" topic="orders" windowCap={3} />)

        // Jump to beginning: opens a forward-direction window (a historical,
        // detached window whose TOP edge has a real forward cursor).
        fireEvent.click(screen.getByTestId('jump-beginning'))
        const beginIdx = FakeEventSource.instances.length - 1
        await emit(beginIdx, 'match', mk(3))
        await emit(beginIdx, 'match', mk(2))
        await emit(beginIdx, 'match', mk(1))
        await emit(beginIdx, 'page_end', { cursor: cur({ 0: 4 }), exhausted: false })
        // Loaded, newest-first: p0·3 (top), p0·2, p0·1 (bottom = jump target) — at cap(3).
        consumeLandingEcho() // close out the jump's own landing-settle cascade first

        await userEvent.click(screen.getByText('p0·1')) // expand the BOTTOM (oldest) row

        // Scroll to a pinned top: with an open forward cursor, this triggers
        // loadNewer — a 'forward' page trims from the BOTTOM on overflow,
        // which is exactly the expanded row, WHILE genuinely pinned at top.
        // scrollHeight/clientHeight stubbed far apart (mirrors
        // scrollToTopFarFromBottom) so the bottom sentinel doesn't ALSO fire
        // in the same event and confuse which request is which.
        const restoreScrollHeight = stubScrollHeight(2000)
        const restoreClientHeight = stubClientHeight(600)
        try {
          fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
        } finally {
          restoreScrollHeight()
          restoreClientHeight()
        }
        const idx = FakeEventSource.instances.length - 1
        await emit(idx, 'match', mk(4))
        // Reports the tail reached (exhausted:true) — this page BOTH trims
        // the expanded bottom row AND re-attaches the window in the same
        // commit (the only way a forward-direction trim's own pinned-top
        // moment ever coincides with attached — see this test block's own
        // structural note below). Re-attachment's existing 'reattached'
        // pauseMachine event resumes `pauseReason` unconditionally (unless
        // explicitly paused) — what THIS fix is responsible for, and what
        // this assertion actually isolates, is that pruning cleared
        // `inspectingRef` in the SAME commit: without it, the live message
        // below would still buffer (routing's `!inspecting` clause would
        // fail) even after `pauseReason` correctly resumed to 'none'.
        await emit(idx, 'page_end', { cursor: null, exhausted: true })
        expect(screen.queryByText('p0·1')).not.toBeInTheDocument() // evicted

        await act(async () => tail.handlers().onMessage(mk(99)))
        expect(screen.getByText('p0·99')).toBeInTheDocument()
        expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      })

      // Structural note: a page-based trim of an expanded row is either a
      // TOP trim (loadOlder, which always detaches — see the sibling test
      // above) or a BOTTOM trim (loadNewer), which can only ever fire while
      // ALREADY detached (a forward cursor only exists while detached) — so
      // the "resume while pinned top" branch inside `noteOutcome`'s pruning
      // can only become externally observable together with a reattach, as
      // exercised above. It still exists as its own code path (mirroring
      // handleToggleExpand's identical check) for the one case that reaches
      // it on its own: closing the LAST inspection by hand (covered by the
      // pre-existing "collapse last while pinned ⇒ resumes" test elsewhere
      // in this describe block).
    })

    // H1: `resetWindow` (both call sites — a jump and a filter change, see
    // Timeline.filter.test.tsx for the latter) used to drop the store but
    // never touch `expandedKeysRef`/`inspectingRef` — the expanded row was
    // gone from `rows()` for good, yet its key stayed "expanded" forever,
    // with no `onToggle` left to ever close it. Permanent phantom pause:
    // live never merges again (useLiveTail gates on `!inspectingRef`), and
    // the header would keep claiming "paused while inspecting" about a row
    // that no longer exists.
    it('a jump while inspecting clears the inspection — no permanent phantom pause (H1)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      await user.click(screen.getByTestId('message-row')) // expand -> inspecting
      expect(screen.getByText('no headers')).toBeInTheDocument()
      expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('title', expect.stringMatching(/inspecting/))

      await user.click(screen.getByTestId('jump-now')) // clears the store AND must clear the inspection
      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'page_end', { cursor: null, exhausted: true })

      // No leftover "paused while inspecting" — the row (and its title) is gone for good.
      expect(screen.getByTestId('play-pause-toggle')).not.toHaveAttribute('title', expect.stringMatching(/inspecting/))

      // Live genuinely resumes: a tail message merges directly instead of buffering forever.
      await act(async () => tail.handlers().onMessage(mk(11)))
      expect(screen.getByText('p0·11')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
    })
  })

  describe('scroll-triggered pagination', () => {
    // Spec: "Scroll down -> next 100 older (cursor pagination)" — scroll is
    // the ONLY pagination affordance (no load-older/load-newer buttons).
    // jsdom reports scrollHeight/clientHeight as 0 with no real layout, so
    // scrollToBottom stubs both alongside a scrollTop to construct a
    // meaningful "near the bottom" position — see the helper above.
    it('scrolling near the bottom loads the next older page automatically', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
      expect(FakeEventSource.instances).toHaveLength(1)

      scrollToBottom()

      expect(FakeEventSource.instances).toHaveLength(2)
      expect(FakeEventSource.instances[1].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 9 }) }))
    })

    it('scrolling near the bottom does nothing once exhausted (no cursor to chase)', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: null, exhausted: true })
      expect(FakeEventSource.instances).toHaveLength(1)

      scrollToBottom()

      expect(FakeEventSource.instances).toHaveLength(1) // no new request issued
    })

    it('scrolling near the bottom while a page is already loading does not issue a second request', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      scrollToBottom() // instance[1] now in flight, loading:true
      expect(FakeEventSource.instances).toHaveLength(2)

      scrollToBottom() // in-flight guard: no second request

      expect(FakeEventSource.instances).toHaveLength(2) // still just the one in-flight request
    })

    // Symmetric top edge: the forward/"load newer" sentinel only ever fires
    // after a beginning-jump opens a forward cursor — mirrors the back
    // sentinel's own guard (forward cursor present, not exhausted).
    it('after jumping to beginning, scrolling to the top loads the next newer page automatically', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      expect(FakeEventSource.instances).toHaveLength(2)
      consumeLandingEcho() // the real browser's own echo of this jump's scrollToEdge — see its own comment

      // Task 3: the beginning anchor bootstrap also seeded a real (non-
      // exhausted) BOTTOM edge from its own rows (the store's documented
      // opposite-side anchor seed), so a plain scrollToTop() here would
      // ALSO fire the bottom sentinel — scrollToTopFarFromBottom isolates
      // the top sentinel under test, same as its own doc comment describes.
      scrollToTopFarFromBottom()

      expect(FakeEventSource.instances).toHaveLength(3)
      expect(FakeEventSource.instances[2].url).toBe(url({ direction: 'forward', limit: '100', cursor: cur({ 0: 3 }) }))
    })

    // Scroll anchoring (design spec v1.3, owner feedback 2026-08-15; row-
    // identity rewrite, fix round 1 M1): a forward page's rows rank newer,
    // so they land near the TOP of the newest-first merge — i.e. they
    // prepend above whatever the reader was looking at. Without
    // compensation, scrollTop stays numerically unchanged, which silently
    // relocates the reader to the top of the newly loaded page instead of
    // keeping them at the junction where they were reading. This only
    // matters when the reader wasn't pinned to the very top by the time the
    // page lands (pinned-top is today's correct behavior — they want to see
    // the incoming content, same as the live-attached case).
    it('a forward page that lands while the reader is mid-scroll anchors the viewport to the junction ROW, not a total-height delta', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      consumeLandingEcho() // the real browser's own echo of this jump's scrollToEdge — see its own comment

      // Task 3: isolates the top sentinel (see scrollToTopFarFromBottom's
      // own doc comment) — the beginning bootstrap also seeded a real,
      // non-exhausted bottom edge, so a plain scrollToTop() would also fire
      // an unrelated bottom-sentinel request in the same scroll event.
      scrollToTopFarFromBottom() // fires the forward page request (instance[2])
      expect(FakeEventSource.instances).toHaveLength(3)

      // While that page is in flight, the reader keeps reading — scrolled
      // away from the exact top edge by the time the page lands.
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 500 } })

      const scroll = spyOnScrollTop()
      try {
        await emit(2, 'match', mk(3))
        scroll.setter.mockClear()
        await emit(2, 'page_end', { cursor: cur({ 0: 4 }), exhausted: false })
        // Before this page lands, rows = [2] (only the beginning-jump's own
        // row survived the earlier jump's clear()) — the junction row is
        // p0·2, at index 0 (rendered offset 0, uniform 40px rows in jsdom —
        // no ResizeObserver). Prepending p0·3 above it shifts p0·2 to index
        // 1 (offset 40). Anchored to THAT row's identity: scrollTop nudged
        // by 40 - 0 = 40, landing on 500 + 40 = 540 — never left at the
        // numerically-unchanged 500, and NOT derived from any total-height
        // reading (this page adds a row but trims nothing, so a total-delta
        // approach would have coincidentally agreed here too — see the
        // OTHER test below, which forces a trim into the SAME commit to
        // prove the two approaches actually diverge).
        expect(scroll.setter).toHaveBeenCalledWith(540)
      } finally {
        scroll.restore()
      }
    })

    // Fix round 1 (review of 079f30f), M1: the test above never actually
    // distinguishes row-identity anchoring from the OLD total-height-delta
    // approach it replaced, because that page adds a row and trims
    // nothing — "added − removed" and "this row's own offset delta" agree
    // by coincidence. THIS test forces a trim into the SAME commit as the
    // prepend (routine at a small cap): a total-height delta would net to
    // ~0 (added 1, removed 1) and wrongly leave scrollTop at the
    // numerically-unchanged 500 — exactly the forbidden relocation
    // scroll-anchoring exists to prevent. Row-identity anchoring is
    // unaffected: the trim only ever removes rows BELOW the junction (a
    // forward insert can only trim the bottom — see enforceCap — never
    // the top it's prepending above), so the junction row's own offset
    // delta is still exactly right.
    it('a forward page that BOTH prepends above the viewport AND trims below the cap in the same commit still anchors correctly (a total-height delta would not)', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(9))
      await emit(0, 'match', mk(8))
      await emit(0, 'page_end', { cursor: cur({ 0: 8 }), exhausted: false })

      // Back page pushes 4 rows against cap(3): the newest (p0·9) trims off
      // the top -> detach, top map recovers to exactly 9. jsdom's default
      // 0/0/0 scroll dimensions make `scrollTop: 0` read as both pinned-
      // to-top (fires the sentinel) and near-the-bottom (harmless here —
      // the back cursor is what fires; see the earlier interior-hole test's
      // own comment on this same jsdom quirk).
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
      await emit(1, 'match', mk(7))
      await emit(1, 'match', mk(6))
      await emit(1, 'page_end', { cursor: cur({ 0: 6 }), exhausted: false })
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument() // trimmed
      // Rows now [8,7,6]. Note: no scroll to 500 yet here — `state.loading`
      // is false at this exact instant (the back page above just landed),
      // so an unstubbed scroll (jsdom's 0/0/0 dimensions make `nearBottom`
      // trivially true for almost any scrollTop) would fire an UNWANTED
      // extra back request and leave `state.loading` true, which would then
      // make the very next line's forward request get skipped entirely
      // (guarded on `!state.loading`) — move to 500 only AFTER starting the
      // forward request below, exactly like the test above does.

      const scroll = spyOnScrollTop()
      try {
        scrollToTopFarFromBottom() // forward request, cursor = top = cur({0:9})
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 500 } })
        scroll.setter.mockClear()

        const idx = FakeEventSource.instances.length - 1
        await emit(idx, 'match', mk(9)) // recovers exactly the trimmed row
        await emit(idx, 'page_end', { cursor: null, exhausted: true })
        // Recovering p0·9 over cap(3) ALSO trims the oldest (p0·6) —
        // simultaneous prepend-above + trim-below. Junction row (p0·8, at
        // index 0 before this insert, offset 0) is now at index 1 (offset
        // 40 — one row prepended above it; the trim below never touches
        // it). scrollTop nudged by 40 - 0 = 40, landing on 500 + 40 = 540
        // — NOT the numerically-unchanged 500 a total-height delta (net
        // ~0: +1 row added, -1 row trimmed) would have wrongly produced.
        expect(scroll.setter).toHaveBeenCalledWith(540)
        expect(screen.queryByText('p0·6')).not.toBeInTheDocument() // trimmed
        for (const o of [9, 8, 7]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()
      } finally {
        scroll.restore()
      }
    })

    // Fix round 2 (re-review of 000fd9f), N5: the display overlay (M2)
    // shows a forward scan's matches progressively, in BATCH_SIZE (25)
    // batches, well before page_end — each batch prepends above the
    // reader exactly like the final commit does, so without per-batch
    // anchoring, an intermediate overlay update would silently relocate
    // the reader the same way M1 originally fixed for the commit alone.
    // This test forces TWO batches (30 total matches) and asserts
    // `adjustScrollTop` fires ONCE PER BATCH, each with the correct
    // per-batch delta — not just once at the very end.
    it('a multi-batch forward scan anchors the viewport on EVERY batch, not just the final commit', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      consumeLandingEcho() // the real browser's own echo of this jump's scrollToEdge — see its own comment
      // Rows now just [2] (offset 2, index 0 — uniform 40px rows in jsdom,
      // no ResizeObserver).

      scrollToTopFarFromBottom() // forward request, cursor = top = cur({0:3})
      const idx = FakeEventSource.instances.length - 1

      const scroll = spyOnScrollTop()
      try {
        // First batch: 25 matches (offsets 4..28, ascending — all newer
        // than the existing row 2) — crosses BATCH_SIZE, flushes mid-scan,
        // well before page_end.
        for (let i = 0; i < 25; i++) {
          await emit(idx, 'match', mk(4 + i))
        }
        // Junction (p0·2) was at index 0 (offset 0) before this batch; the
        // 25 new rows prepend above it, pushing it to index 25 (offset
        // 25*40 = 1000). scrollTop (started at 0, from
        // scrollToTopFarFromBottom) is set to 0 + 1000 = 1000 — DURING the
        // scan, not at page_end. Exactly one call for this batch.
        expect(scroll.setter.mock.calls).toEqual([[1000]])

        // Second (final, trailing) batch: 5 more matches (offsets 29..33),
        // delivered together with page_end (useTimelinePage batches these
        // 5 internally and only flushes at the terminal event, in the SAME
        // synchronous handler as the commit — see useTimelinePage's own
        // doc comment — so this is one combined React update, one call).
        scroll.setter.mockClear()
        for (let i = 25; i < 30; i++) {
          await emit(idx, 'match', mk(4 + i))
        }
        await emit(idx, 'page_end', { cursor: null, exhausted: true })
        // This batch's OWN "before" capture chases whatever is CURRENTLY
        // topmost at ITS capture time — batch 1's own newest row (p0·28,
        // at index 0, per the chaining scheme — see pendingAnchorRef's own
        // comment) — not the original p0·2 again. After the commit (31
        // rows total, newest-first), p0·28 sits at index 5 (33,32,31,30,29
        // are the 5 newer rows ahead of it): offset 5*40 = 200. scrollTop
        // becomes 1000 (from batch 1) + 200 = 1200. Exactly one call for
        // this batch/commit too — TWO calls total across the whole scan,
        // not one lump sum at the very end.
        expect(scroll.setter.mock.calls).toEqual([[1200]])
      } finally {
        scroll.restore()
      }

      // End-to-end sanity: all 31 rows (1 original + 30 scanned) ended up
      // committed, content never reset along the way.
      expect(totalRowsHeight()).toBe(31 * 40)
    })

    // Real-browser stall (rollout drill, 2026-08-15, ~24k-message topic):
    // the down-walk died PERMANENTLY the moment the window reached its row
    // cap — 20 pages in, no 21st request, no error, no affordance. The
    // mechanism is this: past the cap a back page is height-NEUTRAL (it
    // appends N rows below and trims exactly N off the top), so the scroll
    // container's scrollHeight stops growing. Scroll anchoring existed only
    // for 'forward' pages ("a back page appends below, it doesn't move the
    // viewport") — but a back page that ALSO trims above the viewport moves
    // every remaining row UP by the trimmed height while scrollTop stays
    // numerically unchanged. A reader at the bottom (which is what triggered
    // the load) therefore stays pinned at scrollTop === max, the entire
    // window silently slides underneath them (they never see the page they
    // just loaded), and — since scrollTop is already at the max of an
    // unchanged scrollHeight — the browser has no reason to ever emit
    // another scroll event. The bottom sentinel is scroll-event-driven and
    // only scroll-event-driven, so pagination is dead for good. This is the
    // exact mirror of the forward case (which works in the live app
    // precisely BECAUSE its anchoring leaves the reader off the top edge).
    // Invisible to every other test here because jsdom synthesizes scroll
    // events directly rather than deciding whether a real browser would
    // have emitted one — so the property under test is the anchoring
    // itself, which is pure state/measurement logic.
    it('a back page that trims the top keeps the reader on their junction row instead of pinning them to the bottom', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(9))
      await emit(0, 'match', mk(8))
      await emit(0, 'match', mk(7))
      await emit(0, 'page_end', { cursor: cur({ 0: 7 }), exhausted: false })
      // Rows [9,8,7] — exactly at cap(3); uniform 40px rows in jsdom (no
      // ResizeObserver), so offsets are 0/40/80.

      scrollToBottom() // reader at the bottom -> fires the next back page
      expect(FakeEventSource.instances).toHaveLength(2)

      const scroll = spyOnScrollTop()
      try {
        scroll.setter.mockClear()
        await emit(1, 'match', mk(6))
        await emit(1, 'match', mk(5))
        await emit(1, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })
        // 5 rows against cap(3): the two NEWEST (p0·9, p0·8) trim off the
        // top — 80px of content removed above the reader.
        expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
        expect(screen.queryByText('p0·8')).not.toBeInTheDocument()
        // Junction row = the last row committed before this page (p0·7, at
        // index 2, offset 80). After the trim it sits at index 0 (offset 0),
        // so scrollTop is nudged by 0 - 80 = -80: from scrollToBottom's 200
        // down to 120. The reader keeps looking at p0·7 with the newly
        // loaded rows below them — and, in a real browser, is no longer
        // pinned at the maximum scroll offset, so their next downward scroll
        // is a genuine position change that fires a genuine scroll event.
        expect(scroll.setter).toHaveBeenCalledWith(120)
      } finally {
        scroll.restore()
      }
    })
  })

  describe('jump control', () => {
    // H2: `handleJump` used to assign `planJump`'s static intent straight to
    // `pauseReasonRef`, bypassing `pauseMachine` entirely — the one caller
    // that could silently overwrite a user's EXPLICIT pause. A beginning
    // jump's own intent is 'auto', not 'explicit'; if the jump routes
    // through the machine (which lets an explicit pause survive any jump),
    // the later reattach must still leave it paused and buffered — an 'auto'
    // pause dropped by the same jump would instead flush on reattach (see
    // pauseMachine's 'reattached' branch).
    it('an explicit pause survives a jump — the jump\'s own intent never overwrites it (H2)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      await user.click(screen.getByTestId('play-pause-toggle')) // explicit pause
      expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-pressed', 'true')

      await user.click(screen.getByTestId('jump-beginning')) // this jump's OWN intent is 'auto'
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false }) // forward cursor open, not exhausted yet
      consumeLandingEcho()

      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      // Forward-paginate to the tail — this reattaches the window.
      scrollToTopFarFromBottom()
      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(3))
      await emit(idx, 'page_end', { cursor: null, exhausted: true }) // caught the tail: reattached

      // An explicit pause is never implicitly lifted by reattaching — unlike
      // the jump's own 'auto' intent, which WOULD have been dropped here.
      // The buffered message stays buffered; live stays off.
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.queryByText('● live')).not.toBeInTheDocument()
      expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-pressed', 'true')
    })

    it('jump to beginning clears the store, loads a forward/beginning page, and pauses (auto)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
      expect(screen.getByText('p0·9')).toBeInTheDocument()

      await user.click(screen.getByTestId('jump-beginning'))
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument() // store cleared
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&anchor=beginning',
      )
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      expect(screen.getByText('p0·2')).toBeInTheDocument()

      // Paused-auto: a live message buffers instead of prepending.
      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
    })

    it('jump to now clears the store, loads a back/latest page, and restores default live (no pause)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      // Get into a paused (detached) state first via a beginning jump — the
      // forward cursor must stay open (not exhausted) here, since a forward
      // page landing exhausted immediately would legitimately re-attach
      // (caught the tail already) rather than staying paused.
      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      await act(async () => tail.handlers().onMessage(mk(3)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await user.click(screen.getByTestId('jump-now'))
      expect(screen.queryByText('p0·2')).not.toBeInTheDocument() // store cleared
      expect(screen.queryByText('p0·3')).not.toBeInTheDocument() // store + buffer cleared
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      expect(FakeEventSource.instances[2].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
      )
      await emit(2, 'page_end', { cursor: null, exhausted: true })

      // Live is back on (not paused): a tail message inserts directly.
      await act(async () => tail.handlers().onMessage(mk(11)))
      expect(screen.getByText('p0·11')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
    })

    // Owner ruling 2026-08-15: offset/timestamp jumps now read FORWARD from
    // the target (was 'back') — the backend aligns every other partition at
    // the target's own timestamp instead of pinning them at their high
    // watermark, so the old back-anchored "reads as broken" jump is gone.
    it('jump to offset issues a forward page anchored at partition+offset, clears the store, and pauses', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))

      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&anchor=offset&partition=1&offset=77',
      )
    })

    it('jump to timestamp issues a forward page anchored at ts_ms, clears the store, and pauses', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), '1700000000000')
      await user.click(screen.getByTestId('jump-timestamp-apply'))

      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&anchor=timestamp&ts_ms=1700000000000',
      )
    })

    it('after an offset jump, the target row carries the jump-target marker; other rows do not', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))

      await emit(1, 'match', mk(77, { partition: 1 }))
      await emit(1, 'match', mk(78, { partition: 1 }))
      await emit(1, 'page_end', { cursor: cur({ 1: 79 }), exhausted: false })

      expect(screen.getAllByTestId('jump-target')).toHaveLength(1)
      const rows = screen.getAllByTestId('message-row')
      const markedRow = rows.find((r) => r.textContent?.includes('p1·77'))
      expect(markedRow?.querySelector('[data-testid="jump-target"]')).toBeInTheDocument()
      const unmarkedRow = rows.find((r) => r.textContent?.includes('p1·78'))
      expect(unmarkedRow?.querySelector('[data-testid="jump-target"]')).not.toBeInTheDocument()
    })

    it('a subsequent jump clears the previous offset jump highlight', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))
      await emit(1, 'match', mk(77, { partition: 1 }))
      await emit(1, 'page_end', { cursor: cur({ 1: 78 }), exhausted: false })
      expect(screen.getAllByTestId('jump-target')).toHaveLength(1)

      await user.click(screen.getByTestId('jump-now'))
      await emit(2, 'page_end', { cursor: null, exhausted: true })
      expect(screen.queryAllByTestId('jump-target')).toHaveLength(0)
    })

    it('a filter change clears a previous offset jump highlight', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))
      await emit(1, 'match', mk(77, { partition: 1 }))
      await emit(1, 'page_end', { cursor: cur({ 1: 78 }), exhausted: false })
      expect(screen.getAllByTestId('jump-target')).toHaveLength(1)

      vi.useFakeTimers()
      try {
        fireEvent.change(screen.getByLabelText('filter messages'), { target: { value: 'v77' } })
        await act(async () => {
          vi.advanceTimersByTime(500)
        })
      } finally {
        vi.useRealTimers()
      }
      await emit(2, 'page_end', { cursor: null, exhausted: true })
      expect(screen.queryAllByTestId('jump-target')).toHaveLength(0)
    })

    it('a jump resets BOTH directions: a stale pre-jump back cursor cannot fire a request, and a post-jump top-scroll uses the fresh cursor', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      // latest page lands with a back cursor -> bottom-scroll pagination is live.
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
      expect(screen.queryByRole('button', { name: /load older/i })).not.toBeInTheDocument() // no button — scroll only

      // Page further back once, so the back cursor is now 'c2' (not just
      // the initial page's cursor) — this is the reviewer's exact repro.
      // (A match is included so this page doesn't trigger the empty-page
      // auto-continue and open an extra, unrelated EventSource.)
      scrollToBottom()
      expect(FakeEventSource.instances).toHaveLength(2)
      await emit(1, 'match', mk(4))
      await emit(1, 'page_end', { cursor: cur({ 0: 4 }), exhausted: false })

      // Jump to beginning: the old back cursor ('c2') must NOT survive. Note:
      // a bottom-scroll can't even be attempted in this exact window — the
      // store is cleared synchronously on click, so Panel shows a loading
      // skeleton (no `timeline-scroll` element) at least until the page's
      // first flush (a page yielding ≥25 matches remounts the list mid-load;
      // even then both sentinel guards block, because reset() nulled both
      // cursors before the request started). The request-level proof that
      // the stale cursor is really gone (not just visually hidden) comes
      // right below, the moment the list is scrollable again.
      await user.click(screen.getByTestId('jump-beginning'))
      expect(FakeEventSource.instances).toHaveLength(3) // the beginning page itself

      // The beginning page lands and sets a fresh forward cursor.
      await emit(2, 'match', mk(1))
      await emit(2, 'page_end', { cursor: cur({ 0: 2 }), exhausted: false })
      expect(screen.queryByRole('button', { name: /load newer/i })).not.toBeInTheDocument() // no button — scroll only
      consumeLandingEcho() // the real browser's own echo of this jump's scrollToEdge — see its own comment

      // Task 3: the beginning bootstrap ALSO seeded a real bottom edge from
      // its own rows (the store's documented opposite-side anchor seed —
      // it has no way to know "beginning" has nothing below). A
      // bottom-scroll here legitimately fires now; what this test proves is
      // that it carries the FRESH seed, never anything surviving from
      // before the jump (the stale 'c2'-shaped cursor).
      scrollToBottom()
      expect(FakeEventSource.instances).toHaveLength(4)
      expect(FakeEventSource.instances[3].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 1 }) }))
      await emit(3, 'page_end', { cursor: null, exhausted: true })

      // A top-scroll's request must carry the FRESH post-jump forward
      // cursor, never anything from before the jump.
      scrollToTopFarFromBottom()
      expect(FakeEventSource.instances).toHaveLength(5)
      expect(FakeEventSource.instances[4].url).toBe(url({ direction: 'forward', limit: '100', cursor: cur({ 0: 2 }) }))
    })

    // Note on these tests: clearing the store synchronously on every jump
    // (see handleJump) drops `hasData` to false for the click's commit,
    // which Panel renders as a loading skeleton — unmounting MessageList
    // (and its scroll container) until the jump's page lands (matches are
    // batched and only flushed at page_end, per useTimelinePage, so the
    // remount and the loading:false transition happen in the SAME commit).
    // That means the scrollToEdge call always lands on a brand-new
    // container whose default scrollTop is already 0 — checking the final
    // DOM value can't distinguish "we scrolled to top" from "nothing
    // happened". Spying on the assignment itself (spyOnScrollTop) sidesteps
    // that entirely.
    it("jump to now scrolls the viewport back to the top once the jump's first page lands", async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-now'))
        await emit(1, 'match', mk(20))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: null, exhausted: true })
        expect(scroll.setter).toHaveBeenCalledWith(0)
      } finally {
        scroll.restore()
      }
    })

    it('jump to beginning scrolls the viewport to the bottom (oldest visible) once its first page lands', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      const restoreHeight = stubScrollHeight(4000)
      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-beginning'))
        await emit(1, 'match', mk(1))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: cur({ 0: 2 }), exhausted: false })
        // scrollTop = scrollHeight (the clamp-to-bottom trick) — asserting
        // the stubbed 4000 (not a hardcoded 0) proves it read scrollHeight
        // rather than coincidentally landing on the same value as "top".
        expect(scroll.setter).toHaveBeenCalledWith(4000)
      } finally {
        scroll.restore()
        restoreHeight()
      }
    })

    // Owner ruling 2026-08-15: offset/timestamp jumps now land like
    // 'beginning' does — scrolled to the BOTTOM (the target is the oldest
    // loaded row, newer rows above it), not the top.
    it('jump to offset scrolls the viewport to the bottom (oldest visible) once its page lands', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      const restoreHeight = stubScrollHeight(4000)
      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-offset'))
        await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
        await user.type(screen.getByTestId('jump-offset-value-input'), '77')
        await user.click(screen.getByTestId('jump-offset-apply'))
        await emit(1, 'match', mk(30))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: cur({ 0: 31 }), exhausted: false })
        // scrollTop = scrollHeight (the clamp-to-bottom trick) — asserting
        // the stubbed 4000 (not a hardcoded 0) proves it read scrollHeight
        // rather than coincidentally landing on the same value as "top".
        expect(scroll.setter).toHaveBeenCalledWith(4000)
      } finally {
        scroll.restore()
        restoreHeight()
      }
    })

    it('jump to timestamp scrolls the viewport to the bottom (oldest visible) once its page lands', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      const restoreHeight = stubScrollHeight(4000)
      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-timestamp'))
        await user.type(screen.getByTestId('jump-timestamp-input'), '1700000000000')
        await user.click(screen.getByTestId('jump-timestamp-apply'))
        await emit(1, 'match', mk(30))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: cur({ 0: 31 }), exhausted: false })
        expect(scroll.setter).toHaveBeenCalledWith(4000)
      } finally {
        scroll.restore()
        restoreHeight()
      }
    })

    // Owner-reported bug (2026-08-15): after an offset/timestamp jump, the
    // target rendered in the MIDDLE of the page instead of at the bottom
    // edge. Root cause (found via a real-browser probe, not visible in
    // jsdom, and NOT fully explained by the first fix attempt either — see
    // `settlingRef`'s own comment on `Timeline` for the full history): a
    // real browser's virtualizer keeps remeasuring real row heights via
    // `ResizeObserver` for a couple of scroll-event cycles after landing,
    // moving the true bottom each time and firing a genuine native 'scroll'
    // event for every one of those moves — each of which trivially
    // satisfies the bottom-sentinel's "near the bottom" check, firing an
    // UNSOLICITED `loadOlder()`. The older page it fetches appends BELOW
    // the target (correct — back pages don't move the viewport when
    // nothing above the reader trims), but the viewport never follows down
    // to the NEW bottom, so the target — genuinely the bottom-most loaded
    // row a moment ago — ends up stranded above it, rendering "in the
    // middle". This test simulates that settling CASCADE directly (a
    // scrollHeight that keeps changing across a couple of events, then
    // stabilizes) and asserts none of it triggers a pagination request —
    // while a genuinely later, real user scroll still must.
    it('the settling cascade a real browser fires while a jump lands does not trigger an extra pagination request', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      // Stubbed BEFORE the jump lands, so scrollToEdge('bottom') computes a
      // realistic non-zero "bottom" (3405) — same as a real browser's own
      // actual scrollHeight at that moment, unlike jsdom's unstubbed
      // default of 0. The value itself mirrors the real-browser probe that
      // found this bug.
      const restoreScrollHeight = stubScrollHeight(3405)
      const restoreClientHeight = stubClientHeight(600)
      try {
        await user.click(screen.getByTestId('jump-offset'))
        await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
        await user.type(screen.getByTestId('jump-offset-value-input'), '77')
        await user.click(screen.getByTestId('jump-offset-apply'))
        await emit(1, 'match', mk(30))
        // Bottom (back) cursor open, not exhausted — a genuine "more history
        // below" case, exactly what makes the bottom-sentinel fire in a real
        // browser.
        await emit(1, 'page_end', { cursor: cur({ 0: 31 }), exhausted: false })
        expect(FakeEventSource.instances).toHaveLength(2) // landing page only, nothing auto-triggered yet

        // First echo: the virtualizer's remeasurement has already grown
        // scrollHeight (probe-observed: 3405 -> 3768) — still settling.
        // (Re-stubbing directly, not via a second `stubScrollHeight()` call
        // — that would return its OWN restore function pointing back at
        // THIS stub, not the true original; one `restoreScrollHeight` at
        // the end, from the FIRST call, is all the cleanup this needs.)
        Object.defineProperty(Element.prototype, 'scrollHeight', { configurable: true, get: () => 3768 })
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 3768 } })
        expect(FakeEventSource.instances).toHaveLength(2) // still swallowed — content is still resizing

        // Second echo: SAME scrollHeight as the first — settling is done.
        // This closing event is itself consumed too (still just an echo of
        // our own last re-snap, not a real gesture).
        fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 3768 } })
        expect(FakeEventSource.instances).toHaveLength(2) // still no extra request
      } finally {
        restoreScrollHeight()
        restoreClientHeight()
      }

      // A genuinely later, independent user scroll to the bottom must still
      // page normally — settling is a bounded, self-closing window, not a
      // broken feature. Cursor is 30 (mk(30)'s own offset), not the mocked
      // page_end cursor (31): the bottom map is opposite-seeded from this
      // anchor bootstrap's own INSERTED ROW offsets (the only source for
      // the side the request itself didn't touch — direction is 'forward'
      // here, so `31` feeds the TOP map instead; see
      // createSlidingWindowStore's own doc comment).
      scrollToBottom()
      expect(FakeEventSource.instances).toHaveLength(3)
      expect(FakeEventSource.instances[2].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 30 }) }))
    })

  })

  describe('attached vs detached windows', () => {
    it('after a beginning jump, live messages never merge — even when pinned to top', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })
      expect(screen.getByText('p0·2')).toBeInTheDocument()

      // scrollToTopFarFromBottom (not plain scrollToTop): the beginning
      // bootstrap also seeded a real, non-exhausted bottom edge (the
      // store's opposite-side anchor seed) — isolates the top sentinel
      // under test from an unrelated bottom-sentinel fire.
      scrollToTopFarFromBottom() // pinned to top of the DETACHED window — must not resume live
      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
    })

    it('pill click while detached jumps to now', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })

      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('live-pill'))
        // Abandons the historical window in place — does NOT flush the
        // buffer into it (that would recreate the false seam).
        expect(screen.queryByText('p0·2')).not.toBeInTheDocument()
        expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
        expect(FakeEventSource.instances.at(-1)!.url).toBe(
          '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
        )

        const idx = FakeEventSource.instances.length - 1
        await emit(idx, 'match', mk(20))
        scroll.setter.mockClear()
        await emit(idx, 'page_end', { cursor: null, exhausted: true })
        expect(scroll.setter).toHaveBeenCalledWith(0)

        // Offset above what the fresh 'now' page itself just fetched (20) —
        // a real live tail message can never legitimately arrive BELOW a
        // 'latest' anchor's own snapshot (offsets only increase); mk(11)
        // would be. That's not this test's concern (the store's own N2
        // defensive-drop unit tests cover the below-bound case) — this one
        // proves ordinary live merging resumed after re-attaching.
        await act(async () => tail.handlers().onMessage(mk(21)))
        expect(screen.getByText('p0·21')).toBeInTheDocument()
      } finally {
        scroll.restore()
      }
    })

    it('catching the tail re-attaches', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false }) // forward cursor open, not exhausted yet
      consumeLandingEcho() // the real browser's own echo of this jump's scrollToEdge — see its own comment

      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      // Forward-paginate toward the tail (top-scroll sentinel, independent of
      // the live-resume gate). scrollToTopFarFromBottom isolates it from the
      // beginning bootstrap's own (real, non-exhausted) seeded bottom edge.
      scrollToTopFarFromBottom()
      expect(FakeEventSource.instances).toHaveLength(3)
      await emit(2, 'match', mk(3))
      await emit(2, 'page_end', { cursor: null, exhausted: true }) // caught the tail

      // Re-attached: buffered message flushes in (deduped with anything
      // already loaded) and the header goes back to live.
      expect(screen.getByText('p0·50')).toBeInTheDocument()
      expect(screen.queryByTestId('live-pill')).not.toBeInTheDocument()
      expect(screen.getByText('● live')).toBeInTheDocument()

      await act(async () => tail.handlers().onMessage(mk(60)))
      expect(screen.getByText('p0·60')).toBeInTheDocument()
    })

    it('offset jump detaches too', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))
      await emit(1, 'match', mk(5))
      await emit(1, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })

      // Task 3 (hard requirement): an offset jump's own store-level anchor
      // bootstrap is deliberately NOT attached (M-new) — a live message
      // here must buffer, never reach `insertLive` at all, so the store's
      // throw-on-detached precondition can't fire. Asserted explicitly
      // (not just implicitly via "doesn't crash the test run").
      await act(async () => {
        expect(() => tail.handlers().onMessage(mk(50))).not.toThrow()
      })
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      const toggle = screen.getByTestId('play-pause-toggle')
      expect(toggle).toHaveAttribute('aria-pressed', 'true')
      expect(container.querySelector('[data-staleness]')).toBeNull()
    })

    // Task 3 (hard requirement, store review carry-over): `insertLive`
    // throws if ever called while the store is detached — the earlier
    // "offset jump detaches too" test proves the OFFSET-jump case never
    // reaches it (buffers instead). This test proves the more insidious
    // race: 'now' clears the store immediately (genuinely detached for a
    // moment) but the UI's own `attached` state also flips false at the
    // very same click — deferred until the fresh anchor page's own
    // `insertPage` call CONFIRMS the store is attached again (see
    // Timeline.tsx's `attached` state doc comment) — so a live message
    // arriving in the gap between the click and that page landing must
    // buffer, never throw.
    it('a live message arriving between a jump-to-now click and its page landing buffers instead of throwing', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-now'))
      // The fresh 'now' page is in flight but hasn't landed yet.
      await act(async () => {
        expect(() => tail.handlers().onMessage(mk(50))).not.toThrow()
      })
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()

      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(20))
      await emit(idx, 'page_end', { cursor: null, exhausted: true })
      // Landed + confirmed attached: the buffered message flushes in, and
      // live resumes for anything arriving after.
      expect(screen.getByText('p0·50')).toBeInTheDocument()
      await act(async () => tail.handlers().onMessage(mk(60)))
      expect(screen.getByText('p0·60')).toBeInTheDocument()
    })

    it('detached header: toggle shows paused, no chip, no live pulse', async () => {
      mockTail()
      const user = userEvent.setup()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })

      const toggle = screen.getByTestId('play-pause-toggle')
      expect(toggle).toHaveAttribute('aria-pressed', 'true')
      expect(screen.getByTestId('icon-pause')).toHaveClass('text-amber-500')
      expect(screen.queryByText('● live')).not.toBeInTheDocument()
      expect(container.querySelector('[data-staleness]')).toBeNull()
    })

    it('clicking the toggle while detached jumps to now', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: cur({ 0: 3 }), exhausted: false })

      await user.click(screen.getByTestId('play-pause-toggle'))
      // Abandons the historical window in place, same as the pill click.
      expect(screen.queryByText('p0·2')).not.toBeInTheDocument()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
      )

      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(20))
      await emit(idx, 'page_end', { cursor: null, exhausted: true })

      // Re-attached: a subsequent tail message inserts straight into the
      // store — proving live merging actually resumed. Offset above what
      // the fresh 'now' page itself fetched (20) — a real live message can
      // never legitimately arrive below a 'latest' anchor's own snapshot.
      await act(async () => tail.handlers().onMessage(mk(21)))
      expect(screen.getByText('p0·21')).toBeInTheDocument()
    })

    // Contrast case: the live stream dying while still ATTACHED is a
    // genuinely honest alarm (new data really is missing) — this chip keeps
    // the normal aging/alarm tiers (unlike the detached header, which shows
    // no chip at all — see the toggle tests above).
    it('the !live-while-attached chip keeps its aging/alarm tiers', async () => {
      const tail = mockTail()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: null, exhausted: true })
      await act(async () => {
        tail.handlers().onError({ code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })
      })
      // mk(9)'s timestamp_ms (1009) is ancient next to real wall-clock time
      // (this chip gets no `now` override, unlike the unit tests above), so
      // it genuinely reads as a real age-based 'stale' tier.
      const chip = container.querySelector('[data-staleness]')
      expect(chip).toHaveAttribute('data-staleness', 'stale')
    })

    // Fix round 1 (review of 079f30f), C1: the "just became store-attached"
    // catch-up used to gate on `storeRef.current.edges().top === null` —
    // but that reads null for TWO different reasons (genuinely attached,
    // OR an empty topMap that was never seeded at all), and an offset/
    // timestamp jump's anchor page can legitimately return ZERO rows (the
    // empty-page contract) without ever seeding topMap. The UI would then
    // wrongly believe it was attached, and a live tail message right after
    // would reach `insertLive` on a store that's still genuinely detached
    // — throwing. The fix reads the store's OWN `attached` truth (via the
    // synchronous `insertPage` outcome) instead.
    it('an offset jump whose anchor page returns zero rows stays detached: a live message still buffers, never throws', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))
      // Empty-page contract: zero matches, non-null cursor, not exhausted —
      // nothing in this window passed, but the store's real `attached`
      // must stay false (the anchor bootstrap explicitly passed
      // attach:false) regardless of how `edges().top` happens to read.
      await emit(1, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })

      await act(async () => {
        expect(() => tail.handlers().onMessage(mk(50))).not.toThrow()
      })
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      // Header still reads detached — the toggle is lit paused with no
      // live pulse, same as every other detached window.
      expect(screen.queryByText('● live')).not.toBeInTheDocument()
      const toggle = screen.getByTestId('play-pause-toggle')
      expect(toggle).toHaveAttribute('aria-pressed', 'true')
    })

    // REMOVED (owner ruling 2026-08-15): this used to prove that a BACK-
    // anchored offset jump's partial top-map seed (only partitions that
    // returned ≥1 row got seeded) was masked complete-or-null by the store's
    // C2 check, and that Timeline's forward-anchor fallback recovered from
    // it by re-issuing the SAME anchor with direction=forward. Offset jumps
    // are now THEMSELVES forward-anchored (see `handleJump`'s 'offset'
    // case) — a forward bootstrap's top map is always seeded authoritatively
    // from rule 1 (the response's own continuation cursor), regardless of
    // which partitions returned rows, so this exact scenario can no longer
    // arise from any jump. The underlying store mechanism (C2 completeness
    // masking) remains directly unit-tested, unchanged, in
    // `timelineStore.test.ts`.
  })

  // Window cap honesty (design spec v1.4, owner ruling 2026-08-15): the
  // 2000-row default cap is too large to exercise here, so these tests pass
  // a small `windowCap` prop (test-only — production always uses the
  // default) to force drops with just a handful of messages.
  describe('window cap honesty', () => {
    it('top drops while attached detach the window', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(9))
      await emit(0, 'match', mk(8))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
      expect(screen.getByText('p0·9')).toBeInTheDocument()
      expect(screen.getByText('p0·8')).toBeInTheDocument()

      // A back page adds 2 older rows on top of the 2 already loaded — 4
      // rows against a cap of 3, so the newest row (p0·9) drops off the top.
      // NOTE: scrollToBottom() itself scrolls off the top (scrollTop: 200),
      // which independently sets pauseReason to 'auto' — a real side effect
      // of scrolling, NOT of the drop. This means `paused` alone (and thus
      // the toggle's aria-pressed) is already true before the drop even
      // happens, so it can't be used below to prove detach actually fired;
      // see the pill-based assertions instead.
      scrollToBottom()
      await emit(1, 'match', mk(7))
      await emit(1, 'match', mk(6))
      await emit(1, 'page_end', { cursor: cur({ 0: 6 }), exhausted: false })

      expect(screen.queryByText('p0·9')).not.toBeInTheDocument() // dropped off the top
      expect(screen.getByText('p0·8')).toBeInTheDocument()

      // Detached: a live tail message buffers into the pill instead of
      // merging — a flush could otherwise merge against the truncated top.
      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      // Pin the DETACH itself, not merely "paused" (which the scroll above
      // already produces on its own via auto-pause, attached or not): the
      // pill's aria-label only reads this way while attached=false — an
      // attached-but-auto-paused window's pill instead offers "flush
      // buffered live messages" (see LivePill). Clicking it must issue a
      // brand new jump-to-now request — an attached pill click flushes in
      // place and never issues any request at all, so seeing one here is
      // conclusive proof the window really detached.
      expect(screen.getByTestId('live-pill')).toHaveAttribute('aria-label', 'jump to now and show new messages')
      const requestsBefore = FakeEventSource.instances.length
      await user.click(screen.getByTestId('live-pill'))
      expect(FakeEventSource.instances.length).toBe(requestsBefore + 1)
      expect(FakeEventSource.instances.at(-1)!.url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
      )
    })

    // Task 3 (supersedes the v1.4 "bottom drops invalidate the back cursor
    // ... repositions by timestamp" test): the sliding-window store's own
    // bottom map ALWAYS advances to a real, exact, followable cursor when a
    // trim happens — the whole point of the redesign is that there is
    // nothing left to reposition. A further bottom-scroll just uses that
    // cursor directly, and content never resets (previously-held rows stay,
    // only the trimmed range gets re-fetched).
    it('a bottom trim leaves the store\'s own bottom edge exact — a further bottom-scroll re-fetches via that cursor, never a reposition', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'page_end', { cursor: cur({ 0: 10 }), exhausted: false })
      expect(screen.getByText('p0·11')).toBeInTheDocument()
      expect(screen.getByText('p0·10')).toBeInTheDocument()

      // Two live inserts push the store past cap(3): the oldest row
      // (p0·10) trims off the bottom, advancing the store's own bottom edge
      // past it — to exactly the trimmed offset, per the store's rule 3.
      await act(async () => tail.handlers().onMessage(mk(12)))
      await act(async () => tail.handlers().onMessage(mk(13)))
      expect(screen.queryByText('p0·10')).not.toBeInTheDocument() // trimmed off the bottom
      expect(screen.getByText('p0·11')).toBeInTheDocument() // now the bottom row

      scrollToBottom()
      // No reposition: the request carries the store's own minted bottom
      // cursor directly.
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 11 }) }))

      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(10))
      await emit(idx, 'page_end', { cursor: null, exhausted: true })
      // Content never resets: the trimmed row is recovered exactly, on top
      // of what was already held — never by discarding and starting over.
      expect(screen.getByText('p0·12')).toBeInTheDocument()
      expect(screen.getByText('p0·11')).toBeInTheDocument()
      expect(screen.getByText('p0·10')).toBeInTheDocument()
    })

    // Symmetric mirror of the bottom-trim test above: a 'back'-origin top
    // trim also leaves a real, exact top edge — a further top-scroll uses
    // it directly, never a reposition.
    it('a top trim leaves the store\'s own top edge exact — a further top-scroll re-fetches via that cursor, never a reposition', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(9))
      await emit(0, 'match', mk(8))
      await emit(0, 'page_end', { cursor: cur({ 0: 8 }), exhausted: false })

      // Same top-trim setup as the detach test above: a back page pushes 4
      // rows against cap(3), the newest (p0·9) trims off the top -> detach.
      scrollToBottom()
      await emit(1, 'match', mk(7))
      await emit(1, 'match', mk(6))
      await emit(1, 'page_end', { cursor: cur({ 0: 6 }), exhausted: false })
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()

      // The back cursor is still live and not exhausted here — use the
      // far-from-bottom top gesture so the bottom sentinel doesn't also
      // fire in the same scroll event (see the helper's own comment).
      scrollToTopFarFromBottom()
      // No reposition: the request carries the store's own minted top
      // cursor directly — exactly the trimmed offset (9).
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'forward', limit: '100', cursor: cur({ 0: 9 }) }))

      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(9))
      await emit(idx, 'page_end', { cursor: null, exhausted: true })
      // Content never resets: the trimmed row is recovered exactly.
      expect(screen.getByText('p0·9')).toBeInTheDocument()
      expect(screen.getByText('p0·8')).toBeInTheDocument()
    })

    // F2 (task-3 carry-over): the caption claims the window's oldest row IS
    // the topic's first message — a bottom trim makes that false even
    // while `state.exhausted.back` is still (stale) true from before the
    // trim, so it must disappear until a FRESH back page genuinely re-earns
    // exhaustion (bottomTrimmedSinceRef — see Timeline.tsx's own comment —
    // overrides the stale flag for both the caption and loadOlder's gate).
    it('a bottom trim hides the beginning-of-topic caption until a fresh back page genuinely re-earns exhaustion', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'page_end', { cursor: null, exhausted: true }) // genuinely exhausted
      expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()

      // Live traffic pushes the store past cap(3): the oldest row trims off
      // the bottom. The caption's claim is now false, even though
      // state.exhausted.back is still (stale) true.
      await act(async () => tail.handlers().onMessage(mk(12)))
      await act(async () => tail.handlers().onMessage(mk(13)))
      expect(screen.queryByText('— beginning of topic —')).not.toBeInTheDocument()

      // Bottom-scroll fires for real (the same override also un-gates
      // loadOlder itself, not just the caption) using the store's own real
      // bottom edge — ordinary cursor-based pagination, no reposition.
      scrollToBottom()
      const idx = FakeEventSource.instances.length - 1
      expect(FakeEventSource.instances[idx].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 11 }) }))

      await emit(idx, 'match', mk(10))
      await emit(idx, 'page_end', { cursor: null, exhausted: true }) // re-reaches the true edge
      expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()
    })

    // Note (task 3): the v1.4-era "auto-continue does not follow a cursor
    // invalidated by a mid-flight pill-flush drop" regression test is
    // deliberately not carried forward — the failure mode it guarded
    // against (a cursor-follower reading a value invalidated by a
    // concurrent trim) can't happen anymore BY CONSTRUCTION: every
    // cursor-follower (loadOlder/loadNewer/continueScan/auto-continue) now
    // reads `storeRef.current.edges()` at the moment it acts, never a
    // value captured earlier, and the store's own edge maps are always
    // exactly correct regardless of how many trims (page- or live-
    // originated) have landed in between — see timelineStore.test.ts's
    // property walk, which interleaves live inserts into paging for exactly
    // this reason.
  })

  // Fix round 1 (review of 079f30f), C3: reproduces the reviewer's exact
  // probe at the UI level — a live trim recovers an evicted offset WHILE an
  // older back-page (issued before the trim) is still in flight. Landing
  // that stale page must not regress the store's bottom map past the
  // recovered range (an interior hole at the exact seam the reader is
  // watching); Timeline must instead re-issue the request from the store's
  // fresh edge, transparently to the reader (content never resets, the
  // trimmed row is still recoverable).
  describe('stale in-flight page rejection', () => {
    it('a live trim mid-flight does not create an interior hole: the stale page is dropped and re-issued from the fresh edge', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={4} />)

      // Mount: [12,11,10,9], bottom={0:9}.
      await emit(0, 'match', mk(12))
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      // An in-flight back page is issued FROM bottom={0:9} — instance[1] —
      // and deliberately left unresolved for now. Fires the bottom
      // sentinel (jsdom's default 0/0/0 scroll dimensions make `scrollTop:
      // 0` read as BOTH pinned-to-top — keeping live merging active,
      // unlike `scrollToBottom()`'s own scrollTop:200, which would
      // auto-pause and make the live insert below just buffer instead of
      // actually trimming — AND near-the-bottom, so the sentinel still
      // fires loadOlder).
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 9 }) }))
      const inFlightIdx = FakeEventSource.instances.length - 1

      // Meanwhile: live 13 arrives, growing the top past cap(4) and
      // trimming the oldest (9) off the bottom — the store's bottom edge
      // advances to {0:10}, recovering exactly offset 9 on a future
      // back-read.
      await act(async () => tail.handlers().onMessage(mk(13)))
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument() // trimmed off the bottom (for now)
      for (const o of [13, 12, 11, 10]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // The stale in-flight page NOW lands: rows [8,7], its own start (9)
      // sits BELOW the current bottom boundary (10) — a trim recovered
      // [9,10) in the interim that this page never asked about. It must be
      // rejected, not merged (which would otherwise regress bottom to 7,
      // stranding offset 9 forever between 7 and 10 — an interior hole).
      await emit(inFlightIdx, 'match', mk(8))
      await emit(inFlightIdx, 'match', mk(7))
      await emit(inFlightIdx, 'page_end', { cursor: cur({ 0: 7 }), exhausted: false })

      // Rejected: neither of the stale page's own rows ever appear...
      expect(screen.queryByText('p0·8')).not.toBeInTheDocument()
      expect(screen.queryByText('p0·7')).not.toBeInTheDocument()
      // ...and content never reset — everything held before this page
      // landed is still exactly there.
      for (const o of [13, 12, 11, 10]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // Timeline transparently re-issued from the store's OWN fresh bottom
      // edge (10, unregressed by the rejected page) — never the stale
      // page's own contCursor (7).
      const retryIdx = FakeEventSource.instances.length - 1
      expect(retryIdx).toBeGreaterThan(inFlightIdx)
      expect(FakeEventSource.instances[retryIdx].url).toBe(
        url({ direction: 'back', limit: '100', cursor: cur({ 0: 10 }) }),
      )

      // That retry recovers offset 9 exactly — no interior hole.
      await emit(retryIdx, 'match', mk(9))
      await emit(retryIdx, 'page_end', { cursor: null, exhausted: true })
      expect(screen.getByText('p0·9')).toBeInTheDocument()
    })

    // Fix round 2 (re-review of 000fd9f), N6: a rejected page's own SSE
    // response can still report `exhausted: true` to useTimelinePage's
    // internal state, even though the store never committed it — landing
    // this exact page WITH that lie must not show a false "beginning of
    // topic" caption (or gate further pagination), and a retry still fires
    // using the store's own fresh (unregressed) edge, proving the rejection
    // is honored regardless of what the stale page claimed about itself.
    it("a rejected page's own stale exhausted:true claim never shows a false beginning-of-topic caption, and a retry still fires", async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={4} />)
      await emit(0, 'match', mk(12))
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
      const inFlightIdx = FakeEventSource.instances.length - 1

      // Live 13 trims the bottom (evicts 9), advancing bottom to {0:10}.
      await act(async () => tail.handlers().onMessage(mk(13)))

      // The stale page lands claiming exhausted:true (a lie — the store
      // rejects it as stale regardless of what it claims about itself).
      await emit(inFlightIdx, 'match', mk(8))
      await emit(inFlightIdx, 'page_end', { cursor: null, exhausted: true })

      // No false caption — the rejection marks this side's exhausted claim
      // untrustworthy, unconditionally, before even checking whether a
      // retry can fire.
      expect(screen.queryByText('— beginning of topic —')).not.toBeInTheDocument()

      // A retry still fires, using the store's OWN fresh (unregressed)
      // bottom edge — not gated by the stale page's own exhausted claim.
      const retryIdx = FakeEventSource.instances.length - 1
      expect(retryIdx).toBeGreaterThan(inFlightIdx)
      expect(FakeEventSource.instances[retryIdx].url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 10 }) }))

      // Once a REAL page genuinely reaches the start, the caption is
      // honored again normally.
      await emit(retryIdx, 'match', mk(9))
      await emit(retryIdx, 'page_end', { cursor: null, exhausted: true })
      expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()
    })

    // Fix round 2, N3 (charter: no zombie scans): the reject-stale retry
    // loop must consume an iteration-cap slot per retry, same as an
    // empty-page auto-continue — a storm of concurrent trims invalidating
    // one retry after another must eventually land on the SAME
    // continue-scan affordance, never loop silently forever.
    it('a storm of concurrent trims invalidating each retry in turn eventually surfaces the continue-scan affordance instead of looping forever', async () => {
      const tail = mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={3} />)
      await emit(0, 'match', mk(12))
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'page_end', { cursor: cur({ 0: 10 }), exhausted: false })

      // Kick off the first back page — every landing below is deliberately
      // invalidated by a live trim before it resolves, forcing a retry.
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })

      let liveOffset = 13
      let iterations = 0
      while (!screen.queryByTestId('continue-scan') && iterations < 25) {
        const idx = FakeEventSource.instances.length - 1
        // Live insert trims the bottom again, invalidating whatever is
        // currently in flight before it lands.
        await act(async () => tail.handlers().onMessage(mk(liveOffset++)))
        // Land the (now-stale) in-flight page — rejected, triggers a retry
        // (until the cap stops it).
        await emit(idx, 'page_end', { cursor: null, exhausted: false })
        iterations++
      }

      expect(screen.getByTestId('continue-scan')).toBeInTheDocument()
      // Bounded: stopped at (around) the iteration cap, never looped past
      // it — a real zombie-scan bug would run out the `iterations < 25`
      // guard above without ever finding the affordance.
      expect(iterations).toBeLessThan(25)

      // No further request fires once capped, even with more live trims.
      const countAtCap = FakeEventSource.instances.length
      await act(async () => tail.handlers().onMessage(mk(liveOffset++)))
      expect(FakeEventSource.instances).toHaveLength(countAtCap)
    })
  })

  // Acceptance bar (design spec v1.6, task-3 plan): the jsdom PROPERTY WALK
  // — scripted FakeEventSource pages with a small cap, scroll down past it
  // (content never resets — rows present before an insert remain except
  // trimmed ones), then scroll up re-fetching trimmed regions via the
  // store's minted top-edge cursors (exact request URLs asserted) until
  // re-attach on exhausted-forward. Single partition, small integers —
  // exhaustive multi-partition/hole/tie adversarial coverage of the
  // underlying invariant already lives in timelineStore.test.ts's own
  // property walk; this one proves the UI WIRING (real request URLs built
  // from `edges()`, DOM content persisting, re-attachment) drives that
  // store correctly end to end.
  describe('sliding window property walk', () => {
    it('slides down past the cap without ever resetting content, then back up via minted cursors to re-attach at the tail', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={4} />)

      // --- Mount: latest 4 (offsets 12..9), exactly at the cap. ---
      await emit(0, 'match', mk(12))
      await emit(0, 'match', mk(11))
      await emit(0, 'match', mk(10))
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })
      for (const o of [12, 11, 10, 9]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // --- Down 1: +2 older (8,7) over cap(4) -> trims the newest 2 off the
      // top (12,11) and detaches. Content that survives (10,9) stays. ---
      scrollToBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 9 }) }))
      await emit(1, 'match', mk(8))
      await emit(1, 'match', mk(7))
      await emit(1, 'page_end', { cursor: cur({ 0: 7 }), exhausted: false })
      expect(screen.queryByText('p0·12')).not.toBeInTheDocument()
      expect(screen.queryByText('p0·11')).not.toBeInTheDocument()
      for (const o of [10, 9, 8, 7]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // --- Down 2: +2 older (6,5) over cap(4) -> trims the newest 2 (10,9). ---
      scrollToBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 7 }) }))
      await emit(2, 'match', mk(6))
      await emit(2, 'match', mk(5))
      await emit(2, 'page_end', { cursor: cur({ 0: 5 }), exhausted: false })
      expect(screen.queryByText('p0·10')).not.toBeInTheDocument()
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      for (const o of [8, 7, 6, 5]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // --- Up 1: minted top cursor (9) re-fetches the FIRST trimmed pair
      // (9,10) forward — not exhausted yet (11,12 still owed). Recovering
      // them over cap(4) now trims the OLDEST pair (6,5) off the bottom.
      // Content that survives (8,7) stays — never a full reset. ---
      scrollToTopFarFromBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(
        url({ direction: 'forward', limit: '100', cursor: cur({ 0: 9 }) }),
      )
      await emit(3, 'match', mk(9))
      await emit(3, 'match', mk(10))
      await emit(3, 'page_end', { cursor: cur({ 0: 11 }), exhausted: false })
      expect(screen.queryByText('p0·6')).not.toBeInTheDocument()
      expect(screen.queryByText('p0·5')).not.toBeInTheDocument()
      for (const o of [10, 9, 8, 7]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()
      // Still detached — the tail (12) hasn't been reached yet.
      expect(screen.getByTestId('play-pause-toggle')).toHaveAttribute('aria-pressed', 'true')

      // --- Up 2: minted top cursor (11) re-fetches the LAST trimmed pair
      // (11,12) forward — this one reports exhausted (caught the true
      // tail): re-attach. Recovering them over cap(4) trims the oldest pair
      // (8,7), landing EXACTLY back on the original mount window (12..9) —
      // the walk's own round trip, never a reset along the way. ---
      scrollToTopFarFromBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(
        url({ direction: 'forward', limit: '100', cursor: cur({ 0: 11 }) }),
      )
      await emit(4, 'match', mk(11))
      await emit(4, 'match', mk(12))
      await emit(4, 'page_end', { cursor: null, exhausted: true })
      expect(screen.queryByText('p0·8')).not.toBeInTheDocument()
      expect(screen.queryByText('p0·7')).not.toBeInTheDocument()
      for (const o of [12, 11, 10, 9]) expect(screen.getByText(`p0·${o}`)).toBeInTheDocument()

      // Re-attached: scrolling to the top now resumes live (was auto-paused
      // by the earlier scrollToBottom calls) — the pulse only ever shows
      // while genuinely attached (v1.3), so seeing it here is conclusive
      // proof the walk really re-attached, not just that the tail rows
      // happen to match.
      scrollToTopFarFromBottom()
      expect(screen.getByText('● live')).toBeInTheDocument()
    })

    // Round 4 (coordinator correction of a round-3 regression): pins spec
    // v1.6's own binding behavior — design doc "Sliding window (v1.6 —
    // owner ruling)": "scrolling back up re-reads trimmed regions forward
    // from the top map — seamlessly — and re-attaches when a forward page
    // reports exhausted... Content resets ONLY on explicit navigation" —
    // unqualified by anchor context. A round-3 "fix" mistakenly gated this
    // off for a 'default' (opened-at-'now') window, reasoning that
    // recovering forward content competes with the row cap against
    // whatever a LIVE topic produced meanwhile, evicting the reader's own
    // recent backward progress. That reasoning was wrong: the window is
    // DESIGNED to slide — the reader moved up, so the just-vacated bottom
    // region is exactly what should be evicted (nothing there is being
    // read anymore), and it stays perfectly, losslessly re-fetchable via
    // the store's own minted bottom cursor (proven byte-for-byte exact by
    // both rounds' own investigation) — never a gap, never silently
    // dropped data. This test pins the full oscillation the round-3 fix
    // broke: cap-crossing trim (detach) -> top-scroll recovers forward
    // from topMap -> its insert trims the bottom (evicting exactly what
    // the last back page had just added) -> a further bottom-scroll
    // re-issues precisely that back page's own original request cursor —
    // proof the "regression" is the window sliding as specified, not data
    // loss. (Mutation check: reinstating the round-3 gate — `if
    // (!attachedRef.current && anchorContextRef.current.kind === 'default')
    // return` at the top of `loadNewer` — turns this red: the top-scroll
    // assertion below fails to find a new forward request.)
    it('a default-context window sliding up re-covers newer rows and re-exposes the just-vacated bottom region — the oscillation is correct, never a loss', async () => {
      mockTail()
      render(<Timeline cluster="prod" topic="orders" windowCap={2} />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: cur({ 0: 9 }), exhausted: false })

      // First back page fills to the cap exactly — no trim yet.
      scrollToBottom()
      await emit(1, 'match', mk(8))
      await emit(1, 'page_end', { cursor: cur({ 0: 8 }), exhausted: false })
      expect(screen.getByText('p0·9')).toBeInTheDocument()
      expect(screen.getByText('p0·8')).toBeInTheDocument()

      // Second back page crosses cap(2): adds p0·7, trims the newest
      // (p0·9) off the top -> detach. Window is now [8,7]; this page's own
      // REQUEST cursor was 8 — the value the oscillation should return to.
      scrollToBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 8 }) }))
      await emit(2, 'match', mk(7))
      await emit(2, 'page_end', { cursor: cur({ 0: 7 }), exhausted: false })
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      expect(screen.getByText('p0·8')).toBeInTheDocument()
      expect(screen.getByText('p0·7')).toBeInTheDocument()

      // Slide up: a forward request minted straight from topMap — fires
      // for a PLAIN 'default'-context detached window, exactly per spec,
      // no anchor-context gate.
      scrollToTopFarFromBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'forward', limit: '100', cursor: cur({ 0: 9 }) }))

      // Recovering p0·9 over cap(2) evicts the oldest (p0·7) — exactly the
      // row the LAST back page had itself just added. Reports exhausted:
      // re-attaches (spec: "re-attaches when a forward page reports
      // exhausted").
      const idx = FakeEventSource.instances.length - 1
      await emit(idx, 'match', mk(9))
      await emit(idx, 'page_end', { cursor: null, exhausted: true })
      expect(screen.queryByText('p0·7')).not.toBeInTheDocument()
      expect(screen.getByText('p0·9')).toBeInTheDocument()
      expect(screen.getByText('p0·8')).toBeInTheDocument()
      expect(screen.getByText('● live')).toBeInTheDocument() // re-attached

      // The oscillation: a further bottom-scroll re-issues EXACTLY the
      // cursor the second back page was itself fetched with (8) — the
      // just-vacated region, legitimately re-fetchable, never a gap and
      // never silently-lost data.
      scrollToBottom()
      expect(FakeEventSource.instances.at(-1)!.url).toBe(url({ direction: 'back', limit: '100', cursor: cur({ 0: 8 }) }))
    })
  })
})
