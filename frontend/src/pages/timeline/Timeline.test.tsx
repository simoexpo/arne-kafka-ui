import { act, fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../../test/fake-event-source'
import * as sse from '../../api/sse'
import type { MessageOut, SseErrorData } from '../../api/types'
import { Timeline } from './Timeline'

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

// Scroll-anchoring's capture (before insert) and its compensating read
// (after render) both happen inside the SAME synchronous flush — a real
// browser's scrollHeight would genuinely differ between those two reads
// (layout recomputes once the new rows actually land), but jsdom does no
// layout at all, so a single static value can't tell the two reads apart.
// This stub answers the FIRST read with `values[0]` and every read after
// with the last entry — simulating "the DOM grew between capture and
// render" without needing real layout.
function stubScrollHeightSequence(values: number[]) {
  const original = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollHeight')
  let i = 0
  Object.defineProperty(Element.prototype, 'scrollHeight', {
    configurable: true,
    get: () => values[Math.min(i++, values.length - 1)],
  })
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
function scrollToTop() {
  fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 0 } })
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
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

    const rows = screen.getAllByTestId('message-row')
    expect(rows[0]).toHaveTextContent('p0·2') // newest (highest ts) first
    expect(rows[1]).toHaveTextContent('p0·1')
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
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByText('p0·9')).toBeInTheDocument()
    // Scroll is the only pagination affordance — no button, even though a
    // back cursor exists and the direction isn't exhausted.
    expect(screen.queryByTestId('load-older')).not.toBeInTheDocument()

    scrollToBottom()
    expect(FakeEventSource.instances[1].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&cursor=c1',
    )
    await emit(1, 'match', mk(3))
    await emit(1, 'page_end', { cursor: 'c2', exhausted: false })

    expect(screen.getByText('p0·9')).toBeInTheDocument()
    expect(screen.getByText('p0·3')).toBeInTheDocument()
  })

  it('an empty page with a non-null cursor auto-continues without a user click', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    // No matches, but a cursor + not-exhausted: the empty-page contract.
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(FakeEventSource.instances).toHaveLength(2)
    expect(FakeEventSource.instances[1].url).toContain('cursor=c1')
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
      await emit(idx, 'page_end', { cursor: `c${i + 1}`, exhausted: false })
      if (screen.queryByTestId('continue-scan')) break
    }
    expect(screen.getByTestId('continue-scan')).toBeInTheDocument()
    const countAtCap = FakeEventSource.instances.length
    // No further auto-continue happens once capped.
    await new Promise((r) => setTimeout(r, 0))
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
    expect(screen.queryByTestId('load-older')).not.toBeInTheDocument()
    expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()

    scrollToBottom()
    expect(FakeEventSource.instances).toHaveLength(1) // no request: back is exhausted, cursor is null
  })

  it('a tail error stops live and shows the stopped caption', async () => {
    const tail = mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    await act(async () => {
      tail.handlers().onError({ code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })
    })
    expect(screen.queryByText('● live')).not.toBeInTheDocument()
    expect(screen.getByText('live stopped — kafka_error: broker gone')).toBeInTheDocument()
  })

  it('a page error renders as a banner while keeping already-loaded rows visible', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByText('p0·1')).toBeInTheDocument()

    scrollToBottom()
    await emit(1, 'error', { code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })

    expect(screen.getByTestId('panel-error-banner')).toBeInTheDocument()
    expect(screen.getByText('p0·1')).toBeInTheDocument() // rows stay visible
  })

  it('classifies a kafka page error as Kafka-unreachable, not a generic connection-lost banner', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

    scrollToBottom()
    await emit(1, 'error', {
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
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

    scrollToBottom()
    await act(async () => {
      FakeEventSource.instances[1].fireTransportError()
    })

    expect(screen.getByText(/Connection to Betrachtung lost/)).toBeInTheDocument()
  })

  it('renders a decode-error row loudly instead of skipping it', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1, {
      value: { encoding: 'decode_error', text: 'AAECAw==', schema_id: 9, error: 'schema registry returned 404 for schema id 9' },
    }))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByText('schema registry returned 404 for schema id 9')).toBeInTheDocument()
    expect(screen.getByText('decode_error')).toBeInTheDocument()
  })

  describe('pause machinery', () => {
    async function settleWithOneRow(index = 0) {
      await emit(index, 'match', mk(1))
      await emit(index, 'page_end', { cursor: null, exhausted: true })
    }

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
      expect(screen.getByText('501 messages')).toBeInTheDocument()
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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
      expect(FakeEventSource.instances).toHaveLength(1)

      scrollToBottom()

      expect(FakeEventSource.instances).toHaveLength(2)
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&cursor=c1',
      )
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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })
      expect(FakeEventSource.instances).toHaveLength(2)

      scrollToTop()

      expect(FakeEventSource.instances).toHaveLength(3)
      expect(FakeEventSource.instances[2].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&cursor=c9',
      )
    })

    // Scroll anchoring (design spec v1.3, owner feedback 2026-08-15): a
    // forward page's rows rank newer, so they land near the TOP of the
    // newest-first merge — i.e. they prepend above whatever the reader was
    // looking at. Without compensation, scrollTop stays numerically
    // unchanged, which silently relocates the reader to the top of the
    // newly loaded page instead of keeping them at the junction where they
    // were reading. This only matters when the reader wasn't pinned to the
    // very top by the time the page lands (pinned-top is today's correct
    // behavior — they want to see the incoming content, same as the
    // live-attached case).
    it('a forward page that lands while the reader is mid-scroll anchors the viewport at the junction by height delta', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })

      scrollToTop() // fires the forward page request (instance[2])
      expect(FakeEventSource.instances).toHaveLength(3)

      // While that page is in flight, the reader keeps reading — scrolled
      // away from the exact top edge by the time the page lands.
      fireEvent.scroll(screen.getByTestId('timeline-scroll'), { target: { scrollTop: 500 } })

      // A (2000) is what the pre-insert capture reads; B (2400) is what the
      // post-render layout effect reads once the new rows are in — see
      // stubScrollHeightSequence.
      const restoreHeight = stubScrollHeightSequence([2000, 2400])
      const scroll = spyOnScrollTop()
      try {
        await emit(2, 'match', mk(3))
        scroll.setter.mockClear()
        await emit(2, 'page_end', { cursor: 'c-fwd', exhausted: false })
        // Anchored at the junction: scrollTop nudged by exactly B - A
        // (2400 - 2000 = 400), landing on 500 + 400 = 900 — never left at
        // the numerically-unchanged 500.
        expect(scroll.setter).toHaveBeenCalledWith(900)
      } finally {
        scroll.restore()
        restoreHeight()
      }
    })
  })

  describe('jump control', () => {
    it('jump to beginning clears the store, loads a forward/beginning page, and pauses (auto)', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
      expect(screen.getByText('p0·9')).toBeInTheDocument()

      await user.click(screen.getByTestId('jump-beginning'))
      expect(screen.queryByText('p0·9')).not.toBeInTheDocument() // store cleared
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&anchor=beginning',
      )
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })
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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      // Get into a paused (detached) state first via a beginning jump — the
      // forward cursor must stay open (not exhausted) here, since a forward
      // page landing exhausted immediately would legitimately re-attach
      // (caught the tail already) rather than staying paused.
      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c-fwd', exhausted: false })
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

    it('jump to offset issues a back page anchored at partition+offset, clears the store, and pauses', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))

      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=offset&partition=1&offset=77',
      )
    })

    it('jump to timestamp issues a back page anchored at ts_ms, clears the store, and pauses', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-timestamp'))
      await user.type(screen.getByTestId('jump-timestamp-input'), '1700000000000')
      await user.click(screen.getByTestId('jump-timestamp-apply'))

      expect(screen.queryByText('p0·9')).not.toBeInTheDocument()
      expect(FakeEventSource.instances[1].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=timestamp&ts_ms=1700000000000',
      )
    })

    it('a jump resets BOTH directions: a stale pre-jump back cursor cannot fire a request, and a post-jump top-scroll uses the fresh cursor', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      // latest page lands with a back cursor -> bottom-scroll pagination is live.
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
      expect(screen.queryByTestId('load-older')).not.toBeInTheDocument() // no button — scroll only

      // Page further back once, so the back cursor is now 'c2' (not just
      // the initial page's cursor) — this is the reviewer's exact repro.
      // (A match is included so this page doesn't trigger the empty-page
      // auto-continue and open an extra, unrelated EventSource.)
      scrollToBottom()
      expect(FakeEventSource.instances).toHaveLength(2)
      await emit(1, 'match', mk(4))
      await emit(1, 'page_end', { cursor: 'c2', exhausted: false })

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
      await emit(2, 'page_end', { cursor: 'c-fwd-new', exhausted: false })
      expect(screen.queryByTestId('load-newer')).not.toBeInTheDocument() // no button — scroll only

      // Back cursor is still gone (exhausted.back/cursors.back were reset,
      // not silently repopulated by the forward page's own update) —
      // another bottom-scroll still fires no request.
      scrollToBottom()
      expect(FakeEventSource.instances).toHaveLength(3)

      // A top-scroll's request must carry the FRESH post-jump cursor, never
      // the stale pre-jump back cursor ('c2').
      scrollToTop()
      expect(FakeEventSource.instances).toHaveLength(4)
      expect(FakeEventSource.instances[3].url).toBe(
        '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=100&cursor=c-fwd-new',
      )
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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      const restoreHeight = stubScrollHeight(4000)
      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-beginning'))
        await emit(1, 'match', mk(1))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: 'c-fwd', exhausted: false })
        // scrollTop = scrollHeight (the clamp-to-bottom trick) — asserting
        // the stubbed 4000 (not a hardcoded 0) proves it read scrollHeight
        // rather than coincidentally landing on the same value as "top".
        expect(scroll.setter).toHaveBeenCalledWith(4000)
      } finally {
        scroll.restore()
        restoreHeight()
      }
    })

    it('jump to offset scrolls the viewport back to the top once its page lands', async () => {
      mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      const scroll = spyOnScrollTop()
      try {
        await user.click(screen.getByTestId('jump-offset'))
        await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
        await user.type(screen.getByTestId('jump-offset-value-input'), '77')
        await user.click(screen.getByTestId('jump-offset-apply'))
        await emit(1, 'match', mk(30))
        scroll.setter.mockClear()
        await emit(1, 'page_end', { cursor: null, exhausted: true })
        expect(scroll.setter).toHaveBeenCalledWith(0)
      } finally {
        scroll.restore()
      }
    })
  })

  describe('attached vs detached windows', () => {
    it('after a beginning jump, live messages never merge — even when pinned to top', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })
      expect(screen.getByText('p0·2')).toBeInTheDocument()

      scrollToTop() // pinned to top of the DETACHED window — must not resume live
      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
    })

    it('pill click while detached jumps to now', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })

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

        await act(async () => tail.handlers().onMessage(mk(11)))
        expect(screen.getByText('p0·11')).toBeInTheDocument()
      } finally {
        scroll.restore()
      }
    })

    it('catching the tail re-attaches', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false }) // forward cursor open, not exhausted yet

      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      // Forward-paginate toward the tail (top-scroll sentinel, independent of
      // the live-resume gate).
      scrollToTop()
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
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-offset'))
      await user.type(screen.getByTestId('jump-offset-partition-input'), '1')
      await user.type(screen.getByTestId('jump-offset-value-input'), '77')
      await user.click(screen.getByTestId('jump-offset-apply'))
      await emit(1, 'match', mk(5))
      await emit(1, 'page_end', { cursor: 'c2', exhausted: false })

      await act(async () => tail.handlers().onMessage(mk(50)))
      expect(screen.queryByText('p0·50')).not.toBeInTheDocument()
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()
      expect(screen.queryByTestId('play-pause-toggle')).not.toBeInTheDocument()
      expect(container.querySelector('[data-staleness]')).not.toBeNull()
    })

    it('detached header: no toggle, no live pulse, staleness chip present', async () => {
      mockTail()
      const user = userEvent.setup()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })

      expect(screen.queryByTestId('play-pause-toggle')).not.toBeInTheDocument()
      expect(screen.queryByText('● live')).not.toBeInTheDocument()
      expect(container.querySelector('[data-staleness]')).not.toBeNull()
    })

    // Design spec v1.3 ("the detached chip is NEUTRAL"): a historical page
    // is immutable and cannot go stale, so the chip shown while DETACHED
    // must never trip the aging/alarm colors — regardless of how old the
    // loaded window's newest row actually is.
    it('detached header: the staleness chip is neutral, never aging/alarm colors', async () => {
      mockTail()
      const user = userEvent.setup()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'match', mk(2))
      await emit(1, 'page_end', { cursor: 'c9', exhausted: false })

      const chip = container.querySelector('[data-staleness]')
      expect(chip).toHaveAttribute('data-staleness', 'neutral')
      // …and it shows when the window was READ, not how old its messages
      // are: mk(2)'s timestamp is ancient, but the page just landed, so the
      // chip must say "just now" (the fetch is the honest sample time).
      expect(chip).toHaveTextContent(/just now/i)
    })

    // Contrast case: the live stream dying while still ATTACHED is a
    // genuinely honest alarm (new data really is missing) — that chip must
    // keep today's aging/alarm tiers, not go neutral.
    it('the !live-while-attached chip keeps its aging/alarm tiers (not neutral)', async () => {
      const tail = mockTail()
      const { container } = render(<Timeline cluster="prod" topic="orders" />)
      await emit(0, 'match', mk(9))
      await emit(0, 'page_end', { cursor: null, exhausted: true })
      await act(async () => {
        tail.handlers().onError({ code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })
      })
      // mk(9)'s timestamp_ms (1009) is ancient next to real wall-clock time
      // (this chip gets no `now` override, unlike the unit tests above), so
      // it genuinely reads as 'stale' — the point is that it's a real
      // age-based tier, never 'neutral'.
      const chip = container.querySelector('[data-staleness]')
      expect(chip).toHaveAttribute('data-staleness', 'stale')
    })
  })
})
