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

  it('clicking load-older requests the next back page by cursor and appends rows', async () => {
    mockTail()
    const user = userEvent.setup()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(9))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByText('p0·9')).toBeInTheDocument()

    await user.click(screen.getByTestId('load-older'))
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

  it('hides load-older and shows the beginning-of-topic caption once exhausted', async () => {
    mockTail()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: null, exhausted: true })
    expect(screen.queryByTestId('load-older')).not.toBeInTheDocument()
    expect(screen.getByText('— beginning of topic —')).toBeInTheDocument()
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
    const user = userEvent.setup()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })
    expect(screen.getByText('p0·1')).toBeInTheDocument()

    await user.click(screen.getByTestId('load-older'))
    await emit(1, 'error', { code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })

    expect(screen.getByTestId('panel-error-banner')).toBeInTheDocument()
    expect(screen.getByText('p0·1')).toBeInTheDocument() // rows stay visible
  })

  it('classifies a kafka page error as Kafka-unreachable, not a generic connection-lost banner', async () => {
    mockTail()
    const user = userEvent.setup()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

    await user.click(screen.getByTestId('load-older'))
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
    const user = userEvent.setup()
    render(<Timeline cluster="prod" topic="orders" />)
    await emit(0, 'match', mk(1))
    await emit(0, 'page_end', { cursor: 'c1', exhausted: false })

    await user.click(screen.getByTestId('load-older'))
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

    it('caps the buffer at 500, dropping the oldest, and shows the honest "500+ · older dropped" label', async () => {
      const tail = mockTail()
      const user = userEvent.setup()
      render(<Timeline cluster="prod" topic="orders" />)
      await settleWithOneRow()

      scrollTo(100)
      await act(async () => {
        for (let i = 0; i < 501; i++) tail.handlers().onMessage(mk(100 + i))
      })
      expect(screen.getByText('500+ · older dropped')).toBeInTheDocument()
      expect(screen.queryByText(/▲ \d+ new/)).not.toBeInTheDocument()

      await user.click(screen.getByTestId('live-pill'))
      expect(screen.getByText('p0·600')).toBeInTheDocument() // newest kept
      expect(screen.queryByText('p0·100')).not.toBeInTheDocument() // oldest dropped
      // 1 initial row + exactly 500 buffered (the 501st push dropped the
      // very oldest, keeping the cap honest rather than growing unbounded).
      expect(screen.getByText('501 messages')).toBeInTheDocument()
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

      // Get into a paused state first via a beginning jump.
      await user.click(screen.getByTestId('jump-beginning'))
      await emit(1, 'page_end', { cursor: null, exhausted: true })
      await act(async () => tail.handlers().onMessage(mk(3)))
      expect(screen.getByText('▲ 1 new')).toBeInTheDocument()

      await user.click(screen.getByTestId('jump-now'))
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
  })
})
