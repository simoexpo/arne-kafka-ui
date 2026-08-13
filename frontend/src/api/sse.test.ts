import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../test/fake-event-source'
import { searchTopic, tailTopic, timelinePage } from './sse'

beforeEach(() => FakeEventSource.install())
afterEach(() => FakeEventSource.uninstall())

describe('tailTopic', () => {
  it('opens the tail url, forwards message and error events, closes', () => {
    const onMessage = vi.fn()
    const onError = vi.fn()
    const handle = tailTopic('prod', 'orders/x', { onMessage, onError, onTransportError: vi.fn() })
    const es = FakeEventSource.instances[0]
    expect(es.url).toBe('/api/clusters/prod/topics/orders%2Fx/tail')
    es.emit('message', { partition: 0, offset: 7, timestamp_ms: 1, key: null, value: null, headers: [] })
    expect(onMessage).toHaveBeenCalledWith(expect.objectContaining({ offset: 7 }))
    es.emit('error', { code: 'kafka_error', message: 'boom', cluster: 'prod', retriable: true })
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ code: 'kafka_error' }))
    handle.close()
    expect(es.closed).toBe(true)
  })

  it('reports transport errors', () => {
    const onTransportError = vi.fn()
    tailTopic('prod', 't', { onMessage: vi.fn(), onError: vi.fn(), onTransportError })
    FakeEventSource.instances[0].fireTransportError()
    expect(onTransportError).toHaveBeenCalled()
  })
})

describe('searchTopic', () => {
  it('builds the query string and dispatches all event kinds', () => {
    const h = { onProgress: vi.fn(), onMatch: vi.fn(), onDone: vi.fn(), onError: vi.fn(), onTransportError: vi.fn() }
    searchTopic('prod', 't', { range: 'last_n', n: 500 }, { filter: 'json_eq', q: '42', path: 'user.id' }, h)
    const es = FakeEventSource.instances[0]
    expect(es.url).toBe('/api/clusters/prod/topics/t/search?range=last_n&n=500&filter=json_eq&q=42&path=user.id')
    es.emit('progress', { scanned: 10, total: 500, matches: 1 })
    expect(h.onProgress).toHaveBeenCalledWith({ scanned: 10, total: 500, matches: 1 })
    es.emit('match', { partition: 1, offset: 3, timestamp_ms: 9, key: null, value: null, headers: [] })
    expect(h.onMatch).toHaveBeenCalledWith(expect.objectContaining({ partition: 1 }))
    es.emit('done', { reason: 'complete' })
    expect(h.onDone).toHaveBeenCalledWith('complete')
  })

  it('encodes offsets range with optional partition', () => {
    const h = { onProgress: vi.fn(), onMatch: vi.fn(), onDone: vi.fn(), onError: vi.fn(), onTransportError: vi.fn() }
    searchTopic('c', 't', { range: 'offsets', from: 5, to: 20, partition: 2 }, { filter: 'key_eq', q: 'k' }, h)
    expect(FakeEventSource.instances[0].url).toBe('/api/clusters/c/topics/t/search?range=offsets&from=5&to=20&partition=2&filter=key_eq&q=k')
  })
})

describe('timelinePage', () => {
  const mkMsg = (offset: number) => ({
    partition: 0, offset, timestamp_ms: 100, key: null, value: null, headers: [],
  })
  const h = () => ({ onMatch: vi.fn(), onProgress: vi.fn(), onPageEnd: vi.fn(), onError: vi.fn(), onTransportError: vi.fn() })

  it('builds an anchor=latest url with no cursor', () => {
    timelinePage('prod', 'orders', { direction: 'back', limit: 100, anchor: 'latest' }, h())
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest',
    )
  })

  it('builds an anchor=offset url with partition and offset', () => {
    timelinePage('prod', 'orders', { direction: 'forward', limit: 50, anchor: 'offset', partition: 2, offset: 10 }, h())
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=forward&limit=50&anchor=offset&partition=2&offset=10',
    )
  })

  it('builds an anchor=timestamp url', () => {
    timelinePage('prod', 'orders', { direction: 'back', limit: 100, anchor: 'timestamp', ts_ms: 12345 }, h())
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=timestamp&ts_ms=12345',
    )
  })

  it('builds a cursor url with no anchor params, even if anchor is also set', () => {
    timelinePage('prod', 'orders', { direction: 'back', limit: 100, cursor: 'abc123', anchor: 'latest' }, h())
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&cursor=abc123',
    )
  })

  it('appends filter params when present', () => {
    timelinePage(
      'prod',
      'orders',
      { direction: 'back', limit: 100, anchor: 'latest', filter: 'json_eq', q: '42', path: 'user.id' },
      h(),
    )
    expect(FakeEventSource.instances[0].url).toBe(
      '/api/clusters/prod/topics/orders/timeline?direction=back&limit=100&anchor=latest&filter=json_eq&q=42&path=user.id',
    )
  })

  it('omits filter params when absent', () => {
    timelinePage('prod', 'orders', { direction: 'back', limit: 100, anchor: 'latest' }, h())
    expect(FakeEventSource.instances[0].url).not.toContain('filter')
  })

  it('dispatches match, progress and page_end, and closes on demand', () => {
    const handlers = h()
    const handle = timelinePage('prod', 'orders', { direction: 'back', limit: 10, anchor: 'latest' }, handlers)
    const es = FakeEventSource.instances[0]
    es.emit('match', mkMsg(5))
    expect(handlers.onMatch).toHaveBeenCalledWith(expect.objectContaining({ offset: 5 }))
    es.emit('progress', { scanned: 100, matches: 3, budget: 250000 })
    expect(handlers.onProgress).toHaveBeenCalledWith({ scanned: 100, matches: 3, budget: 250000 })
    es.emit('page_end', { cursor: 'next-cursor', exhausted: false })
    expect(handlers.onPageEnd).toHaveBeenCalledWith('next-cursor', false)
    handle.close()
    expect(es.closed).toBe(true)
  })

  it('surfaces error events', () => {
    const handlers = h()
    timelinePage('prod', 'orders', { direction: 'back', limit: 10, anchor: 'latest' }, handlers)
    FakeEventSource.instances[0].emit('error', { code: 'kafka_error', message: 'boom', cluster: 'prod', retriable: true })
    expect(handlers.onError).toHaveBeenCalledWith(expect.objectContaining({ code: 'kafka_error' }))
  })

  it('reports transport errors', () => {
    const handlers = h()
    timelinePage('prod', 'orders', { direction: 'back', limit: 10, anchor: 'latest' }, handlers)
    FakeEventSource.instances[0].fireTransportError()
    expect(handlers.onTransportError).toHaveBeenCalled()
  })
})
