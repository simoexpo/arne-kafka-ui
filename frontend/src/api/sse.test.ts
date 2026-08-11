import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeEventSource } from '../test/fake-event-source'
import { searchTopic, tailTopic } from './sse'

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
