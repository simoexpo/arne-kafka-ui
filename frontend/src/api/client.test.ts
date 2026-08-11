import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, fetchJson, getClusters, getMessages } from './client'

function mockFetchOnce(status: number, body: unknown) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
    new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } }),
  ))
}

afterEach(() => vi.unstubAllGlobals())

describe('fetchJson', () => {
  it('returns parsed JSON on 200', async () => {
    mockFetchOnce(200, { ok: true })
    await expect(fetchJson('/api/x')).resolves.toEqual({ ok: true })
  })

  it('throws ApiError with structured body on non-2xx', async () => {
    mockFetchOnce(404, { code: 'topic_not_found', message: "topic 'x' does not exist", cluster: 'prod', retriable: false })
    const err = await fetchJson('/api/x').catch((e) => e as unknown)
    expect(err).toBeInstanceOf(ApiError)
    expect((err as ApiError).code).toBe('topic_not_found')
    expect((err as ApiError).cluster).toBe('prod')
    expect((err as ApiError).retriable).toBe(false)
    expect((err as ApiError).status).toBe(404)
    expect((err as ApiError).message).toContain('does not exist')
  })

  it('throws ApiError with fallback fields when error body is not structured', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('gateway boom', { status: 502 })))
    const err = await fetchJson('/api/x').catch((e) => e as unknown)
    expect(err).toBeInstanceOf(ApiError)
    expect((err as ApiError).code).toBe('http_502')
    expect((err as ApiError).retriable).toBe(true) // 5xx default retriable
  })
})

describe('endpoints', () => {
  it('getClusters hits /api/clusters', async () => {
    mockFetchOnce(200, { clusters: [] })
    await getClusters()
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe('/api/clusters')
  })

  it('fetchJson forwards an AbortSignal', async () => {
    mockFetchOnce(200, { ok: true })
    const controller = new AbortController()
    await fetchJson('/api/x', controller.signal)
    expect(vi.mocked(fetch).mock.calls[0][1]).toEqual({ signal: controller.signal })
  })

  it('getMessages builds anchor query strings', async () => {
    mockFetchOnce(200, { messages: [], as_of: 1 })
    await getMessages('c', 't', { anchor: 'offset', partition: 0, offset: 42, limit: 50 })
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe('/api/clusters/c/topics/t/messages?anchor=offset&partition=0&offset=42&limit=50')
  })
})
