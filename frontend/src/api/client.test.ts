import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, fetchJson, getClusters } from './client'

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
    const err = await fetchJson('/api/x').catch((e) => e)
    expect(err).toBeInstanceOf(ApiError)
    expect(err.code).toBe('topic_not_found')
    expect(err.cluster).toBe('prod')
    expect(err.retriable).toBe(false)
    expect(err.status).toBe(404)
    expect(err.message).toContain('does not exist')
  })

  it('throws ApiError with fallback fields when error body is not structured', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('gateway boom', { status: 502 })))
    const err = await fetchJson('/api/x').catch((e) => e)
    expect(err).toBeInstanceOf(ApiError)
    expect(err.code).toBe('http_502')
    expect(err.retriable).toBe(true) // 5xx default retriable
  })
})

describe('endpoints', () => {
  it('getClusters hits /api/clusters', async () => {
    mockFetchOnce(200, { clusters: [] })
    await getClusters()
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe('/api/clusters')
  })
})
