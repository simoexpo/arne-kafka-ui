import { describe, expect, it } from 'vitest'
import { ApiError } from './client'
import { describeError } from './errors'

describe('describeError', () => {
  it('kafka_timeout is a kafka failure with a cluster-scoped headline', () => {
    const err = new ApiError(504, 'kafka_timeout', 'fetch metadata timed out', 'playground', true)
    expect(describeError(err)).toEqual({ kind: 'kafka', headline: "Kafka unreachable — cluster 'playground'" })
  })

  it('kafka_error is also a kafka failure', () => {
    const err = new ApiError(502, 'kafka_error', 'broker connection refused', 'playground', true)
    expect(describeError(err).kind).toBe('kafka')
  })

  it('kafka failure headline omits the cluster suffix when cluster is null', () => {
    const err = new ApiError(504, 'kafka_timeout', 'fetch metadata timed out', null, true)
    expect(describeError(err).headline).toBe('Kafka unreachable')
  })

  it('a plain network Error is a connection-lost failure with a retry hint', () => {
    expect(describeError(new Error('Failed to fetch'))).toEqual({
      kind: 'backend',
      headline: 'Connection to Betrachtung lost',
      hint: 'retrying automatically — the server may be restarting or unreachable from your network',
    })
  })

  it('the 15s request timeout error is a connection-lost failure', () => {
    expect(describeError(new Error('request timed out after 15s')).kind).toBe('backend')
  })

  it('an ApiError with an http_5xx fallback code is a connection-lost failure', () => {
    const err = new ApiError(502, 'http_502', 'request failed with status 502', null, true)
    expect(describeError(err)).toEqual({
      kind: 'backend',
      headline: 'Connection to Betrachtung lost',
      hint: 'retrying automatically — the server may be restarting or unreachable from your network',
    })
  })

  it('other ApiError codes are app-level resource errors with no scary headline', () => {
    const err = new ApiError(404, 'topic_not_found', "topic 'x' does not exist", 'prod', false)
    expect(describeError(err)).toEqual({ kind: 'app', headline: null })
  })

  it('bad_request is also app-level with no headline', () => {
    const err = new ApiError(400, 'bad_request', 'invalid offset', null, false)
    expect(describeError(err)).toEqual({ kind: 'app', headline: null })
  })
})
