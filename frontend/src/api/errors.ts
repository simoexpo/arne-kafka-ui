import { ApiError } from './client'

// Distinguishes "Kafka is unreachable" (our backend answered fine; the
// broker didn't) from "the Betrachtung backend is unreachable" (the HTTP
// call itself failed) from ordinary resource errors (topic/group not
// found, bad request, …), which render as today with no scary headline.
export type ErrorKind = 'kafka' | 'backend' | 'app'
export interface ErrorDescription {
  kind: ErrorKind
  headline: string | null
}

const HTTP_5XX_CODE = /^http_5\d\d$/

export function describeError(err: unknown): ErrorDescription {
  if (err instanceof ApiError) {
    if (err.code === 'kafka_error' || err.code === 'kafka_timeout') {
      return {
        kind: 'kafka',
        headline: `Kafka unreachable${err.cluster ? ` — cluster '${err.cluster}'` : ''}`,
      }
    }
    if (HTTP_5XX_CODE.test(err.code)) {
      return { kind: 'backend', headline: 'Betrachtung backend unreachable' }
    }
    return { kind: 'app', headline: null }
  }
  // A plain Error covers network failures (fetch itself rejected) and the
  // 15s client-side request timeout — both mean the backend never answered.
  return { kind: 'backend', headline: 'Betrachtung backend unreachable' }
}
