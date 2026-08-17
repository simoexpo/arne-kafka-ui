import { ApiError } from './client'

// Distinguishes "Kafka is unreachable" (our backend answered fine; the
// broker didn't) from "connection to the Arne server itself was
// lost" (the HTTP call never got an answer — the only real-world case is
// an already-open tab whose server restarted, crashed, or dropped off the
// network; Arne ships as one binary, so this is never phrased as
// two products) from ordinary resource errors (topic/group not found, bad
// request, …), which render as today with no scary headline.
export type ErrorKind = 'kafka' | 'backend' | 'app'
export interface ErrorDescription {
  kind: ErrorKind
  headline: string | null
  hint?: string
}

const HTTP_5XX_CODE = /^http_5\d\d$/
const CONNECTION_LOST: ErrorDescription = {
  kind: 'backend',
  headline: 'Connection to Arne lost',
  hint: 'retrying automatically — the server may be restarting or unreachable from your network',
}

export function describeError(err: unknown): ErrorDescription {
  if (err instanceof ApiError) {
    if (err.code === 'kafka_error' || err.code === 'kafka_timeout') {
      return {
        kind: 'kafka',
        headline: `Kafka unreachable${err.cluster ? ` — cluster '${err.cluster}'` : ''}`,
      }
    }
    if (HTTP_5XX_CODE.test(err.code)) {
      return CONNECTION_LOST
    }
    return { kind: 'app', headline: null }
  }
  // A plain Error covers network failures (fetch itself rejected) and the
  // 15s client-side request timeout — both mean the connection was lost.
  return CONNECTION_LOST
}
