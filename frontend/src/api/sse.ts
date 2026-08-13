import type { MessageOut, SearchFilter, SearchProgress, SearchRange, SseErrorData } from './types'

const enc = encodeURIComponent

export function openSse(
  url: string,
  eventNames: string[],
  handlers: { onEvent: (name: string, data: unknown) => void; onTransportError: () => void },
): { close: () => void } {
  const es = new EventSource(url)
  for (const name of eventNames) {
    es.addEventListener(name, (e) => handlers.onEvent(name, JSON.parse((e as MessageEvent).data)))
  }
  es.onerror = () => handlers.onTransportError()
  return { close: () => es.close() }
}

export function tailTopic(
  cluster: string,
  topic: string,
  h: { onMessage: (m: MessageOut) => void; onError: (e: SseErrorData) => void; onTransportError: () => void },
) {
  return openSse(`/api/clusters/${enc(cluster)}/topics/${enc(topic)}/tail`, ['message', 'error'], {
    onEvent: (name, data) =>
      name === 'message' ? h.onMessage(data as MessageOut) : h.onError(data as SseErrorData),
    onTransportError: h.onTransportError,
  })
}

export interface TimelineProgress { scanned: number; matches: number; budget: number }

export type TimelineDirection = 'back' | 'forward'
export type TimelineAnchor = 'latest' | 'beginning' | 'offset' | 'timestamp'
export type TimelineFilterKind = 'contains' | 'key_contains' | 'value_contains' | 'json_eq'

export interface TimelinePageParams {
  direction: TimelineDirection
  limit: number
  // First page: anchor (+ its own params). Subsequent pages: cursor.
  // Exactly one is meaningful; cursor wins if both are set (see timelinePage).
  anchor?: TimelineAnchor
  partition?: number
  offset?: number
  ts_ms?: number
  cursor?: string
  filter?: TimelineFilterKind
  q?: string
  path?: string
}

export function timelinePage(
  cluster: string,
  topic: string,
  params: TimelinePageParams,
  h: {
    onMatch: (m: MessageOut) => void
    onProgress: (p: TimelineProgress) => void
    onPageEnd: (cursor: string | null, exhausted: boolean) => void
    onError: (e: SseErrorData) => void
    onTransportError: () => void
  },
) {
  const qs = new URLSearchParams()
  qs.set('direction', params.direction)
  qs.set('limit', String(params.limit))
  if (params.cursor !== undefined) {
    qs.set('cursor', params.cursor)
  } else if (params.anchor !== undefined) {
    qs.set('anchor', params.anchor)
    if (params.partition !== undefined) qs.set('partition', String(params.partition))
    if (params.offset !== undefined) qs.set('offset', String(params.offset))
    if (params.ts_ms !== undefined) qs.set('ts_ms', String(params.ts_ms))
  }
  if (params.filter !== undefined) {
    qs.set('filter', params.filter)
    if (params.q !== undefined) qs.set('q', params.q)
    if (params.path !== undefined) qs.set('path', params.path)
  }
  return openSse(
    `/api/clusters/${enc(cluster)}/topics/${enc(topic)}/timeline?${qs}`,
    ['match', 'progress', 'page_end', 'error'],
    {
      onEvent: (name, data) => {
        if (name === 'match') h.onMatch(data as MessageOut)
        else if (name === 'progress') h.onProgress(data as TimelineProgress)
        else if (name === 'page_end') {
          const d = data as { cursor: string | null; exhausted: boolean }
          h.onPageEnd(d.cursor, d.exhausted)
        } else h.onError(data as SseErrorData)
      },
      onTransportError: h.onTransportError,
    },
  )
}

export function searchTopic(
  cluster: string,
  topic: string,
  range: SearchRange,
  filter: SearchFilter,
  h: {
    onProgress: (p: SearchProgress) => void
    onMatch: (m: MessageOut) => void
    onDone: (reason: string) => void
    onError: (e: SseErrorData) => void
    onTransportError: () => void
  },
) {
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(range)) qs.set(k, String(v))
  for (const [k, v] of Object.entries(filter)) qs.set(k, String(v))
  return openSse(
    `/api/clusters/${enc(cluster)}/topics/${enc(topic)}/search?${qs}`,
    ['progress', 'match', 'done', 'error'],
    {
      onEvent: (name, data) => {
        if (name === 'progress') h.onProgress(data as SearchProgress)
        else if (name === 'match') h.onMatch(data as MessageOut)
        else if (name === 'done') h.onDone((data as { reason: string }).reason)
        else h.onError(data as SseErrorData)
      },
      onTransportError: h.onTransportError,
    },
  )
}
