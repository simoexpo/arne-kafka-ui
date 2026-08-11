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
