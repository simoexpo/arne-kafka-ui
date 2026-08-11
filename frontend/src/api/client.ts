import type {
  BrowseAnchor, ClustersResponse, GroupDetail, GroupList, MessagesPage, Overview,
  Throughput, TopicConsumers, TopicDetail, TopicList,
} from './types'

export class ApiError extends Error {
  code: string
  cluster: string | null
  retriable: boolean
  status: number

  constructor(status: number, code: string, message: string, cluster: string | null, retriable: boolean) {
    super(message)
    this.status = status
    this.code = code
    this.cluster = cluster
    this.retriable = retriable
  }
}

export async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { signal })
  if (res.ok) return res.json() as Promise<T>
  let body: { code?: string; message?: string; cluster?: string | null; retriable?: boolean } = {}
  try {
    body = await res.json()
  } catch {
    // non-JSON error body; fall through to defaults
  }
  throw new ApiError(
    res.status,
    body.code ?? `http_${res.status}`,
    body.message ?? `request failed with status ${res.status}`,
    body.cluster ?? null,
    body.retriable ?? res.status >= 500,
  )
}

const enc = encodeURIComponent

export const getClusters = (signal?: AbortSignal) => fetchJson<ClustersResponse>('/api/clusters', signal)
export const getOverview = (c: string, signal?: AbortSignal) => fetchJson<Overview>(`/api/clusters/${enc(c)}/overview`, signal)
export const getTopics = (c: string, signal?: AbortSignal) => fetchJson<TopicList>(`/api/clusters/${enc(c)}/topics`, signal)
export const getTopicDetail = (c: string, t: string, signal?: AbortSignal) => fetchJson<TopicDetail>(`/api/clusters/${enc(c)}/topics/${enc(t)}`, signal)
export const getTopicConsumers = (c: string, t: string, signal?: AbortSignal) => fetchJson<TopicConsumers>(`/api/clusters/${enc(c)}/topics/${enc(t)}/consumers`, signal)
export const getThroughput = (c: string, t: string, signal?: AbortSignal) => fetchJson<Throughput>(`/api/clusters/${enc(c)}/topics/${enc(t)}/throughput`, signal)
export const getGroups = (c: string, signal?: AbortSignal) => fetchJson<GroupList>(`/api/clusters/${enc(c)}/groups`, signal)
export const getGroupDetail = (c: string, g: string, signal?: AbortSignal) => fetchJson<GroupDetail>(`/api/clusters/${enc(c)}/groups/${enc(g)}`, signal)

export const getMessages = (c: string, t: string, a: BrowseAnchor, signal?: AbortSignal) => {
  const qs = new URLSearchParams(
    Object.entries(a).map(([k, v]) => [k, String(v)]),
  )
  return fetchJson<MessagesPage>(`/api/clusters/${enc(c)}/topics/${enc(t)}/messages?${qs}`, signal)
}
