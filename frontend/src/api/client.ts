import type {
  ClustersResponse, GroupDetail, GroupList, Overview,
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

export async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(path)
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

export const getClusters = () => fetchJson<ClustersResponse>('/api/clusters')
export const getOverview = (c: string) => fetchJson<Overview>(`/api/clusters/${enc(c)}/overview`)
export const getTopics = (c: string) => fetchJson<TopicList>(`/api/clusters/${enc(c)}/topics`)
export const getTopicDetail = (c: string, t: string) => fetchJson<TopicDetail>(`/api/clusters/${enc(c)}/topics/${enc(t)}`)
export const getTopicConsumers = (c: string, t: string) => fetchJson<TopicConsumers>(`/api/clusters/${enc(c)}/topics/${enc(t)}/consumers`)
export const getThroughput = (c: string, t: string) => fetchJson<Throughput>(`/api/clusters/${enc(c)}/topics/${enc(t)}/throughput`)
export const getGroups = (c: string) => fetchJson<GroupList>(`/api/clusters/${enc(c)}/groups`)
export const getGroupDetail = (c: string, g: string) => fetchJson<GroupDetail>(`/api/clusters/${enc(c)}/groups/${enc(g)}`)
