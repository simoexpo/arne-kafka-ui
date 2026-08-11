export interface ClusterHealth {
  name: string
  status: 'healthy' | 'unreachable'
  broker_count: number | null
  error: string | null
}
export interface ClustersResponse { clusters: ClusterHealth[] }

export interface BrokerInfo { id: number; host: string; port: number }
export interface Overview {
  brokers: BrokerInfo[]
  controller_id: number | null
  topic_count: number
  partition_count: number
  under_replicated_partitions: number
  as_of: number
}

export interface TopicSummary {
  name: string
  partitions: number
  replication_factor: number
  message_estimate: number
  size_bytes: number | null
  internal: boolean
}
export interface TopicList { topics: TopicSummary[]; as_of: number }

export interface PartitionInfo {
  id: number
  leader: number
  replicas: number[]
  isr: number[]
  start_offset: number
  end_offset: number
}
export interface ConfigEntry { name: string; value: string | null; is_default: boolean }
export interface TopicDetail {
  name: string
  partitions: PartitionInfo[]
  configs: ConfigEntry[]
  as_of: number
}

export interface RatePoint { ts_ms: number; msgs_per_sec: number; bytes_per_sec: number | null }
export interface Throughput { topic: string; samples: RatePoint[]; as_of: number | null }

export interface PartitionLag {
  topic: string
  partition: number
  committed_offset: number
  end_offset: number
  lag: number
}
export interface TopicGroupLag { group_id: string; state: string; total_lag: number; partitions: PartitionLag[] }
export interface TopicConsumers { topic: string; groups: TopicGroupLag[]; as_of: number }

export interface GroupSummary {
  group_id: string
  state: string
  protocol_type: string
  member_count: number
  total_lag: number
}
export interface GroupList { groups: GroupSummary[]; as_of: number }
export interface MemberInfo { member_id: string; client_id: string; client_host: string }
export interface GroupDetail {
  group_id: string
  state: string
  members: MemberInfo[]
  partitions: PartitionLag[]
  as_of: number
}

export type Encoding = 'avro' | 'protobuf' | 'json' | 'utf8' | 'bytes' | 'decode_error'
export interface DecodedPayload { encoding: Encoding; text: string; schema_id: number | null; error: string | null }
export interface MessageHeader { key: string; value: string }
export interface MessageOut {
  partition: number
  offset: number
  timestamp_ms: number | null
  key: DecodedPayload | null
  value: DecodedPayload | null
  headers: MessageHeader[]
}
export interface MessagesPage { messages: MessageOut[]; as_of: number }
export interface SearchProgress { scanned: number; total: number; matches: number }
export interface SseErrorData { code: string; message: string; cluster?: string | null; retriable?: boolean }
export type BrowseAnchor =
  | { anchor: 'latest'; limit: number }
  | { anchor: 'offset'; partition: number; offset: number; limit: number }
  | { anchor: 'timestamp'; ts_ms: number; limit: number }
export type SearchRange =
  | { range: 'last_n'; n: number }
  | { range: 'offsets'; from: number; to: number; partition?: number }
  | { range: 'ts'; from_ms: number; to_ms?: number }
export type SearchFilter =
  | { filter: 'key_eq' | 'key_contains' | 'value_contains'; q: string }
  | { filter: 'json_eq'; q: string; path: string }
