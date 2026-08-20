export interface ClusterHealth {
  name: string
  status: 'healthy' | 'unreachable'
  broker_count: number | null
  error: string | null
}
export interface ClustersResponse { clusters: ClusterHealth[] }

export interface BrokerInfo { id: number; host: string; port: number }
export interface TopicPartitions { name: string; partitions: number }
export interface Overview {
  brokers: BrokerInfo[]
  controller_id: number | null
  topic_count: number
  partition_count: number
  under_replicated_partitions: number
  top_topics: TopicPartitions[]
  as_of: number
}

export interface TopicSummary {
  name: string
  partitions: number
  replication_factor: number
  // The worst partition's in-sync replica count — equal to
  // replication_factor means fully replicated.
  isr: number
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

export interface RatePoint {
  ts_ms: number
  msgs_per_sec: number
  // How long this rate was measured over, and whether that stretch was
  // actually observed: sampling only happens while someone watches the topic,
  // so the first point after a return visit spans the time you were away.
  window_ms: number
  continuous: boolean
  bytes_per_sec: number | null
}
export interface Throughput { topic: string; samples: RatePoint[]; as_of: number | null }

export interface PartitionLag {
  topic: string
  partition: number
  committed_offset: number
  end_offset: number
  lag: number
}
export interface TopicGroupLag {
  group_id: string
  state: string
  // null when this group's position on the topic isn't determinable: it holds
  // an assignment but hasn't committed yet, or `error` says the lookup failed.
  // A lower bound, not a total, when `unreadable_partitions` is non-zero.
  total_lag: number | null
  // Partitions of THIS topic whose head could not be read.
  unreadable_partitions: number
  partitions: PartitionLag[]
  error: string | null
}
// Groups whose offset lookup failed while it was unknown whether they consume
// this topic at all — disclosed rather than listed or silently dropped.
export interface UncheckedGroup { group_id: string; error: string }
export interface TopicConsumers {
  topic: string
  groups: TopicGroupLag[]
  unchecked: UncheckedGroup[]
  as_of: number
}

export interface GroupSummary {
  group_id: string
  state: string
  protocol_type: string
  member_count: number
}
// Lag is asked for by name, for the rows on screen — see getGroupLag.
// `total_lag` is null when the group has committed nothing anywhere (no
// position to be behind) or when the lookup failed (`error` says why).
export interface GroupLagEntry {
  group_id: string
  // A lower bound, not a total, when `unreadable_partitions` is non-zero.
  total_lag: number | null
  unreadable_partitions: number
  error: string | null
}
export interface GroupLagBatch { groups: GroupLagEntry[]; as_of: number }
export interface GroupList { groups: GroupSummary[]; as_of: number }

export interface SubjectList { subjects: string[]; as_of: number }
export interface RegistrySettings { compatibility_level: string; mode: string; url: string; as_of: number }
export interface SubjectStrategyInfo {
  strategy: 'topic_name' | 'topic_record_name' | 'record_name' | null
  topic: string | null
  role: 'key' | 'value' | null
  as_of: number
}
export interface SchemaIdSubject { subject: string; version: number; as_of: number }
export interface CompatibilityLevel { level: string; as_of: number }
export interface CompatibilityResult { is_compatible: boolean; messages: string[]; as_of: number }
export interface SubjectDetail {
  subject: string
  versions: number[]
  version: number
  id: number
  schema_type: string
  schema: string
  as_of: number
}
export interface MemberInfo { member_id: string; client_id: string; client_host: string }
export interface GroupDetail {
  group_id: string
  state: string
  // The assignor the members negotiated; empty when there are no members.
  assignment_strategy: string
  members: MemberInfo[]
  partitions: PartitionLag[]
  // Partitions this group commits on whose head could not be read: absent from
  // `partitions`, so a total summed from those rows alone would under-report.
  unreadable_partitions: number
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
export interface SseErrorData { code: string; message: string; cluster?: string | null; retriable?: boolean }
