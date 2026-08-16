import { Timeline } from './timeline/Timeline'

export function MessagesTab({ cluster, topic, partitionIds }: { cluster: string; topic: string; partitionIds?: number[] }) {
  // Keyed on cluster/topic so switching topics mounts a fresh Timeline (and
  // therefore a fresh timelineStore + page/tail streams) instead of reusing
  // stale state across topics.
  return <Timeline key={`${cluster}/${topic}`} cluster={cluster} topic={topic} partitionIds={partitionIds} />
}
