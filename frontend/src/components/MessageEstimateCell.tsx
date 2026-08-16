import { estimateErrorTitle, formatCount } from '../lib/format'

// Shared by OverviewPage's "Top topics" table and TopicsPage's inventory
// table: a topic's message-count cell is tri-state — a real estimate, a
// dash carrying a Kafka-attributed reason (a watermark fetch failed), or a
// plain dash (e.g. an internal topic, whose estimate is never computed).
// `tabIndex`/`aria-label` make the reason reachable by keyboard and screen
// readers too — a bare `title` attribute is neither.
export function MessageEstimateCell({ estimate, error }: { estimate: number | null; error: string | null }) {
  if (estimate !== null) return <>{formatCount(estimate)}</>
  if (error !== null) {
    const title = estimateErrorTitle(error)
    return (
      <span tabIndex={0} title={title} aria-label={title} className="cursor-help text-zinc-400 underline decoration-dotted">
        —
      </span>
    )
  }
  return <>—</>
}
