import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getGroupDetail } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { Panel } from '../components/Panel'
import { Stat } from '../components/Stat'
import { StalenessChip } from '../components/StalenessChip'
import { statedLag } from '../lib/lag'

export function GroupDetailView({ cluster, group }: { cluster: string; group: string }) {
  const detail = useQuery({
    queryKey: ['group', cluster, group],
    queryFn: ({ signal }) => getGroupDetail(cluster, group, signal),
    refetchInterval: 10_000,
  })
  const rows = detail.data?.partitions ?? []
  const topics = new Set(rows.map((p) => p.topic)).size
  // Partitions it commits on, including the ones whose head we could not read.
  const partitionCount = rows.length + (detail.data?.unreadable_partitions ?? 0)
  const members = [...(detail.data?.members ?? [])].sort((a, b) => a.client_id.localeCompare(b.client_id))
  return (
    // Owns its own scrolling region — see OverviewPage's comment.
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="flex items-center gap-1.5 font-mono text-lg font-semibold">
          {group}
          <CopyButton text={group} label={group} />
        </h1>
        <StalenessChip asOf={detail.data?.as_of ?? null} refreshing={detail.isFetching} failed={detail.isError} />
      </div>
      {/* Same shape as the overview page (owner ruling 2026-08-20): what this
          group consumes on the left, who is consuming it on the right, and the
          per-partition detail below. */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Panel title="Group" error={detail.error} loading={detail.isPending} hasData={detail.data !== undefined}>
          {detail.data && (
            <>
              <dl className="grid grid-cols-3 gap-2 text-sm">
                <Stat label="topics" value={String(topics)} />
                <Stat label="partitions" value={String(partitionCount)} />
                <TotalLagStat rows={detail.data.partitions} unreadable={detail.data.unreadable_partitions} />
              </dl>
              <div
                data-testid="stat-assignor"
                className="mt-3 flex items-baseline gap-2 border-t border-zinc-200 pt-2 dark:border-zinc-800"
                title={
                  detail.data.assignment_strategy
                    ? 'the partition assignor these members negotiated'
                    : 'no members, so the group has negotiated no assignor'
                }
              >
                <span className="text-xs text-zinc-500">assignor</span>
                <span className="font-mono text-sm">{detail.data.assignment_strategy || '—'}</span>
              </div>
            </>
          )}
        </Panel>
        <Panel
          title="Members"
          // The state describes the membership situation, so it belongs beside
          // the members rather than in the page title.
          action={
            detail.data && (
              <span className="text-xs text-zinc-500">
                {detail.data.members.length} member{detail.data.members.length === 1 ? '' : 's'} · {detail.data.state}
              </span>
            )
          }
          error={detail.error}
          loading={detail.isPending}
          hasData={detail.data !== undefined}
        >
          {detail.data && detail.data.members.length === 0 && (
            <p className="text-sm text-zinc-500">no active members</p>
          )}
          {detail.data && (
            // Capped in height so a large roster scrolls itself instead of
            // stretching the row and pushing the partition table off the page.
            <ul className="max-h-44 space-y-1 overflow-y-auto font-mono text-sm">
              {members.map((m) => (
                <li key={m.member_id} data-testid="group-member">
                  {m.client_id} <span className="text-zinc-500">{m.client_host}</span>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>
      <Panel title="Partition lag" error={detail.error} loading={detail.isPending} hasData={detail.data !== undefined}>
        <table className="w-full text-left font-mono text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">topic</th><th>partition</th><th>committed</th><th>end</th><th>lag</th></tr>
          </thead>
          <tbody>
            {detail.data?.partitions.map((p) => (
              <tr key={`${p.topic}-${p.partition}`} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1">{p.topic}</td>
                <td>{p.partition}</td>
                <td>{p.committed_offset}</td>
                <td>{p.end_offset}</td>
                <td className={p.lag > 0 ? 'font-semibold' : 'text-zinc-400'}>{p.lag}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}

// Not tinted as a failure at any size: a healthy consumer on a busy topic sits
// thousands of messages behind, so a red number here would cry wolf.
function TotalLagStat({ rows, unreadable }: { rows: { lag: number }[]; unreadable: number }) {
  const { text, title } = statedLag(rows.reduce((sum, p) => sum + p.lag, 0), unreadable)
  return <Stat label="total lag" value={text} title={title} />
}

export function GroupDetailPage() {
  const { cluster, group } = useParams({ from: '/c/$cluster/consumers/$group' })
  return <GroupDetailView cluster={cluster} group={group} />
}
