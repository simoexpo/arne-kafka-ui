import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getGroupDetail } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { Panel } from '../components/Panel'
import { Stat } from '../components/Stat'
import { StalenessChip } from '../components/StalenessChip'
import { assignorClass } from '../lib/assignor'
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
  // One member with an undecodable blob makes every gap in the map ambiguous:
  // an unclaimed partition may well be that member's. Unknown then, never
  // "unassigned" — the same fail-open the activity tab applies.
  const ownershipIsPartial = members.some((m) => m.assigned == null)
  const ownerOf = new Map<string, string>()
  for (const m of members) {
    for (const a of m.assigned ?? []) {
      for (const p of a.partitions) ownerOf.set(`${a.topic}/${p}`, m.client_id)
    }
  }
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
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Group" error={detail.error} loading={detail.isPending} hasData={detail.data !== undefined}>
          {detail.data && (
            <>
              <dl className="flex justify-between gap-3 text-sm">
                <Stat label="topics" value={String(topics)} />
                <Stat label="partitions" value={String(partitionCount)} />
                <TotalLagStat rows={detail.data.partitions} unreadable={detail.data.unreadable_partitions} />
              </dl>
              <dl className="mt-4 flex justify-between gap-3 text-sm">
                <Stat
                  label="assignment strategy"
                  value={assignorClass(detail.data.assignment_strategy)}
                  title={
                    detail.data.assignment_strategy
                      ? assignorClass(detail.data.assignment_strategy)
                      : 'no members, so no assignor has been negotiated'
                  }
                />
                <Stat label="status" value={detail.data.state} />
                <Stat
                  label="protocol"
                  className="text-right"
                  value={detail.data.group_type || '—'}
                  title={
                    detail.data.group_type
                      ? 'the rebalance protocol this group speaks'
                      : 'the broker did not say which rebalance protocol this group uses'
                  }
                />
              </dl>
            </>
          )}
        </Panel>
        <Panel
          title="Members"
          // The state describes the membership situation, so it belongs beside
          // the members rather than in the page title.
          // Just the count: state and protocol describe the group, not its
          // roster, and live in the Group card.
          action={
            detail.data && (
              <span className="text-xs text-zinc-500">
                {detail.data.members.length} member{detail.data.members.length === 1 ? '' : 's'}
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
                  {m.assigned?.length === 0 && (
                    <span
                      className="ml-1 text-xs text-amber-600 dark:text-amber-500"
                      title="holds no partition: a standby, or more members than partitions"
                    >
                      idle
                    </span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>
      <Panel title="Partition lag" error={detail.error} loading={detail.isPending} hasData={detail.data !== undefined}>
        <table className="w-full table-fixed text-left font-mono text-sm">
          <thead className="text-xs text-zinc-500">
            <tr>
              <th className="w-[26%] py-1">topic</th>
              <th className="w-[11%]">partition</th>
              <th className="w-[16%]">committed</th>
              <th className="w-[16%]">end</th>
              <th className="w-[11%]">lag</th>
              <th className="w-[20%]">owner</th>
            </tr>
          </thead>
          <tbody>
            {detail.data?.partitions.map((p) => (
              <tr
                key={`${p.topic}-${p.partition}`}
                data-testid="partition-row"
                className="border-t border-zinc-100 dark:border-zinc-800"
              >
                <td className="py-1">{p.topic}</td>
                <td>{p.partition}</td>
                <td>{p.committed_offset}</td>
                <td>{p.end_offset}</td>
                <td className={p.lag > 0 ? 'font-semibold' : 'text-zinc-400'}>{p.lag}</td>
                <Owner
                  client={ownerOf.get(`${p.topic}/${p.partition}`)}
                  partial={ownershipIsPartial}
                />
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
// Who is working this partition off. An unowned partition with lag is nobody's
// job — worth seeing — but it can only be stated when every member's
// assignment decoded.
function Owner({ client, partial }: { client?: string; partial: boolean }) {
  if (client) return <td data-testid="partition-owner">{client}</td>
  return partial ? (
    <td
      data-testid="partition-owner"
      className="cursor-help text-zinc-400"
      title="a member's assignment could not be decoded, so ownership of this partition is unknown"
    >
      —
    </td>
  ) : (
    <td
      data-testid="partition-owner"
      className="cursor-help text-amber-600 dark:text-amber-500"
      title="no live member holds this partition, so nothing is consuming it"
    >
      unassigned
    </td>
  )
}

function TotalLagStat({ rows, unreadable }: { rows: { lag: number }[]; unreadable: number }) {
  const { text, title } = statedLag(rows.reduce((sum, p) => sum + p.lag, 0), unreadable)
  return <Stat label="total lag" value={text} title={title} />
}

export function GroupDetailPage() {
  const { cluster, group } = useParams({ from: '/c/$cluster/consumers/$group' })
  return <GroupDetailView cluster={cluster} group={group} />
}
