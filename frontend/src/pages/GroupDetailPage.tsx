import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getGroupDetail } from '../api/client'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

export function GroupDetailView({ cluster, group }: { cluster: string; group: string }) {
  const detail = useQuery({
    queryKey: ['group', cluster, group],
    queryFn: () => getGroupDetail(cluster, group),
    refetchInterval: 10_000,
  })
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <h1 className="font-mono text-lg font-semibold">{group}</h1>
        {detail.data && <span className="text-sm text-zinc-500">{detail.data.state}</span>}
        <StalenessChip asOf={detail.data?.as_of ?? null} refreshing={detail.isFetching} />
      </div>
      <Panel title="Members" error={detail.error} loading={detail.isPending}>
        {detail.data && detail.data.members.length === 0 && (
          <p className="text-sm text-zinc-500">no active members</p>
        )}
        {detail.data && (
          <ul className="space-y-1 font-mono text-sm">
            {detail.data.members.map((m) => (
              <li key={m.member_id}>
                {m.client_id} <span className="text-zinc-500">{m.client_host}</span>
              </li>
            ))}
          </ul>
        )}
      </Panel>
      <Panel title="Partition lag" error={detail.error} loading={detail.isPending}>
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

export function GroupDetailPage() {
  const { cluster, group } = useParams({ from: '/c/$cluster/groups/$group' })
  return <GroupDetailView cluster={cluster} group={group} />
}
