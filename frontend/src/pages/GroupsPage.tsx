import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getGroups } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { formatCount } from '../lib/format'

export function GroupsView({ cluster }: { cluster: string }) {
  const groups = useQuery({
    queryKey: ['groups', cluster],
    queryFn: ({ signal }) => getGroups(cluster, signal),
    refetchInterval: 10_000,
  })
  return (
    // Owns its own scrolling region — see OverviewPage's comment.
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Consumer groups</h1>
        <StalenessChip asOf={groups.data?.as_of ?? null} refreshing={groups.isFetching} failed={groups.isError} />
      </div>
      <Panel
        title={`${groups.data?.groups.length ?? 0} groups`}
        error={groups.error}
        loading={groups.isPending}
        hasData={groups.data !== undefined}
      >
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">group</th><th>state</th><th>protocol</th><th>members</th><th>total lag</th></tr>
          </thead>
          <tbody>
            {groups.data?.groups.map((g) => (
              <tr key={g.group_id} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1.5">
                  <Link
                    to="/c/$cluster/consumers/$group"
                    params={{ cluster, group: g.group_id }}
                    className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                  >
                    {g.group_id}
                  </Link>
                  <span className="ml-1.5">
                    <CopyButton text={g.group_id} label={g.group_id} />
                  </span>
                </td>
                <td>{g.state}</td>
                <td className="text-zinc-500">{g.protocol_type}</td>
                <td>{g.member_count}</td>
                <td className={g.total_lag > 0 ? 'font-semibold' : 'text-zinc-400'}>{formatCount(g.total_lag)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}

export function GroupsPage() {
  const { cluster } = useParams({ from: '/c/$cluster/consumers' })
  return <GroupsView cluster={cluster} />
}
