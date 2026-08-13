import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getOverview, getTopics } from '../api/client'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { formatCount } from '../lib/format'

export function OverviewView({ cluster }: { cluster: string }) {
  const overview = useQuery({
    queryKey: ['overview', cluster],
    queryFn: () => getOverview(cluster),
    refetchInterval: 10_000,
  })
  const topics = useQuery({
    queryKey: ['topics', cluster],
    queryFn: () => getTopics(cluster),
    refetchInterval: 30_000,
  })
  const top = (topics.data?.topics ?? [])
    .filter((t) => !t.internal)
    .sort((a, b) => (b.message_estimate ?? -1) - (a.message_estimate ?? -1))
    .slice(0, 10)

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Overview</h1>
        <StalenessChip asOf={overview.data?.as_of ?? null} refreshing={overview.isFetching} failed={overview.isError} />
      </div>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Panel title="Cluster" error={overview.error} loading={overview.isPending} hasData={overview.data !== undefined}>
          {overview.data && (
            <dl className="grid grid-cols-3 gap-2 text-sm">
              <Stat label="topics" value={String(overview.data.topic_count)} />
              <Stat label="partitions" value={String(overview.data.partition_count)} />
              <Stat label="under-replicated" value={String(overview.data.under_replicated_partitions)}
                warn={overview.data.under_replicated_partitions > 0} />
            </dl>
          )}
        </Panel>
        <Panel title="Brokers" error={overview.error} loading={overview.isPending} hasData={overview.data !== undefined}>
          <ul className="space-y-1 font-mono text-sm">
            {overview.data?.brokers.map((b) => (
              <li key={b.id}>
                <span className="text-zinc-500">#{b.id}</span> {b.host}:{b.port}
              </li>
            ))}
          </ul>
        </Panel>
      </div>
      <Panel title="Top topics" error={topics.error} loading={topics.isPending} hasData={topics.data !== undefined}>
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">topic</th><th>partitions</th><th>messages</th></tr>
          </thead>
          <tbody>
            {top.map((t) => (
              <tr key={t.name} data-testid="top-topic" className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1 font-mono">{t.name}</td>
                <td>{t.partitions}</td>
                <td>{t.message_estimate === null ? '—' : formatCount(t.message_estimate)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}

function Stat({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div>
      <dt className="text-xs text-zinc-500">{label}</dt>
      <dd className={`text-xl font-semibold ${warn ? 'text-red-600 dark:text-red-400' : ''}`}>{value}</dd>
    </div>
  )
}

export function OverviewPage() {
  const { cluster } = useParams({ from: '/c/$cluster/overview' })
  return <OverviewView cluster={cluster} />
}
