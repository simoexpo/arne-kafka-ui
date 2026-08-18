import { useQuery } from '@tanstack/react-query'
import { CopyButton } from '../components/CopyButton'
import { Stat } from '../components/Stat'
import { useParams } from '@tanstack/react-router'
import { getOverview } from '../api/client'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

export function OverviewView({ cluster }: { cluster: string }) {
  const overview = useQuery({
    queryKey: ['overview', cluster],
    queryFn: ({ signal }) => getOverview(cluster, signal),
    refetchInterval: 10_000,
  })

  return (
    // This page owns its own scrolling region (the app shell's `main` is
    // overflow-hidden — see AppShell) so a long "Top topics" table scrolls
    // in place instead of the whole document.
    <div className="h-full space-y-4 overflow-y-auto">
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
              <li key={b.id} className="flex items-center gap-1.5">
                <span className="text-zinc-500">#{b.id}</span> {b.host}:{b.port}
                <CopyButton text={`${b.host}:${b.port}`} label={`${b.host}:${b.port}`} />
              </li>
            ))}
          </ul>
        </Panel>
      </div>
      <Panel title="Top topics" error={overview.error} loading={overview.isPending} hasData={overview.data !== undefined}>
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">topic</th><th>partitions</th></tr>
          </thead>
          <tbody>
            {overview.data?.top_topics.map((t) => (
              <tr key={t.name} data-testid="top-topic" className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1 font-mono">{t.name}</td>
                <td>{t.partitions}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}


export function OverviewPage() {
  const { cluster } = useParams({ from: '/c/$cluster/overview' })
  return <OverviewView cluster={cluster} />
}
