import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getTopics } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { Switch } from '../components/Switch'
import { estimateErrorTitle, formatBytes, formatCount } from '../lib/format'

export function TopicsView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const topics = useQuery({
    queryKey: ['topics', cluster],
    queryFn: ({ signal }) => getTopics(cluster, signal),
    refetchInterval: 30_000,
  })
  const visible = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return (topics.data?.topics ?? [])
      .filter((t) => showInternal || !t.internal)
      .filter((t) => q === '' || t.name.toLowerCase().includes(q))
  }, [topics.data, filter, showInternal])

  return (
    // Owns its own scrolling region — see OverviewPage's comment.
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Topics</h1>
        <StalenessChip asOf={topics.data?.as_of ?? null} refreshing={topics.isFetching} failed={topics.isError} />
      </div>
      <div className="flex items-center gap-4">
        <FilterInput value={filter} onChange={setFilter} placeholder="filter topics…" ariaLabel="filter topics" />
        <Switch checked={showInternal} label="show internal" onChange={() => setShowInternal((v) => !v)} />
      </div>
      <Panel
        title={`${visible.length} topics`}
        error={topics.error}
        loading={topics.isPending}
        hasData={topics.data !== undefined}
      >
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr>
              <th className="py-1">name</th><th>partitions</th>
              <th><abbr title="replication factor" className="cursor-help underline decoration-dotted">RF</abbr></th>
              <th>messages</th><th>size</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((t) => (
              <tr key={t.name} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1.5">
                  <span className="inline-flex items-center gap-1.5">
                    <Link
                      to="/c/$cluster/topics/$topic"
                      params={{ cluster, topic: t.name }}
                      className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                    >
                      {t.name}
                    </Link>
                    <CopyButton text={t.name} label={t.name} />
                  </span>
                </td>
                <td>{t.partitions}</td>
                <td>{t.replication_factor}</td>
                <td>
                  {t.message_estimate !== null ? (
                    formatCount(t.message_estimate)
                  ) : t.estimate_error !== null ? (
                    <span title={estimateErrorTitle(t.estimate_error)} className="cursor-help text-zinc-400 underline decoration-dotted">
                      —
                    </span>
                  ) : (
                    '—'
                  )}
                </td>
                <td className="text-zinc-400">{t.size_bytes === null ? '—' : formatBytes(t.size_bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}

export function TopicsPage() {
  const { cluster } = useParams({ from: '/c/$cluster/topics' })
  return <TopicsView cluster={cluster} />
}
