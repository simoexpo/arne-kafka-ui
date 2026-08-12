import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getTopics } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { formatCount } from '../lib/format'

export function TopicsView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const topics = useQuery({
    queryKey: ['topics', cluster],
    queryFn: () => getTopics(cluster),
    refetchInterval: 30_000,
  })
  const visible = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return (topics.data?.topics ?? [])
      .filter((t) => showInternal || !t.internal)
      .filter((t) => q === '' || t.name.toLowerCase().includes(q))
  }, [topics.data, filter, showInternal])

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Topics</h1>
        <StalenessChip asOf={topics.data?.as_of ?? null} />
      </div>
      <div className="flex items-center gap-4">
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="filter topics…"
          className="w-72 rounded border border-zinc-300 bg-transparent px-3 py-1.5 text-sm outline-none focus:border-zinc-500 dark:border-zinc-700"
        />
        <label className="flex items-center gap-2 text-sm text-zinc-500">
          <button
            type="button"
            role="switch"
            aria-checked={showInternal}
            aria-label="show internal"
            onClick={() => setShowInternal((v) => !v)}
            className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${
              showInternal ? 'bg-blue-600 dark:bg-blue-500' : 'bg-zinc-300 dark:bg-zinc-700'
            }`}
          >
            <span
              className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform ${
                showInternal ? 'translate-x-4' : 'translate-x-0.5'
              }`}
            />
          </button>
          show internal
        </label>
      </div>
      <Panel title={`${visible.length} topics`} error={topics.error} loading={topics.isPending}>
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr>
              <th className="py-1">name</th><th>partitions</th><th>RF</th><th>messages</th><th>size</th>
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
                <td>{formatCount(t.message_estimate)}</td>
                <td className="text-zinc-400">{t.size_bytes === null ? '—' : formatCount(t.size_bytes)}</td>
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
