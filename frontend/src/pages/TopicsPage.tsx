import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getTopics } from '../api/client'
import { useListWindow } from '../lib/listWindow'
import { CopyButton } from '../components/CopyButton'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { Switch } from '../components/Switch'

/// FE windowing over the single topics response: no broker or API cost, only
/// how many rows are mounted at once.
const RENDER_STEP = 100

export function TopicsView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const topics = useQuery({
    queryKey: ['topics', cluster],
    queryFn: ({ signal }) => getTopics(cluster, signal),
    refetchInterval: 30_000,
  })
  const matching = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return (topics.data?.topics ?? [])
      .filter((t) => showInternal || !t.internal)
      .filter((t) => q === '' || t.name.toLowerCase().includes(q))
  }, [topics.data, filter, showInternal])
  const window = useListWindow(matching.length, RENDER_STEP, RENDER_STEP)
  const visible = matching.slice(0, window.count)

  return (
    // The page itself never scrolls: header and filter stay put, and the list
    // below owns the one scrolling region (the app shell's `main` is
    // overflow-hidden — see AppShell).
    <div className="flex h-full min-h-0 flex-col gap-4">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Topics</h1>
        <StalenessChip asOf={topics.data?.as_of ?? null} refreshing={topics.isFetching} failed={topics.isError} />
      </div>
      <div className="flex items-center gap-4">
        <FilterInput
          value={filter}
          onChange={(next) => {
            setFilter(next)
            window.reset()
          }}
          placeholder="filter topics…"
          ariaLabel="filter topics"
        />
        <Switch
          checked={showInternal}
          label="show internal"
          onChange={() => {
            setShowInternal((v) => !v)
            window.reset()
          }}
        />
      </div>
      <Panel
        title={`${matching.length} topics`}
        error={topics.error}
        loading={topics.isPending}
        hasData={topics.data !== undefined}
        className="flex min-h-0 flex-1 flex-col"
      >
        <div data-testid="list-scroller" onScroll={window.onScroll} className="min-h-0 flex-1 overflow-y-auto">
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr>
              <th className="py-1">name</th><th>partitions</th>
              <th><abbr title="replication factor" className="cursor-help underline decoration-dotted">RF</abbr></th>
              <th><abbr title="in-sync replicas (worst partition)" className="cursor-help underline decoration-dotted">ISR</abbr></th>
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
                  <span
                    data-testid="isr"
                    className={
                      t.isr < t.replication_factor
                        ? 'rounded bg-amber-100 px-1.5 py-0.5 font-semibold text-amber-800 dark:bg-amber-900/50 dark:text-amber-300'
                        : undefined
                    }
                  >
                    {t.isr}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </Panel>
    </div>
  )
}

export function TopicsPage() {
  const { cluster } = useParams({ from: '/c/$cluster/topics' })
  return <TopicsView cluster={cluster} />
}
