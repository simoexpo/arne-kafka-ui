import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getTopicDetail } from '../api/client'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

const TABS = ['Messages', 'Partitions', 'Consumers', 'Config'] as const
type Tab = (typeof TABS)[number]

export function TopicDetailView({ cluster, topic }: { cluster: string; topic: string }) {
  const [tab, setTab] = useState<Tab>('Partitions')
  const detail = useQuery({
    queryKey: ['topic', cluster, topic],
    queryFn: () => getTopicDetail(cluster, topic),
    refetchInterval: 10_000,
  })

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <h1 className="font-mono text-lg font-semibold">{topic}</h1>
        <StalenessChip asOf={detail.data?.as_of ?? null} />
      </div>
      <div className="flex gap-1 border-b border-zinc-200 dark:border-zinc-800">
        {TABS.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-3 py-1.5 text-sm ${
              t === tab
                ? 'border-b-2 border-zinc-900 font-medium dark:border-zinc-100'
                : 'text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-300'
            }`}
          >
            {t}
          </button>
        ))}
      </div>
      {tab === 'Messages' && (
        <Panel title="Messages">
          <p className="text-sm text-zinc-500">Message browsing arrives with the message engine (v1 plan 2).</p>
        </Panel>
      )}
      {tab === 'Partitions' && (
        <Panel title="Partitions" error={detail.error} loading={detail.isPending}>
          <table className="w-full text-left text-sm">
            <thead className="text-xs text-zinc-500">
              <tr><th className="py-1">id</th><th>leader</th><th>replicas</th><th>ISR</th><th>start</th><th>end</th><th>health</th></tr>
            </thead>
            <tbody className="font-mono">
              {detail.data?.partitions.map((p) => (
                <tr key={p.id} data-testid="partition-row" className="border-t border-zinc-100 dark:border-zinc-800">
                  <td className="py-1">{p.id}</td>
                  <td>{p.leader}</td>
                  <td>{p.replicas.join(', ')}</td>
                  <td>{p.isr.join(', ')}</td>
                  <td>{p.start_offset}</td>
                  <td>{p.end_offset}</td>
                  <td>
                    {p.isr.length < p.replicas.length
                      ? <span className="text-red-600 dark:text-red-400">under-replicated</span>
                      : <span className="text-emerald-600 dark:text-emerald-400">ok</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      )}
      {tab === 'Consumers' && <ConsumersTab cluster={cluster} topic={topic} />}
      {tab === 'Config' && (
        <Panel title="Config" error={detail.error} loading={detail.isPending}>
          <table className="w-full text-left text-sm">
            <tbody>
              {detail.data?.configs.map((c) => (
                <tr key={c.name} data-testid={`config-${c.name}`} className="border-t border-zinc-100 dark:border-zinc-800">
                  <td className="py-1 font-mono">{c.name}</td>
                  <td className="font-mono text-zinc-600 dark:text-zinc-400">{c.value ?? '—'}</td>
                  <td className="text-right text-xs">
                    {c.is_default
                      ? <span className="text-zinc-400">default</span>
                      : <span className="text-amber-600 dark:text-amber-400">overridden</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      )}
    </div>
  )
}

// Replaced with the real implementation in the next task (throughput + lag).
export function ConsumersTab(_props: { cluster: string; topic: string }) {
  return null
}

export function TopicDetailPage() {
  const { cluster, topic } = useParams({ from: '/c/$cluster/topics/$topic' })
  return <TopicDetailView cluster={cluster} topic={topic} />
}
