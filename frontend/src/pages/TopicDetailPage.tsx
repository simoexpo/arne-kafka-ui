import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getTopicDetail } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { ConfigTab } from './ConfigTab'
import { ConsumersTab } from './ConsumersTab'
import { MessagesTab } from './MessagesTab'

const TABS = ['Messages', 'Partitions', 'Consumers', 'Config'] as const
type Tab = (typeof TABS)[number]

export function TopicDetailView({ cluster, topic }: { cluster: string; topic: string }) {
  const [tab, setTab] = useState<Tab>('Messages')
  const detail = useQuery({
    queryKey: ['topic', cluster, topic],
    queryFn: ({ signal }) => getTopicDetail(cluster, topic, signal),
    refetchInterval: 10_000,
  })

  return (
    // Flex column filling the shell's bounded height (main is
    // overflow-hidden — see AppShell): header + tab bar are fixed-size,
    // the tab body below is the ONE flex-1 min-h-0 slot. Non-Messages tabs
    // get their own overflow-y-auto wrapper (plain content that can grow
    // long — partitions/config/consumer tables); the Messages tab instead
    // hands that slot straight to Timeline, which chains flex-1 min-h-0
    // down to MessageList's own scroller — that scroller must be the ONLY
    // one on this tab.
    <div className="flex h-full flex-col gap-4">
      <div className="flex items-center gap-3">
        <h1 className="flex items-center gap-1.5 font-mono text-lg font-semibold">
          {topic}
          <CopyButton text={topic} label={topic} />
        </h1>
        <StalenessChip asOf={detail.data?.as_of ?? null} refreshing={detail.isFetching} failed={detail.isError} />
      </div>
      <div role="tablist" className="flex gap-1 border-b border-zinc-200 dark:border-zinc-800">
        {TABS.map((t) => (
          <button
            key={t}
            role="tab"
            aria-selected={t === tab}
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
      <div className="min-h-0 flex-1">
        {tab === 'Messages' && <MessagesTab cluster={cluster} topic={topic} partitionIds={detail.data?.partitions.map((p) => p.id)} />}
        {tab === 'Partitions' && (
          <div className="h-full overflow-y-auto">
            <Panel
              title={`${detail.data?.partitions.length ?? 0} partitions`}
              error={detail.error}
              loading={detail.isPending}
              hasData={detail.data !== undefined}
            >
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
          </div>
        )}
        {tab === 'Consumers' && (
          <div className="h-full overflow-y-auto">
            <ConsumersTab cluster={cluster} topic={topic} />
          </div>
        )}
        {tab === 'Config' && (
          <div className="h-full overflow-y-auto">
            <ConfigTab data={detail.data} error={detail.error} loading={detail.isPending} />
          </div>
        )}
      </div>
    </div>
  )
}

export function TopicDetailPage() {
  const { cluster, topic } = useParams({ from: '/c/$cluster/topics/$topic' })
  return <TopicDetailView cluster={cluster} topic={topic} />
}
