import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getTopicDetail, getThroughput, getTopicConsumers } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { Sparkline } from '../components/Sparkline'
import { formatCount, formatRetentionValue, retentionMsHint } from '../lib/format'
import { MessagesTab } from './MessagesTab'
import type { ConfigEntry, TopicDetail, TopicGroupLag } from '../api/types'

const TABS = ['Messages', 'Partitions', 'Consumers', 'Config'] as const
type Tab = (typeof TABS)[number]

export function TopicDetailView({ cluster, topic }: { cluster: string; topic: string }) {
  const [tab, setTab] = useState<Tab>('Messages')
  const detail = useQuery({
    queryKey: ['topic', cluster, topic],
    queryFn: () => getTopicDetail(cluster, topic),
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
      <div className="min-h-0 flex-1">
        {tab === 'Messages' && <MessagesTab cluster={cluster} topic={topic} />}
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

export function ConsumersTab({ cluster, topic }: { cluster: string; topic: string }) {
  const throughput = useQuery({
    queryKey: ['throughput', cluster, topic],
    queryFn: () => getThroughput(cluster, topic),
    refetchInterval: 10_000,
  })
  const consumers = useQuery({
    queryKey: ['consumers', cluster, topic],
    queryFn: () => getTopicConsumers(cluster, topic),
    refetchInterval: 10_000,
  })
  const allSamples = throughput.data?.samples ?? []
  const latest = allSamples.at(-1)
  const windowMinutes = 15
  const cutoff = latest ? latest.ts_ms - windowMinutes * 60_000 : null
  const samples = cutoff === null ? allSamples : allSamples.filter((s) => s.ts_ms >= cutoff)
  const current = samples.at(-1)

  return (
    <div className="space-y-4">
      <Panel title="Throughput" error={throughput.error} loading={throughput.isPending} hasData={throughput.data !== undefined}>
        <div className="flex items-end gap-4">
          <div>
            <Sparkline
              points={samples.map((s) => ({ x: s.ts_ms, y: s.msgs_per_sec }))}
              domain={latest ? { min: cutoff as number, max: latest.ts_ms } : undefined}
            />
            <p className="mt-1 text-xs text-zinc-500">last {windowMinutes}m</p>
          </div>
          <div className="text-sm">
            {current
              ? <span className="text-xl font-semibold">{current.msgs_per_sec.toFixed(1)} msg/s</span>
              : <span className="text-zinc-500">—</span>}
            <div className="mt-1"><StalenessChip asOf={throughput.data?.as_of ?? null} refreshing={throughput.isFetching} failed={throughput.isError} /></div>
          </div>
        </div>
      </Panel>
      <Panel title="Consumer groups" error={consumers.error} loading={consumers.isPending} hasData={consumers.data !== undefined}>
        <div className="mb-2"><StalenessChip asOf={consumers.data?.as_of ?? null} refreshing={consumers.isFetching} failed={consumers.isError} /></div>
        {consumers.data && consumers.data.groups.length === 0 && (
          <p className="text-sm text-zinc-500">no consumer groups are reading this topic</p>
        )}
        <div className="space-y-2">
          {consumers.data?.groups.map((g) => (
            <GroupRow key={g.group_id} group={g} />
          ))}
        </div>
      </Panel>
    </div>
  )
}

function GroupRow({ group }: { group: TopicGroupLag }) {
  const [open, setOpen] = useState(false)
  return (
    <details
      className="rounded border border-zinc-100 p-2 dark:border-zinc-800"
      open={open}
      onToggle={(e) => setOpen(e.currentTarget.open)}
    >
      <summary className="flex cursor-pointer items-center gap-3 text-sm">
        <span className="font-mono font-medium">{group.group_id}</span>
        <span className="text-xs text-zinc-500">{group.state}</span>
        <span className="ml-auto">lag <span className="font-semibold">{formatCount(group.total_lag)}</span></span>
      </summary>
      {open && (
        <table className="mt-2 w-full text-left font-mono text-xs">
          <thead className="text-zinc-500">
            <tr><th className="py-1">partition</th><th>committed</th><th>end</th><th>lag</th></tr>
          </thead>
          <tbody>
            {group.partitions.map((p) => (
              <tr key={p.partition} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1">{p.partition}</td>
                <td>{p.committed_offset}</td>
                <td>{p.end_offset}</td>
                <td>{p.lag}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </details>
  )
}

function ConfigTab({ data, error, loading }: { data?: TopicDetail; error: unknown; loading: boolean }) {
  const [showAll, setShowAll] = useState(false)
  const [filter, setFilter] = useState('')
  const configs = data?.configs ?? []
  const overridden = configs.filter((c) => !c.is_default)
  const filteredConfigs = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return q === '' ? configs : configs.filter((c) => c.name.toLowerCase().includes(q))
  }, [configs, filter])
  const cleanupPolicy = configs.find((c) => c.name === 'cleanup.policy')
  const retentionMs = configs.find((c) => c.name === 'retention.ms')
  const retentionBytes = configs.find((c) => c.name === 'retention.bytes')
  const deleteRetentionMs = configs.find((c) => c.name === 'delete.retention.ms')
  const retentionMsHintText = retentionMs ? retentionMsHint(retentionMs.value) : null
  const deleteRetentionMsHintText = deleteRetentionMs ? retentionMsHint(deleteRetentionMs.value) : null
  // retention.ms/retention.bytes only govern segment deletion, so they're
  // inert on a compact-only topic (no "delete" in cleanup.policy) — swap
  // them for delete.retention.ms, the knob that actually matters there.
  // Any other/missing policy value keeps today's four cards.
  const policies = (cleanupPolicy?.value ?? '').split(',').map((p) => p.trim()).filter(Boolean)
  const isCompactOnly = policies.includes('compact') && !policies.includes('delete')

  return (
    <Panel title="Summary" error={error} loading={loading} hasData={data !== undefined}>
      {data && (
        <div className="space-y-4">
          <dl className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="partitions" value={String(data.partitions.length)} />
            <Stat label="cleanup.policy" value={cleanupPolicy?.value ?? '—'} />
            {isCompactOnly
              ? (
                  <Stat
                    label="delete.retention.ms"
                    value={
                      deleteRetentionMs
                        ? `${formatRetentionValue(deleteRetentionMs.value)}${deleteRetentionMsHintText ? ` (${deleteRetentionMsHintText})` : ''}`
                        : '—'
                    }
                  />
                )
              : (
                  <>
                    <Stat
                      label="retention.ms"
                      value={
                        retentionMs
                          ? `${formatRetentionValue(retentionMs.value)}${retentionMsHintText ? ` (${retentionMsHintText})` : ''}`
                          : '—'
                      }
                    />
                    <Stat label="retention.bytes" value={retentionBytes ? formatRetentionValue(retentionBytes.value) : '—'} />
                  </>
                )}
          </dl>
          <div>
            <h3 className="mb-2 text-xs font-medium text-zinc-500">Overridden values</h3>
            {overridden.length === 0
              ? <p className="text-sm text-zinc-500">no overrides — all values are broker defaults</p>
              : <ConfigTable entries={overridden} testPrefix="config" />}
          </div>
          <label className="inline-flex w-fit items-center gap-2 text-sm text-zinc-500">
            <button
              type="button"
              role="switch"
              aria-checked={showAll}
              aria-label="show all configs"
              onClick={() => {
                setShowAll((v) => !v)
                setFilter('')
              }}
              className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${
                showAll ? 'bg-blue-600 dark:bg-blue-500' : 'bg-zinc-300 dark:bg-zinc-700'
              }`}
            >
              <span
                className={`absolute left-0.5 top-0.5 h-4 w-4 transform rounded-full bg-white transition-transform ${
                  showAll ? 'translate-x-4' : 'translate-x-0'
                }`}
              />
            </button>
            show all configs
          </label>
          {showAll && (
            <div>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-medium text-zinc-500">All configs</h3>
                <FilterInput
                  value={filter}
                  onChange={setFilter}
                  placeholder="filter configs…"
                  ariaLabel="filter configs"
                  className="w-56"
                />
              </div>
              {filteredConfigs.length === 0
                ? <p className="text-sm text-zinc-500">no matching configs</p>
                : <ConfigTable entries={filteredConfigs} testPrefix="config-all" />}
            </div>
          )}
        </div>
      )}
    </Panel>
  )
}

function ConfigTable({ entries, testPrefix }: { entries: ConfigEntry[]; testPrefix: string }) {
  return (
    <table className="w-full text-left text-sm">
      <tbody>
        {entries.map((c) => (
          <tr key={c.name} data-testid={`${testPrefix}-${c.name}`} className="border-t border-zinc-100 dark:border-zinc-800">
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
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div data-testid={`stat-${label}`}>
      <dt className="text-xs text-zinc-500">{label}</dt>
      <dd className="text-xl font-semibold">{value}</dd>
    </div>
  )
}

export function TopicDetailPage() {
  const { cluster, topic } = useParams({ from: '/c/$cluster/topics/$topic' })
  return <TopicDetailView cluster={cluster} topic={topic} />
}
