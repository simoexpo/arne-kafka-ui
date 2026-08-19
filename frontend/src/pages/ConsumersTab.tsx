import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getThroughput, getTopicConsumers } from '../api/client'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { Sparkline } from '../components/Sparkline'
import { formatCount } from '../lib/format'
import type { TopicGroupLag } from '../api/types'

const NO_COMMIT_TITLE = "this group holds an assignment but hasn't committed an offset yet, so its position is unknown"

export function ConsumersTab({ cluster, topic }: { cluster: string; topic: string }) {
  const throughput = useQuery({
    queryKey: ['throughput', cluster, topic],
    queryFn: ({ signal }) => getThroughput(cluster, topic, signal),
    refetchInterval: 10_000,
  })
  const consumers = useQuery({
    queryKey: ['consumers', cluster, topic],
    queryFn: ({ signal }) => getTopicConsumers(cluster, topic, signal),
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
      <Panel
        title="Throughput"
        action={<StalenessChip asOf={throughput.data?.as_of ?? null} refreshing={throughput.isFetching} failed={throughput.isError} />}
        error={throughput.error}
        loading={throughput.isPending}
        hasData={throughput.data !== undefined}
      >
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
          </div>
        </div>
      </Panel>
      <Panel
        title="Consumer groups"
        action={<StalenessChip asOf={consumers.data?.as_of ?? null} refreshing={consumers.isFetching} failed={consumers.isError} />}
        error={consumers.error}
        loading={consumers.isPending}
        hasData={consumers.data !== undefined}
      >
        {consumers.data && consumers.data.groups.length === 0 && (
          <p className="text-sm text-zinc-500">no consumer groups are reading this topic</p>
        )}
        <div className="space-y-2">
          {consumers.data?.groups.map((g) => (
            <GroupRow key={g.group_id} group={g} />
          ))}
        </div>
        {consumers.data && consumers.data.unchecked.length > 0 && (
          <p data-testid="unchecked-groups" className="mt-2 text-xs text-amber-700 dark:text-amber-500">
            {consumers.data.unchecked.length}{' '}
            {consumers.data.unchecked.length === 1 ? "group couldn't" : "groups couldn't"} be checked, so
            they may or may not read this topic:{' '}
            {consumers.data.unchecked.map((u) => `${u.group_id} (${u.error})`).join(', ')}
          </p>
        )}
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
        <span className="ml-auto">
          lag{' '}
          {group.total_lag === null ? (
            <span
              data-testid="group-lag"
              className="cursor-help text-zinc-400"
              title={group.error ? `Kafka couldn't read this group's offsets — ${group.error}` : NO_COMMIT_TITLE}
            >
              —
            </span>
          ) : (
            <span data-testid="group-lag" className={group.total_lag > 0 ? 'font-semibold' : 'text-zinc-400'}>
              {formatCount(group.total_lag)}
            </span>
          )}
        </span>
      </summary>
      {open && (
        <table className="mt-2 w-full text-left font-mono text-sm">
          <thead className="text-zinc-500">
            <tr><th className="py-1">partition</th><th>committed</th><th>end</th><th>lag</th></tr>
          </thead>
          <tbody>
            {group.partitions.map((p) => (
              <tr key={p.partition} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1">{p.partition}</td>
                <td>{p.committed_offset}</td>
                <td>{p.end_offset}</td>
                <td className={p.lag > 0 ? 'font-semibold' : 'text-zinc-400'}>{p.lag}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </details>
  )
}
