import { useMemo, useState } from 'react'
import { Stat } from '../components/Stat'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { Switch } from '../components/Switch'
import { formatRetentionValue, retentionMsHint } from '../lib/format'
import type { ConfigEntry, TopicDetail } from '../api/types'

// A stable reference (not a fresh `[]` literal on every render) so
// `filteredConfigs`'s `useMemo` below actually memoizes while `data` is
// undefined (loading/error) — a new array identity every render otherwise
// defeats the memo's own dependency check.
const EMPTY_CONFIGS: ConfigEntry[] = []

export function ConfigTab({ data, error, loading }: { data?: TopicDetail; error: unknown; loading: boolean }) {
  const [showAll, setShowAll] = useState(false)
  const [filter, setFilter] = useState('')
  const configs = data?.configs ?? EMPTY_CONFIGS
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
            <h3 className="mb-2 text-sm font-medium text-zinc-500">Overridden values</h3>
            {overridden.length === 0
              ? <p className="text-sm text-zinc-500">no overrides — all values are broker defaults</p>
              : <ConfigTable entries={overridden} testPrefix="config" />}
          </div>
          <Switch
            checked={showAll}
            label="show all configs"
            onChange={() => {
              setShowAll((v) => !v)
              setFilter('')
            }}
          />
          {showAll && (
            <div>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-sm font-medium text-zinc-500">All configs</h3>
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
