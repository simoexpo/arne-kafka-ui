import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { Link } from '@tanstack/react-router'
import { getSubjectDetail, getSubjectUsage } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { JsonView } from '../components/messages/JsonView'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

// Same honesty pattern as message payloads (PayloadView): a JSON-able
// schema renders structurally, fully expanded; anything else (protobuf
// text) verbatim without soft-wrap — long lines scroll sideways.
function SchemaBody({ schema }: { schema: string }) {
  let parsed: unknown
  let isJson = true
  try {
    parsed = JSON.parse(schema)
  } catch {
    isJson = false
  }
  return (
    <div data-testid="schema-body" className="overflow-auto whitespace-nowrap font-mono text-sm">
      {isJson ? <JsonView value={parsed} /> : <pre className="whitespace-pre">{schema}</pre>}
    </div>
  )
}

// Compatibility stays a placeholder deliberately (owner request
// 2026-08-17): the structure ships first, mirroring the topic detail
// page's tab pattern.
const TABS = ['Schema', 'Usage', 'Compatibility'] as const
type Tab = (typeof TABS)[number]

const STRATEGY_LABEL = { topic_name: 'topic name', topic_record_name: 'topic-record name' } as const

// The registry doesn't record which topics use a schema — usage is
// inferred server-side from the subject NAME per Confluent's naming
// strategies, and only claimed for topics that exist.
function UsageTab({ cluster, subject }: { cluster: string; subject: string }) {
  const usage = useQuery({
    queryKey: ['subject-usage', cluster, subject],
    queryFn: ({ signal }) => getSubjectUsage(cluster, subject, signal),
  })
  return (
    <Panel title="usage" error={usage.error} loading={usage.isPending} hasData={usage.data !== undefined}>
      {usage.data?.usages.length === 0 ? (
        <p className="text-sm text-zinc-500">
          No topic is derivable from this subject's name. The registry doesn't record which topics
          use a schema — Arne infers it from topic-based subject naming, which this name doesn't follow
          (record-name strategy).
        </p>
      ) : (
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">topic</th><th>strategy</th></tr>
          </thead>
          <tbody>
            {usage.data?.usages.map((u) => (
              <tr key={`${u.topic}-${u.role ?? ''}`} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1.5">
                  <Link
                    to="/c/$cluster/topics/$topic"
                    params={{ cluster, topic: u.topic }}
                    className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                  >
                    {u.topic}
                  </Link>
                </td>
                <td className="text-zinc-500">
                  {STRATEGY_LABEL[u.strategy]}
                  {u.role !== null && ` (${u.role})`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}

export function SubjectDetailView({ cluster, subject }: { cluster: string; subject: string }) {
  const [tab, setTab] = useState<Tab>('Schema')
  // `undefined` = the registry's latest; the served version lands in
  // `detail.data.version`, which is what the selector displays.
  const [version, setVersion] = useState<number | undefined>(undefined)
  const detail = useQuery({
    queryKey: ['subject', cluster, subject, version ?? 'latest'],
    queryFn: ({ signal }) => getSubjectDetail(cluster, subject, version, signal),
  })
  return (
    <div className="flex h-full flex-col gap-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="flex items-center gap-1.5 font-mono text-lg font-semibold">
          {subject}
          <CopyButton text={subject} label={subject} />
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
      {tab === 'Compatibility' && <p className="text-sm text-zinc-500">nothing here yet</p>}
      {tab === 'Usage' && <UsageTab cluster={cluster} subject={subject} />}
      {tab === 'Schema' && (
      <Panel title="schema" error={detail.error} loading={detail.isPending} hasData={detail.data !== undefined}>
        {detail.data && (
          <div className="space-y-3">
            <div className="flex items-center gap-3 text-sm">
              <label className="flex items-center gap-1.5 text-zinc-500">
                version
                <select
                  aria-label="version"
                  value={detail.data.version}
                  onChange={(e) => setVersion(Number(e.target.value))}
                  className="rounded border border-zinc-300 bg-transparent px-1.5 py-0.5 dark:border-zinc-700"
                >
                  {detail.data.versions.map((v) => (
                    <option key={v} value={v}>{v}</option>
                  ))}
                </select>
              </label>
              <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs font-medium text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
                {detail.data.schema_type}
              </span>
              <span className="text-zinc-500">id {detail.data.id}</span>
              <CopyButton text={detail.data.schema} label="schema" />
            </div>
            <SchemaBody schema={detail.data.schema} />
          </div>
        )}
      </Panel>
      )}
    </div>
  )
}

export function SubjectDetailPage() {
  const { cluster, subject } = useParams({ from: '/c/$cluster/schemas/$subject' })
  return <SubjectDetailView cluster={cluster} subject={subject} />
}
