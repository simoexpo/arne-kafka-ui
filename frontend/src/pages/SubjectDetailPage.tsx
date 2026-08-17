import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { checkCompatibility, getCompatibilityLevel, getSubjectDetail, getSubjectStrategy } from '../api/client'
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

const STRATEGY_LABEL = {
  topic_name: 'topic name',
  topic_record_name: 'topic-record name',
  record_name: 'record name',
} as const

// The registry doesn't record topic associations — the strategy is
// resolved server-side from evidence (existing topics, the schema's own
// record name) and rendered honestly as "not derivable" when unproven.
function StrategySection({ cluster, subject }: { cluster: string; subject: string }) {
  const strategy = useQuery({
    queryKey: ['subject-strategy', cluster, subject],
    queryFn: ({ signal }) => getSubjectStrategy(cluster, subject, signal),
  })
  if (!strategy.data) return null
  const s = strategy.data
  return (
    <div className="flex items-center gap-4 border-t border-zinc-100 pt-3 text-sm dark:border-zinc-800">
      <span className="flex items-center gap-1.5 text-zinc-500">
        topic
        {s.topic === null ? (
          <span className="text-zinc-400">—</span>
        ) : (
          <Link
            to="/c/$cluster/topics/$topic"
            params={{ cluster, topic: s.topic }}
            className="font-mono text-blue-600 hover:underline dark:text-blue-400"
          >
            {s.topic}
          </Link>
        )}
      </span>
      <span className="flex items-center gap-1.5 text-zinc-500">
        strategy
        {s.strategy === null ? (
          <span className="text-zinc-400">not derivable</span>
        ) : (
          <span className="text-zinc-700 dark:text-zinc-300">
            {STRATEGY_LABEL[s.strategy]}
            {s.role !== null && ` (${s.role})`}
          </span>
        )}
      </span>
    </div>
  )
}

function CompatibilityTab({ cluster, subject, schemaType }: { cluster: string; subject: string; schemaType: string }) {
  const level = useQuery({
    queryKey: ['compat-level', cluster, subject],
    queryFn: ({ signal }) => getCompatibilityLevel(cluster, subject, signal),
  })
  const [candidate, setCandidate] = useState('')
  const [candidateType, setCandidateType] = useState(schemaType)
  const check = useMutation({
    mutationFn: () => checkCompatibility(cluster, subject, candidate, candidateType),
  })
  return (
    <Panel title="compatibility" error={level.error} loading={level.isPending} hasData={level.data !== undefined}>
      <div className="space-y-3">
        <div className="flex items-center gap-1.5 text-sm text-zinc-500">
          effective level
          <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs font-medium text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
            {level.data?.level}
          </span>
        </div>
        <textarea
          aria-label="candidate schema"
          value={candidate}
          onChange={(e) => {
            setCandidate(e.target.value)
            check.reset()
          }}
          placeholder="paste a candidate schema…"
          rows={10}
          className="w-full rounded border border-zinc-300 bg-transparent p-2 font-mono text-sm outline-none focus:border-zinc-500 dark:border-zinc-700"
        />
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-1.5 text-sm text-zinc-500">
            type
            <select
              aria-label="candidate schema type"
              value={candidateType}
              onChange={(e) => setCandidateType(e.target.value)}
              className="rounded border border-zinc-300 bg-transparent px-1.5 py-0.5 dark:border-zinc-700"
            >
              {['AVRO', 'PROTOBUF', 'JSON'].map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            disabled={candidate.trim() === '' || check.isPending}
            onClick={() => check.mutate()}
            className="rounded border border-zinc-300 px-3 py-1 text-sm hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
          >
            check compatibility
          </button>
        </div>
        {check.isError && (
          <p className="text-sm text-red-700 dark:text-red-400">{(check.error as Error).message}</p>
        )}
        {check.data && (check.data.is_compatible ? (
          <p className="text-sm font-medium text-emerald-700 dark:text-emerald-400">
            compatible with the latest version under {level.data?.level}
          </p>
        ) : (
          <div className="space-y-1 text-sm">
            <p className="font-medium text-red-700 dark:text-red-400">
              not compatible with the latest version under {level.data?.level}
            </p>
            {check.data.messages.map((m, i) => (
              <p key={i} className="font-mono text-xs text-red-600 dark:text-red-400">{m}</p>
            ))}
          </div>
        ))}
      </div>
    </Panel>
  )
}

const TABS = ['Definition', 'Compatibility'] as const
type Tab = (typeof TABS)[number]

export function SubjectDetailView({
  cluster,
  subject,
  // A schema-id link lands on the exact version that id belongs to, not
  // the subject's latest (owner ruling 2026-08-18).
  initialVersion,
}: {
  cluster: string
  subject: string
  initialVersion?: number
}) {
  const [tab, setTab] = useState<Tab>('Definition')
  // `undefined` = the registry's latest; the served version lands in
  // `detail.data.version`, which is what the selector displays.
  const [version, setVersion] = useState<number | undefined>(initialVersion)
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
      {tab === 'Compatibility' && (
        <CompatibilityTab cluster={cluster} subject={subject} schemaType={detail.data?.schema_type ?? 'AVRO'} />
      )}
      {tab === 'Definition' && (
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
            <StrategySection cluster={cluster} subject={subject} />
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
