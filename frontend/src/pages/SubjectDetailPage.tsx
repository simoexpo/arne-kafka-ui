import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { Link, useNavigate, useParams, useSearch } from '@tanstack/react-router'
import { checkCompatibility, getCompatibilityLevel, getSubjectDetail, getSubjectStrategy } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { EncodingBadge } from '../components/messages/EncodingBadge'
import { JsonView } from '../components/messages/JsonView'
import { Panel } from '../components/Panel'
import { ProtoView } from '../components/ProtoView'
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
    // The ONLY scrollable region of the Definition tab (owner ruling
    // 2026-08-18): the page itself is pinned; long/wide schemas scroll here.
    <div data-testid="schema-body" className="min-h-0 flex-1 overflow-auto whitespace-nowrap font-mono text-sm">
      {isJson ? <JsonView value={parsed} /> : <ProtoView source={schema} />}
    </div>
  )
}

// The registry's type IS an encoding the message view already badges —
// same component, same colors, one visual language (consistency review
// 2026-08-18). An unexpected type falls back to a neutral bordered chip.
const SCHEMA_TYPE_ENCODING = { AVRO: 'avro', PROTOBUF: 'protobuf', JSON: 'json' } as const

function SchemaTypeBadge({ schemaType }: { schemaType: string }) {
  const encoding = SCHEMA_TYPE_ENCODING[schemaType as keyof typeof SCHEMA_TYPE_ENCODING]
  if (encoding !== undefined) return <EncodingBadge encoding={encoding} />
  return (
    <span className="rounded border border-zinc-300 px-1.5 py-0.5 font-mono text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
      {schemaType}
    </span>
  )
}

// The Confluent class names the ecosystem knows these strategies by.
const STRATEGY_LABEL = {
  topic_name: 'TopicNameStrategy',
  topic_record_name: 'TopicRecordNameStrategy',
  record_name: 'RecordNameStrategy',
} as const

function CompatibilityTab({ cluster, subject, schemaType }: { cluster: string; subject: string; schemaType: string }) {
  const level = useQuery({
    queryKey: ['compat-level', cluster, subject],
    queryFn: ({ signal }) => getCompatibilityLevel(cluster, subject, signal),
  })
  const [candidate, setCandidate] = useState('')
  // No type selector: a subject's versions all share one schema type (the
  // registry rejects mixed types), so the candidate is always tested as
  // the subject's own type.
  const check = useMutation({
    mutationFn: () => checkCompatibility(cluster, subject, candidate, schemaType),
  })
  return (
    <Panel
      className="min-h-0 flex-1 overflow-y-auto"
      title="Test compatibility"
      error={level.error}
      loading={level.isPending}
      hasData={level.data !== undefined}
    >
      <div className="space-y-3">
        <div className="flex items-center gap-1.5 text-sm text-zinc-500">
          compatibility level
          <span className="rounded border border-zinc-300 px-1.5 py-0.5 font-mono text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
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
          className="w-full rounded border border-zinc-300 bg-transparent px-3 py-1.5 font-mono text-sm outline-none focus:border-zinc-500 dark:border-zinc-700"
        />
        <div className="flex items-center gap-3">
          <button
            type="button"
            disabled={candidate.trim() === '' || check.isPending}
            onClick={() => check.mutate()}
            className="rounded border border-zinc-300 px-2 py-0.5 text-sm text-zinc-600 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
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

export function SubjectDetailView({ cluster, subject }: { cluster: string; subject: string }) {
  const [tab, setTab] = useState<Tab>('Definition')
  // The selected version lives in the URL (`?version=N`, absent = latest):
  // schema-id links land on an exact version, version views are shareable,
  // and back/forward walks the version history. The selector navigates.
  const search = useSearch({ strict: false }) as { version?: number }
  const version = typeof search.version === 'number' ? search.version : undefined
  const navigate = useNavigate()
  const detail = useQuery({
    queryKey: ['subject', cluster, subject, version ?? 'latest'],
    queryFn: ({ signal }) => getSubjectDetail(cluster, subject, version, signal),
  })
  // Per-subject, not per-version: the strategy is resolved from the
  // subject name plus the latest schema's record name.
  const strategy = useQuery({
    queryKey: ['subject-strategy', cluster, subject],
    queryFn: ({ signal }) => getSubjectStrategy(cluster, subject, signal),
  })
  return (
    <div className="flex h-full flex-col gap-4 overflow-hidden">
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
      <Panel
        className="flex min-h-0 flex-1 flex-col overflow-hidden"
        title="Schema"
        error={detail.error}
        loading={detail.isPending}
        hasData={detail.data !== undefined}
      >
        {detail.data && (
          <div className="flex min-h-0 flex-1 flex-col gap-4">
            {/* Mirrors the topic Config tab's grammar (owner ruling
                2026-08-18): a stat grid, a divider, the version/id row,
                then the definition itself. */}
            <dl className="grid grid-cols-2 gap-4 sm:grid-cols-4">
              <div>
                <dt className="text-xs text-zinc-500">format</dt>
                <dd className="mt-1"><SchemaTypeBadge schemaType={detail.data.schema_type} /></dd>
              </div>
              <div>
                <dt className="text-xs text-zinc-500">strategy</dt>
                <dd className="mt-1 text-sm">
                  {strategy.data === undefined ? (
                    <span className="text-zinc-400">…</span>
                  ) : strategy.data.strategy === null ? (
                    <span className="text-zinc-400">not derivable</span>
                  ) : (
                    STRATEGY_LABEL[strategy.data.strategy]
                  )}
                </dd>
              </div>
              {strategy.data?.topic != null && (
                <div>
                  <dt className="text-xs text-zinc-500">topic</dt>
                  <dd className="mt-1 text-sm">
                    <Link
                      to="/c/$cluster/topics/$topic"
                      params={{ cluster, topic: strategy.data.topic }}
                      className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                    >
                      {strategy.data.topic}
                    </Link>
                    {strategy.data.role !== null && <span className="text-zinc-500"> ({strategy.data.role})</span>}
                  </dd>
                </div>
              )}
            </dl>
            <div className="flex items-center gap-3 border-t border-zinc-100 pt-3 text-sm dark:border-zinc-800">
              <label className="flex items-center gap-1.5 text-zinc-500">
                version
                <select
                  aria-label="version"
                  value={detail.data.version}
                  onChange={(e) =>
                    navigate({
                      to: '/c/$cluster/schemas/$subject',
                      params: { cluster, subject },
                      search: { version: Number(e.target.value) },
                    })
                  }
                  className="rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-950"
                >
                  {detail.data.versions.map((v) => (
                    <option key={v} value={v}>{v}</option>
                  ))}
                </select>
              </label>
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
