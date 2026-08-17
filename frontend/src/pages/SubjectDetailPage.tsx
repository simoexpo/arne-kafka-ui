import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getSubjectDetail } from '../api/client'
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

export function SubjectDetailView({ cluster, subject }: { cluster: string; subject: string }) {
  // `undefined` = the registry's latest; the served version lands in
  // `detail.data.version`, which is what the selector displays.
  const [version, setVersion] = useState<number | undefined>(undefined)
  const detail = useQuery({
    queryKey: ['subject', cluster, subject, version ?? 'latest'],
    queryFn: ({ signal }) => getSubjectDetail(cluster, subject, version, signal),
  })
  return (
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="flex items-center gap-1.5 font-mono text-lg font-semibold">
          {subject}
          <CopyButton text={subject} label={subject} />
        </h1>
        <StalenessChip asOf={detail.data?.as_of ?? null} refreshing={detail.isFetching} failed={detail.isError} />
      </div>
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
    </div>
  )
}

export function SubjectDetailPage() {
  const { cluster, subject } = useParams({ from: '/c/$cluster/schema/$subject' })
  return <SubjectDetailView cluster={cluster} subject={subject} />
}
