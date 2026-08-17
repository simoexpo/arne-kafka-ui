import { useQuery } from '@tanstack/react-query'
import { Link } from '@tanstack/react-router'
import { getSubjectOfId } from '../../api/client'
import type { DecodedPayload } from '../../api/types'
import { CopyButton } from '../CopyButton'
import { EncodingBadge } from './EncodingBadge'
import { JsonView } from './JsonView'

// Resolves the id to its subject+version on expansion (one request per
// DISTINCT id, cached for the session) and links straight to the canonical
// URL — no redirect hop. Unresolvable (registry down, id gone) degrades to
// plain text rather than linking to an error page.
function SchemaIdBadge({ cluster, id }: { cluster: string; id: number }) {
  const resolved = useQuery({
    queryKey: ['schema-id', cluster, id],
    queryFn: ({ signal }) => getSubjectOfId(cluster, id, signal),
    staleTime: Infinity,
  })
  if (!resolved.data) return <span className="text-xs">schema id {id}</span>
  return (
    <Link
      to="/c/$cluster/schemas/$subject"
      params={{ cluster, subject: resolved.data.subject }}
      search={{ version: resolved.data.version }}
      className="text-xs text-blue-600 hover:underline dark:text-blue-400"
    >
      schema id {id}
    </Link>
  )
}

function copyTextFor(payload: DecodedPayload): string {
  if (payload.encoding === 'json' || payload.encoding === 'avro' || payload.encoding === 'protobuf') {
    try {
      return JSON.stringify(JSON.parse(payload.text), null, 2)
    } catch {
      return payload.text
    }
  }
  return payload.text
}

export function PayloadView({
  payload,
  label,
  // With a cluster in hand, the schema id links into the schemas section by
  // ID (resolved to its subject server-side — exact under any naming
  // strategy). Optional: callers without router context render plain text.
  cluster,
}: {
  payload: DecodedPayload | null
  label: string
  cluster?: string
}) {
  const heading = (
    <div className="mb-1 flex items-center gap-2 text-zinc-500">
      <span>{label}</span>
      {payload && <EncodingBadge encoding={payload.encoding} />}
      {payload && payload.schema_id !== null && (
        cluster !== undefined ? (
          <SchemaIdBadge cluster={cluster} id={payload.schema_id} />
        ) : (
          <span className="text-xs">schema id {payload.schema_id}</span>
        )
      )}
      {payload && <CopyButton text={copyTextFor(payload)} label={label} />}
    </div>
  )
  if (payload === null) {
    return (
      <div>
        {heading}
        <span className="text-zinc-400">∅ null</span>
      </div>
    )
  }
  if (payload.encoding === 'decode_error') {
    return (
      <div className="space-y-1 font-mono text-sm">
        {heading}
        <p className="text-red-700 dark:text-red-400">{payload.error}</p>
        <pre className="max-h-40 overflow-auto rounded bg-zinc-100 p-2 text-xs text-zinc-500 dark:bg-zinc-950">{payload.text}</pre>
      </div>
    )
  }
  let body = <pre className="whitespace-pre">{payload.text}</pre>
  if (payload.encoding === 'json' || payload.encoding === 'avro' || payload.encoding === 'protobuf') {
    try {
      body = <JsonView value={JSON.parse(payload.text)} />
    } catch {
      // fall through to <pre>
    }
  }
  return (
    <div className="font-mono text-sm">
      {heading}
      {/* Future-proofing for production payloads of any size: the body is
          height-capped (~20 mono lines) and scrolls on either axis ONLY
          when it overflows. nowrap keeps long fields and deep nesting on
          one line each, so they scroll sideways instead of wrapping. */}
      <div data-testid="payload-scroll" className="max-h-80 overflow-auto whitespace-nowrap">
        {body}
      </div>
    </div>
  )
}
