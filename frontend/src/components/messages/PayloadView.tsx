import type { DecodedPayload } from '../../api/types'
import { CopyButton } from '../CopyButton'
import { EncodingBadge } from './EncodingBadge'
import { JsonView } from './JsonView'

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

export function PayloadView({ payload, label }: { payload: DecodedPayload | null; label: string }) {
  const heading = (
    <div className="mb-1 flex items-center gap-2 text-zinc-500">
      <span>{label}</span>
      {payload && <EncodingBadge encoding={payload.encoding} />}
      {payload && payload.schema_id !== null && <span className="text-xs">schema id {payload.schema_id}</span>}
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
  let body = <pre className="overflow-x-auto whitespace-pre-wrap break-all">{payload.text}</pre>
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
      {body}
    </div>
  )
}
