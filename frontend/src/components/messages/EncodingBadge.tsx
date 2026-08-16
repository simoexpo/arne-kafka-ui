import type { Encoding } from '../../api/types'

const STYLES: Record<Encoding, string> = {
  json: 'text-sky-700 border-sky-300 dark:text-sky-300 dark:border-sky-800',
  utf8: 'text-zinc-600 border-zinc-300 dark:text-zinc-400 dark:border-zinc-700',
  avro: 'text-violet-700 border-violet-300 dark:text-violet-300 dark:border-violet-800',
  protobuf: 'text-amber-700 border-amber-300 dark:text-amber-300 dark:border-amber-800',
  bytes: 'text-zinc-500 border-dashed border-zinc-300 dark:border-zinc-700',
  decode_error: 'text-red-700 border-red-300 bg-red-50 dark:text-red-300 dark:border-red-800 dark:bg-red-950',
}

export function EncodingBadge({ encoding }: { encoding: Encoding }) {
  return <span className={`rounded border px-1.5 py-0.5 font-mono text-xs ${STYLES[encoding]}`}>{encoding}</span>
}
