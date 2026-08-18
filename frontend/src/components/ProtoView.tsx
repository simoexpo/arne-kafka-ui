import { Fragment } from 'react'

// Lightweight protobuf syntax coloring — a single tokenizing pass, no
// parser and no external highlighter (the app stays self-contained). The
// palette mirrors JsonView's: strings emerald, numbers sky, structure
// muted; keywords take the amber the protobuf badge already wears.
const TOKEN = new RegExp(
  [
    String.raw`(\/\/[^\n]*|\/\*[\s\S]*?\*\/)`, // 1: comment
    String.raw`("(?:[^"\\]|\\.)*")`, // 2: string
    String.raw`\b(syntax|package|import|option|message|enum|service|rpc|returns|stream|repeated|optional|required|oneof|map|reserved|extend|weak|public)\b`, // 3: keyword
    String.raw`\b(double|float|int32|int64|uint32|uint64|sint32|sint64|fixed32|fixed64|sfixed32|sfixed64|bool|string|bytes)\b`, // 4: scalar type
    String.raw`\b(\d+)\b`, // 5: number (field tags, enum values)
  ].join('|'),
  'g',
)

const CLASSES = [
  'text-zinc-400 italic', // comment
  'text-emerald-700 dark:text-emerald-400', // string
  'text-amber-700 dark:text-amber-300', // keyword
  'text-sky-700 dark:text-sky-400', // scalar type
  'text-sky-700 dark:text-sky-400', // number
]

export function ProtoView({ source }: { source: string }) {
  const parts: React.ReactNode[] = []
  let last = 0
  for (const match of source.matchAll(TOKEN)) {
    if (match.index > last) parts.push(source.slice(last, match.index))
    const group = match.slice(1).findIndex((g) => g !== undefined)
    parts.push(
      <span key={match.index} className={CLASSES[group]}>
        {match[0]}
      </span>,
    )
    last = match.index + match[0].length
  }
  if (last < source.length) parts.push(source.slice(last))
  return (
    <pre className="whitespace-pre">
      {parts.map((p, i) => (
        <Fragment key={i}>{p}</Fragment>
      ))}
    </pre>
  )
}
