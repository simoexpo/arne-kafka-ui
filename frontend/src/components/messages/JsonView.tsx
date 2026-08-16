import { useState } from 'react'

function classForPrimitive(value: unknown): string {
  return typeof value === 'string' ? 'text-emerald-700 dark:text-emerald-400'
    : typeof value === 'number' ? 'text-sky-700 dark:text-sky-400'
    : 'text-amber-700 dark:text-amber-400'
}

function Key({ name }: { name: string }) {
  return (
    <>
      <span className="text-zinc-400">"</span>
      <span className="text-sky-800 dark:text-sky-300">{name}</span>
      <span className="text-zinc-400">"</span>
      <span className="text-zinc-400">: </span>
    </>
  )
}

function Comma({ isLast }: { isLast: boolean }) {
  return isLast ? null : <span className="text-zinc-400">,</span>
}

function entriesOf(value: object): [string, unknown][] {
  return Array.isArray(value)
    ? value.map((v, i) => [String(i), v])
    : Object.entries(value as Record<string, unknown>)
}

export function JsonView({ value, depth = 0 }: { value: unknown; depth?: number }) {
  return <JsonNode value={value} depth={depth} keyName={null} isLast />
}

function JsonNode({
  value,
  depth,
  keyName,
  isLast,
}: {
  value: unknown
  depth: number
  keyName: string | null
  isLast: boolean
}) {
  if (value === null || typeof value !== 'object') {
    return (
      <span>
        {keyName !== null && <Key name={keyName} />}
        <span className={classForPrimitive(value)}>{JSON.stringify(value)}</span>
        <Comma isLast={isLast} />
      </span>
    )
  }

  const isArray = Array.isArray(value)
  const openBracket = isArray ? '[' : '{'
  const closeBracket = isArray ? ']' : '}'
  const entries = entriesOf(value)

  if (entries.length === 0) {
    return (
      <span>
        {keyName !== null && <Key name={keyName} />}
        <span className="text-zinc-400">{openBracket}{closeBracket}</span>
        <Comma isLast={isLast} />
      </span>
    )
  }

  return (
    <CollapsibleNode
      depth={depth}
      keyName={keyName}
      isLast={isLast}
      isArray={isArray}
      entries={entries}
      openBracket={openBracket}
      closeBracket={closeBracket}
    />
  )
}

function CollapsibleNode({
  depth,
  keyName,
  isLast,
  isArray,
  entries,
  openBracket,
  closeBracket,
}: {
  depth: number
  keyName: string | null
  isLast: boolean
  isArray: boolean
  entries: [string, unknown][]
  openBracket: string
  closeBracket: string
}) {
  const [isOpen, setIsOpen] = useState(depth < 2)
  return (
    <details open={isOpen} onToggle={(e) => setIsOpen(e.currentTarget.open)}>
      <summary className="cursor-pointer select-none text-zinc-500">
        {keyName !== null && <Key name={keyName} />}
        <span className="text-zinc-400">{openBracket}</span>
        {!isOpen && (
          <>
            <span className="text-zinc-400 px-1">…</span>
            <span className="text-zinc-400">{closeBracket}</span>
            <Comma isLast={isLast} />
          </>
        )}
      </summary>
      {isOpen && (
        <>
          <div className="border-l border-zinc-200 pl-3 dark:border-zinc-800">
            {entries.map(([k, v], i) => (
              <div key={isArray ? i : k}>
                <JsonNode
                  value={v}
                  depth={depth + 1}
                  keyName={isArray ? null : k}
                  isLast={i === entries.length - 1}
                />
              </div>
            ))}
          </div>
          <div className="text-zinc-400">
            {closeBracket}
            <Comma isLast={isLast} />
          </div>
        </>
      )}
    </details>
  )
}
