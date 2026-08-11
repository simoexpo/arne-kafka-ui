export function JsonView({ value, depth = 0 }: { value: unknown; depth?: number }) {
  if (value === null || typeof value !== 'object') {
    const cls =
      typeof value === 'string' ? 'text-emerald-700 dark:text-emerald-400'
      : typeof value === 'number' ? 'text-sky-700 dark:text-sky-400'
      : 'text-amber-700 dark:text-amber-400'
    return <span className={cls}>{JSON.stringify(value)}</span>
  }
  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v] as const)
    : Object.entries(value as Record<string, unknown>)
  return (
    <details open={depth < 2}>
      <summary className="cursor-pointer select-none text-zinc-500">
        {Array.isArray(value) ? `[${entries.length}]` : `{${entries.length}}`}
      </summary>
      <div className="border-l border-zinc-200 pl-3 dark:border-zinc-800">
        {entries.map(([k, v]) => (
          <div key={k}>
            <span className="text-sky-800 dark:text-sky-300">{k}</span>
            <span className="text-zinc-400">: </span>
            <JsonView value={v} depth={depth + 1} />
          </div>
        ))}
      </div>
    </details>
  )
}
