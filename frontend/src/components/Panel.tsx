import type { ReactNode } from 'react'
import { ApiError } from '../api/client'

export function Panel({ title, error, loading, hasData, children }: {
  title?: string
  error?: unknown
  loading?: boolean
  hasData?: boolean
  children?: ReactNode
}) {
  const failed = error != null
  return (
    <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
      {title && <h2 className="mb-3 text-sm font-medium text-zinc-500 dark:text-zinc-400">{title}</h2>}
      {failed && !hasData && <PanelError error={error} />}
      {failed && hasData && <PanelErrorBanner error={error} />}
      {failed && hasData && children}
      {!failed && (loading && !hasData ? <Skeleton /> : children)}
    </section>
  )
}

function PanelError({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error)
  const code = error instanceof ApiError ? error.code : 'error'
  const retriable = error instanceof ApiError && error.retriable
  return (
    <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm dark:border-red-900 dark:bg-red-950">
      <span className="font-mono text-red-700 dark:text-red-400">{code}</span>
      <p className="mt-1 text-red-800 dark:text-red-300">{message}</p>
      {retriable && <p className="mt-1 text-xs text-red-600 dark:text-red-500">retriable — data may recover on its own</p>}
    </div>
  )
}

// Background refresh failed but we already have data to show: a slim
// single-line banner above the (still-rendered) content, not a full-page
// takeover — the stale-but-visible pattern.
function PanelErrorBanner({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error)
  const code = error instanceof ApiError ? error.code : 'error'
  return (
    <div
      data-testid="panel-error-banner"
      className="mb-3 border-l-2 border-red-500 bg-red-50 px-2 py-1 text-xs text-red-800 dark:border-red-400 dark:bg-red-950 dark:text-red-300"
    >
      <span className="font-mono text-red-700 dark:text-red-400">{code}</span> {message}
    </div>
  )
}

function Skeleton() {
  return (
    <div className="space-y-2">
      {[0, 1, 2].map((i) => (
        <div key={i} data-testid="skeleton" className="h-4 animate-pulse rounded bg-zinc-200 dark:bg-zinc-800" />
      ))}
    </div>
  )
}
