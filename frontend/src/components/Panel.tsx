import type { ReactNode } from 'react'
import { ApiError } from '../api/client'
import { describeError } from '../api/errors'

export function Panel({ title, action, error, loading, hasData, className, children }: {
  title?: string
  // Right-aligned in the header row, opposite the title — where a section's
  // own freshness chip belongs (owner ruling 2026-08-19).
  action?: ReactNode
  error?: unknown
  loading?: boolean
  hasData?: boolean
  // Extra classes merged onto the section's own — used by callers that need
  // this Panel to participate in a flex height chain (e.g. the Messages tab,
  // where the panel must fill its flex-1 slot so MessageList's own scroller,
  // not the page, is what scrolls). Purely additive: omitted, the section
  // keeps its plain block sizing exactly as before.
  className?: string
  children?: ReactNode
}) {
  const failed = error != null
  return (
    <section className={`rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-950 ${className ?? ''}`}>
      {(title || action) && (
        <div data-testid="panel-header" className="mb-3 flex items-center justify-between gap-3">
          <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">{title}</h2>
          {action}
        </div>
      )}
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
  const { kind, headline, hint } = describeError(error)
  // Connection-lost is diagnostics, not the message the user should read
  // first — the code/message line is subdued so the headline + hint carry
  // the story instead.
  const subdued = kind === 'backend'
  return (
    <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm dark:border-red-900 dark:bg-red-950">
      {headline && <p className="mb-1 font-semibold text-red-900 dark:text-red-200">{headline}</p>}
      {hint && <p className="mb-2 text-xs text-red-700/80 dark:text-red-400/80">{hint}</p>}
      <div data-testid="panel-error-detail" className={subdued ? 'text-zinc-500 dark:text-zinc-500' : undefined}>
        <span className={`font-mono ${subdued ? 'text-zinc-500 dark:text-zinc-500' : 'text-red-700 dark:text-red-400'}`}>
          {code}
        </span>
        <p className={`mt-1 ${subdued ? 'text-zinc-500 dark:text-zinc-500' : 'text-red-800 dark:text-red-300'}`}>{message}</p>
      </div>
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
  const { kind, headline, hint } = describeError(error)
  const subdued = kind === 'backend'
  return (
    <div
      data-testid="panel-error-banner"
      className="mb-3 border-l-2 border-red-500 bg-red-50 px-2 py-1 text-xs text-red-800 dark:border-red-400 dark:bg-red-950 dark:text-red-300"
    >
      {headline && <p className="font-semibold">⚠ {headline}</p>}
      {hint && <p className="text-red-700/80 dark:text-red-400/80">{hint}</p>}
      <p data-testid="panel-error-detail">
        <span className={`font-mono ${subdued ? 'text-zinc-500 dark:text-zinc-500' : 'text-red-700 dark:text-red-400'}`}>
          {code}
        </span>{' '}
        <span className={subdued ? 'text-zinc-500 dark:text-zinc-500' : undefined}>{message}</span>
      </p>
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
