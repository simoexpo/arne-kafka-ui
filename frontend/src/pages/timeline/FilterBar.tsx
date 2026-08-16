import { FilterInput } from '../../components/FilterInput'
import { FilterHelp } from '../../components/FilterHelp'

export function FilterBar({
  value,
  onChange,
  progress,
  onCancel,
}: {
  value: string
  onChange: (value: string) => void
  // M6 (charter: "real progress — known total up front"): `budget` is the
  // wire's own known ceiling for the current scan request — null only in
  // the brief window between a fresh page starting to load and its first
  // `progress` event arriving (see Timeline.tsx's `state.progress`).
  progress: { scanned: number; matches: number; budget: number | null } | null
  onCancel: () => void
}) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <div className="min-w-0 flex-1">
        <FilterInput value={value} onChange={onChange} placeholder="filter messages…" ariaLabel="filter messages" className="w-full" fullWidth />
      </div>
      <FilterHelp />
      {progress && (
        <div data-testid="filter-progress" className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          <span>
            {progress.budget === null
              ? `scanned ${progress.scanned} · ${progress.matches} matches`
              : `scanned ${progress.scanned} of ${progress.budget} · ${progress.matches} matches`}
          </span>
          <button
            type="button"
            data-testid="cancel-scan"
            onClick={onCancel}
            className="rounded border border-zinc-300 px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
          >
            Cancel
          </button>
        </div>
      )}
    </div>
  )
}
