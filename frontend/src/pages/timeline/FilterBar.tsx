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
  // Charter: "real progress — known total up front". `budget` is the
  // GESTURE's accumulated known ceiling — the sum of every page's budget in
  // this scan so far, matching `scanned`/`matches` above (see Timeline.tsx's
  // `gestureBudgetRef` and `knownBudget`) — null only until at least one
  // page's budget has actually been observed.
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
