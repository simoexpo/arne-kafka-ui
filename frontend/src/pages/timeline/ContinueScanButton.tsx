// Shared by both continue-scan sites (forward-affordance above MessageList, back-affordance below) — S-9: they are
// mutually exclusive on continueDirection, so the shared data-testid is never ambiguous.
export function ContinueScanButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      data-testid="continue-scan"
      onClick={onClick}
      className="w-full rounded border border-amber-400 py-1 text-xs text-amber-600 dark:border-amber-600 dark:text-amber-400"
    >
      {label}
    </button>
  )
}
