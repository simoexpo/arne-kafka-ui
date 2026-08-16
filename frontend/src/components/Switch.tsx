// Single shared toggle-switch component (same dedup precedent as Stat.tsx):
// TopicsPage.tsx's "show internal" switch and TopicDetailPage.tsx's
// ConfigTab "show all configs" switch each used to define byte-identical
// Tailwind track/thumb markup — nothing enforced they'd keep agreeing. This
// is a consistency guarantee only: no visual change from either page's
// previous rendering.
export function Switch({ checked, label, ariaLabel, onChange }: {
  checked: boolean
  label: string
  ariaLabel?: string
  onChange: () => void
}) {
  return (
    <label className="inline-flex w-fit items-center gap-2 text-sm text-zinc-500">
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={ariaLabel ?? label}
        onClick={onChange}
        className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${
          checked ? 'bg-blue-600 dark:bg-blue-500' : 'bg-zinc-300 dark:bg-zinc-700'
        }`}
      >
        <span
          className={`absolute left-0.5 top-0.5 h-4 w-4 transform rounded-full bg-white transition-transform ${
            checked ? 'translate-x-4' : 'translate-x-0'
          }`}
        />
      </button>
      {label}
    </label>
  )
}
