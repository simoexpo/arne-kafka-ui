// Single shared stat-headline component: OverviewPage.tsx and
// ConfigTab.tsx each used to define their own local `Stat` with
// identical classes
// (text-xs label / text-xl font-semibold value) — nothing enforced they'd
// keep agreeing, just coincidence. This is a consistency guarantee only:
// no visual change from either page's previous rendering.
export function Stat({ label, value, warn, title, className }: { label: string; value: string; warn?: boolean; title?: string; className?: string }) {
  return (
    <div data-testid={`stat-${label}`} title={title} className={`min-w-0 ${title ? 'cursor-help' : ''} ${className ?? ''}`}>
      <dt className="text-xs text-zinc-500">{label}</dt>
      {/* A value never spills out of its panel: numbers never reach the edge,
          but a long name in a narrow column must clip inside its own box. */}
      <dd className={`truncate text-xl font-semibold ${warn ? 'text-red-600 dark:text-red-400' : ''}`}>{value}</dd>
    </div>
  )
}
