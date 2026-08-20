// Single shared stat-headline component: OverviewPage.tsx and
// ConfigTab.tsx each used to define their own local `Stat` with
// identical classes
// (text-xs label / text-xl font-semibold value) — nothing enforced they'd
// keep agreeing, just coincidence. This is a consistency guarantee only:
// no visual change from either page's previous rendering.
export function Stat({ label, value, warn, title }: { label: string; value: string; warn?: boolean; title?: string }) {
  return (
    <div data-testid={`stat-${label}`} title={title} className={title ? 'cursor-help' : undefined}>
      <dt className="text-xs text-zinc-500">{label}</dt>
      <dd className={`text-xl font-semibold ${warn ? 'text-red-600 dark:text-red-400' : ''}`}>{value}</dd>
    </div>
  )
}
