// Single shared stat-headline component (fonts pass, owner-approved
// 2026-08-17 analysis, item 6): OverviewPage.tsx and TopicDetailPage.tsx
// each used to define their own local `Stat` with identical classes
// (text-xs label / text-xl font-semibold value) — nothing enforced they'd
// keep agreeing, just coincidence. This is a consistency guarantee only:
// no visual change from either page's previous rendering.
export function Stat({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div data-testid={`stat-${label}`}>
      <dt className="text-xs text-zinc-500">{label}</dt>
      <dd className={`text-xl font-semibold ${warn ? 'text-red-600 dark:text-red-400' : ''}`}>{value}</dd>
    </div>
  )
}
