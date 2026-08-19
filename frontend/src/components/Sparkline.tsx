export function Sparkline({
  points,
  domain,
}: {
  points: { x: number; y: number }[]
  domain?: { min: number; max: number }
}) {
  if (points.length === 0) {
    return <p className="text-sm text-zinc-500">no samples yet</p>
  }
  const w = 240
  const h = 48
  const pad = 2
  const xs = points.map((p) => p.x)
  const ys = points.map((p) => p.y)
  const xMin = domain ? domain.min : Math.min(...xs)
  const xMax = domain ? domain.max : Math.max(...xs)
  const yMax = Math.max(...ys, 1e-9)
  const sx = (x: number) => (xMax === xMin ? w / 2 : pad + ((x - xMin) / (xMax - xMin)) * (w - 2 * pad))
  const sy = (y: number) => h - pad - (y / yMax) * (h - 2 * pad)
  const pts = points.map((p) => `${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join(' ')
  return (
    <svg role="img" aria-label="throughput sparkline" viewBox={`0 0 ${w} ${h}`} // The plot area gets the palette's hover surface so the chart reads as
      // its own region on the panel, not a line floating on the card.
      className="h-12 w-60 rounded bg-zinc-100 dark:bg-zinc-800">
      <polyline
        points={pts}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="text-blue-600 dark:text-blue-400"
      />
    </svg>
  )
}
