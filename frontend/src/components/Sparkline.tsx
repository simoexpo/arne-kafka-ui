export function Sparkline({
  points,
  domain,
  unit,
}: {
  // `gapBefore` marks a point whose stretch back to its predecessor was never
  // measured — the line breaks there rather than drawing a rate we didn't see.
  points: { x: number; y: number; gapBefore?: boolean }[]
  domain?: { min: number; max: number }
  // Names what y is, on the top axis label ("1.4 msg/s").
  unit?: string
}) {
  if (points.length === 0) {
    return <p className="text-sm text-zinc-500">no samples yet</p>
  }
  const w = 240
  const h = 48
  const pad = 2
  // Left gutter for the two labels that anchor the scale: without them the
  // line heights read as arbitrary values (owner: the flat bottom of the
  // chart was being read as "-1"). y runs from 0 (bottom) to the window's
  // peak (top) — rates are never negative.
  const gutter = 52
  const xs = points.map((p) => p.x)
  const ys = points.map((p) => p.y)
  const xMin = domain ? domain.min : Math.min(...xs)
  const xMax = domain ? domain.max : Math.max(...xs)
  const yMax = Math.max(...ys, 1e-9)
  const sx = (x: number) =>
    xMax === xMin ? gutter + (w - gutter) / 2 : gutter + pad + ((x - xMin) / (xMax - xMin)) * (w - gutter - 2 * pad)
  const sy = (y: number) => h - pad - (y / yMax) * (h - 2 * pad)
  // Segments break at points whose stretch back to the previous sample was
  // never measured — sampling only runs while someone watches the topic, so a
  // return visit leaves a real hole that must not be drawn as a rate.
  const segments: { x: number; y: number }[][] = []
  for (const p of points) {
    if (segments.length === 0 || p.gapBefore) segments.push([])
    segments[segments.length - 1].push(p)
  }
  return (
    // The plot area carries the palette's hover surface so the chart reads as
    // its own region on the panel, not a line floating on the card.
    <svg
      role="img"
      aria-label="throughput sparkline"
      viewBox={`0 0 ${w} ${h}`}
      className="h-12 w-60 rounded"
    >
      <rect x={gutter} y="0" width={w - gutter} height={h} rx="4" className="fill-zinc-100 dark:fill-zinc-800" />
      {/* A zero peak has nothing to scale — the bottom label says it all. */}
      {Math.max(...ys) > 0 && (
        <text x={gutter - 5} y={pad + 7} textAnchor="end" className="fill-zinc-500 text-[9px]">
          {peakLabel(yMax, unit)}
        </text>
      )}
      <text x={gutter - 5} y={h - pad} textAnchor="end" className="fill-zinc-500 text-[9px]">
        0
      </text>
      {segments.map((seg, i) => (
        <g key={i}>
          <polyline
            points={seg.map((p) => `${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join(' ')}
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="text-blue-600 dark:text-blue-400"
          />
          {/* A segment of one draws no stroke — the first sample of a visit
              would be an empty chart — so mark it with a dot. */}
          {seg.length === 1 && (
            <circle cx={sx(seg[0].x)} cy={sy(seg[0].y)} r="2" className="fill-blue-600 dark:fill-blue-400" />
          )}
        </g>
      ))}
    </svg>
  )
}

function peakLabel(peak: number, unit?: string): string {
  const n = peak >= 10 ? peak.toFixed(0) : peak.toFixed(1)
  return unit ? `${n} ${unit}` : n
}
