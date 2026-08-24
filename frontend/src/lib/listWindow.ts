import { useState } from 'react'

// Frontend windowing for lists that arrive as one response: the page renders
// a growing slice and extends it as the reader scrolls, instead of mounting
// thousands of rows up front or paging with buttons. The window only ever
// grows — rows a reader has scrolled through never unmount under them.

export interface ScrollMetrics {
  scrollTop: number
  scrollHeight: number
  clientHeight: number
}

/// Rows to render after a scroll: one more step once the bottom is less than
/// a viewport away (so the extension lands before the reader hits the end),
/// clamped to the total, and never below the current count.
export function grownCount(current: number, total: number, step: number, m: ScrollMetrics): number {
  const nearBottom = m.scrollHeight - (m.scrollTop + m.clientHeight) < m.clientHeight
  return nearBottom ? Math.max(current, Math.min(current + step, total)) : current
}

/// The chunk-quantized [start, end) index range the viewport is showing,
/// estimated from average row height. Quantizing means the range only changes
/// when the reader crosses a chunk boundary — callers can key queries on it
/// without refetching per scrolled pixel. No geometry (jsdom, first render)
/// means the top chunk.
export function viewportSlice(m: ScrollMetrics, renderedRows: number, chunk: number): { start: number; end: number } {
  if (m.scrollHeight <= 0 || renderedRows <= 0) return { start: 0, end: chunk }
  const rowHeight = m.scrollHeight / renderedRows
  const start = Math.floor(m.scrollTop / rowHeight / chunk) * chunk
  const end = Math.ceil((m.scrollTop + m.clientHeight) / rowHeight / chunk) * chunk
  return { start, end: Math.max(end, start + chunk) }
}

/// The state wiring both list pages share: attach `onScroll` to the scroller,
/// render `items.slice(0, count)`, and (where per-row data is fetched) ask for
/// `viewport` only. `reset` belongs on filter changes — a new filter is a new
/// list, and the old window would land somewhere arbitrary in it.
export function useListWindow(total: number, step: number, chunk: number) {
  const [count, setCount] = useState(step)
  const [viewport, setViewport] = useState({ start: 0, end: chunk })
  const rendered = Math.min(count, total)
  const onScroll = (e: React.UIEvent<HTMLElement>) => {
    const m = {
      scrollTop: e.currentTarget.scrollTop,
      scrollHeight: e.currentTarget.scrollHeight,
      clientHeight: e.currentTarget.clientHeight,
    }
    setCount((c) => grownCount(c, total, step, m))
    const next = viewportSlice(m, rendered, chunk)
    setViewport((v) => (v.start === next.start && v.end === next.end ? v : next))
  }
  const reset = () => {
    setCount(step)
    setViewport({ start: 0, end: chunk })
  }
  return { count, viewport, onScroll, reset }
}
