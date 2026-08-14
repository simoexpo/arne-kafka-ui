import { forwardRef, useImperativeHandle, useRef } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { MessageOut } from '../../api/types'
import { MessageRow } from './MessageRow'

export interface MessageListHandle {
  // Jumps reposition the viewport, not just the data: 'now'/'offset'/
  // 'timestamp' land you looking at the top of the new window; 'beginning'
  // lands you at the start of history, looking forward, so the bottom (the
  // oldest-visible edge) is the meaningful anchor there. Assigning
  // scrollTop = scrollHeight is the standard clamp-to-bottom trick (a real
  // browser clamps any out-of-range value to the max scrollable offset).
  scrollToEdge(edge: 'top' | 'bottom'): void
  // Scroll anchoring (design spec v1.3 "Scroll anchoring"): reads the raw
  // scrollTop/scrollHeight off the real scroll element so a caller can
  // capture "before" metrics ahead of a prepend and diff them against
  // "after" metrics once the new rows have rendered. Returns null before
  // the element exists (e.g. mid-jump, while Panel shows a loading
  // skeleton and MessageList itself is unmounted).
  scrollMetrics(): { top: number; height: number } | null
  // Nudges scrollTop by `delta` (added, not set) — compensates for a
  // height change that happened above the viewport (a prepend) so the
  // reader's visual position doesn't silently shift.
  adjustScrollTop(delta: number): void
}

export const MessageList = forwardRef<
  MessageListHandle,
  {
    messages: readonly MessageOut[]
    // Forwarded directly onto the real scrolling element (not a wrapper) —
    // scroll events don't bubble, so Timeline's top-pin/bottom-sentinel
    // detection needs the handler on the exact element the virtualizer
    // scrolls. In real browsers this is `parentRef`'s scrollTop/scrollHeight/
    // clientHeight; jsdom tests drive it via `fireEvent.scroll` on this same
    // element (found via its data-testid), stubbing scrollHeight/clientHeight
    // since jsdom never actually lays anything out.
    onScroll?: (scrollTop: number, scrollHeight: number, clientHeight: number) => void
  }
>(function MessageList({ messages, onScroll }, ref) {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    overscan: 10,
    initialRect: { width: 900, height: 600 },
  })
  useImperativeHandle(
    ref,
    () => ({
      scrollToEdge(edge) {
        const el = parentRef.current
        if (!el) return
        el.scrollTop = edge === 'top' ? 0 : el.scrollHeight
      },
      scrollMetrics() {
        const el = parentRef.current
        if (!el) return null
        return { top: el.scrollTop, height: el.scrollHeight }
      },
      adjustScrollTop(delta) {
        const el = parentRef.current
        if (!el) return
        el.scrollTop = el.scrollTop + delta
      },
    }),
    [],
  )
  if (messages.length === 0) {
    return <p className="p-4 text-sm text-zinc-500">no messages</p>
  }
  return (
    <div
      ref={parentRef}
      data-testid="timeline-scroll"
      onScroll={(e) => onScroll?.(e.currentTarget.scrollTop, e.currentTarget.scrollHeight, e.currentTarget.clientHeight)}
      // Fill the viewport below the tab chrome (title + tabs + timeline
      // controls + main padding ≈ 19rem) instead of a fixed 32rem cap that
      // stranded dead space on tall monitors; the floor keeps the list
      // usable on short windows even if the page then scrolls a little.
      className="max-h-[calc(100dvh-19rem)] min-h-[16rem] overflow-auto"
    >
      <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
        {virtualizer.getVirtualItems().map((item) => {
          const m = messages[item.index]
          const prev = item.index > 0 ? messages[item.index - 1] : null
          const tsInverted =
            prev !== null &&
            m.timestamp_ms !== null &&
            prev.timestamp_ms !== null &&
            m.timestamp_ms > prev.timestamp_ms
          return (
            <div
              key={`${m.partition}-${m.offset}-${item.index}`}
              ref={typeof ResizeObserver !== 'undefined' ? virtualizer.measureElement : undefined}
              data-index={item.index}
              style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${item.start}px)` }}
            >
              <MessageRow message={m} tsInverted={tsInverted} />
            </div>
          )
        })}
      </div>
    </div>
  )
})
