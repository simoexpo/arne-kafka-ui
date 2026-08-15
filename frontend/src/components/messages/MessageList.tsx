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
  // Nudges scrollTop by `delta` (added, not set) — compensates for a
  // height change that happened above the viewport (a prepend) so the
  // reader's visual position doesn't silently shift.
  adjustScrollTop(delta: number): void
  // Scroll anchoring (design spec v1.3 "Scroll anchoring"; fix round 1, M1
  // — review of 079f30f): the virtualizer's own rendered top-offset (its
  // coordinate space, not the scroll element's) of the row currently at
  // `index` in the `messages` array this component was last rendered with,
  // or `null` if `index` is out of range. Row-IDENTITY anchoring (capture
  // a specific row's offset before an insert, find that SAME row's new
  // index and offset after, adjust by the difference) replaced an earlier
  // total-scrollHeight-delta approach that silently broke the moment an
  // insert ALSO trimmed rows below the viewport in the same commit (a
  // trim there changes the total height without moving anything the
  // reader can see above them, so "added − removed" could cancel toward
  // the exact relocation scroll-anchoring exists to prevent — see
  // Timeline.tsx's own comment on its capture/consume sites). Backed by
  // the virtualizer's own `measurementsCache`, which is populated for
  // every index up front (estimated-then-measured), not just the
  // currently visible/overscanned range — so this works regardless of
  // scroll position.
  rowOffsetAt(index: number): number | null
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
      adjustScrollTop(delta) {
        const el = parentRef.current
        if (!el) return
        el.scrollTop = el.scrollTop + delta
      },
      rowOffsetAt(index) {
        return virtualizer.measurementsCache[index]?.start ?? null
      },
    }),
    // `virtualizer` (unlike `parentRef`, whose STABLE ref object already
    // always reads its latest `.current`) must be a real dependency here:
    // `rowOffsetAt` needs THIS render's `measurementsCache`, reflecting the
    // current `messages` count/content, not whatever virtualizer instance
    // existed the first time this handle was created.
    [virtualizer],
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
      // overflow-anchor:none — Timeline does its own junction anchoring on
      // forward prepends; the browser's native scroll anchoring must never
      // compensate a second time (today the index-bearing row keys happen to
      // defeat it, but that's incidental, not something to rely on).
      className="max-h-[calc(100dvh-19rem)] min-h-[16rem] overflow-auto [overflow-anchor:none]"
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
