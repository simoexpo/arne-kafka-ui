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
}

export const MessageList = forwardRef<
  MessageListHandle,
  {
    messages: readonly MessageOut[]
    // Forwarded directly onto the real scrolling element (not a wrapper) —
    // scroll events don't bubble, so Timeline's top-pin detection needs the
    // handler on the exact element the virtualizer scrolls. In real browsers
    // this is `parentRef`'s scrollTop; jsdom tests drive it via
    // `fireEvent.scroll` on this same element (found via its data-testid).
    onScroll?: (scrollTop: number) => void
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
      onScroll={(e) => onScroll?.(e.currentTarget.scrollTop)}
      className="max-h-[32rem] overflow-auto"
    >
      <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
        {virtualizer.getVirtualItems().map((item) => (
          <div
            key={`${messages[item.index].partition}-${messages[item.index].offset}-${item.index}`}
            ref={typeof ResizeObserver !== 'undefined' ? virtualizer.measureElement : undefined}
            data-index={item.index}
            style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${item.start}px)` }}
          >
            <MessageRow message={messages[item.index]} />
          </div>
        ))}
      </div>
    </div>
  )
})
