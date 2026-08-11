import { useRef } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { MessageOut } from '../../api/types'
import { MessageRow } from './MessageRow'

export function MessageList({ messages }: { messages: MessageOut[] }) {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    overscan: 10,
    initialRect: { width: 900, height: 600 },
  })
  if (messages.length === 0) {
    return <p className="p-4 text-sm text-zinc-500">no messages</p>
  }
  return (
    <div ref={parentRef} className="max-h-[32rem] overflow-auto">
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
}
