import { forwardRef, useImperativeHandle, useRef } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { MessageOut } from '../../api/types'
import { rowKey } from '../../lib/timeline/rowKey'
import { MessageRow } from './MessageRow'

const NO_EXPANDED_KEYS: ReadonlySet<string> = new Set()
const noopToggleExpand = () => {}

export interface MessageListHandle {
  // Jumps reposition the viewport, not just the data: 'now' lands looking
  // at the top of the new window; 'beginning'/'offset'/'timestamp' land at
  // the start of their loaded window looking forward, so the bottom (the
  // oldest-visible edge) is the meaningful anchor there. Assigning
  // scrollTop = scrollHeight is the standard clamp-to-bottom trick (a real
  // browser clamps any out-of-range value to the max scrollable offset).
  // Returns the scrollTop it actually set, or `null` if the scroll element
  // isn't mounted (a no-op). The caller uses it ONLY as a "did this land at
  // all" check before arming its settling-detection — matching by value was
  // tried and refuted (see `settlingRef` in Timeline.tsx).
  scrollToEdge(edge: 'top' | 'bottom'): number | null
  // Nudges scrollTop by `delta` (added, not set) — compensates for a
  // height change that happened above the viewport (a prepend) so the
  // reader's visual position doesn't silently shift.
  adjustScrollTop(delta: number): void
  // Scroll anchoring (design spec v1.3 "Scroll anchoring"): the virtualizer's own rendered top-offset (its
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
    // scroll events don't bubble, so Timeline's top-pin/near-bottom
    // classification (`classifyScroll`) needs the handler on the exact
    // element the virtualizer scrolls. In real browsers this is
    // `parentRef`'s scrollTop/scrollHeight/
    // clientHeight; jsdom tests drive it via `fireEvent.scroll` on this same
    // element (found via its data-testid), stubbing scrollHeight/clientHeight
    // since jsdom never actually lays anything out.
    onScroll?: (scrollTop: number, scrollHeight: number, clientHeight: number) => void
    // The (partition, offset) landed on by the most recent offset jump —
    // Timeline clears this on every subsequent jump/filter-change (both call
    // `storeRef.current.clear()`), so it only ever reflects the CURRENT
    // window's own jump, never a stale one from a window already left.
    // `null`/`undefined` (no jump, or a non-offset jump) marks nothing.
    jumpTarget?: { partition: number; offset: number } | null
    // Fix: expansion state survives virtualization, owned by identity —
    // Timeline is the single owner of which (partition, offset) identities
    // are expanded; MessageList only reflects `expandedKeys` back onto each
    // row's `expanded` prop and reports clicks with that row's own identity
    // (never a bare open/close bool — Timeline needs to know WHICH row).
    // See MessageRow's own doc comment for why it's a controlled component.
    expandedKeys?: ReadonlySet<string>
    onToggleExpand?: (partition: number, offset: number) => void
    // Enables the schema-id link in expanded payload headings.
    cluster?: string
  }
>(function MessageList(
  { messages, onScroll, jumpTarget, expandedKeys = NO_EXPANDED_KEYS, onToggleExpand = noopToggleExpand, cluster },
  ref,
) {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    overscan: 10,
    initialRect: { width: 900, height: 600 },
    // Owner-reported bug (2026-08-16): expanding a row and then receiving a
    // live message drew the list jumbled — rows drawn on top of each other,
    // with a hole where the expanded row had been. The virtualizer caches
    // each row's MEASURED height (see `measureElement` on the row wrapper
    // below) under `getItemKey(index)`, and the default `getItemKey` is the
    // index itself. Every prepend — a live message, a forward page — shifts
    // every index by one, so each row inherits the height measured for its
    // neighbour. Rows are near-uniform normally, which hid this as a few
    // pixels of drift; an expanded row is many times taller than its
    // neighbours, and the misattribution becomes the corruption above.
    // (partition, offset) is the same identity the React key below and the
    // store's own dedupe use, so a measurement now travels with its message
    // exactly as its expanded/collapsed state already does.
    getItemKey: (index) => rowKey(messages[index].partition, messages[index].offset),
  })
  useImperativeHandle(
    ref,
    () => ({
      scrollToEdge(edge) {
        const el = parentRef.current
        if (!el) return null
        el.scrollTop = edge === 'top' ? 0 : el.scrollHeight
        return el.scrollTop
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
      // Owner feedback 2026-08-15: this is the ONLY scroller on the Messages
      // tab now — the app shell, the page, and Timeline's own chrome are all
      // fixed/overflow-hidden (see AppShell/TopicDetailPage/Timeline), so
      // this container must genuinely fill whatever vertical space its flex
      // parent hands it (flex-1 min-h-0), not estimate the chrome above it
      // with a `calc(100dvh-...)` fudge factor. overflow-anchor:none —
      // Timeline does its own junction anchoring on forward prepends; the
      // browser's native scroll anchoring must never compensate a second
      // time. This is the mechanism that prevents that, full stop — row
      // keys are (partition, offset)-only (see below) precisely so they do
      // NOT incidentally defeat native scroll anchoring via a spurious
      // remount on every prepend.
      className="min-h-0 flex-1 overflow-auto [overflow-anchor:none]"
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
          const isJumpTarget =
            jumpTarget != null && m.partition === jumpTarget.partition && m.offset === jumpTarget.offset
          const key = rowKey(m.partition, m.offset)
          return (
            <div
              // (partition, offset) is a unique row identity — the store
              // dedupes on it (see timelineStore.ts's `seen` set) — so it's
              // stable across a prepend/append, unlike `item.index`, which
              // shifts for every existing row whenever a forward page
              // prepends. Keying on the volatile index used to force a full
              // remount (and re-measurement) of every row on every prepend
              // and every live insert, and (now that expansion is owned by
              // Timeline rather than local state) would also have re-fired
              // ResizeObserver for rows that never actually moved.
              key={key}
              ref={typeof ResizeObserver !== 'undefined' ? virtualizer.measureElement : undefined}
              data-index={item.index}
              style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${item.start}px)` }}
            >
              <MessageRow
                message={m}
                tsInverted={tsInverted}
                isJumpTarget={isJumpTarget}
                expanded={expandedKeys.has(key)}
                onToggle={() => onToggleExpand(m.partition, m.offset)}
                cluster={cluster}
              />
            </div>
          )
        })}
      </div>
    </div>
  )
})
