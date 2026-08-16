import type { TimelinePageParams } from '../../api/sse'
import type { AnchorContext, PauseReason } from './model'

export type JumpTarget =
  | { kind: 'now' }
  | { kind: 'beginning' }
  | { kind: 'offset'; partition: number; offset: number }
  | { kind: 'timestamp'; ts_ms: number }

export interface JumpPlan {
  anchorContext: AnchorContext
  // This is the jump's own STATIC intent only — never assigned directly
  // to the live pause state. Every jump routes through pauseMachine's
  // 'jump' event (as `intent`), which is the one place that decides what
  // actually happens to the CURRENT pause reason (e.g. an explicit pause
  // survives any jump — planJump has no input for that and must not decide it).
  pauseIntent: PauseReason
  scrollEdge: 'top' | 'bottom'
  attach: boolean
  highlight: { partition: number; offset: number } | null
  params: TimelinePageParams
}

// Owner ruling 2026-08-15: 'beginning'/'offset'/'timestamp' all land forward, the target as the oldest loaded
// row; only 'now' lands at the top of its window. The backend resolves every partition's start position for
// 'offset'/'timestamp' independent of direction (Anchor::OffsetForwardAligned, backend/src/message/timeline/anchor.rs).
export function planJump(target: JumpTarget, pageLimit: number): JumpPlan {
  switch (target.kind) {
    case 'now':
      return {
        anchorContext: 'default',
        pauseIntent: 'none',
        scrollEdge: 'top',
        attach: true,
        highlight: null,
        params: { direction: 'back', limit: pageLimit, anchor: 'latest' },
      }
    case 'beginning':
      return {
        anchorContext: 'beginning',
        pauseIntent: 'auto',
        scrollEdge: 'bottom',
        attach: false,
        highlight: null,
        params: { direction: 'forward', limit: pageLimit, anchor: 'beginning' },
      }
    case 'offset':
      return {
        anchorContext: 'default',
        pauseIntent: 'auto',
        scrollEdge: 'bottom',
        attach: false,
        highlight: { partition: target.partition, offset: target.offset },
        params: {
          direction: 'forward',
          limit: pageLimit,
          anchor: 'offset',
          partition: target.partition,
          offset: target.offset,
        },
      }
    case 'timestamp':
      return {
        anchorContext: 'default',
        pauseIntent: 'auto',
        scrollEdge: 'bottom',
        attach: false,
        highlight: null,
        params: { direction: 'forward', limit: pageLimit, anchor: 'timestamp', ts_ms: target.ts_ms },
      }
  }
}
