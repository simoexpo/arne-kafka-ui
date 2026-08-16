import type { PauseReason } from './model'

export interface PauseInput {
  pauseReason: PauseReason
  attached: boolean
  // True while at least one message row is expanded (design spec v1.7,
  // "Inspection pause"): dominates the scrollPinnedTop resume rule below —
  // an open inspection is a stronger "don't move things" signal than scroll
  // position, so pinning at top never implicitly resumes (or flushes) while
  // any row is still open.
  inspecting: boolean
}

export type PauseEvent =
  | 'pillClick'
  | 'toggleClick'
  | 'scrollPinnedTop'
  | 'scrollAwayFromTop'
  | 'reattached'
  // The LAST open inspection just closed. The caller (Timeline) only fires
  // this once it has already confirmed both preconditions itself — this WAS
  // the last inspection, and the viewport is currently pinned at top — so
  // the decision left here is exactly scrollPinnedTop's own resume rule
  // (design spec v1.7: "mirroring auto-pause").
  | 'lastInspectionClosed'

export interface PauseDecision {
  pause: PauseReason
  flush: boolean
  jumpToNow: boolean
  scrollTop: boolean
}

const idle = (pause: PauseReason): PauseDecision => ({ pause, flush: false, jumpToNow: false, scrollTop: false })

export function nextPause({ pauseReason, attached, inspecting }: PauseInput, event: PauseEvent): PauseDecision {
  switch (event) {
    case 'pillClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      return { pause: pauseReason === 'auto' ? 'none' : pauseReason, flush: true, jumpToNow: false, scrollTop: true }
    case 'toggleClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      if (pauseReason === 'none') return idle('explicit')
      return { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
    case 'scrollPinnedTop':
      // Inspection pause dominance (design spec v1.7): never implicitly
      // resume (or flush) while any row is still expanded.
      if (inspecting) return idle(pauseReason)
      return attached && pauseReason === 'auto'
        ? { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
        : idle(pauseReason)
    case 'scrollAwayFromTop':
      return pauseReason === 'none' ? idle('auto') : idle(pauseReason)
    case 'reattached':
      return { pause: pauseReason === 'explicit' ? 'explicit' : 'none', flush: true, jumpToNow: false, scrollTop: false }
    case 'lastInspectionClosed':
      // Unlike scrollPinnedTop's own resume rule (pauseReason === 'auto'
      // only), a live message buffers while ANY row is expanded regardless
      // of pauseReason (see useLiveTail's routing) — so closing the last
      // inspection can have real buffered content to flush even when
      // pauseReason stayed 'none' the whole time (attached, pinned, never
      // actually auto-paused by scroll). Flush whenever the pause isn't
      // EXPLICIT — an explicit pause is never implicitly lifted.
      return attached && pauseReason !== 'explicit'
        ? { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
        : idle(pauseReason)
  }
}
