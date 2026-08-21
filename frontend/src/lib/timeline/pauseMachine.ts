import type { PauseReason } from './model'

export interface PauseInput {
  pauseReason: PauseReason
  attached: boolean
  // True while at least one message row is expanded (
  // "Inspection pause"): dominates the scrollPinnedTop resume rule below —
  // an open inspection is a stronger "don't move things" signal than scroll
  // position, so pinning at top never implicitly resumes (or flushes) while
  // any row is still open. The 'reattached' branch honors the same
  // dominance — see its own comment below.
  inspecting: boolean
  // Only read by the 'jump' event: the jump's OWN static intent (planJump's
  // `pauseIntent` — see jumpPlan.ts), i.e. what the pause reason would be if
  // no PRIOR state mattered. This table, not the plan, decides what actually
  // happens to an existing pause — ignored by every other event.
  intent?: PauseReason
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
  // (mirroring auto-pause).
  | 'lastInspectionClosed'
  // A jump landed (any kind — now/beginning/offset/timestamp). This is
  // the ONE transition every caller must route through this table (Timeline
  // used to assign planJump's static intent straight to `pauseReasonRef`,
  // which meant a jump was the only place that could silently overwrite a
  // user's EXPLICIT pause). See `intent` on `PauseInput`.
  | 'jump'

export interface PauseDecision {
  pause: PauseReason
  flush: boolean
  jumpToNow: boolean
  scrollTop: boolean
}

const idle = (pause: PauseReason): PauseDecision => ({ pause, flush: false, jumpToNow: false, scrollTop: false })

export function nextPause({ pauseReason, attached, inspecting, intent }: PauseInput, event: PauseEvent): PauseDecision {
  switch (event) {
    case 'pillClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      return { pause: pauseReason === 'auto' ? 'none' : pauseReason, flush: true, jumpToNow: false, scrollTop: true }
    case 'toggleClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      if (pauseReason === 'none') return idle('explicit')
      return { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
    case 'scrollPinnedTop':
      // Inspection pause dominance: never implicitly
      // resume (or flush) while any row is still expanded.
      if (inspecting) return idle(pauseReason)
      return attached && pauseReason === 'auto'
        ? { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
        : idle(pauseReason)
    case 'scrollAwayFromTop':
      return pauseReason === 'none' ? idle('auto') : idle(pauseReason)
    case 'reattached':
      // Inspection dominance applies here exactly like scrollPinnedTop —
      // catching the tail while a row is expanded must never flush/merge the
      // buffer out from under the reader's open inspection. Nothing else
      // about the decision runs until the inspection closes.
      if (inspecting) return idle(pauseReason)
      // An explicit pause is never implicitly lifted — same rule
      // `lastInspectionClosed` states below for the identical pauseReason.
      // Reattaching re-earns nothing for an EXPLICIT pause; only a
      // transient 'auto' pause (or none) is dropped, with a flush, since
      // there is real buffered content to merge in that case.
      if (pauseReason === 'explicit') return idle('explicit')
      return { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
    case 'jump':
      // An explicit pause survives any jump — the strongest "don't move
      // things" signal in the table, and a jump does not lift it (mirrors
      // reattached's own rule immediately above). Every other pauseReason
      // ('auto' or 'none') defers entirely to the jump's own static intent,
      // discarding whatever transient state was active before it.
      return idle(pauseReason === 'explicit' ? 'explicit' : (intent ?? 'none'))
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
