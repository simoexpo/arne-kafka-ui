import { describe, expect, it } from 'vitest'
import { nextPause } from './pauseMachine'

// Every table entry below states `inspecting` explicitly (design spec v1.7,
// "Inspection pause") even where a case doesn't exercise it — a decision
// table is exactly the place a reader should never have to guess a default.
describe('nextPause', () => {
  describe('pillClick', () => {
    it('while detached, jumps to now instead of touching pause state', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false, inspecting: false }, 'pillClick')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: true,
        scrollTop: false,
      })
    })

    it('while attached and auto-paused, flushes, scrolls to top, and resumes', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: false }, 'pillClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })

    it('while attached and explicitly paused, flushes and scrolls but stays paused', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: false }, 'pillClick')).toEqual({
        pause: 'explicit',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })

    it('while attached and not paused, still flushes and scrolls (idempotent)', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: false }, 'pillClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })

    it('flushes exactly the same while inspecting — inspections are untouched by pauseMachine either way', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: true }, 'pillClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })
  })

  describe('toggleClick', () => {
    it('while detached, jumps to now', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false, inspecting: false }, 'toggleClick')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: true,
        scrollTop: false,
      })
    })

    it('while attached and unpaused, pauses explicitly without flushing', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: false }, 'toggleClick')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('while attached and auto-paused, flushes and fully resumes', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: false }, 'toggleClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('while attached and explicitly paused, flushes and fully resumes', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: false }, 'toggleClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('scrollPinnedTop', () => {
    it('resumes an auto-pause while attached', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: false }, 'scrollPinnedTop')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an explicit pause untouched while attached', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: false }, 'scrollPinnedTop')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('does not resume an auto-pause while detached (top-of-window is not now)', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false, inspecting: false }, 'scrollPinnedTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('is a no-op when already unpaused', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: false }, 'scrollPinnedTop')).toEqual({
        pause: 'none',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    // Inspection pause dominance (design spec v1.7): an open inspection is a
    // stronger "don't move things" signal than scroll position — pinning at
    // top must never implicitly resume, or implicitly flush, while any row
    // is still expanded.
    it('does NOT resume an auto-pause while inspecting, even attached and pinned at top', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: true }, 'scrollPinnedTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an explicit pause untouched while inspecting too', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: true }, 'scrollPinnedTop')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('is still a no-op when already unpaused and inspecting', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: true }, 'scrollPinnedTop')).toEqual({
        pause: 'none',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('scrollAwayFromTop', () => {
    it('auto-pauses when unpaused, regardless of attachment', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: false }, 'scrollAwayFromTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
      expect(nextPause({ pauseReason: 'none', attached: false, inspecting: false }, 'scrollAwayFromTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an already-paused reason untouched', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: false }, 'scrollAwayFromTop')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('reattached', () => {
    it('drops an auto-pause on reattach', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false, inspecting: false }, 'reattached')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('preserves an explicit pause across reattach', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: false, inspecting: false }, 'reattached')).toEqual({
        pause: 'explicit',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  // Design spec v1.7: "Live resumes automatically when the LAST inspection
  // closes while pinned at top (mirroring auto-pause)." The caller (Timeline)
  // only ever fires this event once it has already confirmed both
  // preconditions itself (the just-closed inspection was the last one, and
  // the viewport is currently pinned at top) — this table only has to decide
  // what happens to the pause reason at that point, which is exactly
  // scrollPinnedTop's own resume rule.
  describe('lastInspectionClosed', () => {
    it('resumes an auto-pause while attached', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true, inspecting: false }, 'lastInspectionClosed')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an explicit pause untouched — an explicit pause is never implicitly lifted', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true, inspecting: false }, 'lastInspectionClosed')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('does not resume while detached (mirrors scrollPinnedTop: top-of-window is not now)', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false, inspecting: false }, 'lastInspectionClosed')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    // Unlike scrollPinnedTop's own resume rule, this one still flushes even
    // when pauseReason was already 'none' — a live message buffers while ANY
    // row is expanded regardless of pauseReason (see useLiveTail's routing),
    // so an inspection that opened and closed while pinned at top, attached,
    // and never otherwise auto-paused can still have real buffered content
    // that would otherwise sit stuck forever with nothing left to trigger a
    // later flush.
    it('still flushes when pauseReason was already none — buffering while inspecting is independent of pauseReason', () => {
      expect(nextPause({ pauseReason: 'none', attached: true, inspecting: false }, 'lastInspectionClosed')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })
})
