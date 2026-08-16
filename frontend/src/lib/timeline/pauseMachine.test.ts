import { describe, expect, it } from 'vitest'
import { nextPause } from './pauseMachine'

describe('nextPause', () => {
  describe('pillClick', () => {
    it('while detached, jumps to now instead of touching pause state', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false }, 'pillClick')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: true,
        scrollTop: false,
      })
    })

    it('while attached and auto-paused, flushes, scrolls to top, and resumes', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true }, 'pillClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })

    it('while attached and explicitly paused, flushes and scrolls but stays paused', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true }, 'pillClick')).toEqual({
        pause: 'explicit',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })

    it('while attached and not paused, still flushes and scrolls (idempotent)', () => {
      expect(nextPause({ pauseReason: 'none', attached: true }, 'pillClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: true,
      })
    })
  })

  describe('toggleClick', () => {
    it('while detached, jumps to now', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false }, 'toggleClick')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: true,
        scrollTop: false,
      })
    })

    it('while attached and unpaused, pauses explicitly without flushing', () => {
      expect(nextPause({ pauseReason: 'none', attached: true }, 'toggleClick')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('while attached and auto-paused, flushes and fully resumes', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true }, 'toggleClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('while attached and explicitly paused, flushes and fully resumes', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true }, 'toggleClick')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('scrollPinnedTop', () => {
    it('resumes an auto-pause while attached', () => {
      expect(nextPause({ pauseReason: 'auto', attached: true }, 'scrollPinnedTop')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an explicit pause untouched while attached', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true }, 'scrollPinnedTop')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('does not resume an auto-pause while detached (top-of-window is not now)', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false }, 'scrollPinnedTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('is a no-op when already unpaused', () => {
      expect(nextPause({ pauseReason: 'none', attached: true }, 'scrollPinnedTop')).toEqual({
        pause: 'none',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('scrollAwayFromTop', () => {
    it('auto-pauses when unpaused, regardless of attachment', () => {
      expect(nextPause({ pauseReason: 'none', attached: true }, 'scrollAwayFromTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
      expect(nextPause({ pauseReason: 'none', attached: false }, 'scrollAwayFromTop')).toEqual({
        pause: 'auto',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('leaves an already-paused reason untouched', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: true }, 'scrollAwayFromTop')).toEqual({
        pause: 'explicit',
        flush: false,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })

  describe('reattached', () => {
    it('drops an auto-pause on reattach', () => {
      expect(nextPause({ pauseReason: 'auto', attached: false }, 'reattached')).toEqual({
        pause: 'none',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })

    it('preserves an explicit pause across reattach', () => {
      expect(nextPause({ pauseReason: 'explicit', attached: false }, 'reattached')).toEqual({
        pause: 'explicit',
        flush: true,
        jumpToNow: false,
        scrollTop: false,
      })
    })
  })
})
