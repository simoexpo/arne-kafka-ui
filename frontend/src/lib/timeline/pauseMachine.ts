import type { PauseReason } from './model'

export interface PauseInput {
  pauseReason: PauseReason
  attached: boolean
}

export type PauseEvent = 'pillClick' | 'toggleClick' | 'scrollPinnedTop' | 'scrollAwayFromTop' | 'reattached'

export interface PauseDecision {
  pause: PauseReason
  flush: boolean
  jumpToNow: boolean
  scrollTop: boolean
}

const idle = (pause: PauseReason): PauseDecision => ({ pause, flush: false, jumpToNow: false, scrollTop: false })

export function nextPause({ pauseReason, attached }: PauseInput, event: PauseEvent): PauseDecision {
  switch (event) {
    case 'pillClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      return { pause: pauseReason === 'auto' ? 'none' : pauseReason, flush: true, jumpToNow: false, scrollTop: true }
    case 'toggleClick':
      if (!attached) return { ...idle(pauseReason), jumpToNow: true }
      if (pauseReason === 'none') return idle('explicit')
      return { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
    case 'scrollPinnedTop':
      if (attached && pauseReason === 'auto') return { pause: 'none', flush: true, jumpToNow: false, scrollTop: false }
      return idle(pauseReason)
    case 'scrollAwayFromTop':
      return pauseReason === 'none' ? idle('auto') : idle(pauseReason)
    case 'reattached':
      return { pause: pauseReason === 'explicit' ? 'explicit' : 'none', flush: true, jumpToNow: false, scrollTop: false }
  }
}
