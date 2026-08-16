// Transition rules live in pauseMachine.ts and its tests.
export type PauseReason = 'none' | 'auto' | 'explicit'

// Which anchor the currently loaded window was bootstrapped from — consulted only as beginning-vs-not: a settled
// filter change re-reads from 'beginning' when that's current, and falls back to back/latest otherwise.
export type AnchorContext = 'default' | 'beginning'
