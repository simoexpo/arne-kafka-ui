// 'none': live inserts merge in (only takes effect while attached). 'auto': paused by scrolling off the top,
// resumes on return to top (only while attached). 'explicit': paused via the play/pause pill, only the pill resumes it.
export type PauseReason = 'none' | 'auto' | 'explicit'

// Which anchor the currently loaded window was bootstrapped from — consulted only as beginning-vs-not: a settled
// filter change re-reads from 'beginning' when that's current, and falls back to back/latest otherwise.
export type AnchorContext = 'default' | 'beginning'
