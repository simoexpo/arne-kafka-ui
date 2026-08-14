// "▲ {n} new" buffer pill + explicit play/pause toggle. Both live in the
// same file because they're the two halves of one control cluster next to
// the live indicator in Timeline's header — the pill surfaces what's been
// buffered while paused (auto or explicit), the toggle is how a user pauses
// or resumes explicitly. Same visual family as AppShell's ThemeToggle: a
// rounded pill housing both icon states, the active one highlighted.

export function LivePill({ count, capped, attached, onClick }: { count: number; capped: boolean; attached: boolean; onClick: () => void }) {
  if (count === 0) return null
  // `count` is the TOTAL received while paused, not the (500-)capped buffer
  // size — it keeps counting honestly even past the cap. `capped` only
  // controls whether the "· older dropped" suffix is appended, once the
  // underlying buffer has actually started dropping its oldest entries.
  const label = capped ? `▲ ${count} new · older dropped` : `▲ ${count} new`
  return (
    <button
      type="button"
      data-testid="live-pill"
      // Attached: clicking flushes the buffer in place. Detached: clicking
      // jumps to now, abandoning the historical reading position — the
      // label must say which one the click will actually do.
      aria-label={attached ? 'flush buffered live messages' : 'jump to now and show new messages'}
      onClick={onClick}
      className="rounded-full border border-emerald-400 px-2 py-0.5 text-xs font-medium text-emerald-600 dark:border-emerald-600 dark:text-emerald-400"
    >
      {label}
    </button>
  )
}

function PauseIcon({ active }: { active: boolean }) {
  return (
    <svg
      data-testid="icon-pause"
      viewBox="0 0 16 16"
      width="14"
      height="14"
      className={active ? 'text-amber-500' : 'text-zinc-400 dark:text-zinc-600'}
    >
      <rect x="3.5" y="2.5" width="3" height="11" rx="1" fill="currentColor" />
      <rect x="9.5" y="2.5" width="3" height="11" rx="1" fill="currentColor" />
    </svg>
  )
}

function PlayIcon({ active }: { active: boolean }) {
  return (
    <svg
      data-testid="icon-play"
      viewBox="0 0 16 16"
      width="14"
      height="14"
      className={active ? 'text-emerald-500' : 'text-zinc-400 dark:text-zinc-600'}
    >
      <path d="M4 2.3 13.5 8 4 13.7Z" fill="currentColor" />
    </svg>
  )
}

// A single toggle button showing BOTH icons (matches ThemeToggle's visual
// family), the active one highlighted: the lit icon mirrors the CURRENT
// state, not the click action — a lit pause icon would otherwise read as
// "paused" to a user. So: play lit emerald while live/unpaused, pause lit
// amber while paused. Rendered play-then-pause (left-to-right) to match
// that reading order. `aria-label` stays action-based (what clicking does)
// and `aria-pressed` mirrors `paused` — this is a pressed/unpressed toggle,
// not two separate buttons.
export function PlayPauseToggle({ paused, onClick }: { paused: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      data-testid="play-pause-toggle"
      aria-label={paused ? 'resume live updates' : 'pause live updates'}
      aria-pressed={paused}
      onClick={onClick}
      className="flex items-center gap-1 rounded-full border border-zinc-300 p-1 dark:border-zinc-700"
    >
      <span className={`rounded-full p-1 ${!paused ? 'bg-emerald-100 dark:bg-emerald-500/20' : ''}`}>
        <PlayIcon active={!paused} />
      </span>
      <span className={`rounded-full p-1 ${paused ? 'bg-amber-100 dark:bg-amber-500/20' : ''}`}>
        <PauseIcon active={paused} />
      </span>
    </button>
  )
}
