import { zoneSuffix } from '../lib/format'
import { setTimeDisplayMode, useTimeDisplayMode } from '../lib/timeDisplayMode'

// UTC/local display toggle (owner ruling 2026-08-15): same visual family as
// `ThemeToggle` (`layout/AppShell.tsx`) — a single button, two labelled
// halves, one lit at a time, `data-mode` reflecting the current choice.
// Purely display: flipping it only changes how already-loaded epoch-ms
// values are FORMATTED (see `lib/timeDisplayMode` and
// `formatTimestamp`/`formatWindowRange`) — nothing here re-fetches anything.
//
// Owner ruling (moved 2026-08-16): lives in the Messages tab's Timeline
// header now (see Timeline.tsx), next to the window-range display it
// rewrites and the live/pause controls — that's the one place its effect is
// actually felt, unlike the sidebar it used to sit in. The STORE
// (`lib/timeDisplayMode`, persistence, formatters) is unchanged; this was a
// call-site move only, hence its own file here rather than living inside
// whichever page happens to render it.
//
// Owner ruling (relabel, same day — reverts the earlier 2026-08-17
// dynamic-offset label): the two halves read the MODE NAMES, "UTC" /
// "local", never a live numeric offset — a mode selector labeled with a
// current numeric value contradicted rows spanning a DST transition (which
// legitimately carry different, both-correct offsets) and implied a choice
// of offsets that doesn't exist: the toggle names the MODE, timestamps state
// the FACT. The local half instead carries a `title` tooltip telling the
// full story in product voice ("browser time — currently UTC+2"), computed
// live off "now" via the same `zoneSuffix` family every timestamp display
// uses (half-hour zones formatted the same way as row suffixes). Displayed
// timestamp VALUES elsewhere keep their own per-timestamp numeric offsets —
// this only touches the toggle's own labels/aria.
export function TimeZoneToggle() {
  const mode = useTimeDisplayMode()
  const localOffset = zoneSuffix(Date.now(), 'local')
  return (
    <button
      type="button"
      data-testid="timezone-toggle"
      aria-label="toggle time zone display"
      data-mode={mode}
      className="flex items-center gap-1 rounded-full border border-zinc-300 p-1 text-xs dark:border-zinc-700"
      onClick={() => setTimeDisplayMode(mode === 'utc' ? 'local' : 'utc')}
    >
      <span
        className={`rounded-full px-2 py-[3px] ${
          mode === 'utc'
            ? 'bg-sky-100 font-medium text-sky-700 dark:bg-sky-500/20 dark:text-sky-300'
            : 'text-zinc-400 dark:text-zinc-600'
        }`}
      >
        UTC
      </span>
      <span
        title={`browser time — currently ${localOffset}`}
        className={`rounded-full px-2 py-[3px] ${
          mode === 'local'
            ? 'bg-sky-100 font-medium text-sky-700 dark:bg-sky-500/20 dark:text-sky-300'
            : 'text-zinc-400 dark:text-zinc-600'
        }`}
      >
        local
      </span>
    </button>
  )
}
