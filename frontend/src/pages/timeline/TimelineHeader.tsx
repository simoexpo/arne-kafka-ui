import { LivePill, PlayPauseToggle } from './LivePill'
import { StalenessChip } from '../../components/StalenessChip'
import { TimeZoneToggle } from '../../components/TimeZoneToggle'

export function TimelineHeader({
  rangeLabel,
  bufferCount,
  bufferCapped,
  attached,
  tailAlive,
  paused,
  inspecting,
  newestTsMs,
  onPillClick,
  onPlayPauseClick,
}: {
  rangeLabel: string
  bufferCount: number
  bufferCapped: boolean
  attached: boolean
  tailAlive: boolean
  paused: boolean
  inspecting: boolean
  newestTsMs: number | null
  onPillClick: () => void
  onPlayPauseClick: () => void
}) {
  return (
    <div className="flex items-center justify-between">
      <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400" data-testid="window-range">
        {rangeLabel}
      </h2>
      {/* Owner feedback (stability): the pill/live-dot are the VOLATILE
          members of this cluster — they pop in and out with network
          activity, which used to shove the clickable controls (play/pause,
          the zone toggle) sideways underneath the reader's cursor. Fixed
          by ANCHORING from the right rather than relying on ordering
          alone: this row's parent is `justify-between` with exactly two
          children (the `h2` and this div), so this div's own right edge
          sits flush against the header line's right edge regardless of
          its content width — and within it, plain flex-row order means
          the LAST child is flush against THAT edge. Placing the zone
          toggle last (rightmost/outermost) and the play/pause-or-staleness
          slot immediately before it (second-to-last) pins both at a fixed
          distance from that right edge no matter what the volatile
          elements to their left do; the volatile pill/dot come FIRST
          (closest to the free space between the two `h2`/controls
          children), so their own appearing/disappearing only ever grows
          or shrinks that free space, never the position of anything to
          their right. See Timeline.test.tsx's "stable header controls"
          suite for the pixel-stability assertion. */}
      <div className="flex items-center gap-2">
        <LivePill count={bufferCount} capped={bufferCapped} attached={attached} onClick={onPillClick} />
        {tailAlive && attached && !paused && !inspecting && <span className="animate-pulse text-emerald-500">● live</span>}
        {attached ? (
          tailAlive ? (
            // Attached: the normal live play/pause toggle. M3: `inspecting`
            // lights the toggle even when `paused` (the real pause reason)
            // is false — `inspectingOnly` tells PlayPauseToggle so its
            // aria-label can say what the click actually does (pause) rather
            // than the default "resume live updates".
            <PlayPauseToggle
              paused={paused || inspecting}
              inspectingOnly={inspecting && !paused}
              title={inspecting ? 'paused while inspecting — closing the message resumes' : undefined}
              onClick={onPlayPauseClick}
            />
          ) : (
            // Attached but live has died: this alarm is honest (new data
            // is genuinely missing) — keep today's aging/alarm tiers.
            <StalenessChip asOf={newestTsMs} failed={false} />
          )
        ) : (
          // Detached (design spec v1.3, owner ruling 2026-08-15): the
          // toggle IS the mode signal — always shown lit paused (live
          // rendering is off by definition while detached, regardless of
          // pauseReason, which keeps driving buffering underneath), no
          // pulsing "● live", and no staleness chip — a historical page is
          // immutable, there is nothing to be stale about. Clicking the
          // toggle here jumps to now (onPlayPauseClick), same as the pill.
          <PlayPauseToggle paused onClick={onPlayPauseClick} />
        )}
        <TimeZoneToggle />
      </div>
    </div>
  )
}
