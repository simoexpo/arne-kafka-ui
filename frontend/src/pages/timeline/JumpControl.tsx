import { useState } from 'react'

export type JumpTarget =
  | { kind: 'now' }
  | { kind: 'beginning' }
  | { kind: 'offset'; partition: number; offset: number }
  | { kind: 'timestamp'; ts_ms: number }

type Expanded = 'none' | 'offset' | 'timestamp'

// Non-negative integers only — Kafka partitions/offsets/epoch-ms are never
// negative or fractional. Empty string is invalid (nothing typed yet).
function parseNonNegativeInt(text: string): number | null {
  if (!/^\d+$/.test(text)) return null
  return Number(text)
}

function pad(n: number): string {
  return String(n).padStart(2, '0')
}

// `<input type="datetime-local">` has NO timezone of its own — its value
// (e.g. "2026-08-15T14:32:10") is always the BROWSER's own local wall-clock
// reading, per the HTML spec. Converting it with plain local `Date`
// components (rather than trusting `new Date(string)` string-parsing, which
// has historically had cross-engine quirks for non-`Z`-suffixed strings) is
// the one correct way to turn that into an absolute epoch-ms instant — the
// same instant `ts_ms` always meant, just entered via a friendlier control.
// The trailing `(?:\.\d+)?` tolerates a fractional-seconds suffix some
// engines echo back on the element's own `.value` (jsdom always does, even
// at `step="1"`, which per spec asks for whole seconds only) — the sub-
// second part is discarded, never fed into the parsed time.
function datetimeLocalToMs(value: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?$/.exec(value)
  if (!match) return null
  const [, y, mo, d, h, mi, s] = match
  return new Date(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), s ? Number(s) : 0).getTime()
}

// Inverse of the above, for the picker's own `value` — keeps the picker and
// the raw ms field showing the SAME instant regardless of which one the
// reader last edited (arrangement chosen here: bidirectional sync, not a
// one-way "picker fills the field then goes stale" affordance — the ms
// field remains the exact-value escape hatch either way).
function msToDatetimeLocal(ms: number): string {
  const d = new Date(ms)
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

export function JumpControl({ onJump }: { onJump: (target: JumpTarget) => void }) {
  const [expanded, setExpanded] = useState<Expanded>('none')
  const [partitionText, setPartitionText] = useState('')
  const [offsetText, setOffsetText] = useState('')
  const [tsText, setTsText] = useState('')

  const partition = parseNonNegativeInt(partitionText)
  const offset = parseNonNegativeInt(offsetText)
  const tsMs = parseNonNegativeInt(tsText)
  const offsetValid = partition !== null && offset !== null
  const tsValid = tsMs !== null

  const applyOffset = () => {
    if (partition === null || offset === null) return
    onJump({ kind: 'offset', partition, offset })
  }
  const applyTimestamp = () => {
    if (tsMs === null) return
    onJump({ kind: 'timestamp', ts_ms: tsMs })
  }

  return (
    <div className="flex items-center gap-1 text-xs">
      <div role="group" aria-label="jump to" className="flex items-center gap-1 rounded-full border border-zinc-300 p-0.5 dark:border-zinc-700">
        <button
          type="button"
          data-testid="jump-now"
          aria-label="jump to now"
          onClick={() => {
            setExpanded('none')
            onJump({ kind: 'now' })
          }}
          className="rounded-full px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
        >
          now
        </button>
        <button
          type="button"
          data-testid="jump-beginning"
          aria-label="jump to beginning"
          onClick={() => {
            setExpanded('none')
            onJump({ kind: 'beginning' })
          }}
          className="rounded-full px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
        >
          beginning
        </button>
        <button
          type="button"
          data-testid="jump-offset"
          aria-label="jump to offset"
          aria-expanded={expanded === 'offset'}
          onClick={() => setExpanded(expanded === 'offset' ? 'none' : 'offset')}
          className="rounded-full px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
        >
          offset…
        </button>
        <button
          type="button"
          data-testid="jump-timestamp"
          aria-label="jump to timestamp"
          aria-expanded={expanded === 'timestamp'}
          onClick={() => setExpanded(expanded === 'timestamp' ? 'none' : 'timestamp')}
          className="rounded-full px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
        >
          timestamp…
        </button>
      </div>

      {expanded === 'offset' && (
        <div className="flex items-center gap-1">
          <input
            data-testid="jump-offset-partition-input"
            aria-label="partition"
            placeholder="partition"
            value={partitionText}
            onChange={(e) => setPartitionText(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyOffset()}
            className="w-16 rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
          />
          <input
            data-testid="jump-offset-value-input"
            aria-label="offset"
            placeholder="offset"
            value={offsetText}
            onChange={(e) => setOffsetText(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyOffset()}
            className="w-20 rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
          />
          <button
            type="button"
            data-testid="jump-offset-apply"
            aria-label="apply offset jump"
            disabled={!offsetValid}
            onClick={applyOffset}
            className="rounded border border-zinc-300 px-2 py-0.5 disabled:opacity-40 dark:border-zinc-700"
          >
            jump
          </button>
        </div>
      )}

      {expanded === 'timestamp' && (
        <div className="flex flex-wrap items-center gap-1">
          <input
            data-testid="jump-timestamp-input"
            aria-label="timestamp (epoch ms)"
            placeholder="epoch ms"
            value={tsText}
            onChange={(e) => setTsText(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyTimestamp()}
            className="w-32 rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
          />
          <input
            type="datetime-local"
            step="1"
            data-testid="jump-timestamp-picker"
            aria-label="pick timestamp (local time)"
            title="Interpreted in your browser's local time zone, then converted to the exact epoch-ms value on the left."
            value={tsMs !== null ? msToDatetimeLocal(tsMs) : ''}
            onChange={(e) => {
              const ms = datetimeLocalToMs(e.target.value)
              if (ms !== null) setTsText(String(ms))
            }}
            className="rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
          />
          <span className="text-[10px] text-zinc-500 dark:text-zinc-400">local</span>
          {tsValid && (
            <span
              data-testid="jump-timestamp-utc-preview"
              className="text-[10px] text-zinc-500 dark:text-zinc-400"
              title="The absolute instant this will jump to — rows are shown in UTC."
            >
              {new Date(tsMs).toISOString()} UTC
            </span>
          )}
          <button
            type="button"
            data-testid="jump-timestamp-apply"
            aria-label="apply timestamp jump"
            disabled={!tsValid}
            onClick={applyTimestamp}
            className="rounded border border-zinc-300 px-2 py-0.5 disabled:opacity-40 dark:border-zinc-700"
          >
            jump
          </button>
        </div>
      )}
    </div>
  )
}
