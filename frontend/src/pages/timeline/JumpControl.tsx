import { useState } from 'react'
import { DateTimePicker } from '../../components/DateTimePicker'
import { formatTimestamp } from '../../lib/format'
import { useTimeDisplayMode } from '../../lib/timeDisplayMode'

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

export function JumpControl({ onJump, partitionIds = [] }: { onJump: (target: JumpTarget) => void; partitionIds?: number[] }) {
  // UTC/local display toggle (owner ruling 2026-08-15, extended 2026-08-16
  // picker3 feedback round): the preview below AND the picker popover both
  // follow this now — the popover's calendar/columns/internal textbox all
  // interpret and display in whichever zone is currently selected, so the
  // reader always picks off the SAME zone the rows/header are showing. The
  // outer textbox here, however, is the raw epoch-ms number — a zone-free
  // representation, unchanged by the toggle (exactly the original ms field).
  const timeDisplayMode = useTimeDisplayMode()
  const [expanded, setExpanded] = useState<Expanded>('none')
  const [partitionText, setPartitionText] = useState('')
  const [offsetText, setOffsetText] = useState('')
  const [msText, setMsText] = useState('')

  const partition = parseNonNegativeInt(partitionText)
  const offset = parseNonNegativeInt(offsetText)
  const tsMs = parseNonNegativeInt(msText)
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
    <div className="flex items-center gap-1 text-sm">
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
          {partitionIds.length > 0 ? (
            // The partition list is known — offer it instead of free text.
            <select
              data-testid="jump-offset-partition-input"
              aria-label="partition"
              value={partitionText}
              onChange={(e) => setPartitionText(e.target.value)}
              className="rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
            >
              <option value="" disabled>partition</option>
              {partitionIds.map((id) => (
                <option key={id} value={String(id)}>{id}</option>
              ))}
            </select>
          ) : (
            <input
              data-testid="jump-offset-partition-input"
              aria-label="partition"
              placeholder="partition"
              value={partitionText}
              onChange={(e) => setPartitionText(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && applyOffset()}
              className="w-16 rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
            />
          )}
          <input
            data-testid="jump-offset-value-input"
            aria-label="offset"
            placeholder="offset"
            value={offsetText}
            onChange={(e) => setOffsetText(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyOffset()}
            className="w-36 rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
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
          <DateTimePicker
            valueMs={tsMs}
            onChange={(ms) => setMsText(String(ms))}
            ariaLabel="pick timestamp"
            textValue={msText}
            onTextChange={setMsText}
            onTextEnter={applyTimestamp}
            textTestId="jump-timestamp-input"
            textAriaLabel="timestamp (epoch ms)"
            textPlaceholder="epoch ms"
          />
          {tsValid && (
            <span
              data-testid="jump-timestamp-preview"
              className="text-xs text-zinc-500 dark:text-zinc-400"
              title="The absolute instant this will jump to, in the same zone the rows/header are currently showing."
            >
              {formatTimestamp(tsMs, timeDisplayMode)}
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
