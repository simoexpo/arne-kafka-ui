import type { MessageOut } from '../../api/types'
import { formatTimestamp } from '../../lib/format'
import { useTimeDisplayMode } from '../../lib/timeDisplayMode'
import { EncodingBadge } from './EncodingBadge'
import { PayloadView } from './PayloadView'

export function MessageRow({
  message,
  tsInverted = false,
  isJumpTarget = false,
  expanded,
  onToggle,
}: {
  message: MessageOut
  tsInverted?: boolean
  isJumpTarget?: boolean
  // Fix: expansion state survives virtualization, owned by identity — this
  // used to be local `useState`, which a virtualized row scrolled out of the
  // renderer's overscan (routine, not exotic) loses on unmount: the row
  // silently un-expanded, and the count Timeline kept of open inspections
  // leaked by one forever (recoverable only via an unrelated explicit pill/
  // toggle click, with no visible reason why). MessageRow is now a fully
  // CONTROLLED component — Timeline (via MessageList) owns the actual
  // expanded/collapsed truth, keyed by (partition, offset) identity, so a
  // remounted row simply re-derives the same answer from that owner instead
  // of starting over.
  expanded: boolean
  onToggle: () => void
}) {
  const timeDisplayMode = useTimeDisplayMode()
  const ts = message.timestamp_ms === null ? '—' : formatTimestamp(message.timestamp_ms, timeDisplayMode)
  const preview = message.value === null ? '∅ null' : message.value.text.replaceAll('\n', ' ')
  const isError = message.value?.encoding === 'decode_error'
  return (
    <div
      data-testid="message-row"
      onClick={onToggle}
      className={`cursor-pointer border-b border-zinc-100 px-2 py-1.5 font-mono text-sm dark:border-zinc-800 ${
        isJumpTarget
          ? 'bg-emerald-50 hover:bg-emerald-100 dark:bg-emerald-950/40 dark:hover:bg-emerald-950/60'
          : 'hover:bg-zinc-50 dark:hover:bg-zinc-900'
      }`}
    >
      <div className="flex items-center gap-3">
        {isJumpTarget && (
          <span
            data-testid="jump-target"
            role="img"
            aria-label="jump target"
            title={`Jump target: p${message.partition}·${message.offset}`}
            className="h-1.5 w-1.5 shrink-0 rounded-full bg-emerald-500"
          />
        )}
        <span className="whitespace-nowrap text-zinc-500">{ts}</span>
        {tsInverted && (
          <svg
            data-testid="ts-inversion"
            role="img"
            viewBox="0 0 16 16"
            fill="currentColor"
            className="h-3.5 w-3.5 shrink-0 text-amber-500"
          >
            <title>
              Out-of-order timestamp: newer than the message above it. Messages in
              the same partition keep their same-partition order (by offset), even
              when producer timestamps disagree.
            </title>
            <path d="M8 1.5 15 14H1L8 1.5Zm0 4.5a.75.75 0 0 0-.75.75v3a.75.75 0 0 0 1.5 0v-3A.75.75 0 0 0 8 6Zm0 6a1 1 0 1 0 0 2 1 1 0 0 0 0-2Z" />
          </svg>
        )}
        <span className="whitespace-nowrap text-zinc-500">{`p${message.partition}·${message.offset}`}</span>
        <span className="max-w-40 truncate">{message.key?.text ?? '∅'}</span>
        <span className={`min-w-0 flex-1 truncate ${isError ? 'text-red-600 dark:text-red-400' : ''}`}>
          {isError ? message.value?.error : preview}
        </span>
        {message.value && <EncodingBadge encoding={message.value.encoding} />}
      </div>
      {expanded && (
        <div className="mt-2 grid grid-cols-1 gap-3 md:grid-cols-2" onClick={(e) => e.stopPropagation()}>
          <div>
            <div className="mb-1 text-zinc-500">key</div>
            <PayloadView payload={message.key} />
          </div>
          <div>
            <div className="mb-1 text-zinc-500">value</div>
            <PayloadView payload={message.value} />
          </div>
          <div className="md:col-span-2">
            <div className="mb-1 text-zinc-500">headers</div>
            {message.headers.length === 0 && <span className="text-zinc-400">no headers</span>}
            {message.headers.map((h, i) => (
              <div key={i}>
                <span className="text-sky-800 dark:text-sky-300">{h.key}</span>
                <span className="text-zinc-400">: </span>
                {h.value}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
