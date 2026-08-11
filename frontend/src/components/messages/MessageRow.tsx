import { useState } from 'react'
import type { MessageOut } from '../../api/types'
import { EncodingBadge } from './EncodingBadge'
import { PayloadView } from './PayloadView'

export function MessageRow({ message }: { message: MessageOut }) {
  const [open, setOpen] = useState(false)
  const ts = message.timestamp_ms === null ? '—' : new Date(message.timestamp_ms).toISOString()
  const preview = message.value === null ? '∅ null' : message.value.text.replaceAll('\n', ' ')
  const isError = message.value?.encoding === 'decode_error'
  return (
    <div
      data-testid="message-row"
      onClick={() => setOpen((o) => !o)}
      className="cursor-pointer border-b border-zinc-100 px-2 py-1.5 font-mono text-xs hover:bg-zinc-50 dark:border-zinc-800 dark:hover:bg-zinc-900"
    >
      <div className="flex items-center gap-3">
        <span className="whitespace-nowrap text-zinc-500">{ts}</span>
        <span className="whitespace-nowrap text-zinc-500">{`p${message.partition}·${message.offset}`}</span>
        <span className="max-w-40 truncate">{message.key?.text ?? '∅'}</span>
        <span className={`min-w-0 flex-1 truncate ${isError ? 'text-red-600 dark:text-red-400' : ''}`}>
          {isError ? message.value?.error : preview}
        </span>
        {message.value && <EncodingBadge encoding={message.value.encoding} />}
      </div>
      {open && (
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
