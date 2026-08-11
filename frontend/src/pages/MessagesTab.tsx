import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMessages } from '../api/client'
import { tailTopic } from '../api/sse'
import type { BrowseAnchor, MessageOut } from '../api/types'
import { MessageList } from '../components/messages/MessageList'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

export function MessagesTab({ cluster, topic }: { cluster: string; topic: string }) {
  const [mode, setMode] = useState<'browse' | 'tail'>('browse')
  const [anchorKind, setAnchorKind] = useState<'latest' | 'offset' | 'timestamp'>('latest')
  const [partition, setPartition] = useState('')
  const [offset, setOffset] = useState('')
  const [tsMs, setTsMs] = useState('')
  const [limit, setLimit] = useState('50')
  const [anchor, setAnchor] = useState<BrowseAnchor>({ anchor: 'latest', limit: 50 })
  const [tailBuffer, setTailBuffer] = useState<MessageOut[]>([])
  const [tailError, setTailError] = useState<{ text: string; kind: 'error' | 'transport' } | null>(null)
  const tailHandle = useRef<{ close: () => void } | null>(null)

  const messages = useQuery({
    queryKey: ['messages', cluster, topic, anchor],
    queryFn: ({ signal }) => getMessages(cluster, topic, anchor, signal),
  })

  const load = () => {
    setTailError(null)
    const lim = Math.min(Number(limit) || 50, 500)
    if (anchorKind === 'latest') setAnchor({ anchor: 'latest', limit: lim })
    else if (anchorKind === 'offset') setAnchor({ anchor: 'offset', partition: Number(partition) || 0, offset: Number(offset) || 0, limit: lim })
    else setAnchor({ anchor: 'timestamp', ts_ms: Number(tsMs) || 0, limit: lim })
  }

  const stopTail = () => {
    tailHandle.current?.close()
    tailHandle.current = null
    setMode('browse')
  }

  const startTail = () => {
    setTailBuffer([])
    setTailError(null)
    tailHandle.current = tailTopic(cluster, topic, {
      onMessage: (m) => setTailBuffer((b) => [m, ...b].slice(0, 500)),
      onError: (e) => {
        setTailError({ text: `${e.code}: ${e.message}`, kind: 'error' })
        stopTail()
      },
      onTransportError: () => {
        setTailError({ text: 'connection lost — retrying is manual', kind: 'transport' })
        stopTail()
      },
    })
    setMode('tail')
  }

  const toggleTail = () => {
    if (mode === 'tail') stopTail()
    else startTail()
  }

  useEffect(() => {
    return () => {
      tailHandle.current?.close()
      tailHandle.current = null
    }
  }, [])

  const isTailing = mode === 'tail'
  // An error/transport stop freezes the tail buffer on screen (with the error
  // + caption) instead of silently swapping to the possibly-stale browse
  // results underneath. Manual toggle-off (no tailError) is user-initiated
  // and unambiguous, so it returns to the browse view as before.
  const isFrozen = mode === 'browse' && tailError !== null
  const showTailView = isTailing || isFrozen
  const inputCls = 'w-24 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700'
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <select aria-label="anchor" value={anchorKind} onChange={(e) => setAnchorKind(e.target.value as typeof anchorKind)} className={inputCls} disabled={isTailing}>
          <option value="latest">latest</option>
          <option value="offset">offset</option>
          <option value="timestamp">timestamp</option>
        </select>
        {anchorKind === 'offset' && (
          <>
            <input aria-label="partition" value={partition} onChange={(e) => setPartition(e.target.value)} placeholder="partition" className={inputCls} disabled={isTailing} />
            <input aria-label="offset" value={offset} onChange={(e) => setOffset(e.target.value)} placeholder="offset" className={inputCls} disabled={isTailing} />
          </>
        )}
        {anchorKind === 'timestamp' && (
          <input aria-label="timestamp (ms)" value={tsMs} onChange={(e) => setTsMs(e.target.value)} placeholder="epoch ms" className={inputCls} disabled={isTailing} />
        )}
        <input aria-label="limit" value={limit} onChange={(e) => setLimit(e.target.value)} className="w-16 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700" disabled={isTailing} />
        <button onClick={load} disabled={isTailing} className="rounded bg-zinc-900 px-3 py-1 text-sm text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900">Load</button>
        <button
          onClick={toggleTail}
          aria-pressed={isTailing}
          className="rounded border border-zinc-300 px-3 py-1 text-sm dark:border-zinc-700"
        >
          Tail
        </button>
        <div className="ml-auto">
          {isTailing ? (
            <span className="animate-pulse text-emerald-500">● live</span>
          ) : (
            <StalenessChip asOf={messages.data?.as_of ?? null} />
          )}
        </div>
      </div>
      {tailError && (
        <div className="space-y-0.5">
          <p className={tailError.kind === 'error' ? 'text-sm text-red-600 dark:text-red-400' : 'text-sm text-amber-600 dark:text-amber-400'}>
            {tailError.text}
          </p>
          {isFrozen && <p className="text-xs text-zinc-500 dark:text-zinc-400">tail stopped — showing last received messages</p>}
        </div>
      )}
      <Panel
        title={showTailView ? `${tailBuffer.length} messages` : `${messages.data?.messages.length ?? 0} messages`}
        error={showTailView ? null : messages.error}
        loading={showTailView ? false : messages.isPending}
      >
        <MessageList messages={showTailView ? tailBuffer : messages.data?.messages ?? []} />
      </Panel>
    </div>
  )
}
