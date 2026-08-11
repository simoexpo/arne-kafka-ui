import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMessages } from '../api/client'
import type { BrowseAnchor } from '../api/types'
import { MessageList } from '../components/messages/MessageList'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

export function MessagesTab({ cluster, topic }: { cluster: string; topic: string }) {
  const [anchorKind, setAnchorKind] = useState<'latest' | 'offset' | 'timestamp'>('latest')
  const [partition, setPartition] = useState('')
  const [offset, setOffset] = useState('')
  const [tsMs, setTsMs] = useState('')
  const [limit, setLimit] = useState('50')
  const [anchor, setAnchor] = useState<BrowseAnchor>({ anchor: 'latest', limit: 50 })

  const messages = useQuery({
    queryKey: ['messages', cluster, topic, anchor],
    queryFn: ({ signal }) => getMessages(cluster, topic, anchor, signal),
  })

  const load = () => {
    const lim = Math.min(Number(limit) || 50, 500)
    if (anchorKind === 'latest') setAnchor({ anchor: 'latest', limit: lim })
    else if (anchorKind === 'offset') setAnchor({ anchor: 'offset', partition: Number(partition) || 0, offset: Number(offset) || 0, limit: lim })
    else setAnchor({ anchor: 'timestamp', ts_ms: Number(tsMs) || 0, limit: lim })
  }

  const inputCls = 'w-24 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700'
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <select aria-label="anchor" value={anchorKind} onChange={(e) => setAnchorKind(e.target.value as typeof anchorKind)} className={inputCls}>
          <option value="latest">latest</option>
          <option value="offset">offset</option>
          <option value="timestamp">timestamp</option>
        </select>
        {anchorKind === 'offset' && (
          <>
            <input aria-label="partition" value={partition} onChange={(e) => setPartition(e.target.value)} placeholder="partition" className={inputCls} />
            <input aria-label="offset" value={offset} onChange={(e) => setOffset(e.target.value)} placeholder="offset" className={inputCls} />
          </>
        )}
        {anchorKind === 'timestamp' && (
          <input aria-label="timestamp (ms)" value={tsMs} onChange={(e) => setTsMs(e.target.value)} placeholder="epoch ms" className={inputCls} />
        )}
        <input aria-label="limit" value={limit} onChange={(e) => setLimit(e.target.value)} className="w-16 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700" />
        <button onClick={load} className="rounded bg-zinc-900 px-3 py-1 text-sm text-white dark:bg-zinc-100 dark:text-zinc-900">Load</button>
        <div className="ml-auto"><StalenessChip asOf={messages.data?.as_of ?? null} /></div>
      </div>
      <Panel title={`${messages.data?.messages.length ?? 0} messages`} error={messages.error} loading={messages.isPending}>
        <MessageList messages={messages.data?.messages ?? []} />
      </Panel>
    </div>
  )
}
