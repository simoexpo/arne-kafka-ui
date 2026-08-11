import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMessages } from '../api/client'
import { searchTopic, tailTopic } from '../api/sse'
import type { BrowseAnchor, MessageOut, SearchFilter, SearchProgress, SearchRange } from '../api/types'
import { MessageList } from '../components/messages/MessageList'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'

type SearchStatus = 'running' | 'complete' | 'max_matches' | 'cancelled' | 'error'
interface SearchState {
  results: MessageOut[]
  progress: SearchProgress | null
  status: SearchStatus
  error?: string
}
const initialSearchState: SearchState = { results: [], progress: null, status: 'running' }

export function MessagesTab({ cluster, topic }: { cluster: string; topic: string }) {
  const [mode, setMode] = useState<'browse' | 'tail' | 'search'>('browse')
  const [anchorKind, setAnchorKind] = useState<'latest' | 'offset' | 'timestamp'>('latest')
  const [partition, setPartition] = useState('')
  const [offset, setOffset] = useState('')
  const [tsMs, setTsMs] = useState('')
  const [limit, setLimit] = useState('50')
  const [anchor, setAnchor] = useState<BrowseAnchor>({ anchor: 'latest', limit: 50 })
  const [tailBuffer, setTailBuffer] = useState<MessageOut[]>([])
  const [tailError, setTailError] = useState<{ text: string; kind: 'error' | 'transport' } | null>(null)
  const tailHandle = useRef<{ close: () => void } | null>(null)

  const [filterKind, setFilterKind] = useState<'none' | 'key_eq' | 'key_contains' | 'value_contains' | 'json_eq'>('none')
  const [filterQ, setFilterQ] = useState('')
  const [filterPath, setFilterPath] = useState('')
  const [rangeKind, setRangeKind] = useState<'last_n' | 'offsets' | 'ts'>('last_n')
  const [rangeN, setRangeN] = useState('1000')
  const [rangeFrom, setRangeFrom] = useState('')
  const [rangeTo, setRangeTo] = useState('')
  const [rangeFromMs, setRangeFromMs] = useState('')
  const [rangeToMs, setRangeToMs] = useState('')
  const [searchState, setSearchState] = useState<SearchState>(initialSearchState)
  const searchHandle = useRef<{ close: () => void } | null>(null)

  const messages = useQuery({
    queryKey: ['messages', cluster, topic, anchor],
    queryFn: ({ signal }) => getMessages(cluster, topic, anchor, signal),
  })

  // Starting any of Load/Tail/Search must close whatever stream is currently
  // open — only one of tail/search can be live at a time. Idempotent: safe
  // to call when nothing is open.
  const stopStreams = () => {
    tailHandle.current?.close()
    tailHandle.current = null
    searchHandle.current?.close()
    searchHandle.current = null
  }

  const load = () => {
    stopStreams()
    setTailError(null)
    setSearchState(initialSearchState)
    setMode('browse')
    const lim = Math.min(Number(limit) || 50, 500)
    if (anchorKind === 'latest') setAnchor({ anchor: 'latest', limit: lim })
    else if (anchorKind === 'offset') setAnchor({ anchor: 'offset', partition: Number(partition) || 0, offset: Number(offset) || 0, limit: lim })
    else setAnchor({ anchor: 'timestamp', ts_ms: Number(tsMs) || 0, limit: lim })
  }

  const stopTail = () => {
    stopStreams()
    setMode('browse')
  }

  const startTail = () => {
    stopStreams()
    setTailBuffer([])
    setTailError(null)
    setSearchState(initialSearchState)
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

  const buildSearchRange = (): SearchRange => {
    if (rangeKind === 'last_n') return { range: 'last_n', n: Number(rangeN) || 1000 }
    if (rangeKind === 'offsets') return { range: 'offsets', from: Number(rangeFrom) || 0, to: Number(rangeTo) || 0 }
    return { range: 'ts', from_ms: Number(rangeFromMs) || 0, ...(rangeToMs ? { to_ms: Number(rangeToMs) } : {}) }
  }

  const buildSearchFilter = (): SearchFilter =>
    filterKind === 'json_eq'
      ? { filter: 'json_eq', q: filterQ, path: filterPath }
      : { filter: filterKind as 'key_eq' | 'key_contains' | 'value_contains', q: filterQ }

  const runSearch = () => {
    stopStreams()
    setTailError(null)
    setSearchState({ results: [], progress: null, status: 'running' })
    searchHandle.current = searchTopic(cluster, topic, buildSearchRange(), buildSearchFilter(), {
      onProgress: (p) => setSearchState((s) => ({ ...s, progress: p })),
      onMatch: (m) => setSearchState((s) => ({ ...s, results: [...s.results, m] })),
      onDone: (reason) => setSearchState((s) => ({ ...s, status: reason === 'max_matches' ? 'max_matches' : 'complete' })),
      onError: (e) => setSearchState((s) => ({ ...s, status: 'error', error: `${e.code}: ${e.message}` })),
      onTransportError: () => setSearchState((s) => ({ ...s, status: 'error', error: 'connection lost — retrying is manual' })),
    })
    setMode('search')
  }

  const cancelSearch = () => {
    searchHandle.current?.close()
    searchHandle.current = null
    setSearchState((s) => ({ ...s, status: 'cancelled' }))
  }

  useEffect(() => {
    return () => stopStreams()
  }, [])

  const isTailing = mode === 'tail'
  const isSearching = mode === 'search'
  const isSearchRunning = isSearching && searchState.status === 'running'
  // An error/transport stop freezes the tail buffer on screen (with the error
  // + caption) instead of silently swapping to the possibly-stale browse
  // results underneath. Manual toggle-off (no tailError) is user-initiated
  // and unambiguous, so it returns to the browse view as before.
  const isFrozen = mode === 'browse' && tailError !== null
  const showTailView = isTailing || isFrozen
  const inputCls = 'w-24 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700'
  const canSearch = filterKind !== 'none' && filterQ.trim() !== ''
  const searchStatusText =
    searchState.status === 'complete'
      ? 'complete'
      : searchState.status === 'max_matches'
        ? 'max matches reached — refine your filter'
        : searchState.status === 'cancelled'
          ? 'cancelled'
          : searchState.status === 'error'
            ? (searchState.error ?? 'error')
            : ''
  const panelTitle = isSearching
    ? `${searchState.results.length} search results`
    : showTailView
      ? `${tailBuffer.length} messages`
      : `${messages.data?.messages.length ?? 0} messages`
  const panelMessages = isSearching ? searchState.results : showTailView ? tailBuffer : messages.data?.messages ?? []
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
        <button onClick={load} disabled={isTailing || isSearchRunning} className="rounded bg-zinc-900 px-3 py-1 text-sm text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900">Load</button>
        <button
          onClick={toggleTail}
          aria-pressed={isTailing}
          disabled={isSearchRunning}
          className="rounded border border-zinc-300 px-3 py-1 text-sm disabled:opacity-50 dark:border-zinc-700"
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
      <div className="flex flex-wrap items-center gap-2">
        <select aria-label="filter" value={filterKind} onChange={(e) => setFilterKind(e.target.value as typeof filterKind)} className={inputCls}>
          <option value="none">none</option>
          <option value="key_eq">key_eq</option>
          <option value="key_contains">key_contains</option>
          <option value="value_contains">value_contains</option>
          <option value="json_eq">json_eq</option>
        </select>
        <input aria-label="query" value={filterQ} onChange={(e) => setFilterQ(e.target.value)} placeholder="query" className={inputCls} />
        {filterKind === 'json_eq' && (
          <input aria-label="json path" value={filterPath} onChange={(e) => setFilterPath(e.target.value)} placeholder="json path" className={inputCls} />
        )}
        <select aria-label="range" value={rangeKind} onChange={(e) => setRangeKind(e.target.value as typeof rangeKind)} className={inputCls}>
          <option value="last_n">last_n</option>
          <option value="offsets">offsets</option>
          <option value="ts">ts</option>
        </select>
        {rangeKind === 'last_n' && (
          <input aria-label="n" value={rangeN} onChange={(e) => setRangeN(e.target.value)} className="w-16 rounded border border-zinc-300 bg-transparent px-2 py-1 text-sm dark:border-zinc-700" />
        )}
        {rangeKind === 'offsets' && (
          <>
            <input aria-label="from" value={rangeFrom} onChange={(e) => setRangeFrom(e.target.value)} className={inputCls} />
            <input aria-label="to" value={rangeTo} onChange={(e) => setRangeTo(e.target.value)} className={inputCls} />
          </>
        )}
        {rangeKind === 'ts' && (
          <>
            <input aria-label="from (ms)" value={rangeFromMs} onChange={(e) => setRangeFromMs(e.target.value)} className={inputCls} />
            <input aria-label="to (ms)" value={rangeToMs} onChange={(e) => setRangeToMs(e.target.value)} className={inputCls} />
          </>
        )}
        <button onClick={runSearch} disabled={!canSearch} className="rounded bg-zinc-900 px-3 py-1 text-sm text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900">
          Search
        </button>
      </div>
      {tailError && (
        <div className="space-y-0.5">
          <p className={tailError.kind === 'error' ? 'text-sm text-red-600 dark:text-red-400' : 'text-sm text-amber-600 dark:text-amber-400'}>
            {tailError.text}
          </p>
          {isFrozen && <p className="text-xs text-zinc-500 dark:text-zinc-400">tail stopped — showing last received messages</p>}
        </div>
      )}
      {isSearching && (
        <div className="space-y-1">
          {searchState.progress && (
            <div className="flex items-center gap-2">
              <progress max={searchState.progress.total} value={searchState.progress.scanned} />
              <span className="text-sm text-zinc-600 dark:text-zinc-400">
                {`${searchState.progress.scanned}/${searchState.progress.total} scanned · ${searchState.progress.matches} matches`}
              </span>
            </div>
          )}
          {searchState.status === 'running' ? (
            <button onClick={cancelSearch} className="rounded bg-red-600 px-4 py-1 text-sm font-medium text-white">
              Cancel
            </button>
          ) : (
            <p className={searchState.status === 'error' ? 'text-sm text-red-600 dark:text-red-400' : 'text-sm text-zinc-600 dark:text-zinc-400'}>
              {searchStatusText}
            </p>
          )}
        </div>
      )}
      <Panel
        title={panelTitle}
        error={isSearching || showTailView ? null : messages.error}
        loading={isSearching || showTailView ? false : messages.isPending}
      >
        <MessageList messages={panelMessages} />
      </Panel>
    </div>
  )
}
