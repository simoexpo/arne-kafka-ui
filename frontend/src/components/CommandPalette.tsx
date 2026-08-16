import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { getClusters, getGroups, getTopics } from '../api/client'

export interface PaletteItem {
  label: string
  kind: 'cluster' | 'topic' | 'group'
  to: string
}

export function PaletteView({ items, onNavigate, onClose }: {
  items: PaletteItem[]
  onNavigate: (to: string) => void
  onClose: () => void
}) {
  const [query, setQuery] = useState('')
  const matches = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (q === '') return items.slice(0, 12)
    return items.filter((i) => i.label.toLowerCase().includes(q)).slice(0, 12)
  }, [items, query])

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center bg-black/40 pt-24" onClick={onClose}>
      <div
        className="w-[32rem] rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
        onClick={(e) => e.stopPropagation()}
      >
        <input
          autoFocus
          role="combobox"
          aria-expanded={matches.length > 0}
          aria-controls="palette-list"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') onClose()
            if (e.key === 'Enter' && matches[0]) {
              onNavigate(matches[0].to)
              onClose()
            }
          }}
          placeholder="jump to topic, group, cluster…"
          className="w-full border-b border-zinc-200 bg-transparent px-4 py-3 text-sm outline-none dark:border-zinc-700"
        />
        <ul id="palette-list" role="listbox" className="max-h-80 overflow-y-auto p-1">
          {matches.map((m) => (
            <li key={`${m.kind}:${m.to}`} role="option" aria-selected={false}>
              <button
                className="flex w-full items-center gap-2 rounded px-3 py-1.5 text-left text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800"
                onClick={() => { onNavigate(m.to); onClose() }}
              >
                <span className="w-14 text-xs uppercase text-zinc-400">{m.kind}</span>
                <span className="font-mono">{m.label}</span>
              </button>
            </li>
          ))}
          {matches.length === 0 && <li className="px-3 py-2 text-sm text-zinc-500">no matches</li>}
        </ul>
      </div>
    </div>
  )
}

export function CommandPalette({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false)
  const navigate = useNavigate()

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setOpen((o) => !o)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  const enc = encodeURIComponent
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => getClusters(signal), enabled: open })
  const topics = useQuery({ queryKey: ['topics', cluster], queryFn: ({ signal }) => getTopics(cluster, signal), enabled: open })
  const groups = useQuery({ queryKey: ['groups', cluster], queryFn: ({ signal }) => getGroups(cluster, signal), enabled: open })

  if (!open) return null
  const items: PaletteItem[] = [
    ...(clusters.data?.clusters ?? []).map((c) => ({
      label: c.name, kind: 'cluster' as const, to: `/c/${enc(c.name)}/overview`,
    })),
    ...(topics.data?.topics ?? []).filter((t) => !t.internal).map((t) => ({
      label: t.name, kind: 'topic' as const, to: `/c/${enc(cluster)}/topics/${enc(t.name)}`,
    })),
    ...(groups.data?.groups ?? []).map((g) => ({
      label: g.group_id, kind: 'group' as const, to: `/c/${enc(cluster)}/groups/${enc(g.group_id)}`,
    })),
  ]
  return (
    <PaletteView
      items={items}
      onClose={() => setOpen(false)}
      onNavigate={(to) => navigate({ to })}
    />
  )
}
