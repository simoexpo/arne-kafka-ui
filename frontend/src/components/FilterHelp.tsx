import { useEffect, useRef, useState } from 'react'

// "?" affordance for the message filter box: the filter's inline syntax
// (`:` contains, `=` equals, `value.path` fields, `"…"` literal escape) is
// invisible until you know it exists — this popover is where a user
// discovers it. Same dismissal contract as the datetime picker popover:
// Escape or clicking anywhere outside.
export function FilterHelp() {
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    const onClick = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('mousedown', onClick)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('mousedown', onClick)
    }
  }, [open])

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        data-testid="filter-help"
        aria-label="filter syntax help"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        className="flex h-6 w-6 items-center justify-center rounded-full border border-zinc-300 text-xs text-zinc-500 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
      >
        ?
      </button>
      {open && (
        <div
          data-testid="filter-help-popover"
          className="absolute right-0 top-8 z-10 w-[28rem] rounded border border-zinc-300 bg-white p-3 text-xs shadow-lg dark:border-zinc-700 dark:bg-zinc-950"
        >
          <h3 className="mb-2 text-sm font-medium text-zinc-500 dark:text-zinc-400">Filter syntax</h3>
          <dl className="space-y-1.5">
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">anything</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — matches if the key or value contains it</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">key:order</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — only the key contains it (value: searches only the value)</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">key=order-42</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — equals exactly; != means differs; value= compares JSON by content</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value.customer.tier=gold</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — a field inside the JSON value, dot after dot</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value.total&gt;=99.5</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — numeric compare: &gt; &gt;= &lt; &lt;=</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value.items[].sku=abc</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — [] tries every array element: true if ANY matches (items.0 picks one by index)</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value."ship.zone"=EU</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — quote a field name containing . : or =</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">"key:order"</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — search that literal text (the outer quotes are stripped)</dd></div>
          </dl>
          <p className="mt-2 text-zinc-500">All matching is case-insensitive. Content that can't be read — undecodable values, missing fields, non-numbers in comparisons — never matches.</p>
        </div>
      )}
    </div>
  )
}
