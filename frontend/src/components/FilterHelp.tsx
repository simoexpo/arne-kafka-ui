import { useEffect, useRef, useState } from 'react'

// "?" affordance for the message filter box: the filter's inline syntax
// (key:/value:/json-path=) is invisible until you know it exists — this
// popover is where a user discovers it. Same dismissal contract as the
// datetime picker popover: Escape or clicking anywhere outside.
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
          className="absolute right-0 top-8 z-10 w-80 rounded border border-zinc-300 bg-white p-3 text-xs shadow-lg dark:border-zinc-700 dark:bg-zinc-900"
        >
          <h3 className="mb-2 text-sm font-medium text-zinc-500 dark:text-zinc-400">Filter syntax</h3>
          <dl className="space-y-1.5">
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">anything</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — messages whose key or value contains it (case-insensitive)</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">key:foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — key contains foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value:foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — value contains foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">path.to.field=42</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — JSON field equals the value</dd></div>
          </dl>
        </div>
      )}
    </div>
  )
}
