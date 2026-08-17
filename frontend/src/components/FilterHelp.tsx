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
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">anything</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — messages whose key or value contains it</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">key:foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — key contains foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">key=foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — key equals foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value:foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — value contains foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value=foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — value equals foo (JSON compared by content)</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value.path.to.field:foo</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — that field contains foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value.path.to.field=42</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — that field equals 42</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">"key:foo"</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — search the literal text key:foo</dd></div>
            <div><dt className="inline font-mono text-sky-800 dark:text-sky-300">value."field.with.dots"=x</dt><dd className="inline text-zinc-600 dark:text-zinc-400"> — quote a field name containing . : or =</dd></div>
          </dl>
          <p className="mt-2 text-zinc-500">All matching is case-insensitive.</p>
          <p className="mt-1 text-zinc-500">Text that itself starts and ends with a quote can't be searched literally — the outer quotes are always stripped.</p>
        </div>
      )}
    </div>
  )
}
