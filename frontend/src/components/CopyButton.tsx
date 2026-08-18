import { useEffect, useRef, useState } from 'react'

function CopyIcon() {
  return (
    <svg viewBox="0 0 16 16" width="13" height="13" className="text-zinc-500 dark:text-zinc-400">
      <rect x="2" y="2" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.2" />
      <rect x="5" y="5" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.2" />
    </svg>
  )
}

function CheckIcon() {
  return (
    <svg viewBox="0 0 16 16" width="13" height="13" className="text-emerald-600 dark:text-emerald-400">
      <path
        d="M3 8.5 6.5 12 13 4.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

export function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false)
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)

  useEffect(() => () => clearTimeout(timeoutRef.current), [])

  return (
    // relative + the absolutely-positioned hint below: the transient
    // "copied" text must never occupy layout space — inside a table cell
    // it used to widen the column for 1.5s on every click.
    <span className="relative inline-flex items-center">
      <button
        type="button"
        aria-label={`copy ${label}`}
        onClick={(e) => {
          e.stopPropagation()
          e.preventDefault()
          void navigator.clipboard.writeText(text)
          setCopied(true)
          clearTimeout(timeoutRef.current)
          timeoutRef.current = setTimeout(() => setCopied(false), 1500)
        }}
        className="rounded p-0.5 text-zinc-400 hover:bg-zinc-100 hover:text-zinc-700 dark:text-zinc-500 dark:hover:bg-zinc-800 dark:hover:text-zinc-200"
      >
        {copied ? <CheckIcon /> : <CopyIcon />}
      </button>
      {copied && (
        // A balloon ABOVE the icon: to the right there is always a
        // neighbor (title chips, table columns) to collide with; above is
        // dead space in both. The solid background makes the overlay read
        // as a tooltip, not a glitch.
        <span className="absolute bottom-full left-1/2 z-10 mb-1 -translate-x-1/2 whitespace-nowrap rounded bg-zinc-800 px-1.5 py-0.5 text-xs text-emerald-400 shadow-sm dark:bg-zinc-200 dark:text-emerald-700">
          copied
        </span>
      )}
    </span>
  )
}
