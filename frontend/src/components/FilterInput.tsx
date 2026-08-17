import { useEffect, useId, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'

export function FilterInput({
  value,
  onChange,
  placeholder,
  ariaLabel,
  className = 'w-72',
  fullWidth = false,
  proposals,
  onOpenChange,
}: {
  value: string
  onChange: (value: string) => void
  placeholder: string
  ariaLabel?: string
  className?: string
  fullWidth?: boolean
  // Autocomplete rows for the current text (design spec 2026-08-17). Without
  // this prop the input has no combobox semantics at all — the other filter
  // boxes (topics, groups) stay plain inputs.
  proposals?: (text: string) => readonly string[]
  // Fires when the dropdown opens/closes — the parent holds the filter
  // while it's open (spec "Hold while composing").
  onOpenChange?: (open: boolean) => void
}) {
  const inputRef = useRef<HTMLInputElement>(null)
  const listId = useId()
  const [focused, setFocused] = useState(false)
  const [dismissed, setDismissed] = useState(false)
  const [highlight, setHighlight] = useState(0)

  const rows = proposals && focused && !dismissed ? proposals(value) : []
  const open = rows.length > 0
  const highlightClamped = Math.min(highlight, rows.length - 1)

  useEffect(() => {
    onOpenChange?.(open)
  }, [open, onOpenChange])

  const accept = (row: string) => {
    onChange(row)
    setHighlight(0)
    inputRef.current?.focus()
  }

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (!open) return
    // Step from the CLAMPED position: rows can shrink under a stale raw
    // highlight (field set changed while open), and stepping from the raw
    // value would jump arbitrarily instead of moving one visible row.
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setHighlight((h) => (Math.min(h, rows.length - 1) + 1) % rows.length)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setHighlight((h) => (Math.min(h, rows.length - 1) - 1 + rows.length) % rows.length)
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault()
      accept(rows[highlightClamped])
    } else if (e.key === 'Escape') {
      setDismissed(true)
    }
  }

  return (
    <div className={`relative ${fullWidth ? 'block w-full' : 'inline-block w-fit'}`}>
      <input
        ref={inputRef}
        value={value}
        onChange={(e) => {
          setDismissed(false)
          setHighlight(0)
          onChange(e.target.value)
        }}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        aria-label={ariaLabel}
        {...(proposals
          ? {
              role: 'combobox',
              'aria-expanded': open,
              'aria-controls': open ? listId : undefined,
              'aria-activedescendant': open ? `${listId}-${highlightClamped}` : undefined,
            }
          : {})}
        className={`${className} rounded border border-zinc-300 bg-transparent px-3 py-1.5 pr-7 text-sm outline-none focus:border-zinc-500 dark:border-zinc-700`}
      />
      {value !== '' && (
        <button
          type="button"
          aria-label="clear filter"
          onClick={() => {
            onChange('')
            inputRef.current?.focus()
          }}
          className="absolute right-1.5 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-700 dark:text-zinc-500 dark:hover:text-zinc-300"
        >
          ×
        </button>
      )}
      {open && (
        <ul
          id={listId}
          role="listbox"
          className="absolute left-0 top-full z-10 mt-1 w-full rounded border border-zinc-300 bg-white py-1 text-sm shadow-lg dark:border-zinc-700 dark:bg-zinc-950"
        >
          {rows.map((row, i) => (
            <li
              key={row}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={i === highlightClamped}
              data-testid="filter-proposal"
              // mousedown would blur the input and close the list before the
              // click lands — swallow it so the click's accept() wins.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => accept(row)}
              className={`cursor-pointer px-3 py-1 font-mono ${i === highlightClamped ? 'bg-zinc-100 dark:bg-zinc-800' : ''}`}
            >
              {row}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
