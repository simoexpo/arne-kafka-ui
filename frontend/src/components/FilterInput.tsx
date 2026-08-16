import { useRef } from 'react'

export function FilterInput({
  value,
  onChange,
  placeholder,
  ariaLabel,
  className = 'w-72',
  fullWidth = false,
}: {
  value: string
  onChange: (value: string) => void
  placeholder: string
  ariaLabel?: string
  className?: string
  fullWidth?: boolean
}) {
  const inputRef = useRef<HTMLInputElement>(null)

  return (
    <div className={`relative ${fullWidth ? 'block w-full' : 'inline-block w-fit'}`}>
      <input
        ref={inputRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        aria-label={ariaLabel}
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
    </div>
  )
}
