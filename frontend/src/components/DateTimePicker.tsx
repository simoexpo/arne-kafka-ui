import { useEffect, useRef, useState } from 'react'

// Owner ruling 2026-08-15 (picker2, superseding an earlier native-popup
// `color-scheme` attempt): verified empirically that Chrome's own
// `datetime-local` calendar popup is painted from the OS/browser's
// `prefers-color-scheme`, NOT from any page-authored CSS `color-scheme` —
// setting it on the input (or even the document root) changes what
// `getComputedStyle` reports but has ZERO effect on the popup's actual
// rendered colors. There is no CSS lever left to pull. This component
// replaces the native picker outright with our own popover, styled
// entirely from Betrachtung's own `dark:` classes — nothing here is at
// the mercy of the browser's opinion.

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
]
const WEEKDAY_LABELS = ['M', 'T', 'W', 'T', 'F', 'S', 'S'] // Monday-first

function pad2(n: number): string {
  return String(n).padStart(2, '0')
}

function formatTrigger(ms: number | null): string {
  if (ms === null) return ''
  const d = new Date(ms)
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`
}

function daysInMonth(year: number, month: number): number {
  return new Date(year, month + 1, 0).getDate()
}

// `Date#getDay()` is Sunday-first (0-6); the grid below is Monday-first to
// match the layout readers already know from the native picker it replaces.
function leadingBlankCount(year: number, month: number): number {
  return (new Date(year, month, 1).getDay() + 6) % 7
}

function parseTimeField(text: string, max: number): number | null {
  if (!/^\d{1,2}$/.test(text)) return null
  const n = Number(text)
  return n >= 0 && n <= max ? n : null
}

export function DateTimePicker({ valueMs, onChange, ariaLabel }: {
  valueMs: number | null
  onChange: (ms: number) => void
  ariaLabel: string
}) {
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  const [viewYear, setViewYear] = useState(0)
  const [viewMonth, setViewMonth] = useState(0)
  const [selectedDay, setSelectedDay] = useState(1)
  const [hourText, setHourText] = useState('00')
  const [minuteText, setMinuteText] = useState('00')
  const [secondText, setSecondText] = useState('00')

  const openPopover = () => {
    const seed = valueMs !== null ? new Date(valueMs) : new Date()
    setViewYear(seed.getFullYear())
    setViewMonth(seed.getMonth())
    setSelectedDay(seed.getDate())
    setHourText(pad2(seed.getHours()))
    setMinuteText(pad2(seed.getMinutes()))
    setSecondText(pad2(seed.getSeconds()))
    setOpen(true)
  }

  // Outside click / Escape both DISCARD any in-popover picks — only Apply
  // commits. Listening on the document (not a blur handler) is what makes
  // "click outside" work at all: the popover isn't a native focusable
  // control, so nothing here naturally loses focus when the reader clicks
  // elsewhere on the page.
  useEffect(() => {
    if (!open) return
    const onDocMouseDown = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setOpen(false)
    }
    const onDocKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDocMouseDown)
    document.addEventListener('keydown', onDocKeyDown)
    return () => {
      document.removeEventListener('mousedown', onDocMouseDown)
      document.removeEventListener('keydown', onDocKeyDown)
    }
  }, [open])

  const goPrevMonth = () => {
    const d = new Date(viewYear, viewMonth - 1, 1)
    setViewYear(d.getFullYear())
    setViewMonth(d.getMonth())
  }
  const goNextMonth = () => {
    const d = new Date(viewYear, viewMonth + 1, 1)
    setViewYear(d.getFullYear())
    setViewMonth(d.getMonth())
  }

  const hour = parseTimeField(hourText, 23)
  const minute = parseTimeField(minuteText, 59)
  const second = parseTimeField(secondText, 59)
  const timeValid = hour !== null && minute !== null && second !== null

  const apply = () => {
    if (!timeValid) return
    onChange(new Date(viewYear, viewMonth, selectedDay, hour, minute, second).getTime())
    setOpen(false)
  }

  const monthLen = daysInMonth(viewYear, viewMonth)
  const leadBlanks = leadingBlankCount(viewYear, viewMonth)
  const today = new Date()
  const isToday = (day: number) =>
    today.getFullYear() === viewYear && today.getMonth() === viewMonth && today.getDate() === day

  return (
    <div className="relative inline-block" ref={containerRef}>
      <input
        type="text"
        readOnly
        data-testid="datetime-picker-trigger"
        aria-label={ariaLabel}
        placeholder="yyyy-mm-dd hh:mm:ss"
        value={formatTrigger(valueMs)}
        onClick={() => (open ? setOpen(false) : openPopover())}
        className="w-36 cursor-pointer rounded border border-zinc-300 px-1 py-0.5 dark:border-zinc-700 dark:bg-zinc-900"
      />
      {open && (
        <div
          data-testid="datetime-picker-popover"
          className="absolute z-20 mt-1 w-60 rounded-lg border border-zinc-200 bg-white p-2 text-xs shadow-lg dark:border-zinc-800 dark:bg-zinc-900"
        >
          <div className="mb-1 flex items-center justify-between">
            <button
              type="button"
              aria-label="previous month"
              onClick={goPrevMonth}
              className="rounded px-1.5 py-0.5 text-zinc-500 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
            >
              ‹
            </button>
            <span className="font-medium">{MONTH_NAMES[viewMonth]} {viewYear}</span>
            <button
              type="button"
              aria-label="next month"
              onClick={goNextMonth}
              className="rounded px-1.5 py-0.5 text-zinc-500 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
            >
              ›
            </button>
          </div>
          <div className="grid grid-cols-7 gap-0.5 pb-1 text-center text-[10px] text-zinc-400 dark:text-zinc-500">
            {WEEKDAY_LABELS.map((w, i) => <span key={i}>{w}</span>)}
          </div>
          <div className="grid grid-cols-7 gap-0.5 text-center">
            {Array.from({ length: leadBlanks }).map((_, i) => <span key={`blank-${i}`} />)}
            {Array.from({ length: monthLen }).map((_, i) => {
              const day = i + 1
              const selected = day === selectedDay
              return (
                <button
                  key={day}
                  type="button"
                  data-testid={`datetime-picker-day-${day}`}
                  aria-label={`day ${day}`}
                  aria-pressed={selected}
                  onClick={() => setSelectedDay(day)}
                  className={
                    selected
                      ? 'rounded bg-emerald-500 py-0.5 font-medium text-white'
                      : isToday(day)
                        ? 'rounded py-0.5 font-medium text-emerald-600 hover:bg-zinc-100 dark:text-emerald-400 dark:hover:bg-zinc-800'
                        : 'rounded py-0.5 hover:bg-zinc-100 dark:hover:bg-zinc-800'
                  }
                >
                  {day}
                </button>
              )
            })}
          </div>
          <div className="mt-2 flex items-center justify-center gap-1">
            <input
              aria-label="hour"
              data-testid="datetime-picker-hour"
              value={hourText}
              onChange={(e) => setHourText(e.target.value)}
              className="w-8 rounded border border-zinc-300 px-1 py-0.5 text-center dark:border-zinc-700 dark:bg-zinc-950"
            />
            <span>:</span>
            <input
              aria-label="minute"
              data-testid="datetime-picker-minute"
              value={minuteText}
              onChange={(e) => setMinuteText(e.target.value)}
              className="w-8 rounded border border-zinc-300 px-1 py-0.5 text-center dark:border-zinc-700 dark:bg-zinc-950"
            />
            <span>:</span>
            <input
              aria-label="second"
              data-testid="datetime-picker-second"
              value={secondText}
              onChange={(e) => setSecondText(e.target.value)}
              className="w-8 rounded border border-zinc-300 px-1 py-0.5 text-center dark:border-zinc-700 dark:bg-zinc-950"
            />
          </div>
          <div className="mt-2 flex items-center justify-end gap-1">
            <button
              type="button"
              data-testid="datetime-picker-close"
              aria-label="close date picker"
              onClick={() => setOpen(false)}
              className="rounded border border-zinc-300 px-2 py-0.5 dark:border-zinc-700"
            >
              close
            </button>
            <button
              type="button"
              data-testid="datetime-picker-apply"
              aria-label="apply picked date"
              disabled={!timeValid}
              onClick={apply}
              className="rounded border border-zinc-300 px-2 py-0.5 disabled:opacity-40 dark:border-zinc-700"
            >
              apply
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
