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
//
// Owner ruling 2026-08-16 (picker3): reworked layout — calendar grid on
// the LEFT, three independently-scrollable time columns (hour/minute/
// second, wheel-picker style) on the RIGHT. Below date+time sits a single
// editable textbox showing the full picked datetime INCLUDING
// milliseconds; picking a day or a time column value zeroes the
// milliseconds (the textbox is the only way to set a non-zero value).
// The outer trigger is now a bare calendar-icon button — the "current
// value, editable" textbox this used to render itself now lives one level
// up, in the caller (see JumpControl), so there is exactly one textbox
// between the two components, not two.

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
]
const WEEKDAY_LABELS = ['M', 'T', 'W', 'T', 'F', 'S', 'S'] // Monday-first

function pad2(n: number): string {
  return String(n).padStart(2, '0')
}

function pad3(n: number): string {
  return String(n).padStart(3, '0')
}

function daysInMonth(year: number, month: number): number {
  return new Date(year, month + 1, 0).getDate()
}

// `Date#getDay()` is Sunday-first (0-6); the grid below is Monday-first to
// match the layout readers already know from the native picker it replaces.
function leadingBlankCount(year: number, month: number): number {
  return (new Date(year, month, 1).getDay() + 6) % 7
}

type FullDateTime = {
  year: number
  month: number // 0-indexed (Date convention)
  day: number
  hour: number
  minute: number
  second: number
  millis: number
}

const FULL_DATETIME_RE = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})$/

function formatFull(dt: FullDateTime): string {
  return (
    `${dt.year}-${pad2(dt.month + 1)}-${pad2(dt.day)} ` +
    `${pad2(dt.hour)}:${pad2(dt.minute)}:${pad2(dt.second)}.${pad3(dt.millis)}`
  )
}

function parseFull(text: string): FullDateTime | null {
  const m = FULL_DATETIME_RE.exec(text)
  if (!m) return null
  const year = Number(m[1])
  const month = Number(m[2]) - 1
  const day = Number(m[3])
  const hour = Number(m[4])
  const minute = Number(m[5])
  const second = Number(m[6])
  const millis = Number(m[7])
  if (month < 0 || month > 11) return null
  if (day < 1 || day > daysInMonth(year, month)) return null
  if (hour > 23 || minute > 59 || second > 59) return null
  return { year, month, day, hour, minute, second, millis }
}

// Shared with callers (JumpControl's own outer textbox) so the "datetime
// with milliseconds, browser-local" representation is formatted and parsed
// identically everywhere it appears — one source of truth for the format.
export function formatDateTimeMillis(ms: number): string {
  const d = new Date(ms)
  return formatFull({
    year: d.getFullYear(),
    month: d.getMonth(),
    day: d.getDate(),
    hour: d.getHours(),
    minute: d.getMinutes(),
    second: d.getSeconds(),
    millis: d.getMilliseconds(),
  })
}

export function parseDateTimeMillis(text: string): number | null {
  const p = parseFull(text)
  if (!p) return null
  return new Date(p.year, p.month, p.day, p.hour, p.minute, p.second, p.millis).getTime()
}

function CalendarIcon() {
  return (
    <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
      <rect x="3" y="5" width="18" height="16" rx="2" />
      <path d="M3 9h18M8 3v4M16 3v4" />
    </svg>
  )
}

function TimeColumn({ label, testidPrefix, count, selected, onSelect }: {
  label: string
  testidPrefix: string
  count: number
  selected: number
  onSelect: (n: number) => void
}) {
  return (
    <div className="flex flex-col items-center">
      <span className="mb-1 text-[10px] text-zinc-400 dark:text-zinc-500">{label}</span>
      <div
        data-testid={`datetime-picker-${testidPrefix}-list`}
        aria-label={`${label} column`}
        className="h-32 w-10 overflow-y-auto rounded border border-zinc-200 dark:border-zinc-800"
      >
        {Array.from({ length: count }, (_, n) => n).map((n) => {
          const isSelected = n === selected
          return (
            <button
              key={n}
              type="button"
              aria-label={`${label} ${pad2(n)}`}
              aria-pressed={isSelected}
              data-testid={`datetime-picker-${testidPrefix}-${pad2(n)}`}
              onClick={() => onSelect(n)}
              className={
                isSelected
                  ? 'block w-full bg-emerald-500 py-0.5 text-center font-medium text-white'
                  : 'block w-full py-0.5 text-center hover:bg-zinc-100 dark:hover:bg-zinc-800'
              }
            >
              {pad2(n)}
            </button>
          )
        })}
      </div>
    </div>
  )
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
  const [pickedDay, setPickedDay] = useState(1)
  const [pickedHour, setPickedHour] = useState(0)
  const [pickedMinute, setPickedMinute] = useState(0)
  const [pickedSecond, setPickedSecond] = useState(0)
  const [pickedMillis, setPickedMillis] = useState(0)
  const [text, setText] = useState('')

  const openPopover = () => {
    const seed = valueMs !== null ? new Date(valueMs) : new Date()
    const dt: FullDateTime = {
      year: seed.getFullYear(),
      month: seed.getMonth(),
      day: seed.getDate(),
      hour: seed.getHours(),
      minute: seed.getMinutes(),
      second: seed.getSeconds(),
      millis: seed.getMilliseconds(),
    }
    setViewYear(dt.year)
    setViewMonth(dt.month)
    setPickedDay(dt.day)
    setPickedHour(dt.hour)
    setPickedMinute(dt.minute)
    setPickedSecond(dt.second)
    setPickedMillis(dt.millis)
    setText(formatFull(dt))
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

  // Wheel-picker columns are tall lists (24/60/60 rows) in a short viewport
  // — without this, the selected value is routinely scrolled out of view
  // (e.g. minute 45 sits well past the visible 00-05). `scrollIntoView` is a
  // no-op in jsdom (no layout engine, nothing to scroll) so this is inert
  // under the test suite and only does real work in a real browser.
  useEffect(() => {
    if (!open || !containerRef.current) return
    const selectors = [
      '[data-testid^="datetime-picker-hour-"][aria-pressed="true"]',
      '[data-testid^="datetime-picker-minute-"][aria-pressed="true"]',
      '[data-testid^="datetime-picker-second-"][aria-pressed="true"]',
    ]
    for (const sel of selectors) {
      containerRef.current.querySelector(sel)?.scrollIntoView?.({ block: 'center' })
    }
  }, [open, pickedHour, pickedMinute, pickedSecond])

  // Clamping here keeps the selection on a day the new grid actually renders;
  // without it, Jan 31 → Feb would keep an invisible day-31 selection that
  // Date() rolls into March on Apply. Browsing months is not itself "picking
  // a date/time", so milliseconds are preserved (not reset) across nav.
  const goToMonth = (monthOffset: number) => {
    const d = new Date(viewYear, viewMonth + monthOffset, 1)
    const newYear = d.getFullYear()
    const newMonth = d.getMonth()
    const clampedDay = Math.min(pickedDay, daysInMonth(newYear, newMonth))
    setViewYear(newYear)
    setViewMonth(newMonth)
    setPickedDay(clampedDay)
    setText(formatFull({
      year: newYear, month: newMonth, day: clampedDay,
      hour: pickedHour, minute: pickedMinute, second: pickedSecond, millis: pickedMillis,
    }))
  }
  const goPrevMonth = () => goToMonth(-1)
  const goNextMonth = () => goToMonth(1)

  // Picking a day or a time-column value resets milliseconds to 0; the
  // textbox below is the only way to set a non-zero value.
  const selectDay = (day: number) => {
    setPickedDay(day)
    setPickedMillis(0)
    setText(formatFull({
      year: viewYear, month: viewMonth, day,
      hour: pickedHour, minute: pickedMinute, second: pickedSecond, millis: 0,
    }))
  }
  const selectHour = (hour: number) => {
    setPickedHour(hour)
    setPickedMillis(0)
    setText(formatFull({
      year: viewYear, month: viewMonth, day: pickedDay,
      hour, minute: pickedMinute, second: pickedSecond, millis: 0,
    }))
  }
  const selectMinute = (minute: number) => {
    setPickedMinute(minute)
    setPickedMillis(0)
    setText(formatFull({
      year: viewYear, month: viewMonth, day: pickedDay,
      hour: pickedHour, minute, second: pickedSecond, millis: 0,
    }))
  }
  const selectSecond = (second: number) => {
    setPickedSecond(second)
    setPickedMillis(0)
    setText(formatFull({
      year: viewYear, month: viewMonth, day: pickedDay,
      hour: pickedHour, minute: pickedMinute, second, millis: 0,
    }))
  }

  const handleTextChange = (newText: string) => {
    setText(newText)
    const p = parseFull(newText)
    if (!p) return
    setViewYear(p.year)
    setViewMonth(p.month)
    setPickedDay(p.day)
    setPickedHour(p.hour)
    setPickedMinute(p.minute)
    setPickedSecond(p.second)
    setPickedMillis(p.millis)
  }

  const parsed = parseFull(text)
  const textValid = parsed !== null

  const apply = () => {
    if (!parsed) return
    onChange(new Date(parsed.year, parsed.month, parsed.day, parsed.hour, parsed.minute, parsed.second, parsed.millis).getTime())
    setOpen(false)
  }

  const monthLen = daysInMonth(viewYear, viewMonth)
  const leadBlanks = leadingBlankCount(viewYear, viewMonth)
  const today = new Date()
  const isToday = (day: number) =>
    today.getFullYear() === viewYear && today.getMonth() === viewMonth && today.getDate() === day

  return (
    <div className="relative inline-block" ref={containerRef}>
      <button
        type="button"
        data-testid="datetime-picker-trigger"
        aria-label={ariaLabel}
        aria-expanded={open}
        onClick={() => (open ? setOpen(false) : openPopover())}
        className="flex items-center justify-center rounded border border-zinc-300 p-1 text-zinc-600 hover:bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-800"
      >
        <CalendarIcon />
      </button>
      {open && (
        <div
          data-testid="datetime-picker-popover"
          className="absolute z-20 mt-1 w-80 rounded-lg border border-zinc-200 bg-white p-2 text-xs shadow-lg dark:border-zinc-800 dark:bg-zinc-900"
        >
          <div className="flex gap-2">
            <div className="w-44">
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
                  const selected = day === pickedDay
                  return (
                    <button
                      key={day}
                      type="button"
                      data-testid={`datetime-picker-day-${day}`}
                      aria-label={`day ${day}`}
                      aria-pressed={selected}
                      onClick={() => selectDay(day)}
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
            </div>
            <div className="flex flex-1 justify-center gap-1">
              <TimeColumn label="hour" testidPrefix="hour" count={24} selected={pickedHour} onSelect={selectHour} />
              <TimeColumn label="minute" testidPrefix="minute" count={60} selected={pickedMinute} onSelect={selectMinute} />
              <TimeColumn label="second" testidPrefix="second" count={60} selected={pickedSecond} onSelect={selectSecond} />
            </div>
          </div>
          <div className="mt-2">
            <input
              type="text"
              aria-label="picked datetime with milliseconds"
              data-testid="datetime-picker-text"
              placeholder="yyyy-mm-dd hh:mm:ss.mmm"
              value={text}
              onChange={(e) => handleTextChange(e.target.value)}
              aria-invalid={!textValid}
              className="w-full rounded border border-zinc-300 px-1.5 py-1 text-center dark:border-zinc-700 dark:bg-zinc-950"
            />
            {!textValid && (
              <p data-testid="datetime-picker-invalid-hint" className="mt-0.5 text-[10px] text-zinc-500 dark:text-zinc-400">
                doesn't look like a date — expected yyyy-mm-dd hh:mm:ss.mmm
              </p>
            )}
          </div>
          <div className="mt-2 flex items-center justify-between gap-1">
            <button
              type="button"
              data-testid="datetime-picker-close"
              aria-label="cancel date picker"
              onClick={() => setOpen(false)}
              className="rounded border border-zinc-300 px-2 py-0.5 dark:border-zinc-700"
            >
              cancel
            </button>
            <button
              type="button"
              data-testid="datetime-picker-apply"
              aria-label="apply picked date"
              disabled={!textValid}
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
