import { useEffect, useRef, useState, type UIEvent } from 'react'
import { useTimeDisplayMode, type TimeDisplayMode } from '../lib/timeDisplayMode'

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
// Owner ruling 2026-08-16 (picker3): calendar grid on the LEFT, three
// independently-scrollable time columns (hour/minute/second, wheel-picker
// style) on the RIGHT. Below date+time sits a single editable textbox
// showing the full picked datetime INCLUDING milliseconds; picking a day
// or a time column value zeroes the milliseconds (the textbox is the only
// way to set a non-zero value).
//
// Owner ruling 2026-08-16 (picker3 feedback round): the component is
// self-contained like `FilterInput` — it owns BOTH the outer textbox
// (epoch ms, editable, exactly the pre-picker3 field) AND a calendar-icon
// button embedded at its right edge (not a separate sibling button). It
// also now follows the app-wide UTC/local display toggle
// (`lib/timeDisplayMode`): every date/time field in the popover — the
// calendar grid, the three time columns, the "today" marker, and the
// internal datetime-with-millis textbox — is read and written through
// `decompose`/`compose` below, which pick UTC or local `Date` accessors
// per the CURRENT mode. Two instants a day apart in one zone can be the
// same wall-clock day in the other; whichever zone is active, Apply always
// reconstructs the exact same epoch ms the fields represent.

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

// Calendar-only arithmetic (days-in-month, day-of-week for the 1st) never
// depends on which zone is being VIEWED — "August 2026 has 31 days" and
// "August 1 2026 is a Saturday" are zone-independent calendar facts, not
// instants. Only converting an epoch ms INSTANT to/from wall-clock fields
// (`decompose`/`compose` below) is zone-sensitive.
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

// The only two zone-sensitive operations: reading an epoch-ms INSTANT as
// wall-clock fields, and turning wall-clock fields back into an instant.
// `mode` picks UTC or local `Date` accessors/constructors — same instant,
// different fields, in general.
function decompose(ms: number, mode: TimeDisplayMode): FullDateTime {
  const d = new Date(ms)
  return mode === 'utc'
    ? {
        year: d.getUTCFullYear(), month: d.getUTCMonth(), day: d.getUTCDate(),
        hour: d.getUTCHours(), minute: d.getUTCMinutes(), second: d.getUTCSeconds(), millis: d.getUTCMilliseconds(),
      }
    : {
        year: d.getFullYear(), month: d.getMonth(), day: d.getDate(),
        hour: d.getHours(), minute: d.getMinutes(), second: d.getSeconds(), millis: d.getMilliseconds(),
      }
}

function compose(dt: FullDateTime, mode: TimeDisplayMode): number {
  return mode === 'utc'
    ? Date.UTC(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.millis)
    : new Date(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.millis).getTime()
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

// Zone-aware format/parse for the popover's OWN "datetime with
// milliseconds" textbox — exported so the round-trip (and the toggle's
// effect on it) is directly unit-testable without rendering the component.
export function formatDateTimeMillis(ms: number, mode: TimeDisplayMode): string {
  return formatFull(decompose(ms, mode))
}

export function parseDateTimeMillis(text: string, mode: TimeDisplayMode): number | null {
  const p = parseFull(text)
  if (!p) return null
  return compose(p, mode)
}

function CalendarIcon() {
  return (
    <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
      <rect x="3" y="5" width="18" height="16" rx="2" />
      <path d="M3 9h18M8 3v4M16 3v4" />
    </svg>
  )
}

// Looping wheel-column mechanics (owner ruling 2026-08-16, wheel-picker
// follow-up): hour/minute/second are rendered as an iOS-style endless
// wheel — the value list is repeated 3× back-to-back (so there's always
// more list both above and below whatever's centered) and the SELECTED
// value's row in the MIDDLE copy is kept perfectly vertically centered in
// the viewport. Real drag/wheel scroll physics only exist in a real
// browser (jsdom has no layout engine — `scrollTop` is just a stored
// number, `scrollHeight`/`clientHeight` are always 0), so the geometry is
// factored into small pure functions below, unit-tested directly; the
// click-to-select behavior is unit-tested through the DOM as before, and
// the actual scroll/wrap FEEL is verified in the browser pass, not here.
export const WHEEL_ROW_HEIGHT_PX = 24
export const WHEEL_VISIBLE_ROWS = 5
export const WHEEL_VIEWPORT_HEIGHT_PX = WHEEL_ROW_HEIGHT_PX * WHEEL_VISIBLE_ROWS

// Row index (within the 3×-tripled list) of the MIDDLE copy of `selected`.
export function wheelCenteredIndex(selected: number, count: number): number {
  return count + selected
}

// `scrollTop` that puts row `idx` (top-aligned at `idx * rowHeight`)
// exactly vertically centered within a `viewportHeight`-tall viewport.
export function wheelScrollTopForIndex(
  idx: number,
  rowHeight: number = WHEEL_ROW_HEIGHT_PX,
  viewportHeight: number = WHEEL_VIEWPORT_HEIGHT_PX,
): number {
  return idx * rowHeight + rowHeight / 2 - viewportHeight / 2
}

// Standard "3× list, silently re-center near an edge" infinite-scroll
// technique: while the reader is scrolling anywhere within the middle
// third, do nothing; once they've drifted into the outer thirds (meaning
// they're approaching the start/end of the tripled DOM list), jump the
// scroll position by exactly one block-height (`count * rowHeight`) in the
// opposite direction. Every block is an identical copy of the same values,
// so the jump is invisible to the reader — the value under the centerline
// doesn't change, only which copy is now "current". Returns null when no
// rewrap is needed yet.
export function wheelRewrapScrollTop(
  scrollTop: number,
  count: number,
  rowHeight: number = WHEEL_ROW_HEIGHT_PX,
): number | null {
  const blockHeight = count * rowHeight
  if (scrollTop < blockHeight * 0.5) return scrollTop + blockHeight
  if (scrollTop > blockHeight * 1.5) return scrollTop - blockHeight
  return null
}

function TimeColumn({ label, testidPrefix, count, selected, onSelect, open }: {
  label: string
  testidPrefix: string
  count: number
  selected: number
  onSelect: (n: number) => void
  open: boolean
}) {
  const listRef = useRef<HTMLDivElement>(null)

  // Snap-center the selection whenever the popover opens or the selection
  // changes (click, or a typed textbox edit resolving to a new value).
  useEffect(() => {
    if (!open || !listRef.current) return
    listRef.current.scrollTop = wheelScrollTopForIndex(wheelCenteredIndex(selected, count))
  }, [open, selected, count])

  const onScroll = (e: UIEvent<HTMLDivElement>) => {
    const el = e.currentTarget
    const corrected = wheelRewrapScrollTop(el.scrollTop, count)
    if (corrected !== null) el.scrollTop = corrected
  }

  return (
    <div className="flex flex-col items-center">
      <span className="mb-1 text-[10px] text-zinc-400 dark:text-zinc-500">{label}</span>
      <div
        ref={listRef}
        onScroll={onScroll}
        data-testid={`datetime-picker-${testidPrefix}-list`}
        aria-label={`${label} column`}
        // Scrollable, just no visible scrollbar chrome — still
        // wheel/drag/click-scrollable, with the selection auto-centered.
        className="h-[120px] w-10 overflow-y-auto rounded border border-zinc-200 [scrollbar-width:none] dark:border-zinc-800 [&::-webkit-scrollbar]:hidden"
      >
        {[0, 1, 2].map((block) => (
          <div key={block}>
            {Array.from({ length: count }, (_, n) => n).map((n) => {
              const isSelected = n === selected
              // Only the middle (2nd) copy is the "canonical" element:
              // it alone carries the stable data-testid/aria-label/
              // aria-pressed a test (or a screen reader) should see —
              // the other two copies are purely the wrap illusion.
              const canonical = block === 1
              return (
                <button
                  key={`${block}-${n}`}
                  type="button"
                  tabIndex={canonical ? 0 : -1}
                  aria-hidden={canonical ? undefined : true}
                  aria-label={canonical ? `${label} ${pad2(n)}` : undefined}
                  aria-pressed={canonical ? isSelected : undefined}
                  data-testid={canonical ? `datetime-picker-${testidPrefix}-${pad2(n)}` : undefined}
                  onClick={() => onSelect(n)}
                  className={
                    isSelected
                      ? 'flex h-6 w-full items-center justify-center bg-emerald-500 font-medium text-white'
                      : 'flex h-6 w-full items-center justify-center hover:bg-zinc-100 dark:hover:bg-zinc-800'
                  }
                >
                  {pad2(n)}
                </button>
              )
            })}
          </div>
        ))}
      </div>
    </div>
  )
}

export function DateTimePicker({
  valueMs,
  onChange,
  ariaLabel,
  textValue,
  onTextChange,
  onTextEnter,
  textTestId,
  textAriaLabel,
  textPlaceholder,
}: {
  valueMs: number | null
  onChange: (ms: number) => void
  ariaLabel: string
  textValue: string
  onTextChange: (text: string) => void
  onTextEnter?: () => void
  textTestId: string
  textAriaLabel: string
  textPlaceholder: string
}) {
  const mode = useTimeDisplayMode()
  const modeLabel = mode === 'utc' ? 'UTC' : 'local'
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
    const dt = decompose(valueMs !== null ? valueMs : Date.now(), mode)
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


  // The popover follows the app-wide UTC/local toggle LIVE, even while
  // already open: if the mode changes mid-session, the currently-picked
  // fields are re-decomposed from the SAME underlying instant under the
  // new zone, rather than silently reinterpreting the old fields (which
  // would drift the epoch). Closed popovers don't need this — the next
  // `openPopover()` re-derives fresh from `valueMs` under whatever mode is
  // then current.
  const prevMode = useRef(mode)
  useEffect(() => {
    if (prevMode.current === mode) return
    if (open) {
      const epoch = compose(
        { year: viewYear, month: viewMonth, day: pickedDay, hour: pickedHour, minute: pickedMinute, second: pickedSecond, millis: pickedMillis },
        prevMode.current,
      )
      const dt = decompose(epoch, mode)
      setViewYear(dt.year)
      setViewMonth(dt.month)
      setPickedDay(dt.day)
      setPickedHour(dt.hour)
      setPickedMinute(dt.minute)
      setPickedSecond(dt.second)
      setPickedMillis(dt.millis)
      setText(formatFull(dt))
    }
    prevMode.current = mode
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode])

  // Clamping here keeps the selection on a day the new grid actually renders;
  // without it, Jan 31 → Feb would keep an invisible day-31 selection that
  // Date() rolls into March on Apply. Browsing months is not itself "picking
  // a date/time", so milliseconds are preserved (not reset) across nav. This
  // is pure calendar arithmetic (see `daysInMonth` above) — zone-independent.
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

  const handleInternalTextChange = (newText: string) => {
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
    onChange(compose(parsed, mode))
    setOpen(false)
  }

  const monthLen = daysInMonth(viewYear, viewMonth)
  const leadBlanks = leadingBlankCount(viewYear, viewMonth)
  const today = decompose(Date.now(), mode)
  const isToday = (day: number) =>
    today.year === viewYear && today.month === viewMonth && today.day === day

  return (
    <div className="relative inline-block" ref={containerRef}>
      <input
        type="text"
        data-testid={textTestId}
        aria-label={textAriaLabel}
        placeholder={textPlaceholder}
        value={textValue}
        onChange={(e) => onTextChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter') onTextEnter?.()
        }}
        className="w-32 rounded border border-zinc-300 bg-transparent px-1.5 py-0.5 pr-6 dark:border-zinc-700 dark:bg-zinc-900"
      />
      <button
        type="button"
        data-testid="datetime-picker-trigger"
        aria-label={`${ariaLabel} (${modeLabel})`}
        aria-expanded={open}
        onClick={() => (open ? setOpen(false) : openPopover())}
        className="absolute right-1 top-1/2 flex -translate-y-1/2 items-center justify-center rounded p-0.5 text-zinc-500 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
      >
        <CalendarIcon />
      </button>
      {open && (
        <div
          data-testid="datetime-picker-popover"
          className="absolute z-20 mt-1 w-80 rounded-lg border border-zinc-200 bg-white p-2 text-xs shadow-lg dark:border-zinc-800 dark:bg-zinc-900"
        >
          <div className="mb-1 flex justify-end">
            <span
              data-testid="datetime-picker-zone-label"
              className="rounded bg-zinc-100 px-1 py-0.5 text-[9px] font-medium uppercase tracking-wide text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400"
            >
              {modeLabel}
            </span>
          </div>
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
              <TimeColumn label="hour" testidPrefix="hour" count={24} selected={pickedHour} onSelect={selectHour} open={open} />
              <TimeColumn label="minute" testidPrefix="minute" count={60} selected={pickedMinute} onSelect={selectMinute} open={open} />
              <TimeColumn label="second" testidPrefix="second" count={60} selected={pickedSecond} onSelect={selectSecond} open={open} />
            </div>
          </div>
          <div className="mt-2">
            <input
              type="text"
              aria-label="picked datetime with milliseconds"
              data-testid="datetime-picker-text"
              placeholder="yyyy-mm-dd hh:mm:ss.mmm"
              value={text}
              onChange={(e) => handleInternalTextChange(e.target.value)}
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
