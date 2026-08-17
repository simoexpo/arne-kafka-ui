import type { CmpOpWire, TimelineFilterKind } from '../api/sse'
import type { MessageOut } from '../api/types'

export interface FilterQueryApi {
  filter: TimelineFilterKind
  q: string
  path?: string
  op?: CmpOpWire
}

interface ParsedFilterQuery {
  api: FilterQueryApi | null
  predicate: (m: MessageOut) => boolean
}

const alwaysTrue = () => true

// Unquoted `[]`: quantifies over the array — ANY element may satisfy the
// rest of the path (owner ruling 2026-08-17).
const ANY_ELEMENT = Symbol('any-element')
type Segment = string | typeof ANY_ELEMENT

// Path tokenizer, identical on both sides (backend `split_path`): unquoted
// `.` separates segments; a double-quoted run is part of its segment with
// the quotes stripped, so `.`/`:`/`=`/`[]` inside quotes are literal. An
// unclosed quote runs to the end. An unquoted `[]` pair emits an
// any-element marker. Quoting affects tokenization only.
function splitPath(path: string): Segment[] {
  const segments: Segment[] = []
  let current = ''
  let inQuotes = false
  let afterMarker = false
  for (let i = 0; i < path.length; i++) {
    const c = path[i]
    if (c === '"') inQuotes = !inQuotes
    else if (c === '.' && !inQuotes) {
      // The separator right after a `[]` marker introduces the next
      // segment — it must not mint an empty name (which never resolves);
      // a genuinely empty segment (`a..b`) still does.
      if (!(afterMarker && current === '')) segments.push(current)
      current = ''
      afterMarker = false
    } else if (c === '[' && !inQuotes && path[i + 1] === ']') {
      if (current !== '') segments.push(current)
      current = ''
      segments.push(ANY_ELEMENT)
      afterMarker = true
      i++
    } else {
      current += c
      afterMarker = false
    }
  }
  if (!(afterMarker && current === '')) segments.push(current)
  return segments
}

// Every JSON node the path can reach: exactly one without `[]` markers,
// one per matching element combination with them.
function pathCandidates(root: unknown, path: string): unknown[] {
  const out: unknown[] = []
  const walk = (node: unknown, segs: readonly Segment[]) => {
    if (segs.length === 0) {
      out.push(node)
      return
    }
    const [first, ...rest] = segs
    if (first === ANY_ELEMENT) {
      if (Array.isArray(node)) for (const v of node) walk(v, rest)
      return
    }
    if (Array.isArray(node)) {
      if (!/^\+?\d+$/.test(first)) return
      const idx = Number(first)
      if (idx < node.length) walk(node[idx], rest)
    } else if (node !== null && typeof node === 'object') {
      const obj = node as Record<string, unknown>
      if (first in obj) walk(obj[first], rest)
    }
  }
  walk(root, splitPath(path))
  return out
}

// Index of the first operator start (`:`, `=`, `>`, `<`, or `!` immediately
// followed by `=`) sitting outside double quotes, or -1. A lone `!` stays
// literal text — the `!expr` shape is reserved for future boolean
// composition.
function unquotedOperatorIndex(text: string): number {
  let inQuotes = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (c === '"') inQuotes = !inQuotes
    else if (inQuotes) continue
    else if (c === ':' || c === '=' || c === '>' || c === '<') return i
    else if (c === '!' && text[i + 1] === '=') return i
  }
  return -1
}

// "A number" is the JSON number grammar — the backend runs serde_json on
// its side, so using JSON.parse here keeps the mirror exact by
// construction (`1e3` parses; `''`, `+42`, `0x2` don't).
function jsonNumber(text: string): number | null {
  try {
    const v = JSON.parse(text)
    return typeof v === 'number' ? v : null
  } catch {
    return null
  }
}

// The numeric reading of a JSON scalar: a number itself, or a string that
// parses as one. Bools/null/objects/arrays have none.
function scalarNumber(v: unknown): number | null {
  if (typeof v === 'number') return v
  if (typeof v === 'string') return jsonNumber(v)
  return null
}

function cmpHolds(op: CmpOpWire, target: number, expected: number): boolean {
  if (op === 'gt') return target > expected
  if (op === 'gte') return target >= expected
  if (op === 'lt') return target < expected
  return target <= expected
}

function keyCmpPredicate(op: CmpOpWire, q: string) {
  const expected = jsonNumber(q)
  return (m: MessageOut) => {
    if (expected === null || !m.key) return false
    const target = jsonNumber(m.key.text)
    return target !== null && cmpHolds(op, target, expected)
  }
}

function valueCmpPredicate(op: CmpOpWire, q: string) {
  const expected = jsonNumber(q)
  return (m: MessageOut) => {
    if (expected === null || !m.value || m.value.encoding === 'decode_error') return false
    let parsed: unknown
    try {
      parsed = JSON.parse(m.value.text)
    } catch {
      return false
    }
    const target = scalarNumber(parsed)
    return target !== null && cmpHolds(op, target, expected)
  }
}

// True when some node the path reaches in a readable JSON value (exactly
// one node without `[]` markers, any element combination with them)
// satisfies `pred`.
function anyJsonPathMatch(m: MessageOut, path: string, pred: (c: unknown) => boolean): boolean {
  if (!m.value || m.value.encoding === 'decode_error') return false
  let root: unknown
  try {
    root = JSON.parse(m.value.text)
  } catch {
    return false
  }
  return pathCandidates(root, path).some(pred)
}

function jsonCmpPredicate(path: string, op: CmpOpWire, q: string) {
  const expected = jsonNumber(q)
  return (m: MessageOut) =>
    expected !== null &&
    anyJsonPathMatch(m, path, (c) => {
      const target = scalarNumber(c)
      return target !== null && cmpHolds(op, target, expected)
    })
}

// Every predicate mirrors the server exactly (backend/src/message/filter.rs):
// all matching is case-insensitive; a decode-error value never content-matches;
// a null key never key-matches.
function keyContainsPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => !!m.key && m.key.text.toLowerCase().includes(needle)
}

function keyEqPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => !!m.key && m.key.text.toLowerCase() === needle
}

// `!=` matches only where the target is readable: a null key, decode-error
// value, or missing/non-scalar field never matches — we never assert
// content we couldn't read is "different".
function keyNeqPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => !!m.key && m.key.text.toLowerCase() !== needle
}

function valueContainsPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) =>
    !!m.value && m.value.encoding !== 'decode_error' && m.value.text.toLowerCase().includes(needle)
}

function containsPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => {
    const keyHit = !!m.key && m.key.text.toLowerCase().includes(needle)
    const valueHit = !!m.value && m.value.encoding !== 'decode_error' && m.value.text.toLowerCase().includes(needle)
    return keyHit || valueHit
  }
}

// Case-insensitive structural equality: string keys and string values
// compare lower-cased; numbers/bools/null compare exactly.
function jsonEqCi(a: unknown, b: unknown): boolean {
  if (typeof a === 'string' && typeof b === 'string') return a.toLowerCase() === b.toLowerCase()
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => jsonEqCi(x, b[i]))
  if (
    a !== null && b !== null && typeof a === 'object' && typeof b === 'object' &&
    !Array.isArray(a) && !Array.isArray(b)
  ) {
    const ea = Object.entries(a as Record<string, unknown>)
    const eb = Object.entries(b as Record<string, unknown>)
    return (
      ea.length === eb.length &&
      ea.every(([k, va]) => {
        const hit = eb.find(([kb]) => kb.toLowerCase() === k.toLowerCase())
        return hit !== undefined && jsonEqCi(va, hit[1])
      })
    )
  }
  return a === b
}

// The `=`/`!=` equality reading of a value: `null` when unreadable
// (missing or decode-error) — `=` needs `true`, `!=` needs `false`.
function valueEqualsCheck(q: string) {
  const needle = q.toLowerCase()
  let expected: unknown
  let expectedIsJson = true
  try {
    expected = JSON.parse(q)
  } catch {
    expectedIsJson = false
  }
  return (m: MessageOut): boolean | null => {
    if (!m.value || m.value.encoding === 'decode_error') return null
    if (expectedIsJson) {
      try {
        return jsonEqCi(JSON.parse(m.value.text), expected)
      } catch {
        // value is not JSON: falls back to text equality below
      }
    }
    return m.value.text.toLowerCase() === needle
  }
}

function valueEqPredicate(q: string) {
  const eq = valueEqualsCheck(q)
  return (m: MessageOut) => eq(m) === true
}

function valueNeqPredicate(q: string) {
  const eq = valueEqualsCheck(q)
  return (m: MessageOut) => eq(m) === false
}

function scalarText(v: unknown): string | null {
  if (typeof v === 'string') return v.toLowerCase()
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  if (v === null) return 'null'
  return null
}

function jsonEqPredicate(path: string, q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => anyJsonPathMatch(m, path, (c) => scalarText(c) === needle)
}

function jsonNeqPredicate(path: string, q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) =>
    anyJsonPathMatch(m, path, (c) => {
      const s = scalarText(c)
      return s !== null && s !== needle
    })
}

function jsonContainsPredicate(path: string, q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) =>
    anyJsonPathMatch(m, path, (c) => {
      const s = scalarText(c)
      return s !== null && s.includes(needle)
    })
}

// Owner ruling 2026-08-17: operator prefixes match case-insensitively
// (`KEY:` = `key:`) — deliberately undocumented in the help popup.
function prefixIs(text: string, prefix: string): boolean {
  return text.slice(0, prefix.length).toLowerCase() === prefix
}

// The comparison operator starting at index i, two-character forms first
// (`>=` before `>`), or null if the char there isn't one.
function cmpAt(text: string, i: number): { op: CmpOpWire; len: number } | null {
  const c = text[i]
  if (c !== '>' && c !== '<') return null
  const twoChar = text[i + 1] === '='
  const op: CmpOpWire = c === '>' ? (twoChar ? 'gte' : 'gt') : twoChar ? 'lte' : 'lt'
  return { op, len: twoChar ? 2 : 1 }
}

// A `value.`-prefixed expression whose operator hasn't been typed yet —
// the state right after accepting a field proposal. Timeline holds the
// filter while this is true (spec "Hold while composing"): applying it as
// a bare contains would filter the window by half-typed text.
export function isIncompleteFieldExpression(text: string): boolean {
  return prefixIs(text, 'value.') && unquotedOperatorIndex(text.slice('value.'.length)) === -1
}

export function parseFilterQuery(text: string): ParsedFilterQuery {
  if (text === '') return { api: null, predicate: alwaysTrue }

  if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
    const q = text.slice(1, -1)
    if (q === '') return { api: null, predicate: alwaysTrue }
    return { api: { filter: 'contains', q }, predicate: containsPredicate(q) }
  }

  if (prefixIs(text, 'key:')) {
    const q = text.slice('key:'.length)
    return { api: { filter: 'key_contains', q }, predicate: keyContainsPredicate(q) }
  }
  if (prefixIs(text, 'key=')) {
    const q = text.slice('key='.length)
    return { api: { filter: 'key_eq', q }, predicate: keyEqPredicate(q) }
  }
  if (prefixIs(text, 'key!=')) {
    const q = text.slice('key!='.length)
    return { api: { filter: 'key_neq', q }, predicate: keyNeqPredicate(q) }
  }
  const keyCmp = prefixIs(text, 'key>') || prefixIs(text, 'key<') ? cmpAt(text, 3) : null
  if (keyCmp !== null) {
    const q = text.slice(3 + keyCmp.len)
    return { api: { filter: 'key_cmp', q, op: keyCmp.op }, predicate: keyCmpPredicate(keyCmp.op, q) }
  }
  if (prefixIs(text, 'value:')) {
    const q = text.slice('value:'.length)
    return { api: { filter: 'value_contains', q }, predicate: valueContainsPredicate(q) }
  }
  if (prefixIs(text, 'value=')) {
    const q = text.slice('value='.length)
    return { api: { filter: 'value_eq', q }, predicate: valueEqPredicate(q) }
  }
  if (prefixIs(text, 'value!=')) {
    const q = text.slice('value!='.length)
    return { api: { filter: 'value_neq', q }, predicate: valueNeqPredicate(q) }
  }
  const valueCmp = prefixIs(text, 'value>') || prefixIs(text, 'value<') ? cmpAt(text, 5) : null
  if (valueCmp !== null) {
    const q = text.slice(5 + valueCmp.len)
    return { api: { filter: 'value_cmp', q, op: valueCmp.op }, predicate: valueCmpPredicate(valueCmp.op, q) }
  }
  if (prefixIs(text, 'value.')) {
    const rest = text.slice('value.'.length)
    const opIdx = unquotedOperatorIndex(rest)
    if (opIdx > 0) {
      const path = rest.slice(0, opIdx)
      const ch = rest[opIdx]
      if (ch === ':') {
        const q = rest.slice(opIdx + 1)
        return { api: { filter: 'json_contains', q, path }, predicate: jsonContainsPredicate(path, q) }
      }
      if (ch === '=') {
        const q = rest.slice(opIdx + 1)
        return { api: { filter: 'json_eq', q, path }, predicate: jsonEqPredicate(path, q) }
      }
      if (ch === '!') {
        const q = rest.slice(opIdx + 2)
        return { api: { filter: 'json_neq', q, path }, predicate: jsonNeqPredicate(path, q) }
      }
      const c = cmpAt(rest, opIdx)!
      const q = rest.slice(opIdx + c.len)
      return { api: { filter: 'json_cmp', q, path, op: c.op }, predicate: jsonCmpPredicate(path, c.op, q) }
    }
  }

  return { api: { filter: 'contains', q: text }, predicate: containsPredicate(text) }
}
