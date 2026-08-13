import type { MessageOut } from '../api/types'

export interface FilterQueryApi {
  filter: string
  q: string
  path?: string
}

export interface ParsedFilterQuery {
  api: FilterQueryApi | null
  predicate: (m: MessageOut) => boolean
}

const alwaysTrue = () => true

function scalarEq(v: unknown, expected: string): boolean {
  if (typeof v === 'string') return v === expected
  if (typeof v === 'number') return String(v) === expected
  if (typeof v === 'boolean') return String(v) === expected
  if (v === null) return expected === 'null'
  return false
}

function jsonAtPath(root: unknown, path: string): { found: true; value: unknown } | { found: false } {
  let current: unknown = root
  for (const seg of path.split('.')) {
    if (Array.isArray(current)) {
      const idx = Number(seg)
      if (!Number.isInteger(idx) || idx < 0 || idx >= current.length) return { found: false }
      current = current[idx]
    } else if (current !== null && typeof current === 'object') {
      const obj = current as Record<string, unknown>
      if (!(seg in obj)) return { found: false }
      current = obj[seg]
    } else {
      return { found: false }
    }
  }
  return { found: true, value: current }
}

function keyContainsPredicate(q: string) {
  return (m: MessageOut) => !!m.key && m.key.text.includes(q)
}

function valueContainsPredicate(q: string) {
  return (m: MessageOut) => !!m.value && m.value.encoding !== 'decode_error' && m.value.text.includes(q)
}

// Case-insensitive contains on key OR value text. A decode-error value is
// the base64 of raw bytes, not real content, so it never content-matches —
// mirrors the server's `Filter::Contains` semantics exactly.
function containsPredicate(q: string) {
  const needle = q.toLowerCase()
  return (m: MessageOut) => {
    const keyHit = !!m.key && m.key.text.toLowerCase().includes(needle)
    const valueHit = !!m.value && m.value.encoding !== 'decode_error' && m.value.text.toLowerCase().includes(needle)
    return keyHit || valueHit
  }
}

function jsonEqPredicate(path: string, q: string) {
  return (m: MessageOut) => {
    if (!m.value || m.value.encoding === 'decode_error') return false
    let root: unknown
    try {
      root = JSON.parse(m.value.text)
    } catch {
      return false
    }
    const found = jsonAtPath(root, path)
    return found.found && scalarEq(found.value, q)
  }
}

export function parseFilterQuery(text: string): ParsedFilterQuery {
  if (text === '') return { api: null, predicate: alwaysTrue }

  if (text.startsWith('key:')) {
    const q = text.slice('key:'.length)
    return { api: { filter: 'key_contains', q }, predicate: keyContainsPredicate(q) }
  }
  if (text.startsWith('value:')) {
    const q = text.slice('value:'.length)
    return { api: { filter: 'value_contains', q }, predicate: valueContainsPredicate(q) }
  }

  const eqIdx = text.indexOf('=')
  if (eqIdx > 0) {
    const path = text.slice(0, eqIdx)
    const q = text.slice(eqIdx + 1)
    return { api: { filter: 'json_eq', q, path }, predicate: jsonEqPredicate(path, q) }
  }

  return { api: { filter: 'contains', q: text }, predicate: containsPredicate(text) }
}
