const MAX_FIELD_ROWS = 5

interface RawToken {
  any: boolean
  raw: string
  start: number
}

// Raw-token view of a path for completion matching: names keep their
// canonical spelling (quotes included), `[]` markers are separate tokens.
// A trailing `.` yields an empty name token — "the next segment has begun".
function rawTokens(path: string): RawToken[] {
  const tokens: RawToken[] = []
  let inQuotes = false
  let start = 0
  let i = 0
  while (i < path.length) {
    const c = path[i]
    if (c === '"') {
      inQuotes = !inQuotes
      i++
    } else if (c === '.' && !inQuotes) {
      tokens.push({ any: false, raw: path.slice(start, i), start })
      i++
      start = i
    } else if (c === '[' && !inQuotes && path[i + 1] === ']') {
      if (i > start) tokens.push({ any: false, raw: path.slice(start, i), start })
      tokens.push({ any: true, raw: '[]', start: i })
      i += 2
      if (path[i] === '.') i++
      start = i
    } else {
      i++
    }
  }
  if (start < path.length || path.endsWith('.') || path === '') {
    tokens.push({ any: false, raw: path.slice(start), start })
  }
  return tokens
}

const isIndex = (s: string) => /^\+?\d+$/.test(s)

// The full expression `field` completes `typed`, or null when it doesn't
// match. A typed numeric index matches a `[]` position and KEEPS the
// user's spelling (`items.2.` completes to `items.2.sku`); a typed `[]`
// matches only a `[]` position. Returns null for an already-complete path.
function completeField(field: string, typed: string): string | null {
  const fieldTokens = rawTokens(field)
  const typedTokens = rawTokens(typed)
  if (typedTokens.length > fieldTokens.length) return null
  for (let i = 0; i < typedTokens.length - 1; i++) {
    const t = typedTokens[i]
    const f = fieldTokens[i]
    const ok =
      (t.any && f.any) ||
      (!t.any && !f.any && t.raw === f.raw) ||
      (!t.any && f.any && isIndex(t.raw))
    if (!ok) return null
  }
  const last = typedTokens[typedTokens.length - 1]
  const atLast = fieldTokens[typedTokens.length - 1]
  let head: string
  if (last.any) {
    if (!atLast.any) return null
    head = typed
  } else if (!atLast.any) {
    if (!atLast.raw.startsWith(last.raw)) return null
    head = typed.slice(0, last.start) + atLast.raw
  } else {
    if (!isIndex(last.raw)) return null
    head = typed
  }
  let out = head
  for (let i = typedTokens.length; i < fieldTokens.length; i++) {
    out += fieldTokens[i].any ? '[]' : `.${fieldTokens[i].raw}`
  }
  return out === typed ? null : out
}

// Dropdown rows for the filter box (design spec 2026-08-17 "Autocomplete"):
// pure function of the typed text and the known field paths (already sorted
// by extractFieldPaths, `[]` canonical for arrays). Field rows replace the
// bare `value.` row when fields are known; a quoted or completed-operator
// input proposes nothing.
export function proposalsFor(text: string, fields: readonly string[]): string[] {
  if (text === '' || text.startsWith('"')) return []
  // Prefix matching mirrors the grammar's case-insensitive prefixes;
  // proposals themselves stay canonical lowercase.
  const lower = text.toLowerCase()
  if (lower.startsWith('value.')) {
    const typed = text.slice('value.'.length)
    if (/[:=<>]/.test(typed)) return []
    const out: string[] = []
    for (const f of fields) {
      const completed = completeField(f, typed)
      if (completed !== null) out.push(`value.${completed}`)
      if (out.length === MAX_FIELD_ROWS) break
    }
    return out
  }
  if ('key'.startsWith(lower)) return ['key:', 'key=']
  if ('value'.startsWith(lower)) {
    const fieldRows = fields.slice(0, MAX_FIELD_ROWS).map((f) => `value.${f}`)
    return fieldRows.length > 0 ? ['value:', 'value=', ...fieldRows] : ['value:', 'value=', 'value.']
  }
  return []
}
