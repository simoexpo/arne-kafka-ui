const MAX_FIELD_ROWS = 5

// Dropdown rows for the filter box (design spec 2026-08-17 "Autocomplete"):
// pure function of the typed text and the known field paths (already sorted
// A→Z by extractFieldPaths). Field rows replace the bare `value.` row when
// fields are known; a quoted or completed-operator input proposes nothing.
export function proposalsFor(text: string, fields: readonly string[]): string[] {
  if (text === '' || text.startsWith('"')) return []
  if (text.startsWith('value.')) {
    const typed = text.slice('value.'.length)
    if (/[:=]/.test(typed)) return []
    return fields
      .filter((f) => f.startsWith(typed) && f !== typed)
      .slice(0, MAX_FIELD_ROWS)
      .map((f) => `value.${f}`)
  }
  if ('key'.startsWith(text)) return ['key:', 'key=']
  if ('value'.startsWith(text)) {
    const fieldRows = fields.slice(0, MAX_FIELD_ROWS).map((f) => `value.${f}`)
    return fieldRows.length > 0 ? ['value:', 'value=', ...fieldRows] : ['value:', 'value=', 'value.']
  }
  return []
}
