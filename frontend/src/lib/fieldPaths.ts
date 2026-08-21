import type { MessageOut } from '../api/types'

// Field paths for autocomplete, extracted from the window's own JSON-able
// values. Scalar leaves only —
// the grammar's path filters match scalars only. Arrays descend via index 0
// (owner ruling: `items.0.sku` is directly usable). Caps keep a huge window
// from stalling typing; callers memoize per rows identity.
export function extractFieldPaths(rows: readonly MessageOut[], maxRows = 200, maxDepth = 6): string[] {
  const paths = new Set<string>()
  const walk = (node: unknown, prefix: string, depth: number) => {
    if (prefix !== '' && (node === null || typeof node !== 'object')) {
      paths.add(prefix)
      return
    }
    if (depth >= maxDepth) return
    if (Array.isArray(node)) {
      // Arrays emit the `[]` any-element token (owner ruling 2026-08-17):
      // a proposal like `items[].sku` matches every element, and typing an
      // explicit index instead still completes (see filterProposals).
      // Element 0 stands in for the shape.
      if (node.length > 0) walk(node[0], `${prefix}[]`, depth + 1)
      return
    }
    if (node !== null && typeof node === 'object') {
      for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
        // Names containing grammar characters are emitted double-quoted so
        // the proposal is directly typeable (`value."a.b"=1`); a name
        // containing a quote itself has no escape and is skipped.
        if (k.includes('"')) continue
        const seg = /[.:=[\]]/.test(k) ? `"${k}"` : k
        walk(v, prefix === '' ? seg : `${prefix}.${seg}`, depth + 1)
      }
    }
  }
  for (const m of rows.slice(0, maxRows)) {
    if (!m.value || m.value.encoding === 'decode_error') continue
    try {
      walk(JSON.parse(m.value.text), '', 0)
    } catch {
      // non-JSON value contributes nothing
    }
  }
  return [...paths].sort()
}
