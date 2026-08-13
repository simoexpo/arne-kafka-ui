import { describe, expect, it } from 'vitest'
import { parseFilterQuery } from './filterQuery'
import type { MessageOut } from '../api/types'

const mk = (partition: number, offset: number, ts: number, text = `v${offset}`): MessageOut => ({
  partition, offset, timestamp_ms: ts, key: { encoding: 'utf8', text: `k${offset}`, schema_id: null, error: null },
  value: { encoding: 'utf8', text, schema_id: null, error: null }, headers: [],
})

describe('parseFilterQuery', () => {
  it('empty means no filter', () => {
    const f = parseFilterQuery('')
    expect(f.api).toBeNull()
    expect(f.predicate(mk(0, 1, 1))).toBe(true)
  })
  it('bare text is contains on key or value', () => {
    const f = parseFilterQuery('V2')
    expect(f.api).toEqual({ filter: 'contains', q: 'V2' })
    expect(f.predicate(mk(0, 2, 1, 'v2-x'))).toBe(true)
    expect(f.predicate(mk(0, 3, 1, 'nope'))).toBe(false)
  })
  it('key: and value: prefixes', () => {
    expect(parseFilterQuery('key:k7').api).toEqual({ filter: 'key_contains', q: 'k7' })
    expect(parseFilterQuery('value:foo').api).toEqual({ filter: 'value_contains', q: 'foo' })
  })
  it('dot-path equals becomes json_eq and predicate matches json', () => {
    const f = parseFilterQuery('user.id=42')
    expect(f.api).toEqual({ filter: 'json_eq', q: '42', path: 'user.id' })
    const m = mk(0, 1, 1, '{"user":{"id":42}}')
    m.value!.encoding = 'json'
    expect(f.predicate(m)).toBe(true)
  })
  it('decode-error values never content-match', () => {
    const f = parseFilterQuery('AAEC')
    const m = mk(0, 1, 1, 'AAECAw==')
    m.value!.encoding = 'decode_error'
    expect(f.predicate(m)).toBe(false)
  })
})
