import { describe, expect, it } from 'vitest'
import { parseFilterQuery } from './filterQuery'
import type { MessageOut } from '../api/types'

const mk = (partition: number, offset: number, ts: number, text = `v${offset}`): MessageOut => ({
  partition, offset, timestamp_ms: ts, key: { encoding: 'utf8', text: `k${offset}`, schema_id: null, error: null },
  value: { encoding: 'utf8', text, schema_id: null, error: null }, headers: [],
})

const jsonRow = (text: string): MessageOut => {
  const m = mk(0, 1, 1, text)
  m.value!.encoding = 'json'
  return m
}

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
  it('key: and value: contains are now case-insensitive', () => {
    expect(parseFilterQuery('key:K7').predicate(mk(0, 7, 1))).toBe(true)
    expect(parseFilterQuery('value:V7').predicate(mk(0, 7, 1))).toBe(true)
  })
  it('key= is case-insensitive key equality', () => {
    expect(parseFilterQuery('key=K7').api).toEqual({ filter: 'key_eq', q: 'K7' })
    const p = parseFilterQuery('key=K7').predicate
    expect(p(mk(0, 7, 1))).toBe(true)
    expect(p(mk(0, 71, 1))).toBe(false)
  })
  it('value= is case-insensitive equality, semantic when both sides are JSON', () => {
    expect(parseFilterQuery('value=x').api).toEqual({ filter: 'value_eq', q: 'x' })
    const m = jsonRow('{"a": 1, "b": "X"}')
    expect(parseFilterQuery('value={"b":"x","a":1}').predicate(m)).toBe(true)
    expect(parseFilterQuery('value={"b":"x","a":2}').predicate(m)).toBe(false)
    expect(parseFilterQuery('value=HELLO').predicate(mk(0, 1, 1, 'hello'))).toBe(true)
  })
  it('value.path:q is field contains, value.path=q is field equality', () => {
    expect(parseFilterQuery('value.user.name:ali').api).toEqual({ filter: 'json_contains', q: 'ali', path: 'user.name' })
    expect(parseFilterQuery('value.user.id=42').api).toEqual({ filter: 'json_eq', q: '42', path: 'user.id' })
    const m = jsonRow('{"user":{"name":"Alice","id":42}}')
    expect(parseFilterQuery('value.user.name:ALI').predicate(m)).toBe(true)
    expect(parseFilterQuery('value.user.id=42').predicate(m)).toBe(true)
    expect(parseFilterQuery('value.user.id=43').predicate(m)).toBe(false)
  })
  it('the operator is the FIRST : or = after value.', () => {
    expect(parseFilterQuery('value.a:b=c').api).toEqual({ filter: 'json_contains', q: 'b=c', path: 'a' })
    expect(parseFilterQuery('value.a=b:c').api).toEqual({ filter: 'json_eq', q: 'b:c', path: 'a' })
  })
  it('bare path= no longer parses as a filter — it is a contains search', () => {
    expect(parseFilterQuery('a=b').api).toEqual({ filter: 'contains', q: 'a=b' })
    expect(parseFilterQuery('customer.id=42').api).toEqual({ filter: 'contains', q: 'customer.id=42' })
  })
  it('quoted input escapes the grammar into a literal contains', () => {
    expect(parseFilterQuery('"key:asd"').api).toEqual({ filter: 'contains', q: 'key:asd' })
    expect(parseFilterQuery('"a"b"').api).toEqual({ filter: 'contains', q: 'a"b' })
    expect(parseFilterQuery('"abc').api).toEqual({ filter: 'contains', q: '"abc' })
    expect(parseFilterQuery('""').api).toBeNull()
  })
  it('value. with no operator or empty path falls through to contains', () => {
    expect(parseFilterQuery('value.abc').api).toEqual({ filter: 'contains', q: 'value.abc' })
    expect(parseFilterQuery('value.:x').api).toEqual({ filter: 'contains', q: 'value.:x' })
  })
  it('JSON numbers compare as doubles, mirroring the server (1.0 equals 1)', () => {
    const m = jsonRow('{"qty":1}')
    expect(parseFilterQuery('value={"qty":1.0}').predicate(m)).toBe(true)
    expect(parseFilterQuery('value={"qty":2}').predicate(m)).toBe(false)
  })
  it('path scalars stringify via the double model, mirroring the server', () => {
    const m = jsonRow('{"amount":100.0,"id":12345678901234567890}')
    expect(parseFilterQuery('value.amount=100').predicate(m)).toBe(true)
    expect(parseFilterQuery('value.amount=100.0').predicate(m)).toBe(false)
    expect(parseFilterQuery('value.id=12345678901234567000').predicate(m)).toBe(true)
  })
  it('value.path: with empty needle means the field exists as a scalar', () => {
    const m = jsonRow('{"a":{"b":1},"c":"x"}')
    expect(parseFilterQuery('value.c:').predicate(m)).toBe(true)
    expect(parseFilterQuery('value.a:').predicate(m)).toBe(false) // object, not scalar
    expect(parseFilterQuery('value.missing:').predicate(m)).toBe(false)
  })
  it('objects and arrays sitting AT the path never match', () => {
    const m = jsonRow('{"a":{"b":1},"list":[1]}')
    expect(parseFilterQuery('value.a=1').predicate(m)).toBe(false)
    expect(parseFilterQuery('value.list:1').predicate(m)).toBe(false)
  })
  it('value= JSON needle against a non-JSON value falls back to text equality', () => {
    expect(parseFilterQuery('value={"a":1}').predicate(mk(0, 1, 1, 'not json'))).toBe(false)
    expect(parseFilterQuery('value={"a": 1}').predicate(jsonRow('{"a":1}'))).toBe(true)
  })
  it('array segments must be canonical integers, mirroring the server parser', () => {
    const m = jsonRow('{"items":["hit"]}')
    expect(parseFilterQuery('value.items.0:hit').predicate(m)).toBe(true)
    // Number(' ') coerces to 0 in JS; Rust's parse::<usize> rejects it — a
    // whitespace segment must be a no-match, not index 0.
    expect(parseFilterQuery('value.items. :hit').predicate(m)).toBe(false)
    expect(parseFilterQuery('value.items.1e0:hit').predicate(m)).toBe(false)
  })
  it('decode-error values never content-match', () => {
    const f = parseFilterQuery('AAEC')
    const m = mk(0, 1, 1, 'AAECAw==')
    m.value!.encoding = 'decode_error'
    expect(f.predicate(m)).toBe(false)
    const eq = parseFilterQuery('value=AAECAw==')
    expect(eq.predicate(m)).toBe(false)
  })
})
