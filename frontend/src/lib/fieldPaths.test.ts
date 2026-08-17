import { describe, expect, it } from 'vitest'
import type { MessageOut } from '../api/types'
import { extractFieldPaths } from './fieldPaths'

const row = (text: string, encoding: 'json' | 'utf8' | 'decode_error' = 'json'): MessageOut => ({
  partition: 0, offset: 0, timestamp_ms: 1, key: null, headers: [],
  value: { encoding, text, schema_id: null, error: null },
})

describe('extractFieldPaths', () => {
  it('collects scalar paths, deduped and sorted A→Z', () => {
    const rows = [row('{"b":1,"a":{"x":"s"}}'), row('{"a":{"x":2},"c":true}')]
    expect(extractFieldPaths(rows)).toEqual(['a.x', 'b', 'c'])
  })

  it('descends arrays via .0', () => {
    expect(extractFieldPaths([row('{"items":[{"sku":"x"}],"tags":["a"]}')])).toEqual(['items.0.sku', 'tags.0'])
  })

  it('skips non-JSON, decode-error and null values', () => {
    const rows = [row('not json', 'utf8'), row('{"a":1}', 'decode_error'), { ...row('{"b":1}'), value: null }]
    expect(extractFieldPaths(rows)).toEqual([])
  })

  it('quotes segments containing . : or = so proposals stay grammar-usable', () => {
    expect(extractFieldPaths([row('{"a.b":1,"x":{"c=d":2,"e:f":3}}')])).toEqual(['"a.b"', 'x."c=d"', 'x."e:f"'])
  })

  it('skips field names containing a double quote (unaddressable)', () => {
    expect(extractFieldPaths([row('{"a\\"b":1,"ok":2}')])).toEqual(['ok'])
  })

  it('caps depth and row sample', () => {
    const deep = row('{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}')
    expect(extractFieldPaths([deep], 200, 3)).toEqual([])
    const rows = [row('{"a":1}'), row('{"b":1}')]
    expect(extractFieldPaths(rows, 1)).toEqual(['a'])
  })
})
