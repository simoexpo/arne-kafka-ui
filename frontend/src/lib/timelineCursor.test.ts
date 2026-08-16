import { describe, expect, it } from 'vitest'
import { decodeCursor, encodeCursor } from './timelineCursor'

describe('timelineCursor', () => {
  it('round-trips a position map', () => {
    const positions = { 0: 60, 1: 5, 2: 0 }
    expect(decodeCursor(encodeCursor(positions))).toEqual(positions)
  })

  it('encodes without a direction field (direction rides the query param, never the cursor)', () => {
    const encoded = encodeCursor({ 0: 5 })
    const json = JSON.parse(atob(encoded))
    expect(json).toEqual({ positions: [[0, 5]] })
    expect(json.direction).toBeUndefined()
  })

  // Pinned cross-format fixture: base64(JSON.stringify({"positions":[[0,5],[1,42]]}))
  // computed by hand (standard alphabet, padded) — see timelineCursor.ts's doc
  // comment for the documented wire format this mirrors.
  it('decodes a hand-built fixture matching the documented wire format', () => {
    const fixture = 'eyJwb3NpdGlvbnMiOltbMCw1XSxbMSw0Ml1dfQ=='
    expect(decodeCursor(fixture)).toEqual({ 0: 5, 1: 42 })
  })

  // Pinned fixture mirroring an actual backend PageEnd cursor (the Rust
  // `Cursor` struct always serializes `direction` since it's a plain,
  // non-optional field on the *writer* side — only *decoding* treats it as
  // optional). base64(JSON.stringify({"direction":"back","positions":[[0,60],[1,5]]})),
  // matching backend/src/message/timeline/cursor.rs's own `cursor_roundtrips` test
  // fixture values (Cursor { direction: Back, positions: [(0,60),(1,5)] }).
  it('decodes a backend-minted cursor that carries a direction field, ignoring it', () => {
    const fixture = 'eyJkaXJlY3Rpb24iOiJiYWNrIiwicG9zaXRpb25zIjpbWzAsNjBdLFsxLDVdXX0='
    expect(decodeCursor(fixture)).toEqual({ 0: 60, 1: 5 })
  })

  it('throws on garbage input', () => {
    expect(() => decodeCursor('not base64 json!!')).toThrow()
  })
})
