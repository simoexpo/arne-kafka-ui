// Client-side mirror of the backend's cursor codec (see the `Cursor` doc
// comment in `backend/src/message/timeline.rs`, spec v1.6 "Sliding window").
//
// Wire format (documented contract, not an implementation detail): standard-
// alphabet base64 (with padding — `+`/`/`/`=`, NOT the URL-safe variant) of
// compact JSON `{"positions":[[partition,offset],...]}` — `positions` is an
// array of 2-element `[number, number]` pairs, one per partition, in no
// particular guaranteed order. The backend's `direction` field is
// `#[serde(default)]`, i.e. entirely OPTIONAL on the wire (a missing field
// decodes fine) and, per that same doc comment, informational only — the
// backend never reads it back for anything but logging, since direction
// belongs to the request (the `direction` query param), not the cursor. This
// sliding-window frontend therefore never emits it: `encodeCursor` takes no
// direction argument and only ever writes `positions`.
//
// `decodeCursor` accepts cursors with OR without a `direction` field (it
// simply ignores it if present) so it can decode both self-minted cursors
// and real cursors handed back by the backend's `PageEnd` event (which do
// carry `direction`, since the Rust `Cursor` struct always serializes it).

function bytesToBase64(bytes: Uint8Array): string {
  let binary = ''
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i])
  return btoa(binary)
}

function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  return bytes
}

/** Encodes a per-partition position map into the wire cursor format. */
export function encodeCursor(positions: Record<number, number>): string {
  const pairs: [number, number][] = Object.entries(positions).map(([p, o]) => [Number(p), o])
  const json = JSON.stringify({ positions: pairs })
  return bytesToBase64(new TextEncoder().encode(json))
}

interface DecodedWire {
  positions: [number, number][]
  // Present on real backend-minted cursors; never read, kept only so the
  // shape lines up with what `JSON.parse` actually returns.
  direction?: 'back' | 'forward'
}

/**
 * Decodes a wire cursor (self-minted or backend-minted) into a per-partition
 * position map. Throws if the string isn't valid base64 or doesn't decode to
 * the documented `{"positions":[...]}` shape.
 */
export function decodeCursor(cursor: string): Record<number, number> {
  const json = new TextDecoder().decode(base64ToBytes(cursor))
  const parsed = JSON.parse(json) as DecodedWire
  if (!Array.isArray(parsed.positions)) throw new Error('cursor missing positions array')
  const out: Record<number, number> = {}
  for (const [partition, offset] of parsed.positions) out[partition] = offset
  return out
}
