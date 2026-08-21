use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// Which way a page reads relative to its cursor's positions.
///
/// Per the design doc: for `Back`, a position is the *exclusive upper
/// bound* of the next page (the next record read is strictly below it);
/// for `Forward`, a position is the *inclusive lower bound* (the next
/// record read is at-or-above it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Back,
    Forward,
}

impl Default for Direction {
    /// Arbitrary. `Cursor.direction` is informational only (see `Cursor`'s
    /// own "`direction` is informational only" doc comment section) — the
    /// backend never enforces it against anything, and nothing reads it
    /// back today — so this only exists to give
    /// `#[serde(default)]` something to produce when a client-constructed
    /// cursor omits the field entirely, which the documented wire format
    /// explicitly allows.
    fn default() -> Self {
        Direction::Forward
    }
}

/// An opaque, per-partition offset map plus the direction it was minted in.
///
/// **Wire format (documented contract):** standard-alphabet
/// base64 of the compact JSON `{"positions":[[partition,offset],...]
/// ,"direction":"back"|"forward"}` — `positions` is an array of 2-element
/// `[i32, i64]` pairs, one per partition, in no particular guaranteed
/// order; `direction` is OPTIONAL (omitting it decodes fine, see below).
/// The format is deliberately client-constructible, and therefore a
/// contract rather than an implementation detail: the sliding-window
/// frontend mints its own cursors from row offsets it already tracks
/// instead of only ever replaying one the backend handed back. `tests/api.rs`'s
/// `timeline_accepts_a_client_constructed_cursor` and
/// `timeline_accepts_a_client_cursor_without_direction_field` exercise
/// cursors built without this Rust type at all.
///
/// **`direction` is informational only.** It records which direction the
/// cursor was minted in — potentially useful context if this ever gets
/// logged, but nothing in this codebase reads it back today; the backend
/// never enforces it against a request. The REQUEST's own `direction` query param
/// decides how `positions` are read (`api::messages::timeline_sse` leaves
/// the decoded field unused on purpose). That reading is exact, not a loose
/// convention: a `Back` request treats every position as an exclusive upper
/// bound, a `Forward` request treats the identical numbers as inclusive
/// lower bounds (see `Direction`, and `page_windows` for the arithmetic
/// that implements it) — so following a back-minted cursor with
/// `direction=forward` is a well-defined re-read of the region just below
/// it, not a version mismatch. The field being optional follows from that:
/// `#[serde(default)]` makes a missing key decode as `Direction::default()`
/// (an arbitrary placeholder) rather than an error, so a client cursor
/// built purely from `positions` — the only part that ever matters — need
/// not invent a meaningless `direction` to satisfy the codec.
///
/// Every decoded cursor is untrusted input regardless of origin: positions
/// are clamped into each partition's *current* watermark range before use,
/// and any partition id absent from today's watermarks is dropped (see
/// `clamp_positions`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(default)]
    pub direction: Direction,
    pub positions: Vec<(i32, i64)>,
}

impl Cursor {
    pub fn encode(&self) -> String {
        // `serde_json::to_vec` on a struct made only of enums/tuples/Vecs
        // never fails (no maps with non-string keys, no floats), so this
        // is infallible in practice.
        let json = serde_json::to_vec(self).expect("Cursor always serializes");
        base64::engine::general_purpose::STANDARD.encode(json)
    }

    pub fn decode(s: &str) -> Result<Cursor, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrips() {
        let c = Cursor {
            direction: Direction::Back,
            positions: vec![(0, 60), (1, 5)],
        };
        let c2 = Cursor::decode(&c.encode()).unwrap();
        assert_eq!(c2.direction, Direction::Back);
        assert_eq!(c2.positions, vec![(0, 60), (1, 5)]);
        assert!(Cursor::decode("garbage!").is_err());
    }

    /// Pins the optional `direction` half of the wire contract: a
    /// client-constructed cursor built purely from `positions`, with no
    /// `direction` key at all, decodes cleanly (to `Direction::default()`)
    /// instead of failing on a missing required field.
    #[test]
    fn cursor_decodes_without_direction_field() {
        let json = r#"{"positions":[[0,5],[1,3]]}"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(json);
        let decoded = Cursor::decode(&encoded).unwrap();
        assert_eq!(decoded.positions, vec![(0, 5), (1, 3)]);
        assert_eq!(decoded.direction, Direction::default());
    }
}
