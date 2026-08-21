//! Cursor codec, window math, and the paging engine for the messages
//! timeline.
//!
//! One module per role:
//!
//! - `cursor` — the `Direction`/`Cursor` wire contract: bound semantics and
//!   the base64+JSON codec.
//! - `anchor` — where a fresh (non-cursor) page starts: `Anchor` and
//!   `initial_positions`, plus resolving a validated `TimelineAnchorInput`
//!   into positions (`resolve_positions_blocking`) — the only blocking Kafka
//!   round trips outside `engine` (`OffsetsForTimes`, and for a forward
//!   offset anchor, one single-record fetch).
//! - `window` — per-partition window arithmetic and the hardening applied to
//!   untrusted positions: `page_windows`, `at_edge`, `clamp_positions`,
//!   `cap_windows_to_budget`.
//! - `merge` — the cross-partition ordering rules: `merge_prefers` (merge
//!   step) and `chunk_display_order` (emission order).
//! - `event` — `TimelineEvent`, what a page emits.
//! - `engine` — `PageRequest` (one page's bundled parameters) and `run_page`,
//!   the async scan loop that reads and decodes pages.
//!
//! Everything outside `engine` and `anchor`'s two `_blocking` functions is
//! pure: no I/O, no Kafka client, nothing async, so it is unit-tested
//! without a broker. `api::messages` fetches watermarks once per request,
//! calls `resolve_positions_blocking` for a fresh page (a cursor page's
//! positions are already known from decoding alone), then hands the
//! resolved positions to `run_page`, which computes windows, scans and
//! decodes them, and emits `TimelineEvent`s over an mpsc channel — the SSE
//! handler just maps those to wire events.
//!
//! The contract lives in these modules and their tests: each rule is stated
//! where it is enforced, and the acceptance tests are the specification. A
//! rule with no test is not a rule.

mod anchor;
mod cursor;
mod engine;
mod event;
mod merge;
mod window;

pub use anchor::{resolve_positions_blocking, TimelineAnchorInput};
pub use cursor::{Cursor, Direction};
pub use engine::{run_page, PageRequest};
pub use event::TimelineEvent;
