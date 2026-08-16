//! Cursor codec, window math, and the paging engine for the messages
//! timeline.
//!
//! One module per role:
//!
//! - `cursor` — the `Direction`/`Cursor` wire contract: bound semantics and
//!   the base64+JSON codec.
//! - `anchor` — where a fresh (non-cursor) page starts: `Anchor` and
//!   `initial_positions`.
//! - `window` — per-partition window arithmetic and the hardening applied to
//!   untrusted positions: `page_windows`, `at_edge`, `clamp_positions`,
//!   `cap_windows_to_budget`.
//! - `merge` — the cross-partition ordering rules: `merge_prefers` (merge
//!   step) and `chunk_display_order` (emission order).
//! - `event` — `TimelineEvent`, what a page emits.
//! - `engine` — `run_page`, the only part that talks to a cluster.
//!
//! Everything outside `engine` is pure: no I/O, no Kafka client, nothing
//! async, so it is unit-tested without a broker. `run_page` drives those
//! pure pieces against a real cluster: watermarks and starting positions are
//! resolved by the caller (`api::messages`, one watermarks round trip per
//! request) and handed in; `run_page` computes windows, scans and decodes
//! them, and emits `TimelineEvent`s over an mpsc channel — the SSE handler
//! just maps those to wire events.
//!
//! The contract these modules implement is in the design spec
//! (`docs/superpowers/specs/2026-08-09-betrachtung-kafka-ui-design.md`); the
//! reasoning that produced this shape is in
//! `docs/superpowers/plans/2026-08-13-messages-timeline.md` and its
//! `-followups` companion.

mod anchor;
mod cursor;
mod engine;
mod event;
mod merge;
mod window;

pub use anchor::{initial_positions, Anchor};
pub use cursor::{Cursor, Direction};
pub use engine::run_page;
pub use event::TimelineEvent;
pub use window::page_windows;
