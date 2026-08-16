//! Cursor codec, window math, and the paging engine for the messages
//! timeline.
//!
//! `Direction`/`Cursor`/`Anchor`/`initial_positions`/`page_windows`/`at_edge`
//! are pure functions: no I/O, no Kafka client, nothing async. `run_page`
//! (this task) is the engine that drives them against a real cluster: it
//! fetches fresh watermarks, computes windows, scans and decodes them, and
//! emits `TimelineEvent`s over an mpsc channel — the SSE handler in
//! `api::messages` just maps those to wire events.

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
