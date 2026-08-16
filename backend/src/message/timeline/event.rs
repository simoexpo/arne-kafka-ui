use serde::Serialize;

use crate::error::ApiError;
use crate::message::MessageOut;

/// One event of a `run_page` SSE stream. Serialized untagged: the SSE
/// `event:` field carries the discriminant (`.name()`), so the JSON `data:`
/// payload is just the variant's own fields.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TimelineEvent {
    Match(Box<MessageOut>),
    Progress { scanned: u64, matches: u64, budget: u64 },
    PageEnd { cursor: Option<String>, exhausted: bool },
    Error { code: String, message: String, cluster: Option<String>, retriable: bool },
}

impl TimelineEvent {
    pub fn name(&self) -> &'static str {
        match self {
            TimelineEvent::Match(_) => "match",
            TimelineEvent::Progress { .. } => "progress",
            TimelineEvent::PageEnd { .. } => "page_end",
            // Deliberately NOT "error". SSE's `EventSource` reserves that
            // name for its own connection-level error type: a server frame
            // whose `event:` field is literally "error" fires both
            // `addEventListener('error', ...)` AND the `onerror` IDL
            // handler in a real browser, so the frontend's transport-error
            // handler wins the race and shows generic "connection lost"
            // wording, discarding the structured error this event carries.
            // Trap: the frontend's `EventSource` test double
            // (`frontend/src/test/fake-event-source.ts`) dispatches only to
            // registered listeners, so it does NOT reproduce the collision —
            // this name has to be defended by knowing it, not by a test.
            // "app_error" carries the identical
            // envelope under a name no EventSource type reserves.
            TimelineEvent::Error { .. } => "app_error",
        }
    }
}

impl From<ApiError> for TimelineEvent {
    fn from(e: ApiError) -> Self {
        TimelineEvent::Error { code: e.code.to_string(), message: e.message, cluster: e.cluster, retriable: e.retriable }
    }
}
