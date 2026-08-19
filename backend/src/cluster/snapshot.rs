//! One shared answer per cluster per endpoint, for a short window (owner
//! design 2026-08-19). Without it, every browser tab polling the same page
//! issues its own broker calls for byte-identical data — k tabs, k times the
//! cost. With it, the first request inside the window does the work and every
//! other tab reads memory.
//!
//! The TTL is chosen per call site to sit just under that page's poll
//! interval, so a lone tab still refreshes on every poll (nothing gets staler
//! than it was before) while extra tabs ride along. Cached payloads carry the
//! `as_of` they were built with, so a shared answer states its own age rather
//! than pretending to be new.

use super::single_flight::{SingleFlight, MAX_WAIT};
use crate::error::ApiError;
use crate::util::now_ms;
use std::sync::Mutex;

pub struct SnapshotCache<T> {
    inner: Mutex<Option<(T, i64)>>,
}

impl<T> Default for SnapshotCache<T> {
    fn default() -> Self {
        Self { inner: Mutex::new(None) }
    }
}

impl<T: Clone> SnapshotCache<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached value if it is younger than `ttl_ms`.
    pub fn fresh(&self, ttl_ms: i64, now: i64) -> Option<T> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().filter(|(_, at)| now - at < ttl_ms).map(|(v, _)| v.clone())
    }

    /// Whatever is cached, regardless of age.
    pub fn any(&self) -> Option<T> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).as_ref().map(|(v, _)| v.clone())
    }

    pub fn put(&self, value: T, now: i64) {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = Some((value, now));
    }
}

/// Serve `cache` if fresh; otherwise refresh it exactly once across all
/// concurrent callers.
///
/// A caller that loses the race waits for the winner (bounded — see
/// `single_flight`) and then reads what it produced. If the winner left
/// nothing at all (it failed, or is still going), the loser does the work
/// itself rather than answering with nothing: liveness matters more than
/// perfect deduplication on a path that only runs when something is wrong.
///
/// A failed refresh is NOT papered over with the stale value: the error
/// reaches the client, which already keeps its own last-good copy and shows
/// it beside the failure.
pub fn cached_or_refresh<T: Clone>(
    cache: &SnapshotCache<T>,
    flight: &SingleFlight<&'static str>,
    key: &'static str,
    ttl_ms: i64,
    refresh: impl FnOnce() -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    if let Some(fresh) = cache.fresh(ttl_ms, now_ms()) {
        return Ok(fresh);
    }
    match flight.begin_or_wait(key, MAX_WAIT) {
        Some(_flight) => {
            let fresh = refresh()?;
            cache.put(fresh.clone(), now_ms());
            Ok(fresh)
        }
        None => match cache.fresh(ttl_ms, now_ms()).or_else(|| cache.any()) {
            Some(value) => Ok(value),
            None => refresh(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn flight() -> SingleFlight<&'static str> {
        SingleFlight::new()
    }

    #[test]
    fn a_fresh_snapshot_is_served_without_refreshing() {
        let cache = SnapshotCache::new();
        let sf = flight();
        let calls = AtomicUsize::new(0);
        let refresh = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ApiError>("value".to_string())
        };
        assert_eq!(cached_or_refresh(&cache, &sf, "k", 10_000, refresh).unwrap(), "value");
        assert_eq!(cached_or_refresh(&cache, &sf, "k", 10_000, refresh).unwrap(), "value");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the second caller read memory");
    }

    #[test]
    fn an_expired_snapshot_is_refreshed() {
        let cache = SnapshotCache::new();
        let sf = flight();
        cache.put("old".to_string(), now_ms() - 5_000);
        let value = cached_or_refresh(&cache, &sf, "k", 1_000, || Ok::<_, ApiError>("new".into())).unwrap();
        assert_eq!(value, "new");
    }

    /// The whole point: k tabs, one broker round of work.
    #[test]
    fn concurrent_callers_share_one_refresh() {
        let cache: Arc<SnapshotCache<String>> = Arc::new(SnapshotCache::new());
        let sf = Arc::new(flight());
        let calls = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (cache, sf, calls) = (cache.clone(), sf.clone(), calls.clone());
                std::thread::spawn(move || {
                    cached_or_refresh(&cache, &sf, "k", 10_000, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(120));
                        Ok::<_, ApiError>("value".to_string())
                    })
                    .unwrap()
                })
            })
            .collect();
        for t in threads {
            assert_eq!(t.join().unwrap(), "value", "every caller is served");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one refresh served all eight");
    }

    /// A failed refresh must reach the client rather than being hidden behind
    /// the previous value: the UI shows the error next to its own last copy.
    #[test]
    fn a_failed_refresh_is_reported_not_papered_over() {
        let cache = SnapshotCache::new();
        let sf = flight();
        cache.put("old".to_string(), now_ms() - 60_000);
        let err = cached_or_refresh(&cache, &sf, "k", 1_000, || {
            Err::<String, _>(ApiError::kafka("c", "broker said no"))
        })
        .unwrap_err();
        assert!(err.message.contains("broker said no"));
        assert_eq!(cache.any().unwrap(), "old", "the old snapshot is still there for the next caller");
    }
}
