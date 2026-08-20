//! One refresh per key at a time, with a bounded wait (owner design
//! 2026-08-19). Without this, k concurrent requests that all find the same
//! cache entry stale each go and fetch it: the broker cost multiplies by the
//! number of open tabs for data that is identical by definition.
//!
//! The wait is deliberately bounded. Blocking readers indefinitely on the
//! in-flight refresh would turn our slowest refresh into a frozen UI, so a
//! caller that waits too long gives up and serves whatever the cache holds —
//! honest, because every value we serve carries its own sample timestamp.
//! What it never does on its own is start a second fetch of the same thing —
//! though a caller may choose to, rather than answer with nothing, when the
//! wait expires and there is no cached value to fall back on.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// How long a caller waits for someone else's refresh before serving what the
/// cache already has. Generous next to a healthy refresh (tens of
/// milliseconds) and short enough that a pathological one cannot hang a page.
pub const MAX_WAIT: Duration = Duration::from_secs(2);

pub struct SingleFlight<K> {
    in_flight: Mutex<HashSet<K>>,
    finished: Condvar,
}

impl<K: Eq + Hash + Clone> Default for SingleFlight<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone> SingleFlight<K> {
    pub fn new() -> Self {
        Self { in_flight: Mutex::new(HashSet::new()), finished: Condvar::new() }
    }

    /// `Some(guard)` means this caller owns the refresh and must do the work;
    /// the guard releases the key (and wakes the waiters) when dropped, even
    /// on panic or early return.
    ///
    /// `None` means another caller was already refreshing this key. We waited
    /// for it — up to `wait` — and did NOT duplicate its work; the caller
    /// should re-read the cache and serve what it finds.
    pub fn begin_or_wait(&self, key: K, wait: Duration) -> Option<Flight<'_, K>> {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if in_flight.insert(key.clone()) {
            return Some(Flight { owner: self, key: Some(key) });
        }
        // Someone else owns it: wait for their guard to drop, then let the
        // caller re-read. A timeout is not an error — it just means we serve
        // slightly older data instead of hanging.
        let _ = self
            .finished
            .wait_timeout_while(in_flight, wait, |flights| flights.contains(&key))
            .unwrap_or_else(|e| e.into_inner());
        None
    }
}

impl<K: Eq + Hash + Clone> SingleFlight<K> {
    /// Like `begin_or_wait`, but the guard owns its registry instead of
    /// borrowing it — so it can be held across an `await` while the refresh
    /// runs. Acquiring inside a `spawn_blocking` and dropping the guard when
    /// that closure returns would release the key BEFORE the work starts,
    /// which protects nothing.
    pub fn begin_or_wait_owned(
        self: &std::sync::Arc<Self>,
        key: K,
        wait: Duration,
    ) -> Option<OwnedFlight<K>> {
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if in_flight.insert(key.clone()) {
            return Some(OwnedFlight { owner: self.clone(), key: Some(key) });
        }
        let _ = self
            .finished
            .wait_timeout_while(in_flight, wait, |flights| flights.contains(&key))
            .unwrap_or_else(|e| e.into_inner());
        None
    }
}

/// Same contract as `Flight`, holding its registry by `Arc` so it can outlive
/// the scope that acquired it.
pub struct OwnedFlight<K: Eq + Hash + Clone> {
    owner: std::sync::Arc<SingleFlight<K>>,
    key: Option<K>,
}

impl<K: Eq + Hash + Clone> Drop for OwnedFlight<K> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let mut in_flight = self.owner.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            in_flight.remove(&key);
            drop(in_flight);
            self.owner.finished.notify_all();
        }
    }
}

pub struct Flight<'a, K: Eq + Hash + Clone> {
    owner: &'a SingleFlight<K>,
    key: Option<K>,
}

impl<K: Eq + Hash + Clone> Drop for Flight<'_, K> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let mut in_flight = self.owner.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            in_flight.remove(&key);
            drop(in_flight);
            self.owner.finished.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn the_first_caller_owns_the_refresh() {
        let sf: SingleFlight<String> = SingleFlight::new();
        assert!(sf.begin_or_wait("t".into(), MAX_WAIT).is_some());
    }

    #[test]
    fn a_released_key_can_be_refreshed_again() {
        let sf: SingleFlight<String> = SingleFlight::new();
        drop(sf.begin_or_wait("t".into(), MAX_WAIT));
        assert!(sf.begin_or_wait("t".into(), MAX_WAIT).is_some(), "the guard released the key");
    }

    #[test]
    fn different_keys_never_block_each_other() {
        let sf: SingleFlight<String> = SingleFlight::new();
        let a = sf.begin_or_wait("a".into(), MAX_WAIT);
        let started = Instant::now();
        let b = sf.begin_or_wait("b".into(), MAX_WAIT);
        assert!(a.is_some() && b.is_some());
        assert!(started.elapsed() < Duration::from_millis(200), "an unrelated key must not wait");
    }

    /// The point of the whole thing: concurrent callers for one key do the
    /// work ONCE, and the ones that waited see the finished result.
    #[test]
    fn concurrent_callers_for_one_key_do_the_work_once() {
        let sf: Arc<SingleFlight<String>> = Arc::new(SingleFlight::new());
        let refreshes = Arc::new(AtomicUsize::new(0));
        // the owner holds the key long enough that every other thread arrives
        // while the refresh is genuinely in flight
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let sf = sf.clone();
                let refreshes = refreshes.clone();
                std::thread::spawn(move || {
                    if let Some(_flight) = sf.begin_or_wait("t".into(), MAX_WAIT) {
                        refreshes.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(150));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(refreshes.load(Ordering::SeqCst), 1, "one fetch served every caller");
    }

    #[test]
    fn a_waiter_wakes_as_soon_as_the_refresh_finishes() {
        let sf: Arc<SingleFlight<String>> = Arc::new(SingleFlight::new());
        let flight = sf.begin_or_wait("t".into(), MAX_WAIT).expect("owner");
        let waiter = {
            let sf = sf.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                assert!(sf.begin_or_wait("t".into(), MAX_WAIT).is_none(), "not the owner");
                started.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(80));
        drop(flight);
        let waited = waiter.join().unwrap();
        assert!(waited < MAX_WAIT, "woken by the release, not by the timeout: {waited:?}");
    }

    /// A refresh that outlasts the wait must not hang the caller: it gives up
    /// and serves what the cache has rather than freezing the page.
    #[test]
    fn a_slow_refresh_does_not_hang_the_waiter() {
        let sf: Arc<SingleFlight<String>> = Arc::new(SingleFlight::new());
        let _flight = sf.begin_or_wait("t".into(), MAX_WAIT).expect("owner");
        let started = Instant::now();
        assert!(sf.begin_or_wait("t".into(), Duration::from_millis(120)).is_none());
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(100), "it did wait: {waited:?}");
        assert!(waited < Duration::from_secs(1), "but not forever: {waited:?}");
    }

    /// The owned guard must behave exactly like the borrowed one — it exists
    /// so a refresh can hold the key across an await, which is where the
    /// earlier wiring bug lived (guard dropped before the work began).
    #[test]
    fn an_owned_guard_holds_and_releases_the_same_way() {
        let sf: Arc<SingleFlight<String>> = Arc::new(SingleFlight::new());
        let held = sf.begin_or_wait_owned("t".into(), MAX_WAIT).expect("owner");
        assert!(
            sf.begin_or_wait_owned("t".into(), Duration::from_millis(60)).is_none(),
            "the key is held while the guard lives"
        );
        drop(held);
        assert!(sf.begin_or_wait_owned("t".into(), MAX_WAIT).is_some(), "released on drop");
    }

    /// A panicking refresher must release its key — otherwise that entry
    /// would never be refreshable again.
    #[test]
    fn a_panicking_refresher_releases_the_key() {
        let sf: Arc<SingleFlight<String>> = Arc::new(SingleFlight::new());
        let panicker = {
            let sf = sf.clone();
            std::thread::spawn(move || {
                let _flight = sf.begin_or_wait("t".into(), MAX_WAIT).expect("owner");
                panic!("refresh blew up");
            })
        };
        assert!(panicker.join().is_err(), "the thread did panic");
        assert!(sf.begin_or_wait("t".into(), MAX_WAIT).is_some(), "the key is free again");
    }
}
