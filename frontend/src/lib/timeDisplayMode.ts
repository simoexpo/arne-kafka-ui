import { useSyncExternalStore } from 'react'

// Owner ruling (2026-08-15): a small, persisted, DISPLAY-ONLY preference —
// every already-loaded epoch-ms value re-renders in the chosen zone. This
// module never touches the network, the timeline store, or TanStack Query:
// it only decides how numbers already in memory get FORMATTED. Default is
// 'utc' — today's behavior — so nobody's view changes until they opt in.
export type TimeDisplayMode = 'utc' | 'local'

const STORAGE_KEY = 'timeDisplayMode'

// Reads localStorage directly on every call rather than caching a
// module-level variable: localStorage is already the single source of
// truth, so there's nothing to keep in sync, and a test (or another tab)
// that changes it directly is picked up immediately.
export function getTimeDisplayMode(): TimeDisplayMode {
  return localStorage.getItem(STORAGE_KEY) === 'local' ? 'local' : 'utc'
}

const listeners = new Set<() => void>()

export function setTimeDisplayMode(mode: TimeDisplayMode): void {
  localStorage.setItem(STORAGE_KEY, mode)
  listeners.forEach((fn) => fn())
}

function subscribe(onStoreChange: () => void): () => void {
  listeners.add(onStoreChange)
  return () => listeners.delete(onStoreChange)
}

// `useSyncExternalStore` (built into React, no new dependency) rather than a
// Context provider: the mode is read from a handful of components scattered
// across the tree (sidebar toggle, message rows, the timeline header, the
// jump-timestamp preview) that don't otherwise share a common ancestor worth
// wrapping — a plain external store keeps every one of them independently
// subscribed without threading a provider through every route.
export function useTimeDisplayMode(): TimeDisplayMode {
  return useSyncExternalStore(subscribe, getTimeDisplayMode, () => 'utc')
}
