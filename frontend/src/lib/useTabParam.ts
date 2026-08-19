import { useNavigate, useSearch } from '@tanstack/react-router'

/// The selected tab lives in the URL (`?tab=…`, lowercased label) so a reload
/// or a shared link lands on the tab you were reading — the same reason the
/// subject page keeps `?version=` there. An absent or unrecognised value
/// falls back to the first tab instead of rendering nothing.
export function useTabParam<T extends string>(tabs: readonly T[], fallback: T): [T, (tab: T) => void] {
  const navigate = useNavigate()
  const search = useSearch({ strict: false }) as { tab?: unknown }
  const raw = typeof search.tab === 'string' ? search.tab.toLowerCase() : undefined
  const tab = tabs.find((t) => t.toLowerCase() === raw) ?? fallback
  // Other search params (a subject's `version`) are carried through untouched.
  const setTab = (next: T) =>
    navigate({ to: '.', search: (prev) => ({ ...prev, tab: next.toLowerCase() }) })
  return [tab, setTab]
}
