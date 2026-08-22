import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Outlet, useLocation, useParams } from '@tanstack/react-router'
import { getClusters } from '../api/client'
import { describeError } from '../api/errors'
import type { ClusterHealth } from '../api/types'
import { CommandPalette } from '../components/CommandPalette'

export function useClusters() {
  return useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => getClusters(signal), refetchInterval: 10_000 })
}

const SECTIONS = ['overview', 'topics', 'consumers', 'schemas'] as const
type Section = (typeof SECTIONS)[number]

// Registered route path per section — kept as literals (rather than a single
// generic `/c/$cluster/$section` template) because that's what's actually
// registered in the route tree, so typed `<Link to>` can check them.
const SECTION_PATHS = {
  overview: '/c/$cluster/overview',
  topics: '/c/$cluster/topics',
  consumers: '/c/$cluster/consumers',
  schemas: '/c/$cluster/schemas',
} as const satisfies Record<Section, string>

// Derives the active sidebar section from the exact path segment that follows
// `/c/{cluster}/`, rather than a substring match against the whole pathname —
// a topic or group literally named e.g. "groups-events" must not be
// misclassified, and this must be a pure function of `pathname` so callers
// can subscribe reactively (via useLocation) instead of reading a stale
// snapshot of `location.pathname`.
export function sectionFromPathname(pathname: string, cluster: string): Section {
  const prefix = `/c/${encodeURIComponent(cluster)}/`
  if (!pathname.startsWith(prefix)) return 'overview'
  const segment = pathname.slice(prefix.length).split('/')[0]
  return segment === 'topics' || segment === 'consumers' || segment === 'schemas' ? segment : 'overview'
}

export function Sidebar({ cluster, clusters, active, error, version }: {
  cluster: string
  clusters: ClusterHealth[]
  active: Section
  // The commit the running build was built from; shown only when known.
  version?: string | null
  // The clusters-list query failing is not "zero other clusters" — silently
  // rendering an empty switcher would look identical to a healthy single-
  // cluster setup. Surface it the same way Panel does: a real headline via
  // describeError, not a bare "error" string.
  error?: unknown
}) {
  return (
    // pt-6 mirrors main's p-6 so the brand tops out level with every page's
    // title — the ONE cross-column line all pages share. The nav floats
    // free below it: chasing each page's own content line from here was
    // tried and reverted (2026-08-17) — headers differ per page, so it
    // always misaligned somewhere.
    <aside className="flex w-56 flex-col gap-6 border-r border-zinc-200 px-4 pb-4 pt-6 dark:border-zinc-800">
      <div className="flex items-center gap-2">
        <img src="/logo.svg" alt="Arne logo" className="h-9 w-auto" />
        <div>
          <div className="font-[Cinzel] text-lg font-semibold tracking-wide">Arne</div>
          <div className="text-[10px] italic leading-tight text-zinc-500 dark:text-zinc-400">mythological Kafka UI</div>
        </div>
      </div>
      <nav className="flex flex-col gap-1">
        {SECTIONS.map((s) => (
          <Link
            key={s}
            to={SECTION_PATHS[s]}
            params={{ cluster }}
            className={`rounded px-2 py-1 text-sm capitalize ${
              s === active
                ? 'bg-zinc-200 font-medium dark:bg-zinc-800'
                : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-900'
            }`}
          >
            {s}
          </Link>
        ))}
      </nav>
      <div className="mt-auto">
        <div className="mb-1 text-xs text-zinc-500">cluster</div>
        <div className="flex items-center gap-2 text-sm font-medium">
          <HealthDot status={clusters.find((c) => c.name === cluster)?.status} />
          {cluster}
        </div>
        <div className="mt-2 flex flex-col gap-1">
          {error
            ? <ClustersFailure error={error} />
            : clusters.filter((c) => c.name !== cluster).map((c) => (
              <Link
                key={c.name}
                to={SECTION_PATHS[active]}
                params={{ cluster: c.name }}
                className="flex items-center gap-2 rounded px-1 py-0.5 text-sm text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-900"
              >
                <HealthDot status={c.status} />
                {c.name}
              </Link>
            ))}
        </div>
        <div className="mt-4 flex items-center justify-between gap-2">
          <ThemeToggle />
          {version && (
            <span data-testid="app-version" className="font-mono text-[10px] text-zinc-400 dark:text-zinc-600">
              {version}
            </span>
          )}
        </div>
      </div>
    </aside>
  )
}

function ClustersFailure({ error }: { error: unknown }) {
  const { headline } = describeError(error)
  return (
    <p data-testid="sidebar-clusters-error" className="text-xs text-red-600 dark:text-red-400">
      {headline ?? 'failed to load clusters'}
    </p>
  )
}

function HealthDot({ status }: { status?: ClusterHealth['status'] }) {
  const color = status === 'healthy' ? 'bg-emerald-500' : status === 'unreachable' ? 'bg-red-500' : 'bg-zinc-400'
  return <span title={status ?? 'unknown'} className={`inline-block h-2 w-2 rounded-full ${color}`} />
}

function SunIcon({ active }: { active: boolean }) {
  return (
    <svg
      data-testid="icon-sun"
      viewBox="0 0 16 16"
      width="14"
      height="14"
      className={active ? 'text-amber-500' : 'text-zinc-400 dark:text-zinc-600'}
    >
      <circle cx="8" cy="8" r="3.2" fill="currentColor" />
      <g stroke="currentColor" strokeWidth="1.3" strokeLinecap="round">
        <line x1="8" y1="0.5" x2="8" y2="2.3" />
        <line x1="8" y1="13.7" x2="8" y2="15.5" />
        <line x1="0.5" y1="8" x2="2.3" y2="8" />
        <line x1="13.7" y1="8" x2="15.5" y2="8" />
        <line x1="2.6" y1="2.6" x2="3.9" y2="3.9" />
        <line x1="12.1" y1="12.1" x2="13.4" y2="13.4" />
        <line x1="2.6" y1="13.4" x2="3.9" y2="12.1" />
        <line x1="12.1" y1="3.9" x2="13.4" y2="2.6" />
      </g>
    </svg>
  )
}

function MoonIcon({ active }: { active: boolean }) {
  return (
    <svg
      data-testid="icon-moon"
      viewBox="0 0 16 16"
      width="14"
      height="14"
      className={active ? 'text-indigo-400' : 'text-zinc-400 dark:text-zinc-600'}
    >
      <path
        d="M13.5 10.2A6 6 0 0 1 5.8 2.5a6 6 0 1 0 7.7 7.7Z"
        fill="currentColor"
      />
    </svg>
  )
}

export function ThemeToggle() {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'))
  return (
    <button
      aria-label="toggle theme"
      data-mode={dark ? 'dark' : 'light'}
      className="flex items-center gap-1 rounded-full border border-zinc-300 p-1 dark:border-zinc-700"
      onClick={() => {
        const el = document.documentElement
        const isDark = el.classList.toggle('dark')
        localStorage.theme = isDark ? 'dark' : 'light'
        setDark(isDark)
      }}
    >
      <span className={`rounded-full p-1 ${!dark ? 'bg-amber-100 dark:bg-amber-500/20' : ''}`}>
        <SunIcon active={!dark} />
      </span>
      <span className={`rounded-full p-1 ${dark ? 'bg-indigo-100 dark:bg-indigo-500/20' : ''}`}>
        <MoonIcon active={dark} />
      </span>
    </button>
  )
}

export function AppShell() {
  const params = useParams({ strict: false }) as { cluster?: string }
  const cluster = params.cluster ?? ''
  const { data, error } = useClusters()
  const { pathname } = useLocation()
  const active = sectionFromPathname(pathname, cluster)
  return (
    <div className="flex h-dvh bg-white text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
      {cluster && (
        <Sidebar cluster={cluster} clusters={data?.clusters ?? []} active={active} error={error} version={data?.version} />
      )}
      {/* Viewport-fixed shell (owner feedback 2026-08-15): the app itself
          never scrolls — `main` is a bounded-height flex item (h-dvh ->
          flex stretch) with overflow-hidden, so document/body can't scroll
          on any page. `h-dvh` (not `h-screen`) is deliberate, per review:
          on mobile, `100vh` includes the browser chrome (address bar, etc.)
          that can slide away, which — combined with overflow-hidden — would
          make the bottom of the page permanently unreachable; the dynamic
          viewport unit tracks the ACTUAL visible height instead. Each routed
          page owns its OWN scrolling region from here down (a plain h-full
          overflow-y-auto wrapper for most pages; the Messages tab instead
          chains flex-1 min-h-0 all the way to MessageList's own scroller —
          see TopicDetailPage/Timeline). */}
      <main className="flex-1 overflow-hidden p-6">
        <Outlet />
      </main>
      {cluster && <CommandPalette cluster={cluster} />}
    </div>
  )
}
