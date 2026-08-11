import { useQuery } from '@tanstack/react-query'
import { Outlet, useParams } from '@tanstack/react-router'
import { getClusters } from '../api/client'
import type { ClusterHealth } from '../api/types'
import { CommandPalette } from '../components/CommandPalette' // Task 11 stub

export function useClusters() {
  return useQuery({ queryKey: ['clusters'], queryFn: getClusters, refetchInterval: 10_000 })
}

const SECTIONS = ['overview', 'topics', 'groups'] as const
type Section = (typeof SECTIONS)[number]

export function Sidebar({ cluster, clusters, active }: {
  cluster: string
  clusters: ClusterHealth[]
  active: Section
}) {
  return (
    <aside className="flex w-56 flex-col gap-6 border-r border-zinc-200 p-4 dark:border-zinc-800">
      <div className="text-sm font-semibold tracking-wide">Betrachtung</div>
      <nav className="flex flex-col gap-1">
        {SECTIONS.map((s) => (
          <a
            key={s}
            href={`/c/${encodeURIComponent(cluster)}/${s}`}
            className={`rounded px-2 py-1 text-sm capitalize ${
              s === active
                ? 'bg-zinc-200 font-medium dark:bg-zinc-800'
                : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-900'
            }`}
          >
            {s}
          </a>
        ))}
      </nav>
      <div className="mt-auto">
        <div className="mb-1 text-xs text-zinc-500">cluster</div>
        <div className="flex items-center gap-2 text-sm font-medium">
          <HealthDot status={clusters.find((c) => c.name === cluster)?.status} />
          {cluster}
        </div>
        <div className="mt-2 flex flex-col gap-1">
          {clusters.filter((c) => c.name !== cluster).map((c) => (
            <a
              key={c.name}
              href={`/c/${encodeURIComponent(c.name)}/${active}`}
              className="flex items-center gap-2 rounded px-1 py-0.5 text-sm text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-900"
            >
              <HealthDot status={c.status} />
              {c.name}
            </a>
          ))}
        </div>
        <div className="mt-4"><ThemeToggle /></div>
      </div>
    </aside>
  )
}

function HealthDot({ status }: { status?: ClusterHealth['status'] }) {
  const color = status === 'healthy' ? 'bg-emerald-500' : status === 'unreachable' ? 'bg-red-500' : 'bg-zinc-400'
  return <span className={`inline-block h-2 w-2 rounded-full ${color}`} />
}

export function ThemeToggle() {
  return (
    <button
      aria-label="toggle theme"
      className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
      onClick={() => {
        const el = document.documentElement
        const dark = el.classList.toggle('dark')
        localStorage.theme = dark ? 'dark' : 'light'
      }}
    >
      theme
    </button>
  )
}

export function AppShell() {
  const params = useParams({ strict: false }) as { cluster?: string }
  const cluster = params.cluster ?? ''
  const { data } = useClusters()
  const active: Section =
    location.pathname.includes('/groups') ? 'groups'
    : location.pathname.includes('/topics') ? 'topics'
    : 'overview'
  return (
    <div className="flex min-h-screen bg-white text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
      {cluster && <Sidebar cluster={cluster} clusters={data?.clusters ?? []} active={active} />}
      <main className="flex-1 p-6">
        <Outlet />
      </main>
      {cluster && <CommandPalette cluster={cluster} />}
    </div>
  )
}
