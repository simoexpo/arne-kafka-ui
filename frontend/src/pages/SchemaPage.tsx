import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getRegistrySettings, getSubjects } from '../api/client'
import { CopyButton } from '../components/CopyButton'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { Stat } from '../components/Stat'

export function SchemaView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  const subjects = useQuery({
    queryKey: ['subjects', cluster],
    queryFn: ({ signal }) => getSubjects(cluster, signal),
    refetchInterval: 30_000,
  })
  const settings = useQuery({
    queryKey: ['registry-settings', cluster],
    queryFn: ({ signal }) => getRegistrySettings(cluster, signal),
    refetchInterval: 30_000,
  })
  const visible = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return (subjects.data?.subjects ?? []).filter((s) => q === '' || s.toLowerCase().includes(q))
  }, [subjects.data, filter])
  return (
    // Owns its own scrolling region — see OverviewPage's comment.
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Schemas</h1>
        <StalenessChip asOf={subjects.data?.as_of ?? null} refreshing={subjects.isFetching} failed={subjects.isError} />
      </div>
      {/* Mirrors Overview's cluster panel — the shared idiom for
          infrastructure facts up top (owner ruling 2026-08-18). Panel also
          renders settings failures honestly in place. */}
      <Panel
        title="Schema Registry"
        error={settings.error}
        loading={settings.isPending}
        hasData={settings.data !== undefined}
      >
        {settings.data && (
          <div className="space-y-3">
            <dl className="grid grid-cols-2 gap-2 text-sm sm:grid-cols-3">
              <Stat label="default compatibility" value={settings.data.compatibility_level} />
              <Stat label="mode" value={settings.data.mode} />
            </dl>
            <div className="flex items-center gap-1.5 font-mono text-sm">
              <span className="text-zinc-500">url</span>
              {settings.data.url}
              <CopyButton text={settings.data.url} label="registry url" />
            </div>
          </div>
        )}
      </Panel>
      <FilterInput value={filter} onChange={setFilter} placeholder="filter subjects…" ariaLabel="filter subjects" />
      <Panel
        title={`${visible.length} subjects`}
        error={subjects.error}
        loading={subjects.isPending}
        hasData={subjects.data !== undefined}
      >
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">subject</th></tr>
          </thead>
          <tbody>
            {visible.map((s) => (
              <tr key={s} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1.5">
                  <Link
                    to="/c/$cluster/schemas/$subject"
                    params={{ cluster, subject: s }}
                    className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                  >
                    {s}
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  )
}

export function SchemaPage() {
  const { cluster } = useParams({ from: '/c/$cluster/schemas' })
  return <SchemaView cluster={cluster} />
}
