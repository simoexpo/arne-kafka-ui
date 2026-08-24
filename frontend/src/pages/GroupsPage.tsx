import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from '@tanstack/react-router'
import { getGroupLag, getGroups } from '../api/client'
import type { GroupLagEntry } from '../api/types'
import { CopyButton } from '../components/CopyButton'
import { FilterInput } from '../components/FilterInput'
import { Panel } from '../components/Panel'
import { StalenessChip } from '../components/StalenessChip'
import { statedLag } from '../lib/lag'
import { useListWindow } from '../lib/listWindow'

/// FE windowing over the single groups response: rendering is free, so the
/// window grows as the reader scrolls and never unmounts under them.
const RENDER_STEP = 100
/// Lag costs one broker request per group, so what we ask for is bounded by
/// the viewport — the chunk of rows on screen — never by scroll depth.
const LAG_CHUNK = 50

export function GroupsView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  const groups = useQuery({
    queryKey: ['groups', cluster],
    queryFn: ({ signal }) => getGroups(cluster, signal),
    refetchInterval: 10_000,
  })
  // Narrows the fetched snapshot in place: no request per keystroke, and the
  // count below reports what the filter actually left.
  const matching = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return (groups.data?.groups ?? []).filter((g) => q === '' || g.group_id.toLowerCase().includes(q))
  }, [groups.data, filter])
  const window = useListWindow(matching.length, RENDER_STEP, LAG_CHUNK)
  const visible = matching.slice(0, window.count)

  // Only the rows on screen: off-screen groups are never inspected.
  const shown = matching.slice(window.viewport.start, window.viewport.end).map((g) => g.group_id)
  const lag = useQuery({
    queryKey: ['group-lag', cluster, shown],
    queryFn: ({ signal }) => getGroupLag(cluster, shown, signal),
    enabled: shown.length > 0,
    refetchInterval: 10_000,
  })
  const lagOf = (group: string) => lag.data?.groups.find((e) => e.group_id === group)

  return (
    // The page itself never scrolls: header and filter stay put, and the list
    // below owns the one scrolling region (the app shell's `main` is
    // overflow-hidden — see AppShell).
    <div className="flex h-full min-h-0 flex-col gap-4">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Consumer groups</h1>
        <StalenessChip asOf={groups.data?.as_of ?? null} refreshing={groups.isFetching} failed={groups.isError} />
      </div>
      <div className="flex items-center gap-4">
        <FilterInput
          value={filter}
          onChange={(next) => {
            // A new filter means a different list; a window scrolled deep into
            // the old one would land somewhere arbitrary.
            setFilter(next)
            window.reset()
          }}
          placeholder="filter consumers…"
          ariaLabel="filter consumers"
        />
      </div>
      <Panel
        title={`${matching.length} groups`}
        error={groups.error}
        loading={groups.isPending}
        hasData={groups.data !== undefined}
        className="flex min-h-0 flex-1 flex-col"
      >
        <div data-testid="list-scroller" onScroll={window.onScroll} className="min-h-0 flex-1 overflow-y-auto">
        <table className="w-full text-left text-sm">
          <thead className="text-xs text-zinc-500">
            <tr><th className="py-1">group</th><th>state</th><th>protocol</th><th>members</th><th>total lag</th></tr>
          </thead>
          <tbody>
            {visible.map((g) => (
              <tr key={g.group_id} data-testid="group-row" className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="py-1.5">
                  <Link
                    to="/c/$cluster/consumers/$group"
                    params={{ cluster, group: g.group_id }}
                    className="font-mono text-blue-600 hover:underline dark:text-blue-400"
                  >
                    {g.group_id}
                  </Link>
                  <span className="ml-1.5">
                    <CopyButton text={g.group_id} label={g.group_id} />
                  </span>
                </td>
                <td>{g.state}</td>
                <td className="text-zinc-500">{protocolLabel(g)}</td>
                <MemberCount count={g.member_count ?? lagOf(g.group_id)?.member_count ?? null} />
                <td>
                  <TotalLag entry={lagOf(g.group_id)} pending={lag.isPending} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </Panel>
    </div>
  )
}

/// The rebalance protocol the coordinator named, which is what an operator
/// wants here — `Consumer` groups behave differently and report less through
/// the classic APIs. A broker too old to name it leaves the raw protocol type
/// standing rather than labelling the group wrongly.
function protocolLabel(g: { group_type: string; protocol_type: string }): string {
  return g.group_type && g.group_type !== 'Unknown' ? g.group_type : g.protocol_type
}

/// A count the cluster-wide list could not take comes from the visible page's
/// batch instead. Until it arrives it is stated as unknown, never as zero: the
/// classic describe reports no members for a KIP-848 group however many it has.
function MemberCount({ count }: { count: number | null }) {
  if (count != null) return <td data-testid="group-members">{count}</td>
  return (
    <td
      data-testid="group-members"
      className="cursor-help text-zinc-400"
      title="a KIP-848 group's members cannot be counted from this view — open the group to see them"
    >
      —
    </td>
  )
}

/// Lag for one row. A dash is not zero: it means the group has committed
/// nothing anywhere (no position to be behind), its lookup failed, or we have
/// not asked yet — and the title says which.
function TotalLag({ entry, pending }: { entry?: GroupLagEntry; pending: boolean }) {
  if (entry?.total_lag != null) {
    const { text, title } = statedLag(entry.total_lag, entry.unreadable_partitions)
    return (
      <span
        data-testid="group-total-lag"
        title={title}
        className={`${entry.total_lag > 0 ? 'font-semibold' : 'text-zinc-400'} ${title ? 'cursor-help' : ''}`}
      >
        {text}
      </span>
    )
  }
  const title = entry?.error
    ? `Kafka couldn't read this group's offsets — ${entry.error}`
    : entry && entry.unreadable_partitions > 0
      ? `none of this group's ${entry.unreadable_partitions} committed partition(s) could be read`
      : entry
        ? 'this group has not committed any offset, so it has no position to be behind'
        : pending
          ? "reading this group's offsets…"
          : 'no lag reported for this group'
  return (
    <span data-testid="group-total-lag" className="cursor-help text-zinc-400" title={title}>
      —
    </span>
  )
}

export function GroupsPage() {
  const { cluster } = useParams({ from: '/c/$cluster/consumers' })
  return <GroupsView cluster={cluster} />
}
