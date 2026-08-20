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
import { nextAnchor, pageFrom, prevAnchor } from '../lib/namePage'

/// Lag costs one broker request per group, so a page is what we ask for.
const PAGE_SIZE = 50

export function GroupsView({ cluster }: { cluster: string }) {
  const [filter, setFilter] = useState('')
  // Pages are anchored to a group id, never to an index: groups come and go
  // between polls, and an index would shift the page under the reader.
  const [anchor, setAnchor] = useState<string | null>(null)
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
  const id = (g: { group_id: string }) => g.group_id
  const visible = useMemo(() => pageFrom(matching, id, anchor, PAGE_SIZE), [matching, anchor])
  const forward = nextAnchor(matching, id, anchor, PAGE_SIZE)
  const back = prevAnchor(matching, id, anchor, PAGE_SIZE)
  const onFirstPage = anchor === null

  // Only the rows on screen: off-screen groups are never inspected.
  const shown = visible.map(id)
  const lag = useQuery({
    queryKey: ['group-lag', cluster, shown],
    queryFn: ({ signal }) => getGroupLag(cluster, shown, signal),
    enabled: shown.length > 0,
    refetchInterval: 10_000,
  })
  const lagOf = (group: string) => lag.data?.groups.find((e) => e.group_id === group)

  return (
    // Owns its own scrolling region — see OverviewPage's comment.
    <div className="h-full space-y-4 overflow-y-auto">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Consumer groups</h1>
        <StalenessChip asOf={groups.data?.as_of ?? null} refreshing={groups.isFetching} failed={groups.isError} />
      </div>
      <div className="flex items-center gap-4">
        <FilterInput
          value={filter}
          onChange={(next) => {
            // A new filter means a different list; paging into it from the old
            // list's anchor would land somewhere arbitrary.
            setFilter(next)
            setAnchor(null)
          }}
          placeholder="filter consumers…"
          ariaLabel="filter consumers"
        />
      </div>
      <Panel
        title={`${visible.length} groups`}
        error={groups.error}
        loading={groups.isPending}
        hasData={groups.data !== undefined}
      >
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
                <MemberCount count={g.member_count} />
                <td>
                  <TotalLag entry={lagOf(g.group_id)} pending={lag.isPending} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {matching.length > PAGE_SIZE && (
          <div className="mt-3 flex items-center gap-3 text-sm">
            <button
              onClick={() => setAnchor(back)}
              disabled={onFirstPage}
              className="rounded border border-zinc-300 px-2 py-0.5 enabled:hover:bg-zinc-100 disabled:text-zinc-400 dark:border-zinc-700 dark:enabled:hover:bg-zinc-800"
            >
              prev
            </button>
            <button
              onClick={() => setAnchor(forward)}
              disabled={forward === null}
              className="rounded border border-zinc-300 px-2 py-0.5 enabled:hover:bg-zinc-100 disabled:text-zinc-400 dark:border-zinc-700 dark:enabled:hover:bg-zinc-800"
            >
              next
            </button>
            <span className="text-zinc-500">
              {visible.length} of {matching.length}
            </span>
          </div>
        )}
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

/// A count this page cannot take is stated as unknown, never as zero: the
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
