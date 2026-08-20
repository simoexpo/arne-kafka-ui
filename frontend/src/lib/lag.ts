import { formatCount } from './format'

// A lag total summed from an incomplete snapshot is a lower bound, never a
// total: the partitions we could not read can only add to it. Every view that
// totals lag states it through here, so none of them can quietly under-report.
export function statedLag(total: number, unreadable: number): { text: string; title?: string } {
  if (unreadable === 0) return { text: formatCount(total) }
  return {
    text: `≥ ${formatCount(total)}`,
    title: `at least this much — ${unreadable} partition(s) could not be read, so the real total is higher`,
  }
}
