// librdkafka reports the negotiated protocol by its wire name; operators know
// these by their assignor class. Anything unrecognised — a custom assignor, or
// a KIP-848 group's server-side name — passes through untranslated rather than
// being forced into one of these four.
const CLASSES: Record<string, string> = {
  range: 'Range',
  roundrobin: 'RoundRobin',
  sticky: 'Sticky',
  'cooperative-sticky': 'CooperativeSticky',
}

export function assignorClass(protocol: string): string {
  return protocol ? (CLASSES[protocol] ?? protocol) : '—'
}
