// (partition, offset) is a message row's unique identity — used by
// MessageList (the virtualizer's `getItemKey`) and Timeline (expansion
// ownership, `expandedKeysRef`) to agree on the SAME key format without
// drifting apart. `lib/timelineStore.ts`'s own `partitionKey` is a separate,
// differently-formatted (`:`-joined) key for that module's internal dedupe
// set only — it is not a third copy of this one.
export function rowKey(partition: number, offset: number): string {
  return `${partition}-${offset}`
}
