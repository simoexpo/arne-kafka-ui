import { afterEach, beforeEach } from 'vitest'

/**
 * Sets `process.env.TZ` to `zone` for the duration of the enclosing
 * `describe` block, restoring whatever it was before afterward. No global
 * `TZ` is set by default (`vite.config.ts` only sets `environment` +
 * `setupFiles`), so this is load-bearing wherever it's used, not incidental.
 * Call once, directly inside a `describe(...)` callback.
 */
export function withFixedTZ(zone: string): void {
  let original: string | undefined
  beforeEach(() => {
    original = process.env.TZ
    process.env.TZ = zone
  })
  afterEach(() => {
    process.env.TZ = original
  })
}
