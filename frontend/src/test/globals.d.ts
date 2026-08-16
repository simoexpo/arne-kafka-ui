// Ambient `process.env` type for tests that read env vars (e.g. a fixed-TZ
// fixture reading `process.env.TZ`). This app is browser-only and
// deliberately doesn't include `@types/node` in tsconfig — this narrow
// declaration provides just the one shape several test files need, instead
// of each file re-declaring it locally.
export {}

declare global {
  const process: { env: Record<string, string | undefined> }
}
