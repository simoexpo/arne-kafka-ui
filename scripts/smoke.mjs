#!/usr/bin/env node
// End-to-end smoke check for Betrachtung.
//
// Expects the dev stack to already be running (this script boots nothing):
//   - backend on :8080 with a `local` cluster configured
//   - vite on :5173 (proxying /api to the backend)
// Run `just dev` (or the raw equivalent in the README) first, then `just smoke`
// or `node scripts/smoke.mjs`.
//
// Requires Google Chrome to be installed locally — the script launches it via
// Playwright's `channel: 'chrome'`, so no browser binary is downloaded by
// `npm install`. Playwright itself is a frontend dev dependency
// (frontend/node_modules); it's resolved here via createRequire so this
// script works when invoked as `node scripts/smoke.mjs` from the repo root.
import { createRequire } from 'node:module'

const require = createRequire(new URL('../frontend/', import.meta.url))
const { chromium } = require('playwright')

const BASE = process.env.SMOKE_BASE ?? 'http://localhost:5173'

async function up(url) {
  try {
    return (await fetch(url)).ok
  } catch {
    return false
  }
}

if (!(await up(`${BASE}/`))) {
  console.error(`dev server not reachable at ${BASE} — run \`just dev\``)
  process.exit(1)
}
if (!(await up(`${BASE}/api/clusters`))) {
  console.error('backend not reachable through the /api proxy')
  process.exit(1)
}

const browser = await chromium.launch({ channel: 'chrome', headless: true })
const page = await (await browser.newContext({ viewport: { width: 1400, height: 900 } })).newPage()
const errors = []
page.on('pageerror', (e) => errors.push(String(e)))

await page.goto(BASE + '/')
await page.getByText('Top topics').waitFor({ timeout: 20000 })
await page.getByRole('link', { name: 'Topics' }).click()
await page.getByPlaceholder('filter topics…').waitFor({ timeout: 10000 })

// open a demo topic (the playground producers keep these well past one page,
// which the scroll-pagination check below needs) and check the flagship tab
await page.getByPlaceholder('filter topics…').fill('demo-')
await page.locator('table a').first().click()
await page.getByRole('button', { name: 'Messages' }).waitFor({ timeout: 10000 })
await page.getByTestId('message-row').first().waitFor({ timeout: 10000 })
await page.getByText('● live').waitFor({ timeout: 10000 })
await page.getByPlaceholder('filter messages…').waitFor({ timeout: 10000 })

// scrolling to the bottom loads an older page. Pause live first so the list
// can only grow through pagination, not through prepended live messages.
await page.getByTestId('play-pause-toggle').click()
const scroller = page.getByTestId('timeline-scroll')
const heightBefore = await scroller.evaluate((el) => el.scrollHeight)
await scroller.evaluate((el) => { el.scrollTop = el.scrollHeight })
await page.waitForFunction(
  (prev) => {
    const el = document.querySelector('[data-testid="timeline-scroll"]')
    return el !== null && el.scrollHeight > prev
  },
  heightBefore,
  { timeout: 10000 },
)

if (errors.length) {
  console.error('page errors:', errors)
  process.exit(1)
}
console.log('smoke OK')
await browser.close()
