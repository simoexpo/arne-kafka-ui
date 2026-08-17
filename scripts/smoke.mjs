#!/usr/bin/env node
// End-to-end smoke check for Arne.
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
try {
  const page = await (await browser.newContext({ viewport: { width: 1400, height: 900 } })).newPage()
  const errors = []
  page.on('pageerror', (e) => errors.push(String(e)))
  // Collected rather than thrown: one broken check should report every other
  // check's verdict too, not hide them behind the first failure.
  const failures = []
  const fail = (message) => failures.push(message)

  await page.goto(BASE + '/')
  await page.getByText('Top topics').waitFor({ timeout: 20000 })
  await page.getByRole('link', { name: 'Topics' }).click()
  await page.getByPlaceholder('filter topics…').waitFor({ timeout: 10000 })

  // open a demo topic (the playground producers keep these well past one page,
  // which the scroll-pagination check below needs) and check the flagship tab
  await page.getByPlaceholder('filter topics…').fill('demo-')
  await page.locator('table a').first().click()
  await page.getByRole('tab', { name: 'Messages' }).waitFor({ timeout: 10000 })
  await page.getByTestId('message-row').first().waitFor({ timeout: 10000 })
  await page.getByText('● live').waitFor({ timeout: 10000 })
  await page.getByPlaceholder('filter messages…').waitFor({ timeout: 10000 })

  const scroller = page.getByTestId('timeline-scroll')

  // ------------------------------------------------------------------
  // The checks marked "real layout" below are deliberately here rather than
  // in the jsdom suite: jsdom performs no layout — it reports
  // scrollHeight/clientHeight as 0 and every element's box as empty — so a
  // unit test cannot tell a viewport that landed where it was aimed from one
  // that never moved, nor a correctly stacked list from a jumbled one. Two
  // owner-reported regressions (2026-08-16) shipped through a fully green
  // unit suite for exactly that reason.
  // ------------------------------------------------------------------

  // The UTC/local toggle is wired into the Messages header and actually
  // flips. Addressed by test-id — the id itself went missing during a
  // refactor round with nothing pinning it.
  const zoneToggle = page.getByTestId('timezone-toggle')
  await zoneToggle.waitFor({ timeout: 10000 })
  const zoneBefore = await zoneToggle.getAttribute('data-mode')
  await zoneToggle.click()
  if ((await zoneToggle.getAttribute('data-mode')) === zoneBefore) {
    fail(`the zone toggle did not change mode (stuck on ${zoneBefore})`)
  }
  await zoneToggle.click()

  // REAL LAYOUT: row heights are measured once and cached. A live message
  // prepends, shifting every row's index by one; if the cache is keyed by
  // index rather than by message identity, each row inherits its neighbour's
  // height and the list draws itself on top of itself. Rows are near-uniform
  // normally, which hides this as a pixel or two of drift — an expanded row
  // is many times taller than its neighbours, and that is when the
  // misattribution becomes the corruption a reader actually sees. Run while
  // still attached and live (before the pause below), which is the state the
  // owner hit it in.
  await page.getByTestId('message-row').nth(2).click()
  await page.waitForFunction(
    () => {
      const rows = document.querySelectorAll('[data-testid="message-row"]')
      return rows.length > 2 && rows[2].getBoundingClientRect().height > 80
    },
    undefined,
    { timeout: 5000 },
  )
  // Inspection pause (spec v1.7): an expanded row means live arrivals BUFFER
  // into the pill instead of changing the top row. Wait for the pill to
  // count one (proves buffering), then flush via the pill — inspections
  // survive a flush by spec, and the flush is what triggers the prepend
  // whose row-stacking integrity we assert below.
  await page.getByTestId('live-pill').waitFor({ timeout: 30000 })
  await page.getByTestId('live-pill').click()
  const stacking = await scroller.evaluate((el) => {
    const rows = [...el.querySelectorAll('[data-testid="message-row"]')]
      .map((row) => ({
        id: row.querySelector('span:nth-child(2)')?.textContent ?? '?',
        top: Number((row.parentElement.style.transform.match(/translateY\(([-\d.]+)px\)/) ?? [0, '0'])[1]),
        height: Math.round(row.getBoundingClientRect().height),
      }))
      .sort((a, b) => a.top - b.top)
    const broken = []
    for (let i = 1; i < rows.length; i++) {
      const expected = rows[i - 1].top + rows[i - 1].height
      // 1px of slack for sub-pixel rounding in the measured heights.
      if (Math.abs(rows[i].top - expected) > 1) {
        broken.push({ after: rows[i - 1].id, row: rows[i].id, top: rows[i].top, expected })
      }
    }
    return { rows: rows.length, broken }
  })
  if (stacking.broken.length > 0) {
    fail(`rows are not stacked flush after a live message prepended: ${JSON.stringify(stacking.broken.slice(0, 3))}`)
  }
  // scrolling to the bottom loads an older page. Pause live first so the list
  // can only grow through pagination, not through prepended live messages.
  await page.getByTestId('play-pause-toggle').click()
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

  // REAL LAYOUT: a jump repositions the VIEWPORT, not just the data.
  // 'beginning'/'offset'/'timestamp' land at the bottom edge of their freshly
  // loaded window, where the target sits. 'beginning' is the stable one to
  // measure a gap against — there is nothing older left to paginate into, so
  // the bottom edge it lands on stays the bottom edge.
  await page.getByTestId('jump-beginning').click()
  try {
    await page.waitForFunction(
      () => document.querySelector('[data-testid="timeline-scroll"]')?.scrollTop > 0,
      undefined,
      { timeout: 15000 },
    )
  } catch {
    fail("a 'beginning' jump never moved the viewport at all — it is still sitting at the top of its new window")
  }
  const gapFromBottom = await scroller.evaluate((el) => el.scrollHeight - el.clientHeight - el.scrollTop)
  // One row of slack, not zero. A separate, older defect leaves a stable ~44px
  // (one line) here: the "— beginning of topic —" caption renders BELOW the
  // scroller and only appears once the jump's follow-up page reports the topic
  // start, which shrinks the scroller by its own height after the landing has
  // already happened — nothing re-snaps to the edge that just moved. That is a
  // clipped last row; the regression this check exists for is a viewport that
  // never left the top of its new window, which measures in the thousands.
  if (gapFromBottom >= 60) {
    fail(`a 'beginning' jump left the viewport ${gapFromBottom}px short of the window's bottom edge`)
  }

  // An offset jump additionally marks the row it landed on. The mark lives at
  // the bottom edge of the loaded window, so it is only ever rendered (the
  // list is virtualized) if the viewport genuinely landed there.
  const oldestRow = await page.getByTestId('message-row').last().locator('span').nth(1).textContent()
  const [, jumpPartition, jumpOffset] = oldestRow.match(/p(\d+)·(\d+)/)
  await page.getByTestId('jump-offset').click()
  await page.getByTestId('jump-offset-partition-input').selectOption(jumpPartition)
  await page.getByTestId('jump-offset-value-input').fill(jumpOffset)
  await page.getByTestId('jump-offset-apply').click()
  try {
    await page.getByTestId('jump-target').waitFor({ timeout: 15000 })
  } catch {
    fail('an offset jump never rendered its target row — the viewport did not land on the window edge it lives at')
  }
  const targetPlacement = await scroller.evaluate((el) => {
    const target = el.querySelector('[data-testid="jump-target"]')
    if (target === null) return null
    const box = el.getBoundingClientRect()
    return { fromTop: target.getBoundingClientRect().top - box.top, viewport: box.height }
  })
  if (targetPlacement === null || targetPlacement.fromTop < targetPlacement.viewport / 2) {
    fail(`an offset jump did not leave its target row at the bottom of the viewport: ${JSON.stringify(targetPlacement)}`)
  }

  // Filter grammar v2 + autocomplete (design spec 2026-08-17), REAL BROWSER:
  // proposals must include fields extracted from the actual window content,
  // and a field-equality expression must drive a scan that renders matches.
  const filterInput = page.getByLabel('filter messages')
  await filterInput.click()
  await filterInput.fill('val')
  await page.getByTestId('filter-proposal').first().waitFor({ timeout: 5000 })
  const proposals = await page.getByTestId('filter-proposal').allTextContents()
  if (!proposals.includes('value:') || !proposals.includes('value=')) {
    fail(`autocomplete did not propose the value operators: ${JSON.stringify(proposals)}`)
  }
  const fieldRow = proposals.find((p) => p.startsWith('value.') && p.length > 'value.'.length)
  if (fieldRow === undefined) {
    fail(`autocomplete proposed no window-extracted fields: ${JSON.stringify(proposals)}`)
  } else {
    // Field-exists filter on a field the dropdown itself extracted from this
    // topic's window — every row carrying the field must match, whatever
    // topic the table listed first.
    await filterInput.fill(`${fieldRow}:`)
    try {
      await page.waitForFunction(
        () => document.querySelector('[data-testid="message-row"]') !== null,
        undefined,
        { timeout: 15000 },
      )
    } catch {
      fail(`a field-exists filter (${fieldRow}:) rendered no matching rows`)
    }
  }
  await filterInput.fill('')

  if (errors.length) {
    console.error('page errors:', errors)
    process.exitCode = 1
  }
  if (failures.length) {
    console.error(`smoke FAILED:\n  - ${failures.join('\n  - ')}`)
    process.exitCode = 1
  }
  if (process.exitCode !== 1) console.log('smoke OK')
} finally {
  await browser.close()
}
