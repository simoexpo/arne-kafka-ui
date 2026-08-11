import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery } from '../test/utils'
import { FakeEventSource } from '../test/fake-event-source'
import { MessagesTab } from './MessagesTab'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getMessages: vi.fn().mockResolvedValue({ messages: [], as_of: 1 }),
}))

beforeEach(() => FakeEventSource.install())
afterEach(() => FakeEventSource.uninstall())

const mk = (offset: number) => ({
  partition: 0, offset, timestamp_ms: 1, key: null,
  value: { encoding: 'utf8', text: `v${offset}`, schema_id: null, error: null }, headers: [],
})

async function startSearch() {
  renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
  await screen.findByText('no messages')
  await userEvent.selectOptions(screen.getByLabelText('filter'), 'value_contains')
  await userEvent.type(screen.getByLabelText('query'), 'v4')
  await userEvent.click(screen.getByRole('button', { name: 'Search' }))
  return FakeEventSource.instances.at(-1)!
}

describe('MessagesTab search', () => {
  it('streams matches with progress and completes', async () => {
    const es = await startSearch()
    expect(es.url).toContain('range=last_n&n=1000&filter=value_contains&q=v4')
    es.emit('progress', { scanned: 100, total: 1000, matches: 1 })
    es.emit('match', mk(4))
    es.emit('progress', { scanned: 1000, total: 1000, matches: 1 })
    es.emit('done', { reason: 'complete' })
    expect(FakeEventSource.instances.at(-1)!.closed).toBe(true)
    expect(await screen.findByText('p0·4')).toBeInTheDocument()
    expect(screen.getByText(/1000\/1000 scanned · 1 matches/)).toBeInTheDocument()
    expect(screen.getByText(/complete/)).toBeInTheDocument()
  })

  it('cancel closes the stream', async () => {
    const es = await startSearch()
    es.emit('progress', { scanned: 10, total: 1000, matches: 0 })
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(es.closed).toBe(true)
    expect(screen.getByText(/cancelled/)).toBeInTheDocument()
  })

  it('max_matches advises refining the filter', async () => {
    const es = await startSearch()
    es.emit('done', { reason: 'max_matches' })
    expect(await screen.findByText(/refine your filter/)).toBeInTheDocument()
  })

  it('server errors surface in red', async () => {
    const es = await startSearch()
    es.emit('error', { code: 'kafka_timeout', message: 'fetch metadata timed out' })
    expect(FakeEventSource.instances.at(-1)!.closed).toBe(true)
    expect(await screen.findByText(/kafka_timeout/)).toBeInTheDocument()
  })

  it('disables Search for json_eq with an empty path', async () => {
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    await screen.findByText('no messages')
    await userEvent.selectOptions(screen.getByLabelText('filter'), 'json_eq')
    await userEvent.type(screen.getByLabelText('query'), '42')
    expect(screen.getByRole('button', { name: 'Search' })).toBeDisabled()
  })

  it('disables Search for an offsets range with empty bounds', async () => {
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    await screen.findByText('no messages')
    await userEvent.selectOptions(screen.getByLabelText('filter'), 'value_contains')
    await userEvent.type(screen.getByLabelText('query'), 'v4')
    await userEvent.selectOptions(screen.getByLabelText('range'), 'offsets')
    expect(screen.getByRole('button', { name: 'Search' })).toBeDisabled()
  })
})
