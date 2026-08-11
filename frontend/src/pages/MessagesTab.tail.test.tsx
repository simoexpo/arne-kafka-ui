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

describe('MessagesTab tail', () => {
  it('streams messages newest-first while tailing and closes on toggle off', async () => {
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    await screen.findByText('no messages')
    await userEvent.click(screen.getByRole('button', { name: 'Tail' }))
    const es = FakeEventSource.instances[0]
    expect(es.url).toContain('/tail')
    es.emit('message', mk(1))
    es.emit('message', mk(2))
    expect(await screen.findByText('p0·2')).toBeInTheDocument()
    const rows = screen.getAllByTestId('message-row')
    expect(rows[0]).toHaveTextContent('p0·2') // newest first
    expect(screen.getByText(/live/)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Tail' }))
    expect(es.closed).toBe(true)
  })

  it('surfaces server error events and stops', async () => {
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    await screen.findByText('no messages')
    await userEvent.click(screen.getByRole('button', { name: 'Tail' }))
    FakeEventSource.instances[0].emit('message', mk(1))
    FakeEventSource.instances[0].emit('error', { code: 'kafka_error', message: 'broker gone', cluster: 'prod', retriable: true })
    expect(await screen.findByText(/broker gone/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Tail' })).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByText('p0·1')).toBeInTheDocument()
    expect(screen.getByText('tail stopped — showing last received messages')).toBeInTheDocument()
  })
})
