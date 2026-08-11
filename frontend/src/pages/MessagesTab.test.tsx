import { describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery } from '../test/utils'
import { MessagesTab } from './MessagesTab'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getMessages: vi.fn(),
}))

const page = {
  messages: [
    { partition: 0, offset: 9, timestamp_ms: 1, key: null, value: { encoding: 'utf8' as const, text: 'v9', schema_id: null, error: null }, headers: [] },
    { partition: 0, offset: 8, timestamp_ms: 1, key: null, value: { encoding: 'utf8' as const, text: 'v8', schema_id: null, error: null }, headers: [] },
  ],
  as_of: Date.now(),
}

describe('MessagesTab browse', () => {
  it('loads latest messages by default and renders them', async () => {
    vi.mocked(client.getMessages).mockResolvedValue(page)
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    expect(await screen.findByText('p0·9')).toBeInTheDocument()
    expect(vi.mocked(client.getMessages).mock.calls[0].slice(0, 3)).toEqual([
      'prod', 'orders', { anchor: 'latest', limit: 50 },
    ])
  })

  it('switches to offset anchor and reloads', async () => {
    vi.mocked(client.getMessages).mockResolvedValue(page)
    renderWithQuery(<MessagesTab cluster="prod" topic="orders" />)
    await screen.findByText('p0·9')
    await userEvent.selectOptions(screen.getByLabelText('anchor'), 'offset')
    await userEvent.type(screen.getByLabelText('partition'), '0')
    await userEvent.clear(screen.getByLabelText('offset'))
    await userEvent.type(screen.getByLabelText('offset'), '5')
    await userEvent.click(screen.getByRole('button', { name: 'Load' }))
    const last = vi.mocked(client.getMessages).mock.calls.at(-1)!
    expect(last.slice(0, 3)).toEqual(['prod', 'orders', { anchor: 'offset', partition: 0, offset: 5, limit: 50 }])
  })

  it('renders api errors in the panel', async () => {
    vi.mocked(client.getMessages).mockRejectedValue(
      new client.ApiError(404, 'topic_not_found', "topic 'x' does not exist", 'prod', false),
    )
    renderWithQuery(<MessagesTab cluster="prod" topic="x" />)
    expect(await screen.findByText(/topic_not_found/)).toBeInTheDocument()
  })
})
