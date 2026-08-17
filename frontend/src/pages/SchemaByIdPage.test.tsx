import { beforeEach, describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { SchemaByIdView } from './SchemaByIdPage'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getSubjectOfId: vi.fn(),
  getSubjectDetail: vi.fn(),
  getSubjectStrategy: vi.fn(),
  getCompatibilityLevel: vi.fn(),
}))

beforeEach(() => {
  vi.mocked(client.getSubjectStrategy).mockResolvedValue({ strategy: null, topic: null, role: null, as_of: 1 })
  vi.mocked(client.getCompatibilityLevel).mockResolvedValue({ level: 'BACKWARD', as_of: 1 })
})

describe('SchemaByIdView', () => {
  // A schema id names one exact version of a subject — the page must land
  // on THAT version, not the subject's latest (owner ruling 2026-08-18).
  it('forwards to the version the id belongs to, not latest', async () => {
    vi.mocked(client.getSubjectOfId).mockResolvedValue({ subject: 'sr-avro-value', version: 1, as_of: 1 })
    vi.mocked(client.getSubjectDetail).mockResolvedValue({
      subject: 'sr-avro-value',
      versions: [1, 2, 3],
      version: 1,
      id: 40,
      schema_type: 'AVRO',
      schema: '"string"',
      as_of: 1,
    })
    await renderWithRouter(<SchemaByIdView cluster="prod" id={40} />, { initialPath: '/c/prod/schemas/by-id/40' })
    expect(await screen.findByText('id 40')).toBeInTheDocument()
    expect((screen.getByLabelText('version') as HTMLSelectElement).value).toBe('1')
    expect(vi.mocked(client.getSubjectDetail)).toHaveBeenCalledWith('prod', 'sr-avro-value', 1, expect.anything())
  })

  it('renders the not-found envelope for an unknown id', async () => {
    vi.mocked(client.getSubjectOfId).mockRejectedValue(
      new client.ApiError(404, 'schema_id_not_found', 'no subject registered schema id 99', 'prod', false),
    )
    await renderWithRouter(<SchemaByIdView cluster="prod" id={99} />, { initialPath: '/c/prod/schemas/by-id/99' })
    expect(await screen.findByText(/no subject registered schema id 99/)).toBeInTheDocument()
  })
})
