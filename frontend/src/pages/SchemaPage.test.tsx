import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { SchemaView } from './SchemaPage'
import * as client from '../api/client'
import { ApiError } from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getSubjects: vi.fn(),
  getRegistrySettings: vi.fn(),
}))

beforeEach(() => {
  vi.mocked(client.getRegistrySettings).mockResolvedValue({
    compatibility_level: 'BACKWARD',
    mode: 'READWRITE',
    as_of: 1,
  })
})

describe('SchemaView', () => {
  it('shows registry-wide settings above the list', async () => {
    vi.mocked(client.getSubjects).mockResolvedValue({ subjects: [], as_of: 1 })
    await renderWithRouter(<SchemaView cluster="prod" />, { initialPath: '/c/prod/schemas' })
    expect(await screen.findByText('BACKWARD')).toBeInTheDocument()
    expect(screen.getByText('READWRITE')).toBeInTheDocument()
    expect(screen.getByText(/compatibility/i)).toBeInTheDocument()
  })

  it('lists subjects and links to detail via SPA navigation', async () => {
    vi.mocked(client.getSubjects).mockResolvedValue({
      subjects: ['sr-avro-value', 'sr-json-value'],
      as_of: 1,
    })
    await renderWithRouter(<SchemaView cluster="prod" />, { initialPath: '/c/prod/schemas' })
    expect(await screen.findByText('sr-avro-value')).toBeInTheDocument()
    const link = screen.getByRole('link', { name: 'sr-avro-value' })
    expect(link).toHaveAttribute('href', '/c/prod/schemas/sr-avro-value')
    expect(screen.getByText('2 subjects')).toBeInTheDocument()
  })

  it('filters subjects by the filter box', async () => {
    vi.mocked(client.getSubjects).mockResolvedValue({
      subjects: ['sr-avro-value', 'orders-value'],
      as_of: 1,
    })
    await renderWithRouter(<SchemaView cluster="prod" />, { initialPath: '/c/prod/schemas' })
    await screen.findByText('sr-avro-value')
    fireEvent.change(screen.getByLabelText('filter subjects'), { target: { value: 'orders' } })
    expect(screen.queryByText('sr-avro-value')).not.toBeInTheDocument()
    expect(screen.getByText('orders-value')).toBeInTheDocument()
  })

  it('renders the no-registry envelope honestly in the panel', async () => {
    vi.mocked(client.getSubjects).mockRejectedValue(
      new ApiError(400, 'no_schema_registry', "no schema registry configured for cluster 'prod'", 'prod', false),
    )
    await renderWithRouter(<SchemaView cluster="prod" />, { initialPath: '/c/prod/schemas' })
    expect(await screen.findByText(/no schema registry configured/)).toBeInTheDocument()
  })
})
