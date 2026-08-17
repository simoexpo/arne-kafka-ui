import { describe, expect, it, vi } from 'vitest'
import { fireEvent, screen } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { SubjectDetailView } from './SubjectDetailPage'
import * as client from '../api/client'
import type { SubjectDetail } from '../api/types'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getSubjectDetail: vi.fn(),
  getSubjectUsage: vi.fn(),
}))

const detail = (over: Partial<SubjectDetail> = {}): SubjectDetail => ({
  subject: 'sr-avro-value',
  versions: [1, 2, 3],
  version: 3,
  id: 42,
  schema_type: 'AVRO',
  schema: '{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}',
  as_of: 1,
  ...over,
})

describe('SubjectDetailView', () => {
  it('shows versions with the served one selected, type, id, and the schema as JSON', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    expect(await screen.findByText('AVRO')).toBeInTheDocument()
    expect(screen.getByText('id 42')).toBeInTheDocument()
    const select = screen.getByLabelText('version') as HTMLSelectElement
    expect(select.value).toBe('3')
    expect([...select.options].map((o) => o.value)).toEqual(['1', '2', '3'])
    // JSON-able schema renders through JsonView, fully expanded — string
    // values keep their JSON quotes.
    expect(screen.getByText('"Order"')).toBeInTheDocument()
    expect(screen.getByText('"record"')).toBeInTheDocument()
  })

  it('selecting a version re-queries with that version', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('AVRO')
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail({ version: 1, id: 40 }))
    fireEvent.change(screen.getByLabelText('version'), { target: { value: '1' } })
    expect(await screen.findByText('id 40')).toBeInTheDocument()
    expect(vi.mocked(client.getSubjectDetail)).toHaveBeenLastCalledWith('prod', 'sr-avro-value', 1, expect.anything())
  })

  it('is tabbed like the topic page: Schema first, placeholder switchable', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('AVRO')
    expect(screen.getAllByRole('tab').map((t) => t.textContent)).toEqual(['Schema', 'Usage', 'Compatibility'])
    fireEvent.click(screen.getByRole('tab', { name: 'Compatibility' }))
    expect(screen.getByText(/nothing here yet/i)).toBeInTheDocument()
    expect(screen.queryByLabelText('version')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('tab', { name: 'Schema' }))
    expect(screen.getByLabelText('version')).toBeInTheDocument()
  })

  it('Usage tab lists topics inferred from the subject name with their strategy', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    vi.mocked(client.getSubjectUsage).mockResolvedValue({
      usages: [{ topic: 'sr-avro', strategy: 'topic_name', role: 'value' }],
      as_of: 1,
    })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('AVRO')
    fireEvent.click(screen.getByRole('tab', { name: 'Usage' }))
    const link = await screen.findByRole('link', { name: 'sr-avro' })
    expect(link).toHaveAttribute('href', '/c/prod/topics/sr-avro')
    expect(screen.getByText('topic name (value)')).toBeInTheDocument()
  })

  it('Usage tab is honest when no topic is derivable from the name', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail({ subject: 'com.acme.Order' }))
    vi.mocked(client.getSubjectUsage).mockResolvedValue({ usages: [], as_of: 1 })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="com.acme.Order" />, {
      initialPath: '/c/prod/schemas/com.acme.Order',
    })
    await screen.findByText('AVRO')
    fireEvent.click(screen.getByRole('tab', { name: 'Usage' }))
    expect(await screen.findByText(/registry doesn't record/i)).toBeInTheDocument()
  })

  it('a non-JSON schema (protobuf) renders verbatim, no soft-wrap', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(
      detail({ schema_type: 'PROTOBUF', schema: 'syntax = "proto3"; message Event { int64 id = 1; }' }),
    )
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-proto-value" />, {
      initialPath: '/c/prod/schemas/sr-proto-value',
    })
    expect(await screen.findByText('PROTOBUF')).toBeInTheDocument()
    const pre = screen.getByTestId('schema-body').querySelector('pre')
    expect(pre?.textContent).toContain('syntax = "proto3"')
    expect(pre?.className).toMatch(/\bwhitespace-pre\b/)
  })
})
