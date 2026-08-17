import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { SubjectDetailView } from './SubjectDetailPage'
import * as client from '../api/client'
import type { SubjectDetail } from '../api/types'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getSubjectDetail: vi.fn(),
  getSubjectStrategy: vi.fn(),
  getCompatibilityLevel: vi.fn(),
  checkCompatibility: vi.fn(),
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

beforeEach(() => {
  vi.mocked(client.getSubjectStrategy).mockResolvedValue({
    strategy: 'topic_name',
    topic: 'sr-avro',
    role: 'value',
    as_of: 1,
  })
  vi.mocked(client.getCompatibilityLevel).mockResolvedValue({ level: 'BACKWARD', as_of: 1 })
})

describe('SubjectDetailView', () => {
  it('Definition tab shows versions, type, id, the schema, and the strategy section', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    expect(screen.getAllByRole('tab').map((t) => t.textContent)).toEqual(['Definition', 'Compatibility'])
    expect(await screen.findByText('avro')).toBeInTheDocument()
    expect(screen.getByText('id 42')).toBeInTheDocument()
    const select = screen.getByLabelText('version') as HTMLSelectElement
    expect(select.value).toBe('3')
    expect(screen.getByText('"Order"')).toBeInTheDocument()
    // Strategy section: format is the badge above; topic links to its page.
    const topicLink = await screen.findByRole('link', { name: 'sr-avro' })
    expect(topicLink).toHaveAttribute('href', '/c/prod/topics/sr-avro')
    expect(screen.getByText('topic name (value)')).toBeInTheDocument()
  })

  it('Definition strategy section is honest when nothing is provable', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail({ subject: 'weird' }))
    vi.mocked(client.getSubjectStrategy).mockResolvedValue({ strategy: null, topic: null, role: null, as_of: 1 })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="weird" />, {
      initialPath: '/c/prod/schemas/weird',
    })
    await screen.findByText('avro')
    expect(await screen.findByText(/not derivable/i)).toBeInTheDocument()
  })

  it('selecting a version re-queries with that version', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('avro')
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail({ version: 1, id: 40 }))
    fireEvent.change(screen.getByLabelText('version'), { target: { value: '1' } })
    expect(await screen.findByText('id 40')).toBeInTheDocument()
    expect(vi.mocked(client.getSubjectDetail)).toHaveBeenLastCalledWith('prod', 'sr-avro-value', 1, expect.anything())
  })

  it('a non-JSON schema (protobuf) renders verbatim, no soft-wrap', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(
      detail({ schema_type: 'PROTOBUF', schema: 'syntax = "proto3"; message Event { int64 id = 1; }' }),
    )
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-proto-value" />, {
      initialPath: '/c/prod/schemas/sr-proto-value',
    })
    expect(await screen.findByText('protobuf')).toBeInTheDocument()
    const pre = screen.getByTestId('schema-body').querySelector('pre')
    expect(pre?.textContent).toContain('syntax = "proto3"')
    expect(pre?.className).toMatch(/\bwhitespace-pre\b/)
  })

  it('Compatibility tab shows the effective level and checks a pasted schema', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    vi.mocked(client.checkCompatibility).mockResolvedValue({ is_compatible: true, messages: [], as_of: 1 })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('avro')
    fireEvent.click(screen.getByRole('tab', { name: 'Compatibility' }))
    expect(await screen.findByText('BACKWARD')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('candidate schema'), { target: { value: '{"type":"long"}' } })
    fireEvent.click(screen.getByRole('button', { name: /check compatibility/i }))
    expect(await screen.findByText(/^compatible/i)).toBeInTheDocument()
    expect(vi.mocked(client.checkCompatibility)).toHaveBeenCalledWith('prod', 'sr-avro-value', '{"type":"long"}', 'AVRO')
  })

  it('Compatibility tab reports incompatibility with the registry messages', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    vi.mocked(client.checkCompatibility).mockResolvedValue({
      is_compatible: false,
      messages: ['READER_FIELD_MISSING_DEFAULT_VALUE'],
      as_of: 1,
    })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    await screen.findByText('avro')
    fireEvent.click(screen.getByRole('tab', { name: 'Compatibility' }))
    await screen.findByText('BACKWARD')
    fireEvent.change(screen.getByLabelText('candidate schema'), { target: { value: '{"type":"string"}' } })
    fireEvent.click(screen.getByRole('button', { name: /check compatibility/i }))
    expect(await screen.findByText(/not compatible/i)).toBeInTheDocument()
    expect(screen.getByText('READER_FIELD_MISSING_DEFAULT_VALUE')).toBeInTheDocument()
  })
})
