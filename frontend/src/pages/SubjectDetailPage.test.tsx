import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
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
  // Definition mirrors the topic Config tab's grammar (owner ruling
  // 2026-08-18): a stat grid (format, strategy by its Confluent name,
  // topic only when topic-based) → divider → version + copyable id →
  // the schema itself.
  it('Definition tab: stat grid, then version and copyable id, then the schema', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value',
    })
    expect(screen.getAllByRole('tab').map((t) => t.textContent)).toEqual(['Definition', 'Compatibility'])
    expect(await screen.findByText('avro')).toBeInTheDocument()
    expect(await screen.findByText('TopicNameStrategy')).toBeInTheDocument()
    const topicLink = screen.getByRole('link', { name: /sr-avro/ })
    expect(topicLink).toHaveAttribute('href', '/c/prod/topics/sr-avro')
    const select = screen.getByLabelText('version') as HTMLSelectElement
    expect(select.value).toBe('3')
    expect(screen.getByText('id 42')).toBeInTheDocument()
    expect(screen.getByText('"Order"')).toBeInTheDocument()
  })

  it('Definition hides the topic tile and stays honest when nothing is provable', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail({ subject: 'weird' }))
    vi.mocked(client.getSubjectStrategy).mockResolvedValue({ strategy: null, topic: null, role: null, as_of: 1 })
    await renderWithRouter(<SubjectDetailView cluster="prod" subject="weird" />, {
      initialPath: '/c/prod/schemas/weird',
    })
    await screen.findByText('avro')
    expect(await screen.findByText(/not derivable/i)).toBeInTheDocument()
    expect(screen.queryByText('topic')).not.toBeInTheDocument()
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

  it('opens the tab named in the URL and keeps the version when tabs change', async () => {
    vi.mocked(client.getSubjectDetail).mockResolvedValue(detail())
    const { router } = await renderWithRouter(<SubjectDetailView cluster="prod" subject="sr-avro-value" />, {
      initialPath: '/c/prod/schemas/sr-avro-value?version=1&tab=compatibility',
    })
    expect(screen.getByRole('tab', { name: 'Compatibility' })).toHaveAttribute('aria-selected', 'true')
    await userEvent.click(screen.getByRole('tab', { name: 'Definition' }))
    expect(router.state.location.search).toEqual(
      expect.objectContaining({ tab: 'definition', version: 1 }),
    )
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
    const box = screen.getByLabelText('candidate schema')
    expect(box.className).toMatch(/\bresize-none\b/)
    // Grid split: textarea and result share row 1 (equal heights), the
    // button lives alone in the auto row below.
    expect(box.parentElement?.className).toMatch(/\bgrid-cols-2\b/)
    expect(box.parentElement?.className).toMatch(/grid-rows-\[minmax\(0,1fr\)_auto\]/)
    expect(screen.getByRole('button', { name: /check compatibility/i }).className).toMatch(/enabled:hover/)
    fireEvent.change(box, { target: { value: '{"type":"long"}' } })
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
