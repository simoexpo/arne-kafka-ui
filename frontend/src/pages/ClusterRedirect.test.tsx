import { describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import { renderWithRouter } from '../test/utils'
import { ClusterRedirect } from './ClusterRedirect'
import * as client from '../api/client'

vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getClusters: vi.fn(),
}))

describe('ClusterRedirect', () => {
  it('shows a loading state while the clusters query is pending', async () => {
    vi.mocked(client.getClusters).mockReturnValue(new Promise(() => {}))
    await renderWithRouter(<ClusterRedirect />, { initialPath: '/' })
    expect(screen.getByText(/loading clusters/i)).toBeInTheDocument()
  })

  it('redirects to the first configured cluster once loaded', async () => {
    vi.mocked(client.getClusters).mockResolvedValue({
      clusters: [{ name: 'prod', status: 'healthy', broker_count: 3, error: null }],
    })
    const { router } = await renderWithRouter(<ClusterRedirect />, { initialPath: '/' })
    await waitFor(() => expect(router.state.location.pathname).toBe('/c/prod/overview'))
  })

  it('shows an explicit empty state when the clusters query succeeds with zero clusters, instead of hanging on "loading"', async () => {
    vi.mocked(client.getClusters).mockResolvedValue({ clusters: [] })
    await renderWithRouter(<ClusterRedirect />, { initialPath: '/' })
    expect(await screen.findByText(/no clusters configured/i)).toBeInTheDocument()
    expect(screen.queryByText(/loading clusters/i)).not.toBeInTheDocument()
    // product voice: this is a config-shape issue, not a "backend" failure
    expect(screen.queryByText(/backend/i)).not.toBeInTheDocument()
  })

  it('shows a failure state when the clusters query fails', async () => {
    vi.mocked(client.getClusters).mockRejectedValue(new Error('network down'))
    await renderWithRouter(<ClusterRedirect />, { initialPath: '/' })
    expect(await screen.findByText(/failed to load clusters/i)).toBeInTheDocument()
  })
})
