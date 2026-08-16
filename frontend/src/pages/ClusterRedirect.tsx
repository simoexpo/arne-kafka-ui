import { useEffect } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useClusters } from '../layout/AppShell'

export function ClusterRedirect() {
  const { data, error } = useClusters()
  const navigate = useNavigate()
  useEffect(() => {
    const first = data?.clusters[0]?.name
    if (first) navigate({ to: '/c/$cluster/overview', params: { cluster: first } })
  }, [data, navigate])
  if (error) return <p className="p-6 text-sm text-red-600">failed to load clusters: {String(error)}</p>
  // The clusters query can succeed with an empty list (e.g. nothing
  // configured) — without this branch the effect above silently no-ops and
  // the "loading" paragraph renders forever, indistinguishable from a slow
  // request. Once `data` has resolved with zero clusters, say so explicitly.
  if (data && data.clusters.length === 0) {
    return <p className="p-6 text-sm text-zinc-500">no clusters configured — add one to get started</p>
  }
  return <p className="p-6 text-sm text-zinc-500">loading clusters…</p>
}
