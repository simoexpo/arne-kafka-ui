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
  return <p className="p-6 text-sm text-zinc-500">loading clusters…</p>
}
