import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { getSubjectOfId } from '../api/client'
import { Panel } from '../components/Panel'
import { SubjectDetailView } from './SubjectDetailPage'

// Landing page for schema-id links from the message tab: resolves the id
// to the subject that registered it (server-side, exact under any naming
// strategy), then renders that subject's detail in place.
export function SchemaByIdView({ cluster, id }: { cluster: string; id: number }) {
  const resolved = useQuery({
    queryKey: ['schema-id', cluster, id],
    queryFn: ({ signal }) => getSubjectOfId(cluster, id, signal),
  })
  if (resolved.data) {
    return <SubjectDetailView cluster={cluster} subject={resolved.data.subject} />
  }
  return (
    <Panel
      title={`schema id ${id}`}
      error={resolved.error}
      loading={resolved.isPending}
      hasData={resolved.data !== undefined}
    >
      {null}
    </Panel>
  )
}

export function SchemaByIdPage() {
  const { cluster, id } = useParams({ from: '/c/$cluster/schemas/by-id/$id' })
  return <SchemaByIdView cluster={cluster} id={Number(id)} />
}
