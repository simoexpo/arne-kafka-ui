import {
  createRootRoute, createRoute, createRouter, Outlet,
} from '@tanstack/react-router'
import { AppShell } from './layout/AppShell'
import { ClusterRedirect } from './pages/ClusterRedirect'
import { OverviewPage } from './pages/OverviewPage'
import { TopicsPage } from './pages/TopicsPage'
import { TopicDetailPage } from './pages/TopicDetailPage'
import { GroupsPage } from './pages/GroupsPage'
import { GroupDetailPage } from './pages/GroupDetailPage'
import { SchemaPage } from './pages/SchemaPage'
import { SubjectDetailPage } from './pages/SubjectDetailPage'

// The selected tab lives in the URL so a reload keeps the reader in place;
// unvalidated params are dropped by TanStack, so every tabbed route keeps it.
const tabSearch = (search: Record<string, unknown>): { tab?: string } =>
  typeof search.tab === 'string' ? { tab: search.tab } : {}

const rootRoute = createRootRoute({ component: AppShell })
const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: ClusterRedirect })
const clusterRoute = createRoute({ getParentRoute: () => rootRoute, path: 'c/$cluster', component: Outlet })
const overviewRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'overview', component: OverviewPage })
const topicsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'topics', component: TopicsPage })
const topicDetailRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: 'topics/$topic',
  component: TopicDetailPage,
  validateSearch: (search: Record<string, unknown>): { tab?: string } => tabSearch(search),
})
const groupsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers', component: GroupsPage })
const groupDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers/$group', component: GroupDetailPage })
const schemaRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schemas', component: SchemaPage })
const subjectDetailRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: 'schemas/$subject',
  component: SubjectDetailPage,
  // The selected version lives in the URL (`?version=N`) so schema links
  // land on an exact version and versions are shareable; absent = latest.
  validateSearch: (search: Record<string, unknown>): { version?: number; tab?: string } => {
    const v = Number(search.version)
    return { ...(Number.isInteger(v) && v > 0 ? { version: v } : {}), ...tabSearch(search) }
  },
})

const routeTree = rootRoute.addChildren([
  indexRoute,
  clusterRoute.addChildren([overviewRoute, topicsRoute, topicDetailRoute, groupsRoute, groupDetailRoute, schemaRoute, subjectDetailRoute]),
])

export const router = createRouter({ routeTree })

declare module '@tanstack/react-router' {
  interface Register { router: typeof router }
}
