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

const rootRoute = createRootRoute({ component: AppShell })
const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: ClusterRedirect })
const clusterRoute = createRoute({ getParentRoute: () => rootRoute, path: 'c/$cluster', component: Outlet })
const overviewRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'overview', component: OverviewPage })
const topicsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'topics', component: TopicsPage })
const topicDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'topics/$topic', component: TopicDetailPage })
const groupsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers', component: GroupsPage })
const groupDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers/$group', component: GroupDetailPage })
const schemaRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schema', component: SchemaPage })
const subjectDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schema/$subject', component: SubjectDetailPage })

const routeTree = rootRoute.addChildren([
  indexRoute,
  clusterRoute.addChildren([overviewRoute, topicsRoute, topicDetailRoute, groupsRoute, groupDetailRoute, schemaRoute, subjectDetailRoute]),
])

export const router = createRouter({ routeTree })

declare module '@tanstack/react-router' {
  interface Register { router: typeof router }
}
