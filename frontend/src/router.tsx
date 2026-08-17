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
import { SchemaByIdPage } from './pages/SchemaByIdPage'
import { SubjectDetailPage } from './pages/SubjectDetailPage'

const rootRoute = createRootRoute({ component: AppShell })
const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: ClusterRedirect })
const clusterRoute = createRoute({ getParentRoute: () => rootRoute, path: 'c/$cluster', component: Outlet })
const overviewRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'overview', component: OverviewPage })
const topicsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'topics', component: TopicsPage })
const topicDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'topics/$topic', component: TopicDetailPage })
const groupsRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers', component: GroupsPage })
const groupDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'consumers/$group', component: GroupDetailPage })
const schemaRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schemas', component: SchemaPage })
const subjectDetailRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schemas/$subject', component: SubjectDetailPage })
const schemaByIdRoute = createRoute({ getParentRoute: () => clusterRoute, path: 'schemas/by-id/$id', component: SchemaByIdPage })

const routeTree = rootRoute.addChildren([
  indexRoute,
  clusterRoute.addChildren([overviewRoute, topicsRoute, topicDetailRoute, groupsRoute, groupDetailRoute, schemaRoute, subjectDetailRoute, schemaByIdRoute]),
])

export const router = createRouter({ routeTree })

declare module '@tanstack/react-router' {
  interface Register { router: typeof router }
}
