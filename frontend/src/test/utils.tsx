import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterProvider,
} from '@tanstack/react-router'
import { render } from '@testing-library/react'
import type { ReactElement } from 'react'

export function renderWithQuery(ui: ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

// Renders `ui` under a QueryClientProvider and a minimal memory-history
// router, for components that navigate via <Link>/useNavigate. The root
// route always renders `ui` regardless of the current path; a catch-all
// splat child route means any path a <Link> navigates to resolves instead
// of hitting the router's not-found state. The router is preloaded via
// `router.load()` before mounting so the first render is already settled
// (no async "pending" flash the caller would otherwise have to await).
export async function renderWithRouter(ui: ReactElement, { initialPath = '/' }: { initialPath?: string } = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const rootRoute = createRootRoute({ component: () => ui })
  const catchAllRoute = createRoute({ getParentRoute: () => rootRoute, path: '$', component: () => null })
  const routeTree = rootRoute.addChildren([catchAllRoute])
  const history = createMemoryHistory({ initialEntries: [initialPath] })
  const router = createRouter({ routeTree, history })
  await router.load()
  const utils = render(
    <QueryClientProvider client={qc}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  )
  return { ...utils, router }
}
