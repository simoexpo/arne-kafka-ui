import { describe, expect, it } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery, renderWithRouter } from '../test/utils'
import { Sidebar, ThemeToggle, sectionFromPathname } from './AppShell'

const clusters = [
  { name: 'prod', status: 'healthy', broker_count: 3, error: null } as const,
  { name: 'dead', status: 'unreachable', broker_count: null, error: 'boom' } as const,
]

describe('Sidebar', () => {
  it('renders nav sections and cluster switcher with health dots', async () => {
    await renderWithRouter(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    expect(screen.getByRole('link', { name: /overview/i })).toHaveAttribute('href', '/c/prod/overview')
    expect(screen.getByRole('link', { name: /topics/i })).toHaveAttribute('href', '/c/prod/topics')
    expect(screen.getByRole('link', { name: /consumers/i })).toHaveAttribute('href', '/c/prod/consumers')
    expect(screen.getByRole('link', { name: /schema/i })).toHaveAttribute('href', '/c/prod/schemas')
    expect(screen.getByText('prod')).toBeInTheDocument()
    // switcher lists the other cluster as a link preserving the section
    expect(screen.getByRole('link', { name: /dead/i })).toHaveAttribute('href', '/c/dead/topics')
    // SPA navigation, not a full page reload: router-rendered <Link> marks
    // the link matching the current location as active itself (data-status),
    // something a plain <a href> can never do.
    expect(screen.getByRole('link', { name: /topics/i })).toHaveAttribute('data-status', 'active')
  })

  it('lists the other clusters alphabetically, whatever order the config declared', async () => {
    const unordered = [
      { name: 'zeta', status: 'healthy', broker_count: 1, error: null } as const,
      { name: 'prod', status: 'healthy', broker_count: 3, error: null } as const,
      { name: 'alpha', status: 'healthy', broker_count: 1, error: null } as const,
      { name: 'Mid', status: 'healthy', broker_count: 1, error: null } as const,
    ]
    await renderWithRouter(<Sidebar cluster="prod" clusters={unordered} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    const others = screen.getAllByRole('link', { name: /alpha|Mid|zeta/ }).map((l) => l.textContent)
    expect(others).toEqual(['alpha', 'Mid', 'zeta'])
  })

  it('the build commit links to that commit on GitHub, verbatim', async () => {
    await renderWithRouter(
      <Sidebar cluster="prod" clusters={[...clusters]} active="topics" version="730bfd6" />,
      { initialPath: '/c/prod/topics' },
    )
    const sha = screen.getByTestId('app-version')
    // as-is — no reformatting, no invented prefix
    expect(sha).toHaveTextContent('730bfd6')
    expect(sha).toHaveAttribute('href', 'https://github.com/simoexpo/arne-kafka-ui/commit/730bfd6')
  })

  it('links to the repository from the corner, build identity or not', async () => {
    await renderWithRouter(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    expect(screen.getByRole('link', { name: /arne on github/i }))
      .toHaveAttribute('href', 'https://github.com/simoexpo/arne-kafka-ui')
    // no commit known -> no commit link, and nothing invented
    expect(screen.queryByTestId('app-version')).toBeNull()
  })

  it('renders the brand logo', async () => {
    await renderWithRouter(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    expect(screen.getByAltText('Arne logo')).toHaveAttribute('src', '/logo.svg')
    expect(screen.getByText('mythological Kafka UI')).toBeInTheDocument()
  })

  it('surfaces a clusters-fetch failure honestly instead of silently rendering as zero other clusters', async () => {
    await renderWithRouter(
      <Sidebar cluster="prod" clusters={[]} active="topics" error={new Error('Failed to fetch')} />,
      { initialPath: '/c/prod/topics' },
    )
    expect(screen.getByText(/connection to arne lost/i)).toBeInTheDocument()
  })

  // The UTC/local toggle moved out of the sidebar into the Messages tab's
  // Timeline header (see TimeZoneToggle.tsx / Timeline.tsx) — its effect
  // (rewriting the window-range/row timestamps) is only ever visible there,
  // never in the sidebar itself.
  it('does not render the time zone toggle (moved to the Timeline header)', async () => {
    await renderWithRouter(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    expect(screen.queryByRole('button', { name: /time zone/i })).not.toBeInTheDocument()
  })
})

describe('sectionFromPathname', () => {
  it('maps the overview path', () => {
    expect(sectionFromPathname('/c/prod/overview', 'prod')).toBe('overview')
  })

  it('maps a topic detail path whose slug contains "groups" to topics, not consumers', () => {
    expect(sectionFromPathname('/c/prod/topics/groups-events', 'prod')).toBe('topics')
  })

  it('maps a group detail path to consumers', () => {
    expect(sectionFromPathname('/c/prod/consumers/billing', 'prod')).toBe('consumers')
  })
})

describe('ThemeToggle', () => {
  it('flips the dark class and persists preference', async () => {
    document.documentElement.classList.add('dark')
    renderWithQuery(<ThemeToggle />)
    await userEvent.click(screen.getByRole('button', { name: /theme/i }))
    expect(document.documentElement.classList.contains('dark')).toBe(false)
    expect(localStorage.theme).toBe('light')
  })

  it('renders both sun and moon icons and flips data-mode on click', async () => {
    document.documentElement.classList.add('dark')
    renderWithQuery(<ThemeToggle />)
    const button = screen.getByRole('button', { name: /theme/i })
    expect(screen.getByTestId('icon-sun')).toBeInTheDocument()
    expect(screen.getByTestId('icon-moon')).toBeInTheDocument()
    expect(button).toHaveAttribute('data-mode', 'dark')
    await userEvent.click(button)
    expect(button).toHaveAttribute('data-mode', 'light')
  })
})
