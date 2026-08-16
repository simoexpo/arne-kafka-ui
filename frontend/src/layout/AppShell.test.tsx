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
    expect(screen.getByRole('link', { name: /groups/i })).toHaveAttribute('href', '/c/prod/groups')
    expect(screen.getByText('prod')).toBeInTheDocument()
    // switcher lists the other cluster as a link preserving the section
    expect(screen.getByRole('link', { name: /dead/i })).toHaveAttribute('href', '/c/dead/topics')
    // SPA navigation, not a full page reload: router-rendered <Link> marks
    // the link matching the current location as active itself (data-status),
    // something a plain <a href> can never do.
    expect(screen.getByRole('link', { name: /topics/i })).toHaveAttribute('data-status', 'active')
  })

  it('renders the brand logo', async () => {
    await renderWithRouter(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />, {
      initialPath: '/c/prod/topics',
    })
    expect(screen.getByAltText('Betrachtung logo')).toHaveAttribute('src', '/logo.svg')
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

  it('maps a topic detail path whose slug contains "groups" to topics, not groups', () => {
    expect(sectionFromPathname('/c/prod/topics/groups-events', 'prod')).toBe('topics')
  })

  it('maps a group detail path to groups', () => {
    expect(sectionFromPathname('/c/prod/groups/billing', 'prod')).toBe('groups')
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
