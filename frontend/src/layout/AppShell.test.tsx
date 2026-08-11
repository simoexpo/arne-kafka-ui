import { describe, expect, it } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithQuery } from '../test/utils'
import { Sidebar, ThemeToggle, sectionFromPathname } from './AppShell'

const clusters = [
  { name: 'prod', status: 'healthy', broker_count: 3, error: null } as const,
  { name: 'dead', status: 'unreachable', broker_count: null, error: 'boom' } as const,
]

describe('Sidebar', () => {
  it('renders nav sections and cluster switcher with health dots', () => {
    renderWithQuery(<Sidebar cluster="prod" clusters={[...clusters]} active="topics" />)
    expect(screen.getByRole('link', { name: /overview/i })).toHaveAttribute('href', '/c/prod/overview')
    expect(screen.getByRole('link', { name: /topics/i })).toHaveAttribute('href', '/c/prod/topics')
    expect(screen.getByRole('link', { name: /groups/i })).toHaveAttribute('href', '/c/prod/groups')
    expect(screen.getByText('prod')).toBeInTheDocument()
    // switcher lists the other cluster as a link preserving the section
    expect(screen.getByRole('link', { name: /dead/i })).toHaveAttribute('href', '/c/dead/topics')
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
})
