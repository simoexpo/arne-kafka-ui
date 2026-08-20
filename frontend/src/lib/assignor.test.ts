import { describe, expect, it } from 'vitest'
import { assignorClass } from './assignor'

describe('assignorClass', () => {
  it('names the four standard assignors by their class', () => {
    expect(assignorClass('range')).toBe('Range')
    expect(assignorClass('roundrobin')).toBe('RoundRobin')
    expect(assignorClass('sticky')).toBe('Sticky')
    expect(assignorClass('cooperative-sticky')).toBe('CooperativeSticky')
  })

  it('leaves anything else exactly as the broker reported it', () => {
    expect(assignorClass('uniform')).toBe('uniform')
    expect(assignorClass('com.acme.MyAssignor')).toBe('com.acme.MyAssignor')
  })

  it('has nothing to state when no assignor was negotiated', () => {
    expect(assignorClass('')).toBe('—')
  })
})
