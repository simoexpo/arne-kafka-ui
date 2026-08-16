import { describe, expect, it } from 'vitest'
import { stepSettling } from './settling'

describe('stepSettling', () => {
  it('resnaps on the first event (no prior scrollHeight to compare against)', () => {
    const step = stepSettling({ edge: 'bottom', lastScrollHeight: null }, 500, 1, 10)
    expect(step).toEqual({ action: 'resnap', next: { edge: 'bottom', lastScrollHeight: 500 } })
  })

  it('resnaps while scrollHeight keeps changing and the attempt cap is not exceeded', () => {
    const step = stepSettling({ edge: 'top', lastScrollHeight: 500 }, 600, 2, 10)
    expect(step).toEqual({ action: 'resnap', next: { edge: 'top', lastScrollHeight: 600 } })
  })

  it('is done once two consecutive events report the same scrollHeight', () => {
    const step = stepSettling({ edge: 'top', lastScrollHeight: 600 }, 600, 3, 10)
    expect(step).toEqual({ action: 'done' })
  })

  it('is done once attempts exceed the cap, even if content is still moving', () => {
    const step = stepSettling({ edge: 'bottom', lastScrollHeight: 500 }, 700, 11, 10)
    expect(step).toEqual({ action: 'done' })
  })

  it('still resnaps at exactly the cap', () => {
    const step = stepSettling({ edge: 'bottom', lastScrollHeight: 500 }, 700, 10, 10)
    expect(step).toEqual({ action: 'resnap', next: { edge: 'bottom', lastScrollHeight: 700 } })
  })

  it('preserves edge identity across a resnap', () => {
    const step = stepSettling({ edge: 'top', lastScrollHeight: 100 }, 200, 1, 10)
    expect(step.action).toBe('resnap')
    if (step.action === 'resnap') expect(step.next.edge).toBe('top')
  })
})
