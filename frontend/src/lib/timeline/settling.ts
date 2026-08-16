export interface SettlingState {
  edge: 'top' | 'bottom'
  lastScrollHeight: number | null
}

export type SettlingStep = { action: 'resnap'; next: SettlingState } | { action: 'done' }

export function stepSettling(state: SettlingState, scrollHeight: number, attempts: number, cap: number): SettlingStep {
  const stillMoving = state.lastScrollHeight === null || scrollHeight !== state.lastScrollHeight
  if (stillMoving && attempts <= cap) {
    return { action: 'resnap', next: { edge: state.edge, lastScrollHeight: scrollHeight } }
  }
  return { action: 'done' }
}
