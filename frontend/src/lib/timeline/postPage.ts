export interface PostPageInput {
  error: boolean
  exhausted: boolean
  matched: boolean
  filterActive: boolean
  pageMatches: number
  pageLimit: number
  nextCursor: string | null
  iteration: number
  iterationCap: number
}

export type PostPageDecision = 'error-drop-overlay' | 'stop' | 'offer-continue' | { kind: 'continue'; cursor: string }

export function decidePostPage(input: PostPageInput): PostPageDecision {
  if (input.error) return 'error-drop-overlay'
  if (input.exhausted) return 'stop'
  if (input.matched) {
    return input.filterActive && input.pageMatches < input.pageLimit ? 'offer-continue' : 'stop'
  }
  if (input.nextCursor === null) return 'stop'
  if (input.iteration >= input.iterationCap) return 'offer-continue'
  return { kind: 'continue', cursor: input.nextCursor }
}
