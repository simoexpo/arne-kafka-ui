import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ProtoView } from './ProtoView'

const SOURCE = `syntax = "proto3";
package com.acme;
// the event
message Event {
  int64 id = 1;
  repeated string tags = 2;
}`

describe('ProtoView', () => {
  it('colors keywords, scalar types, strings, numbers and comments', () => {
    const { container } = render(<ProtoView source={SOURCE} />)
    // Full text survives tokenization verbatim.
    expect(container.textContent).toBe(SOURCE)
    expect(screen.getByText('message').className).toMatch(/amber/)
    expect(screen.getByText('repeated').className).toMatch(/amber/)
    expect(screen.getByText('int64').className).toMatch(/sky/)
    expect(screen.getByText('"proto3"').className).toMatch(/emerald/)
    expect(screen.getByText('// the event').className).toMatch(/zinc/)
    // Field tags are numbers.
    expect(screen.getAllByText('1')[0].className).toMatch(/sky/)
  })
})
