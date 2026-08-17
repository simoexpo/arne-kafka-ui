import { describe, expect, it } from 'vitest'
import { proposalsFor } from './filterProposals'

describe('proposalsFor', () => {
  it('prefix of key proposes both key operators', () => {
    expect(proposalsFor('k', [])).toEqual(['key:', 'key='])
    expect(proposalsFor('key', [])).toEqual(['key:', 'key='])
  })

  it('prefix of value proposes operators plus bare value. when no fields known', () => {
    expect(proposalsFor('val', [])).toEqual(['value:', 'value=', 'value.'])
  })

  it('prefix of value proposes up to 5 field rows instead of value. when fields known', () => {
    const fields = ['a', 'b', 'c', 'd', 'e', 'f']
    expect(proposalsFor('val', fields)).toEqual(['value:', 'value=', 'value.a', 'value.b', 'value.c', 'value.d', 'value.e'])
  })

  it('value. prefix filters fields by typed path, max 5, no operator rows', () => {
    const fields = ['customer.id', 'customer.name', 'status']
    expect(proposalsFor('value.cus', fields)).toEqual(['value.customer.id', 'value.customer.name'])
    expect(proposalsFor('value.customer.id', fields)).toEqual([])
  })

  it('prefix matching is case-insensitive, proposals stay canonical lowercase', () => {
    expect(proposalsFor('KEY', [])).toEqual(['key:', 'key='])
    expect(proposalsFor('Val', [])).toEqual(['value:', 'value=', 'value.'])
    expect(proposalsFor('VALUE.cus', ['customer.id', 'status'])).toEqual(['value.customer.id'])
  })

  it('array fields propose with [] and complete a typed [] or ANY numeric index', () => {
    const fields = ['items[].qty', 'items[].sku', 'nums[]']
    expect(proposalsFor('value.items[].s', fields)).toEqual(['value.items[].sku'])
    expect(proposalsFor('value.items[]', fields)).toEqual(['value.items[].qty', 'value.items[].sku'])
    expect(proposalsFor('value.items.2.s', fields)).toEqual(['value.items.2.sku'])
    expect(proposalsFor('value.items.10.', fields)).toEqual(['value.items.10.qty', 'value.items.10.sku'])
    expect(proposalsFor('value.num', fields)).toEqual(['value.nums[]'])
    expect(proposalsFor('value.nums[]', fields)).toEqual([]) // fully typed
    expect(proposalsFor('value.nums.3', fields)).toEqual([]) // fully typed via index
  })

  it('no proposals for empty, quoted, or completed-operator input', () => {
    expect(proposalsFor('', ['a'])).toEqual([])
    expect(proposalsFor('"key', ['a'])).toEqual([])
    expect(proposalsFor('key:', ['a'])).toEqual([])
    expect(proposalsFor('value.a:x', ['a'])).toEqual([])
    expect(proposalsFor('banana', ['a'])).toEqual([])
  })
})
