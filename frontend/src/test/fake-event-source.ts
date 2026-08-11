type Listener = (e: MessageEvent) => void

export class FakeEventSource {
  static instances: FakeEventSource[] = []
  static install() {
    FakeEventSource.instances = []
    ;(globalThis as Record<string, unknown>).EventSource = FakeEventSource
  }
  static uninstall() {
    delete (globalThis as Record<string, unknown>).EventSource
  }

  url: string
  closed = false
  onerror: (() => void) | null = null
  private listeners = new Map<string, Listener[]>()

  constructor(url: string) {
    this.url = url
    FakeEventSource.instances.push(this)
  }
  addEventListener(name: string, fn: Listener) {
    this.listeners.set(name, [...(this.listeners.get(name) ?? []), fn])
  }
  emit(name: string, data: unknown) {
    for (const fn of this.listeners.get(name) ?? []) {
      fn({ data: JSON.stringify(data) } as MessageEvent)
    }
  }
  fireTransportError() {
    this.onerror?.()
  }
  close() {
    this.closed = true
  }
}
