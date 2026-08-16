import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: { proxy: { '/api': 'http://localhost:8080' } },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    // Clears vi.fn() call history (not implementations) before every test —
    // without this, a module-level `vi.mock(...)` factory's vi.fn()s
    // accumulate call history across every test in a file, so a test that
    // forgets to re-arrange its mock would silently inherit the previous
    // test's call counts.
    clearMocks: true,
  },
})
