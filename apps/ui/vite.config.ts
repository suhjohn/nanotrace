import tailwindcss from '@tailwindcss/vite'
import viteReact from '@vitejs/plugin-react'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig, loadEnv } from 'vite'

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..')

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, repoRoot, '')
  const apiTarget = env.VITE_NANOTRACE_URL || env.NANOTRACE_URL || env.TRACER_URL || 'http://localhost:18473'

  return {
    define: {
      'import.meta.env.VITE_NANOTRACE_URL': JSON.stringify(apiTarget)
    },
    envPrefix: ['VITE_'],
    server: {
      port: Number(env.NANOTRACE_UI_PORT || env.TRACER_UI_PORT || '41233'),
      strictPort: true
    },
    plugins: [tailwindcss(), viteReact()]
  }
})
