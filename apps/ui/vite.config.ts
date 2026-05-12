import tailwindcss from '@tailwindcss/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiTarget = env.NANOTRACE_URL || env.TRACER_URL || 'http://localhost:18473'
  const apiToken = env.NANOTRACE_KEY || env.SECRET_KEY || env.TRACER_SECRET_KEY

  return {
    envPrefix: ['VITE_'],
    server: {
      port: Number(env.NANOTRACE_UI_PORT || env.TRACER_UI_PORT || '41233'),
      proxy: {
        '/query': {
          target: apiTarget,
          changeOrigin: true,
          ...(apiToken ? { headers: { Authorization: `Bearer ${apiToken}` } } : {})
        }
      },
      strictPort: true
    },
    plugins: [tailwindcss(), viteReact()]
  }
})
