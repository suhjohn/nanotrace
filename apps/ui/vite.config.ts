import tailwindcss from '@tailwindcss/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiTarget = env.NANOTRACE_URL || env.TRACER_URL || 'http://localhost:18473'
  const apiToken = env.NANOTRACE_API_KEY || env.NANOTRACE_KEY
  const proxyTarget = {
    target: apiTarget,
    changeOrigin: true,
    ...(apiToken ? { headers: { Authorization: `Bearer ${apiToken}` } } : {})
  }

  return {
    envPrefix: ['VITE_'],
    server: {
      port: Number(env.NANOTRACE_UI_PORT || env.TRACER_UI_PORT || '41233'),
      proxy: {
        '/events': proxyTarget,
        '/facets': proxyTarget,
        '/auth': proxyTarget,
        '/api-keys': proxyTarget,
        '/query': proxyTarget
      },
      strictPort: true
    },
    plugins: [tailwindcss(), viteReact()]
  }
})
