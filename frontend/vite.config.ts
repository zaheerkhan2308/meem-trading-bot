import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '..', '')
  return {
    envDir: '..',
    plugins: [react()],
    server: {
      port: Number(env.FRONTEND_PORT || 5173),
      proxy: {
        '/api': { target: env.API_PROXY_TARGET || 'http://127.0.0.1:8000', changeOrigin: true },
        '/ws': { target: (env.API_PROXY_TARGET || 'http://127.0.0.1:8000').replace(/^http/, 'ws'), ws: true },
      },
    },
  }
})
