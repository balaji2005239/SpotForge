import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// One backend entry point in every mode: the API gateway (devstack: python tools/devstack.py, compose: gateway:8000).
const proxy = {
  '/api': { target: 'http://127.0.0.1:8000', ws: true },
  '/auth': { target: 'http://127.0.0.1:8000' },
}

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy },
  preview: { port: 5173, proxy },
})
