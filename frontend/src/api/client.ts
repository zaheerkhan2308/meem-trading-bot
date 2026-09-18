import type { CircuitBreaker, Portfolio, Snapshot, Trade, Watchlist } from '../types/api'

async function get<T>(path: string): Promise<T> {
  const response = await fetch(path)
  if (!response.ok) throw new Error(`Request failed: ${response.status}`)
  return response.json() as Promise<T>
}
export const api = {
  status: () => get<{ running: boolean; market_open: boolean; last_scan: string | null }>('/api/status'),
  portfolio: () => get<Portfolio>('/api/portfolio'),
  trades: () => get<{ trades: Trade[] }>('/api/trades'),
  watchlist: () => get<Watchlist>('/api/watchlist'),
  chart: () => get<{ snapshots: Snapshot[] }>('/api/chart'),
  circuitBreaker: () => get<CircuitBreaker>('/api/circuit-breaker'),
  setDryRun: (active: boolean) => post('/api/dry-run', { active }),
  setKillSwitch: (active: boolean, password: string) => post('/api/kill-switch', { active, password }),
}
async function post(path: string, body: unknown) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  if (!response.ok) throw new Error((await response.json().catch(() => null))?.detail ?? 'Request failed')
  return response.json()
}
