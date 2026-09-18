import type { CircuitBreaker, Portfolio, Snapshot, StrategySettings, Trade, TradingSettings, Watchlist } from '../types/api'

async function get<T>(path: string): Promise<T> {
  const response = await fetch(path)
  if (!response.ok) throw new Error(`Request failed: ${response.status}`)
  return response.json() as Promise<T>
}
export const api = {
  status: () => get<{ running: boolean; market_open: boolean; last_scan: string | null }>('/api/status'),
  settings: () => get<TradingSettings>('/api/settings'),
  strategySettings: (strategy: 'default' | 'selected') => get<StrategySettings>(`/api/strategy-settings?strategy=${strategy}`),
  tickerSearch: (query: string) => get<{ results: { symbol: string; name: string }[] }>(`/api/ticker-search?q=${encodeURIComponent(query)}`),
  portfolio: (strategy: 'shared' | 'default' | 'selected') => get<Portfolio>(`/api/portfolio?strategy=${strategy}`),
  trades: (strategy: 'default' | 'selected') => get<{ trades: Trade[] }>(`/api/trades?strategy=${strategy}`),
  watchlist: () => get<Watchlist>('/api/watchlist'),
  selectedWatchlist: () => get<Watchlist>('/api/selected-watchlist'),
  chart: () => get<{ snapshots: Snapshot[] }>('/api/chart'),
  circuitBreaker: () => get<CircuitBreaker>('/api/circuit-breaker'),
  setDryRun: (active: boolean) => post('/api/dry-run', { active }),
  setKillSwitch: (active: boolean, password: string) => post('/api/kill-switch', { active, password }),
  updateSettings: (settings: TradingSettings) => post('/api/settings', settings),
  updateStrategySettings: (settings: StrategySettings) => post('/api/strategy-settings', settings),
}
async function post(path: string, body: unknown) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  if (!response.ok) throw new Error((await response.json().catch(() => null))?.detail ?? 'Request failed')
  return response.json()
}
