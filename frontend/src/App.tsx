import { useCallback, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useQueryClient } from '@tanstack/react-query'
import { api } from './api/client'
import { useMeemSocket } from './hooks/useMeemSocket'
import { Controls } from './components/Controls'
import { PerformanceChart } from './components/PerformanceChart'
import { PortfolioSummary, StrategyPortfolioMetrics } from './components/PortfolioSummary'
import { TradeHistory } from './components/TradeHistory'
import { Watchlist } from './components/Watchlist'
import { StrategySettings } from './components/StrategySettings'
import type { StrategySettings as StrategySettingsData, TradingSettings } from './types/api'
export default function App() {
  const [controls, setControls] = useState({ marketOpen: null as boolean | null, lastScan: null as string | null, killSwitch: false, dryRun: false, circuitBreaker: null as string | null })
  const [theme, setTheme] = useState<'dark' | 'light'>(() => (localStorage.getItem('meem-theme') as 'dark' | 'light') || 'dark')
  const [settingsOpen, setSettingsOpen] = useState<'default' | 'selected' | null>(null)
  const updateControls = useCallback((next: Partial<typeof controls>) => setControls(current => ({ ...current, ...next })), [])
  const queryClient = useQueryClient()
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem('meem-theme', theme) }, [theme])
  const defaultSettings = useQuery({ queryKey: ['strategy-settings', 'default'], queryFn: () => api.strategySettings('default') })
  const selectedSettings = useQuery({ queryKey: ['strategy-settings', 'selected'], queryFn: () => api.strategySettings('selected') })
  const updateSettingsQuery = useCallback((next: TradingSettings) => queryClient.setQueryData(['settings'], next), [queryClient])
  useMeemSocket(updateControls, updateSettingsQuery)
  const sharedPortfolio = useQuery({ queryKey: ['portfolio', 'shared'], queryFn: () => api.portfolio('shared') })
  const defaultPortfolio = useQuery({ queryKey: ['portfolio', 'default'], queryFn: () => api.portfolio('default') })
  const selectedPortfolio = useQuery({ queryKey: ['portfolio', 'selected'], queryFn: () => api.portfolio('selected') })
  const status = useQuery({ queryKey: ['status'], queryFn: api.status, refetchInterval: 30_000 })
  const defaultTrades = useQuery({ queryKey: ['trades', 'default'], queryFn: () => api.trades('default') })
  const selectedTrades = useQuery({ queryKey: ['trades', 'selected'], queryFn: () => api.trades('selected') })
  const watchlist = useQuery({ queryKey: ['watchlist'], queryFn: api.watchlist })
  const selectedWatchlist = useQuery({ queryKey: ['selected-watchlist'], queryFn: api.selectedWatchlist })
  const chart = useQuery({ queryKey: ['chart'], queryFn: api.chart })
  const circuit = useQuery({ queryKey: ['circuit'], queryFn: api.circuitBreaker })
  const breaker = controls.circuitBreaker ?? circuit.data?.reason ?? null
  const marketOpen = controls.marketOpen ?? status.data?.market_open ?? null
  async function saveStrategySettings(next: StrategySettingsData) { const saved = await api.updateStrategySettings(next); queryClient.setQueryData(['strategy-settings', next.strategy], saved); }
  return <main><header><div><p className="eyebrow">MEEM TRADING BOT</p><h1>Trading dashboard</h1></div><div className="header-actions"><span className="status">● Live API</span><button className="theme-button" onClick={() => setTheme(current => current === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? 'Light Theme' : 'Dark Theme'}</button></div></header><Controls {...controls} marketOpen={marketOpen} circuitBreaker={breaker} onControls={updateControls} /><PortfolioSummary portfolio={sharedPortfolio.data} /><div className="strategy-columns"><section className="strategy-column"><div className="strategy-heading"><div><h2>Default selection</h2><p className="strategy-description">S&P 500 momentum and volume strategy</p></div><button className="strategy-settings-icon" onClick={() => setSettingsOpen('default')} aria-label="Open default strategy settings" title="Default strategy settings">⚙</button></div><StrategyPortfolioMetrics title="Default positions and today" portfolio={defaultPortfolio.data} /><PerformanceChart snapshots={chart.data?.snapshots} /><Watchlist title="Top 10 watchlist" emptyMessage="No default watchlist data yet" watchlist={watchlist.data} /><TradeHistory title="Default strategy trades" trades={defaultTrades.data?.trades} /></section><section className="strategy-column"><div className="strategy-heading"><div><h2>Selected stocks</h2><p className="strategy-description">Manual stock selection strategy</p></div><button className="strategy-settings-icon" onClick={() => setSettingsOpen('selected')} aria-label="Open selected stock settings" title="Selected stock settings">⚙</button></div><StrategyPortfolioMetrics title="Selected positions and today" portfolio={selectedPortfolio.data} /><PerformanceChart snapshots={chart.data?.snapshots} /><Watchlist title="Selected stocks watchlist" emptyMessage="Select stocks in Settings to activate" watchlist={selectedWatchlist.data} /><TradeHistory title="Selected strategy trades" trades={selectedTrades.data?.trades} /></section></div>{settingsOpen === 'default' && defaultSettings.data && <StrategySettings settings={defaultSettings.data} onSave={saveStrategySettings} onClose={() => setSettingsOpen(null)} />}{settingsOpen === 'selected' && selectedSettings.data && <StrategySettings settings={selectedSettings.data} onSave={saveStrategySettings} onClose={() => setSettingsOpen(null)} />}</main>
}
