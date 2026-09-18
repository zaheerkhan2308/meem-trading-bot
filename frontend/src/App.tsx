import { useCallback, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from './api/client'
import { useMeemSocket } from './hooks/useMeemSocket'
import { Controls } from './components/Controls'
import { PerformanceChart } from './components/PerformanceChart'
import { PortfolioSummary } from './components/PortfolioSummary'
import { TradeHistory } from './components/TradeHistory'
import { Watchlist } from './components/Watchlist'
export default function App() {
  const [controls, setControls] = useState({ marketOpen: null as boolean | null, lastScan: null as string | null, killSwitch: false, dryRun: false, circuitBreaker: null as string | null })
  const [theme, setTheme] = useState<'dark' | 'light'>(() => (localStorage.getItem('meem-theme') as 'dark' | 'light') || 'dark')
  const updateControls = useCallback((next: Partial<typeof controls>) => setControls(current => ({ ...current, ...next })), [])
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem('meem-theme', theme) }, [theme])
  useMeemSocket(updateControls)
  const portfolio = useQuery({ queryKey: ['portfolio'], queryFn: api.portfolio })
  const status = useQuery({ queryKey: ['status'], queryFn: api.status, refetchInterval: 30_000 })
  const trades = useQuery({ queryKey: ['trades'], queryFn: api.trades })
  const watchlist = useQuery({ queryKey: ['watchlist'], queryFn: api.watchlist })
  const chart = useQuery({ queryKey: ['chart'], queryFn: api.chart })
  const circuit = useQuery({ queryKey: ['circuit'], queryFn: api.circuitBreaker })
  const breaker = controls.circuitBreaker ?? circuit.data?.reason ?? null
  const marketOpen = controls.marketOpen ?? status.data?.market_open ?? null
  return <main><header><div><p className="eyebrow">MEEM TRADING BOT</p><h1>Trading dashboard</h1></div><div className="header-actions"><span className="status">● Live API</span><button className="theme-button" onClick={() => setTheme(current => current === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? 'Light Theme' : 'Dark Theme'}</button></div></header><Controls {...controls} marketOpen={marketOpen} circuitBreaker={breaker} onControls={updateControls} /><PortfolioSummary portfolio={portfolio.data} /><div className="two-col"><PerformanceChart snapshots={chart.data?.snapshots} /><Watchlist watchlist={watchlist.data} /></div><TradeHistory trades={trades.data?.trades} /></main>
}
