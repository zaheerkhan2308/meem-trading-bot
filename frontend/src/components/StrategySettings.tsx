import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { StrategySettings as StrategySettingsData } from '../types/api'

const fields = [
  ['MAX_POSITION_SIZE', 'Max position size'], ['MAX_POSITIONS', 'Max positions'],
  ['MAX_CAPITAL', 'Max capital'], ['BUY_THRESHOLD', 'Buy threshold'],
  ['SELL_THRESHOLD', 'Sell threshold'], ['STOP_LOSS_PCT', 'Stop loss %'],
  ['TRAILING_STOP_PCT', 'Trailing stop %'], ['DAILY_LOSS_LIMIT', 'Daily loss limit'],
  ['DAILY_PROFIT_TARGET', 'Daily profit target'],
] as const

export function StrategySettings({ settings, onSave, onClose }: { settings: StrategySettingsData; onSave: (value: StrategySettingsData) => Promise<void>; onClose: () => void }) {
  const [tickers, setTickers] = useState(settings.tickers)
  const [overrides, setOverrides] = useState<Record<string, number>>(settings.overrides)
  const [query, setQuery] = useState('')
  const [error, setError] = useState('')
  const suggestions = useQuery({ queryKey: ['ticker-search', query], queryFn: () => api.tickerSearch(query), enabled: settings.strategy === 'selected' && query.trim().length > 0 })
  useEffect(() => { setTickers(settings.tickers); setOverrides(settings.overrides) }, [settings])
  function addTicker(value: string) { const ticker = value.trim().toUpperCase(); if (ticker && !tickers.includes(ticker)) setTickers(current => [...current, ticker]); setQuery('') }
  async function save() { try { await onSave({ strategy: settings.strategy, tickers, overrides }); onClose() } catch (saveError) { setError(saveError instanceof Error ? saveError.message : 'Could not save settings') } }
  return <div className="modal-backdrop" onMouseDown={onClose}><section className="settings-modal" onMouseDown={event => event.stopPropagation()}><div className="settings-header"><div><p className="eyebrow">{settings.strategy === 'default' ? 'DEFAULT SELECTION' : 'SELECTED STOCKS'}</p><h2>Strategy settings</h2></div><button className="icon-button" onClick={onClose} aria-label="Close settings">×</button></div>{settings.strategy === 'selected' && <><label className="field-label" htmlFor="ticker-search">Selected stocks</label><div className="ticker-input"><input id="ticker-search" value={query} onChange={event => setQuery(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') addTicker(query) }} placeholder="Type a ticker or company" list="ticker-suggestions" autoFocus /><button onClick={() => addTicker(query)}>Add</button></div><datalist id="ticker-suggestions">{suggestions.data?.results.map(result => <option key={result.symbol} value={result.symbol}>{result.name}</option>)}</datalist><div className="ticker-chips">{tickers.map(ticker => <span className="ticker-chip" key={ticker}>{ticker}<button onClick={() => setTickers(current => current.filter(item => item !== ticker))} aria-label={`Remove ${ticker}`}>×</button></span>)}</div></>}{settings.strategy === 'default' && <div className="override-grid">{fields.map(([key, label]) => <label className="override-field" key={key}>{label}<input type="number" step="any" value={overrides[key] ?? ''} onChange={event => setOverrides(current => ({ ...current, [key]: Number(event.target.value) }))} /></label>)}</div>}{error && <p className="error">{error}</p>}<div className="settings-actions"><button onClick={onClose}>Cancel</button><button className="active" onClick={save}>Save settings</button></div></section></div>
}