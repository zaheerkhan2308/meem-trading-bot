import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { TradingSettings } from '../types/api'

export function Settings({ settings, onSave, onClose }: { settings: TradingSettings; onSave: (settings: TradingSettings) => Promise<void>; onClose: () => void }) {
  const [mode, setMode] = useState(settings.mode)
  const [tickers, setTickers] = useState(settings.tickers)
  const [query, setQuery] = useState('')
  const [error, setError] = useState('')
  const suggestions = useQuery({ queryKey: ['ticker-search', query], queryFn: () => api.tickerSearch(query), enabled: query.trim().length > 0 })

  useEffect(() => {
    setMode(settings.mode)
    setTickers(settings.tickers)
  }, [settings])

  function addTicker(value: string) {
    const ticker = value.trim().toUpperCase()
    if (ticker && !tickers.includes(ticker)) setTickers(current => [...current, ticker])
    setQuery('')
  }

  async function save() {
    if (mode === 'manual' && tickers.length === 0) {
      setError('Add at least one ticker for manual mode')
      return
    }
    try {
      await onSave({ mode, tickers })
      onClose()
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : 'Could not save settings')
    }
  }

  return <div className="modal-backdrop" onMouseDown={onClose}><section className="settings-modal" onMouseDown={event => event.stopPropagation()}><div className="settings-header"><div><p className="eyebrow">TRADING SETTINGS</p><h2>Choose scan universe</h2></div><button className="icon-button" onClick={onClose} aria-label="Close settings">×</button></div><div className="mode-options"><label className={mode === 'default' ? 'mode-option selected' : 'mode-option'}><input type="radio" checked={mode === 'default'} onChange={() => setMode('default')} />Default selection<div>Scan the S&P 500 using the current momentum and volume logic.</div></label><label className={mode === 'manual' ? 'mode-option selected' : 'mode-option'}><input type="radio" checked={mode === 'manual'} onChange={() => setMode('manual')} />Selected stocks<div>Trade only the tickers listed below.</div></label></div>{mode === 'manual' && <><label className="field-label" htmlFor="ticker-search">Add stock ticker</label><div className="ticker-input"><input id="ticker-search" value={query} onChange={event => setQuery(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') addTicker(query) }} placeholder="Type a ticker, for example AAPL" list="ticker-suggestions" autoFocus /><button onClick={() => addTicker(query)}>Add</button></div><datalist id="ticker-suggestions">{suggestions.data?.results.map(result => <option key={result.symbol} value={result.symbol}>{result.name}</option>)}</datalist><div className="ticker-chips">{tickers.map(ticker => <span className="ticker-chip" key={ticker}>{ticker}<button onClick={() => setTickers(current => current.filter(item => item !== ticker))} aria-label={`Remove ${ticker}`}>×</button></span>)}</div></>}{error && <p className="error">{error}</p>}<div className="settings-actions"><button onClick={onClose}>Cancel</button><button className="active" onClick={save}>Save settings</button></div></section></div>
}