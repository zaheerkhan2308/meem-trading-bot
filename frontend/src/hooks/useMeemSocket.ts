import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import type { Portfolio, Trade, TradingSettings, Watchlist } from '../types/api'

type SocketEvent = {
  type?: string
  status?: { market_open?: boolean; last_scan?: string | null }
  portfolio?: Portfolio
  watchlist?: Watchlist
  selected_watchlist?: Watchlist
  trades?: Trade[]
  trade?: Trade
  kill_switch?: boolean
  dry_run?: boolean
  circuit_breaker?: string | null
  reason?: string | null
  settings?: TradingSettings
  strategy_portfolios?: { default?: Portfolio; selected?: Portfolio }
}

export function useMeemSocket(setControls: (controls: { marketOpen?: boolean | null; lastScan?: string | null; killSwitch?: boolean; dryRun?: boolean; circuitBreaker?: string | null }) => void, onSettings?: (settings: TradingSettings) => void) {
  const client = useQueryClient()
  useEffect(() => {
    let retry: number | undefined
    let socket: WebSocket
    let stopped = false
    const connect = () => {
      if (stopped) return
      socket = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`)
      socket.onmessage = ({ data }) => {
        let event: SocketEvent
        try {
          event = JSON.parse(data) as SocketEvent
        } catch {
          return
        }
        if (event.status) setControls({ marketOpen: event.status.market_open, lastScan: event.status.last_scan })
        if (event.settings) onSettings?.(event.settings)
        if (event.portfolio) client.setQueryData<Portfolio>(['portfolio'], event.portfolio)
        if (event.portfolio) client.setQueryData<Portfolio>(['portfolio', 'shared'], event.portfolio)
        if (event.strategy_portfolios) {
          if (event.strategy_portfolios.default) client.setQueryData<Portfolio>(['portfolio', 'default'], event.strategy_portfolios.default)
          if (event.strategy_portfolios.selected) client.setQueryData<Portfolio>(['portfolio', 'selected'], event.strategy_portfolios.selected)
        }
        if (event.watchlist) client.setQueryData<Watchlist>(['watchlist'], event.watchlist)
        if (event.selected_watchlist) client.setQueryData<Watchlist>(['selected-watchlist'], event.selected_watchlist)
        if (event.trades) client.setQueryData(['trades'], { trades: event.trades })
        if (event.type === 'trade_event' && event.trade) {
          const strategy = event.trade.strategy === 'selected' ? 'selected' : 'default'
          client.setQueryData<{ trades: Trade[] }>(['trades', strategy], old => ({
            trades: [...(old?.trades ?? []), event.trade!],
          }))
        }
        if (event.kill_switch !== undefined || event.dry_run !== undefined || event.circuit_breaker !== undefined) setControls({ killSwitch: event.kill_switch, dryRun: event.dry_run, circuitBreaker: event.circuit_breaker })
        if (event.type === 'circuit_breaker_alert') setControls({ circuitBreaker: event.reason, killSwitch: true })
      }
      socket.onclose = () => {
        if (stopped) return
        retry = window.setTimeout(connect, 2000)
      }
    }
    connect()
    return () => {
      stopped = true
      window.clearTimeout(retry)
      if (socket.readyState === WebSocket.CONNECTING) {
        socket.onopen = () => socket.close()
      } else if (socket.readyState === WebSocket.OPEN) {
        socket.close()
      }
    }
  }, [client, onSettings, setControls])
}
