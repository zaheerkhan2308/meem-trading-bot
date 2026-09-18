export type Portfolio = { total_value: number; cash: number; positions_value: number; daily_pnl: number; daily_pnl_pct: number }
export type Trade = { ticker: string; action: string; qty: number; price: number; score: number; reason: string; scan_time: string; dry_run: boolean; pnl: number; strategy?: 'default' | 'selected' }
export type WatchlistItem = { ticker: string; composite_score?: number; current_price?: number; streak?: number; [key: string]: unknown }
export type Watchlist = { scan_time: string | null; tickers: WatchlistItem[] }
export type Snapshot = { date: string; total_value: number }
export type CircuitBreaker = { active: boolean; reason: string | null }
export type TradingSettings = { mode: 'default' | 'manual'; tickers: string[] }
export type StrategySettings = { strategy: 'default' | 'selected'; tickers: string[]; overrides: Record<string, number> }
