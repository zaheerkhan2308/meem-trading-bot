import type { Trade } from '../types/api'
import { Panel } from './Panel'
export function TradeHistory({ trades = [], title = 'Trade history' }: { trades?: Trade[]; title?: string }) {
  return <Panel title={title}><div className="table-wrap"><table><thead><tr><th>Time</th><th>Action</th><th>Ticker</th><th>Quantity / price</th><th>P&L</th><th>Reason</th></tr></thead><tbody>{trades.length ? [...trades].reverse().map((trade, i) => <tr key={`${trade.ticker}-${i}`}><td>{trade.scan_time || '—'}</td><td><span className={`pill ${trade.action.toLowerCase()}`}>{trade.action}{trade.dry_run && ' · DRY'}</span></td><td><b>{trade.ticker}</b></td><td>{trade.qty} @ ${trade.price.toFixed(2)}</td><td className={trade.pnl >= 0 ? 'positive' : 'negative'}>{trade.pnl >= 0 ? '+' : ''}${trade.pnl.toFixed(2)}</td><td>{trade.reason || '—'}</td></tr>) : <tr><td colSpan={6} className="empty">No trades for this strategy yet</td></tr>}</tbody></table></div></Panel>
}
