import type { Watchlist as WatchlistData } from '../types/api'
import { Panel } from './Panel'
export function Watchlist({ watchlist }: { watchlist?: WatchlistData }) {
  return <Panel title="Top 10 watchlist"><p className="muted">{watchlist?.scan_time ? `Last scan: ${watchlist.scan_time}` : 'Waiting for first scan'}</p><div className="table-wrap"><table><thead><tr><th>Ticker</th><th>Score</th><th>Price</th><th>Streak</th></tr></thead><tbody>{watchlist?.tickers.length ? watchlist.tickers.map(item => <tr key={item.ticker}><td><b>{item.ticker}</b></td><td>{Number(item.composite_score ?? 0).toFixed(3)}</td><td>{item.current_price ? `$${Number(item.current_price).toFixed(2)}` : '—'}</td><td>{item.streak ?? 1}×</td></tr>) : <tr><td colSpan={4} className="empty">No watchlist data yet</td></tr>}</tbody></table></div></Panel>
}
