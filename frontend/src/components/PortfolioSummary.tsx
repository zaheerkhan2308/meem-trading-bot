import type { Portfolio } from '../types/api'
import { Panel } from './Panel'
const money = (n: number) => n.toLocaleString('en-US', { style: 'currency', currency: 'USD' })
export function PortfolioSummary({ portfolio, title = 'Portfolio' }: { portfolio?: Portfolio; title?: string }) {
  const p = portfolio ?? { total_value: 0, cash: 0, positions_value: 0, daily_pnl: 0, daily_pnl_pct: 0 }
  return <Panel title={title}><div className="portfolio-grid"><Metric label="Total value" value={money(p.total_value)} /><Metric label="Cash" value={money(p.cash)} /><Metric label="Positions" value={money(p.positions_value)} /><Metric label="Today" value={`${p.daily_pnl >= 0 ? '+' : ''}${money(p.daily_pnl)} (${p.daily_pnl_pct.toFixed(2)}%)`} positive={p.daily_pnl >= 0} /></div></Panel>
}
export function StrategyPortfolioMetrics({ portfolio, title }: { portfolio?: Portfolio; title: string }) {
  const p = portfolio ?? { total_value: 0, cash: 0, positions_value: 0, daily_pnl: 0, daily_pnl_pct: 0 }
  return <Panel title={title}><div className="portfolio-grid strategy-metrics"><Metric label="Positions" value={money(p.positions_value)} /><Metric label="Today" value={`${p.daily_pnl >= 0 ? '+' : ''}${money(p.daily_pnl)} (${p.daily_pnl_pct.toFixed(2)}%)`} positive={p.daily_pnl >= 0} /></div></Panel>
}
function Metric({ label, value, positive }: { label: string; value: string; positive?: boolean }) { return <div><small>{label}</small><strong className={positive === undefined ? '' : positive ? 'positive' : 'negative'}>{value}</strong></div> }
