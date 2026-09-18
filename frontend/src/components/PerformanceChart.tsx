import type { Snapshot } from '../types/api'
import { Panel } from './Panel'
export function PerformanceChart({ snapshots = [] }: { snapshots?: Snapshot[] }) {
  if (!snapshots.length) return <Panel title="Performance"><p className="empty">Portfolio snapshots save daily after market close.</p></Panel>
  const values = snapshots.map(s => s.total_value); const min = Math.min(...values); const max = Math.max(...values); const range = max - min || 1
  const points = values.map((v, i) => `${(i / Math.max(values.length - 1, 1)) * 100},${92 - ((v - min) / range) * 82}`).join(' ')
  const first = values[0], last = values.at(-1)!, change = last - first
  return <Panel title="Performance"><div className={change >= 0 ? 'positive' : 'negative'}>{change >= 0 ? '+' : ''}${change.toFixed(2)} since first snapshot</div><svg className="chart" viewBox="0 0 100 100" preserveAspectRatio="none"><polyline points={points} fill="none" stroke="currentColor" strokeWidth="2" vectorEffect="non-scaling-stroke" /></svg></Panel>
}
