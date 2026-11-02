import { useQuery } from '@tanstack/react-query'
import { Area, AreaChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api } from '../api'
import { heat, price } from '../format'
import { useStore } from '../store'
import { useReserves } from './MarketStrip'

export function PriceChart() {
  const gpu = useStore((s) => s.gpu)
  const reserve = useReserves()[gpu]
  const ticks = useQuery({ queryKey: ['ticks', gpu], queryFn: () => api.ticks(gpu), refetchInterval: false })
  const latest = useQuery({ queryKey: ['price', gpu], queryFn: () => api.price(gpu) }).data
  const data = (ticks.data ?? []).map((t) => ({ n: t.tick_no, p: t.clearing_price_micro / 1e6, used: t.filled_qty, cap: t.capacity }))
  const cur = latest?.clearing_price_micro ?? reserve
  const color = heat(cur, reserve)
  const r = reserve / 1e6

  return (
    <section className="block">
      <div className="block-head">
        <h2>{gpu} clearing price</h2>
        <p className="sub">
          Everyone who wins an auction pays this price per GPU-hour, whatever they bid.
          Reserve {price(reserve)}.
        </p>
      </div>
      <div className="chart" role="img" aria-label={`Clearing price history for ${gpu}, currently ${price(cur)} per GPU-hour`}>
        {data.length < 2 ? (
          <p className="empty">Waiting for the first auctions to clear…</p>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="fillp" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={color} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid vertical={false} stroke="var(--line)" />
              <XAxis dataKey="n" hide />
              <YAxis
                width={54}
                tick={{ fill: 'var(--muted)', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => `$${v.toFixed(2)}`}
                domain={[(min: number) => Math.max(0, Math.min(min, r) * 0.96), (max: number) => max * 1.08]}
              />
              <ReferenceLine y={r} stroke="var(--muted)" strokeDasharray="4 4" />
              <Tooltip
                cursor={{ stroke: 'var(--muted)' }}
                contentStyle={{ background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 6, color: 'var(--text)' }}
                labelFormatter={(n) => `Auction ${String(n)}`}
                formatter={(v, _k, item) => [
                  `$${Number(v).toFixed(3)}  (${(item.payload as { used: number }).used} of ${(item.payload as { cap: number }).cap} GPUs running)`,
                  'Price',
                ]}
              />
              <Area type="stepAfter" dataKey="p" stroke={color} strokeWidth={2} fill="url(#fillp)" isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  )
}
