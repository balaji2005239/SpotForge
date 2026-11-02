import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import { heat, price } from '../format'
import { useStore } from '../store'
import { useReserves } from './MarketStrip'

interface Row { price: number; running: number; queued: number }

export function Ladder() {
  const gpu = useStore((s) => s.gpu)
  const reserve = useReserves()[gpu]
  const book = useQuery({ queryKey: ['book', gpu], queryFn: () => api.book(gpu) })
  const clearing = useQuery({ queryKey: ['price', gpu], queryFn: () => api.price(gpu) }).data?.clearing_price_micro ?? reserve

  const byPrice = new Map<number, Row>()
  for (const l of book.data ?? []) {
    const r = byPrice.get(l.price_micro) ?? { price: l.price_micro, running: 0, queued: 0 }
    if (l.status === 'RUNNING') r.running += l.qty
    else r.queued += l.qty
    byPrice.set(l.price_micro, r)
  }
  const rows = [...byPrice.values()].sort((a, b) => b.price - a.price)
  const max = Math.max(1, ...rows.map((r) => r.running + r.queued))
  const items: ({ kind: 'row'; row: Row } | { kind: 'line' })[] = []
  let placed = false
  for (const row of rows) {
    if (!placed && row.price < clearing) { items.push({ kind: 'line' }); placed = true }
    items.push({ kind: 'row', row })
  }
  if (!placed) items.push({ kind: 'line' })

  return (
    <section className="block">
      <div className="block-head">
        <h2>Who is bidding</h2>
        <p className="sub">
          Bids above the line are running. Bids below wait for the price to fall or for more GPUs.
        </p>
      </div>
      <ol className="ladder" aria-label={`Open bids for ${gpu}, highest first`}>
        {items.map((it, i) =>
          it.kind === 'line' ? (
            <li key="line" className="mark" style={{ ['--c' as string]: heat(clearing, reserve) }}>
              <span>Clearing price {price(clearing)}</span>
            </li>
          ) : (
            <li key={i} className="lrow">
              <span className="lp">{price(it.row.price)}</span>
              <span className="lbar" aria-hidden="true">
                <i className="run" style={{ width: `${(it.row.running / max) * 100}%` }} />
                <i className="wait" style={{ width: `${(it.row.queued / max) * 100}%` }} />
              </span>
              <span className="lq">
                {it.row.running > 0 && <span className="t-run">{it.row.running} running</span>}
                {it.row.queued > 0 && <span className="t-wait">{it.row.queued} waiting</span>}
              </span>
            </li>
          ),
        )}
        {rows.length === 0 && <li className="empty">No open bids in this market. Place one from the ticket.</li>}
      </ol>
    </section>
  )
}
