import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useRef, useState } from 'react'
import { ApiError, api } from '../api'
import { MICRO, holdAmount, money, price, tickCharge } from '../format'
import { useStore } from '../store'
import { useReserves } from './MarketStrip'

const DURATIONS = [
  [60, '1 minute'], [120, '2 minutes'], [300, '5 minutes'], [900, '15 minutes'], [3600, '1 hour'],
] as const

export function BidTicket() {
  const { gpu, userId, tickSeconds, toast } = useStore()
  const qc = useQueryClient()
  const reserve = useReserves()[gpu]
  const p = useQuery({ queryKey: ['price', gpu], queryFn: () => api.price(gpu) }).data
  const clearing = p?.clearing_price_micro ?? reserve
  const free = p ? Math.max(0, p.capacity - p.filled_qty) : 0

  const [qty, setQty] = useState(2)
  const [maxPrice, setMaxPrice] = useState('')
  const [duration, setDuration] = useState<number>(120)
  const [divisible, setDivisible] = useState(false)
  const touched = useRef(false)

  useEffect(() => { touched.current = false }, [gpu])
  useEffect(() => {
    if (!touched.current) setMaxPrice(((Math.max(clearing, reserve) * 1.25) / MICRO).toFixed(3))
  }, [gpu, clearing, reserve])

  const maxMicro = Math.round((parseFloat(maxPrice) || 0) * MICRO)
  const belowReserve = maxMicro < reserve
  const escrow = holdAmount(maxMicro, qty, duration, tickSeconds)
  const expected = Math.ceil(duration / tickSeconds) * tickCharge(clearing, qty, tickSeconds)

  let outlook: { tone: 'ok' | 'warn' | 'bad'; text: string }
  if (belowReserve) outlook = { tone: 'bad', text: `Below the reserve price of ${price(reserve)}. This bid can't win.` }
  else if (qty <= free) outlook = { tone: 'ok', text: `${free} GPUs are free, so this should start at the next auction.` }
  else if (maxMicro > clearing) outlook = { tone: 'warn', text: `Not enough free GPUs. You'd start by outbidding lower bids, which get preempted.` }
  else outlook = { tone: 'bad', text: `Not enough free GPUs, and ${price(clearing)} is the price to beat. This bid would wait in the queue.` }

  const submit = useMutation({
    mutationFn: () => api.submitBid({ user_id: userId, gpu_type: gpu, qty, max_price_micro: maxMicro, duration_s: duration, divisible }),
    onSuccess: (b) => {
      if (b.status === 'REJECTED') toast('bad', `Bid rejected: ${b.reject_reason ?? 'insufficient funds'}. Add funds and try again.`)
      else toast('ok', `Bid placed. It joins the next auction.`)
      void qc.invalidateQueries({ queryKey: ['wallet'] })
      void qc.invalidateQueries({ queryKey: ['allocs'] })
      void qc.invalidateQueries({ queryKey: ['holds'] })
    },
    onError: (e: Error) => toast('bad', e instanceof ApiError && e.status === 503 ? 'Wallet is unavailable. Try again in a moment.' : `Bid failed: ${e.message}`),
  })

  const nudge = (label: string, micro: number) => (
    <button type="button" className="chip" onClick={() => { touched.current = true; setMaxPrice((micro / MICRO).toFixed(3)) }}>
      {label}
    </button>
  )

  return (
    <section className="block ticket">
      <div className="block-head">
        <h2>Bid for {gpu} GPUs</h2>
        <p className="sub">Your bid is the most you'll pay. You are charged the clearing price.</p>
      </div>
      <form onSubmit={(e) => { e.preventDefault(); submit.mutate() }}>
        <label className="field">
          <span>GPUs</span>
          <div className="stepper">
            <button type="button" aria-label="Fewer GPUs" onClick={() => setQty((q) => Math.max(1, q - 1))}>−</button>
            <input type="number" min={1} max={64} value={qty} onChange={(e) => setQty(Math.min(64, Math.max(1, parseInt(e.target.value) || 1)))} />
            <button type="button" aria-label="More GPUs" onClick={() => setQty((q) => Math.min(64, q + 1))}>+</button>
          </div>
        </label>

        <label className="field">
          <span>Most you'll pay per GPU-hour</span>
          <div className="money-in">
            <i>$</i>
            <input inputMode="decimal" value={maxPrice} onChange={(e) => { touched.current = true; setMaxPrice(e.target.value) }} />
          </div>
          <div className="chips">
            {nudge('Reserve', reserve)}
            {nudge('Market', clearing)}
            {nudge('+25%', Math.round(clearing * 1.25))}
            {nudge('2×', clearing * 2)}
          </div>
        </label>

        <label className="field">
          <span>How long</span>
          <select value={duration} onChange={(e) => setDuration(Number(e.target.value))}>
            {DURATIONS.map(([s, l]) => <option key={s} value={s}>{l}</option>)}
          </select>
        </label>

        <label className="check">
          <input type="checkbox" checked={divisible} onChange={(e) => setDivisible(e.target.checked)} />
          <span>Accept fewer GPUs if the full amount isn't available</span>
        </label>

        <dl className="est">
          <div><dt>Held in escrow</dt><dd>{money(escrow)}</dd></div>
          <div><dt>Cost if the price holds at {price(clearing)}</dt><dd>{money(expected)}</dd></div>
        </dl>
        <p className={`outlook ${outlook.tone}`}>{outlook.text}</p>

        <button className="btn primary wide" type="submit" disabled={submit.isPending || belowReserve || maxMicro <= 0}>
          {submit.isPending ? 'Placing bid…' : 'Place bid'}
        </button>
      </form>
    </section>
  )
}
