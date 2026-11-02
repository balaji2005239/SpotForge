import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { GPUS, type Check, api } from '../api'
import { MICRO, ago, money, uuid } from '../format'
import { useStore } from '../store'
import { useReserves } from './MarketStrip'

function CheckResult({ r }: { r?: Check }) {
  if (!r) return null
  return r.ok
    ? <span className="t-run">All consistent</span>
    : <span className="t-bad">{r.problems.length} problem(s): {r.problems.slice(0, 2).join('; ')}</span>
}

export function Ops() {
  const { gpu, toast } = useStore()
  const reserves = useReserves()
  const qc = useQueryClient()
  const nodes = useQuery({ queryKey: ['nodes'], queryFn: api.nodes, refetchInterval: 3000 }).data ?? []
  const stats = useQuery({ queryKey: ['settlement'], queryFn: api.settlement, refetchInterval: 4000 }).data
  const [rec, setRec] = useState<Check>()
  const [led, setLed] = useState<Check>()

  const crash = useMutation({
    mutationFn: (id: string) => api.crashNode(id),
    onSuccess: (_d, id) => {
      toast('warn', `${id} crashed for 20 seconds. Jobs on it lose their GPUs at the next auction.`)
      void qc.invalidateQueries({ queryKey: ['nodes'] })
    },
    onError: (e: Error) => toast('bad', e.message),
  })

  const spike = useMutation({
    mutationFn: async () => {
      const p = await api.price(gpu)
      if (!p.capacity) throw new Error(`No ${gpu} GPUs are online.`)
      const user = `whale-${Math.random().toString(36).slice(2, 6)}`
      await api.deposit(user, 1000 * MICRO, uuid())
      return api.submitBid({
        user_id: user, gpu_type: gpu, qty: Math.min(64, p.capacity),
        max_price_micro: Math.max(p.clearing_price_micro * 3, reserves[gpu] * 4), duration_s: 60, divisible: false,
      })
    },
    onSuccess: () => toast('warn', `A big bidder just asked for every ${gpu} GPU at 3× the price. Watch who gets preempted.`),
    onError: (e: Error) => toast('bad', e.message),
  })

  const total = (key: string) => stats?.by_state[key]?.count ?? 0

  return (
    <div className="ops">
      <section>
        <h3>GPU fleet</h3>
        {GPUS.map((g) => {
          const group = nodes.filter((n) => n.gpu_type === g)
          return (
            <div className="fleet-row" key={g}>
              <b>{g}</b>
              <ul>
                {group.map((n) => (
                  <li key={n.node_id} className={n.status === 'UP' ? 'up' : 'down'}>
                    <span className="dot" aria-hidden="true" />
                    <span>{n.node_id}<small>{n.gpus} GPUs, {n.status === 'UP' ? `seen ${ago(n.last_heartbeat)}` : 'offline'}</small></span>
                    <button className="btn small" disabled={n.status !== 'UP' || crash.isPending} onClick={() => crash.mutate(n.node_id)}>Crash</button>
                  </li>
                ))}
                {group.length === 0 && <li className="empty">No nodes registered</li>}
              </ul>
            </div>
          )
        })}
      </section>

      <section>
        <h3>Stress the market</h3>
        <p className="sub">Crash a node to shrink supply, or send a big bidder to push the price up. Both preempt the lowest bidders.</p>
        <button className="btn" onClick={() => spike.mutate()} disabled={spike.isPending}>Outbid every {gpu} GPU</button>
      </section>

      <section>
        <h3>Settlement</h3>
        <dl className="totals tight">
          <div><dt>Charges applied</dt><dd>{total('CAPTURE.DONE')}</dd></div>
          <div><dt>Refunds applied</dt><dd>{total('RELEASE.DONE')}</dd></div>
          <div><dt>Waiting or retrying</dt><dd>{total('CAPTURE.PENDING') + total('RELEASE.PENDING')}</dd></div>
          <div><dt>Charged in total</dt><dd>{money(stats?.total_captured_micro ?? 0)}</dd></div>
        </dl>
        <div className="checks">
          <button className="btn small" onClick={() => api.reconcile().then(setRec).catch((e: Error) => toast('bad', e.message))}>Check charges against the wallet</button>
          <CheckResult r={rec} />
          <button className="btn small" onClick={() => api.integrity().then(setLed).catch((e: Error) => toast('bad', e.message))}>Verify the ledger</button>
          <CheckResult r={led} />
        </div>
      </section>
    </div>
  )
}
