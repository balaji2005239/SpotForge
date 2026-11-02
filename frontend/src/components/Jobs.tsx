import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Fragment, useEffect, useState } from 'react'
import { type Allocation, type JobStatus, api } from '../api'
import { MICRO, clamp, holdAmount, money, price, shortId } from '../format'
import { useStore } from '../store'

const LABEL: Record<JobStatus, string> = {
  QUEUED: 'Waiting', RUNNING: 'Running', PREEMPTED: 'Preempted', COMPLETED: 'Finished', CANCELLED: 'Cancelled',
}

function useNow(ms = 1000) {
  const [now, setNow] = useState(Date.now())
  useEffect(() => { const t = setInterval(() => setNow(Date.now()), ms); return () => clearInterval(t) }, [ms])
  return now
}

function RaiseForm({ a, tickSeconds, onDone }: { a: Allocation; tickSeconds: number; onDone: () => void }) {
  const { toast } = useStore()
  const qc = useQueryClient()
  const [text, setText] = useState(((a.max_price_micro * 1.5) / MICRO).toFixed(3))
  const next = Math.round((parseFloat(text) || 0) * MICRO)
  const extra = Math.max(0, holdAmount(next, a.qty, a.duration_s, tickSeconds) - holdAmount(a.max_price_micro, a.qty, a.duration_s, tickSeconds))
  const raise = useMutation({
    mutationFn: () => api.raiseBid(a.bid_id, next),
    onSuccess: () => {
      toast('ok', `Max bid raised to ${price(next)}.${a.grace_deadline_at ? ' Your job is back in the auction.' : ''}`)
      void qc.invalidateQueries({ queryKey: ['allocs'] })
      void qc.invalidateQueries({ queryKey: ['wallet'] })
      void qc.invalidateQueries({ queryKey: ['holds'] })
      onDone()
    },
    onError: (e: Error) => toast('bad', `Couldn't raise the bid: ${e.message}`),
  })
  return (
    <form className="raise" onSubmit={(e) => { e.preventDefault(); raise.mutate() }}>
      <label>
        New max per GPU-hour
        <input type="number" step="any" inputMode="decimal" value={text} onChange={(e) => setText(e.target.value)} autoFocus />
      </label>
      <span className="hint">
        {next <= a.max_price_micro ? `Must be higher than ${price(a.max_price_micro)}.` : `Holds ${money(extra)} more in escrow. You still pay only the clearing price.`}
      </span>
      <button className="btn primary small" disabled={raise.isPending || next <= a.max_price_micro}>Raise</button>
      <button type="button" className="btn small" onClick={onDone}>Not now</button>
    </form>
  )
}

export function Jobs() {
  const now = useNow()
  const [raising, setRaising] = useState<string | null>(null)
  const { userId, tickSeconds, toast } = useStore()
  const qc = useQueryClient()
  const allocs = useQuery({ queryKey: ['allocs', userId], queryFn: () => api.allocations(userId) })
  const cancel = useMutation({
    mutationFn: (id: string) => api.cancelBid(id),
    onSuccess: () => {
      toast('ok', 'Job cancelled. The unused escrow is back in your balance.')
      void qc.invalidateQueries({ queryKey: ['allocs'] })
      void qc.invalidateQueries({ queryKey: ['wallet'] })
    },
    onError: (e: Error) => toast('bad', `Couldn't cancel: ${e.message}`),
  })
  const rows = allocs.data ?? []

  if (rows.length === 0) {
    return <p className="empty pad">No jobs yet for {userId}. Add test funds, then place a bid from the ticket.</p>
  }
  return (
    <div className="scroll">
      <table>
        <thead>
          <tr><th>Job</th><th>Status</th><th>Progress</th><th className="num">Your max bid</th><th><span className="sr">Actions</span></th></tr>
        </thead>
        <tbody>
          {rows.map((a) => {
            const pct = clamp(((a.ticks_run * tickSeconds) / a.duration_s) * 100, 0, 100)
            const live = a.status === 'QUEUED' || a.status === 'RUNNING'
            const left = a.status === 'RUNNING' && a.grace_deadline_at
              ? Math.max(0, Math.ceil((Date.parse(a.grace_deadline_at) - now) / 1000)) : null
            return (
              <Fragment key={a.bid_id}>
              <tr>
                <td>
                  <b>{a.alloc_qty && a.status === 'RUNNING' ? a.alloc_qty : a.qty} × {a.gpu_type}</b>
                  <span className="id">{shortId(a.bid_id)}</span>
                </td>
                <td>
                  {left === null
                    ? <span className={`pill s-${a.status.toLowerCase()}`}>{LABEL[a.status]}</span>
                    : <span className="pill s-notice" title="Outbid. Runs free of charge until the notice ends.">Outbid · {left}s left</span>}
                </td>
                <td>
                  <span className="prog" role="progressbar" aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100}>
                    <i className={`s-${a.status.toLowerCase()}`} style={{ width: `${a.status === 'QUEUED' ? 0 : pct}%` }} />
                  </span>
                </td>
                <td className="num">{price(a.max_price_micro)}</td>
                <td className="act">
                  {live && (
                    <>
                      <button className={left !== null ? 'btn small primary' : 'btn small'} onClick={() => setRaising(raising === a.bid_id ? null : a.bid_id)}>Raise bid</button>
                      <button className="btn small" onClick={() => cancel.mutate(a.bid_id)} disabled={cancel.isPending}>Cancel</button>
                    </>
                  )}
                </td>
              </tr>
              {raising === a.bid_id && live && (
                <tr className="raise-row"><td colSpan={5}><RaiseForm a={a} tickSeconds={tickSeconds} onDone={() => setRaising(null)} /></td></tr>
              )}
              </Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
