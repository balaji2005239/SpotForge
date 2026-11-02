import { useQueryClient } from '@tanstack/react-query'
import { useEffect } from 'react'
import type { Allocation, Gpu, TickPoint } from './api'
import { useStore } from './store'

interface TickMsg {
  type: 'tick'; gpu_type: Gpu; tick_no: number; clearing_price_micro: number; capacity: number
  filled_qty: number; at: string; preempted: string[]; completed: string[]; warned?: string[]; grace_seconds?: number; tick_seconds: number
}
interface SnapshotMsg { type: 'snapshot'; tick_seconds: number; markets: Partial<Record<Gpu, TickMsg>> }

/** One WebSocket for all markets. Appends each tick to the chart cache and refreshes the rest. */
export function useMarketStream() {
  const qc = useQueryClient()

  useEffect(() => {
    let ws: WebSocket | null = null
    let retry = 0
    let closed = false
    let timer: ReturnType<typeof setTimeout>

    const connect = () => {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      ws = new WebSocket(`${proto}://${location.host}/api/market/ws/markets`)
      ws.onopen = () => { retry = 0; useStore.getState().setLive(true) }
      ws.onclose = () => {
        useStore.getState().setLive(false)
        if (!closed) timer = setTimeout(connect, Math.min(1000 * 2 ** retry++, 10000))
      }
      ws.onmessage = (e) => {
        const m = JSON.parse(e.data) as SnapshotMsg | TickMsg
        const st = useStore.getState()
        if (m.type === 'snapshot') {
          for (const [g, t] of Object.entries(m.markets) as [Gpu, TickMsg][]) st.onTick(g, t.tick_no, m.tick_seconds)
          return
        }
        st.onTick(m.gpu_type, m.tick_no, m.tick_seconds)

        qc.setQueryData<TickPoint[]>(['ticks', m.gpu_type], (old) =>
          old && !old.some((p) => p.tick_no === m.tick_no)
            ? [...old.slice(-239), { tick_no: m.tick_no, clearing_price_micro: m.clearing_price_micro, capacity: m.capacity, filled_qty: m.filled_qty, at: m.at }]
            : old,
        )
        qc.setQueryData(['price', m.gpu_type], {
          gpu_type: m.gpu_type, clearing_price_micro: m.clearing_price_micro, tick_no: m.tick_no,
          capacity: m.capacity, filled_qty: m.filled_qty,
        })
        void qc.invalidateQueries({ queryKey: ['book', m.gpu_type] })
        void qc.invalidateQueries({ queryKey: ['allocs'] })
        void qc.invalidateQueries({ queryKey: ['wallet'] })
        void qc.invalidateQueries({ queryKey: ['holds'] })

        const mine = new Set((qc.getQueryData<Allocation[]>(['allocs', st.userId]) ?? []).map((a) => a.bid_id))
        for (const id of m.preempted) st.pushFeed({ kind: 'preempted', gpu: m.gpu_type, bidId: id })
        for (const id of m.warned ?? []) st.pushFeed({ kind: 'warned', gpu: m.gpu_type, bidId: id })
        for (const id of m.warned ?? []) if (mine.has(id)) st.toast('bad', `Your ${m.gpu_type} job ${id.slice(4, 10)} was outbid. It keeps running, free of charge, until the notice ends. Raise your bid or it will be preempted.`)
        for (const id of m.completed) st.pushFeed({ kind: 'completed', gpu: m.gpu_type, bidId: id })
        for (const id of m.preempted) if (mine.has(id)) st.toast('bad', `Your ${m.gpu_type} job ${id.slice(4, 10)} was preempted. The unused escrow is being refunded.`)
        for (const id of m.completed) if (mine.has(id)) st.toast('ok', `Your ${m.gpu_type} job ${id.slice(4, 10)} finished.`)
      }
    }
    connect()
    return () => { closed = true; clearTimeout(timer); ws?.close() }
  }, [qc])
}
