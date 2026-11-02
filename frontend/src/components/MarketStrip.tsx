import { useQuery } from '@tanstack/react-query'
import { GPUS, type Gpu, api } from '../api'
import { heat, price } from '../format'
import { useStore } from '../store'

export function useReserves(): Record<Gpu, number> {
  const q = useQuery({ queryKey: ['reserves'], queryFn: api.reserves, staleTime: Infinity, refetchInterval: false })
  const out: Record<Gpu, number> = { T4: 100_000, A100: 800_000, H100: 1_500_000 }
  for (const r of q.data ?? []) out[r.gpu_type] = r.reserve_price_micro
  return out
}

function Tile({ gpu }: { gpu: Gpu }) {
  const { gpu: selected, setGpu } = useStore()
  const reserves = useReserves()
  const p = useQuery({ queryKey: ['price', gpu], queryFn: () => api.price(gpu) })
  const cur = p.data?.clearing_price_micro ?? reserves[gpu]
  const used = p.data?.filled_qty ?? 0
  const cap = p.data?.capacity ?? 0
  const pct = cap ? Math.round((used / cap) * 100) : 0
  const color = heat(cur, reserves[gpu])
  return (
    <button className={`tile ${selected === gpu ? 'sel' : ''}`} aria-pressed={selected === gpu} onClick={() => setGpu(gpu)}>
      <span className="tile-name">{gpu}</span>
      <span className="tile-price" style={{ color }} key={cur}>
        {price(cur)}<small>/GPU-hr</small>
      </span>
      <span className="tile-meta">
        {cap ? `${used} of ${cap} GPUs running` : 'No GPUs online'}
      </span>
      <span className="util" role="img" aria-label={`${pct}% of capacity in use`}>
        <i style={{ width: `${pct}%`, background: color }} />
      </span>
    </button>
  )
}

export function MarketStrip() {
  return (
    <nav className="strip" aria-label="Markets">
      {GPUS.map((g) => <Tile key={g} gpu={g} />)}
    </nav>
  )
}

/** Thin bar that fills once per auction tick, restarted by each tick message. */
export function Sweep() {
  const gpu = useStore((s) => s.gpu)
  const tickSeconds = useStore((s) => s.tickSeconds)
  const last = useStore((s) => s.lastTick[gpu])
  return (
    <div className="sweep" aria-hidden="true">
      <i key={`${gpu}-${last?.no ?? 0}`} style={{ animationDuration: `${tickSeconds}s` }} />
    </div>
  )
}
