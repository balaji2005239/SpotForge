export const MICRO = 1_000_000

/** $ per GPU-hour, e.g. $0.800 */
export const price = (micro: number) => `$${(micro / MICRO).toFixed(3)}`

/** Money amounts range from fractions of a cent (one tick) to hundreds of dollars. */
export function money(micro: number): string {
  const v = micro / MICRO
  const a = Math.abs(v)
  const digits = a >= 100 ? 2 : a >= 1 ? 3 : a >= 0.01 ? 4 : 5
  return `${v < 0 ? '-' : ''}$${a.toFixed(digits)}`
}

export const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n))

const COOL: [number, number, number] = [91, 168, 255]
const WARM: [number, number, number] = [255, 184, 77]
const HOT: [number, number, number] = [255, 90, 77]

const mix = (a: number[], b: number[], t: number) => a.map((x, i) => Math.round(x + (b[i] - x) * t))

/** Price on a thermal scale: reserve = cool blue, ~2x reserve = amber, 3.5x+ = hot red. */
export function heat(priceMicro: number, reserveMicro: number): string {
  const t = clamp((priceMicro / reserveMicro - 1) / 2.5, 0, 1)
  const [r, g, b] = t < 0.5 ? mix(COOL, WARM, t / 0.5) : mix(WARM, HOT, (t - 0.5) / 0.5)
  return `rgb(${r} ${g} ${b})`
}

export function ago(iso: string): string {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000))
  return s < 60 ? `${s}s ago` : `${Math.floor(s / 60)}m ago`
}

export const shortId = (id: string) => id.replace(/^bid_/, '').slice(0, 6)

export const uuid = () =>
  typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(16).slice(2)}`

export function durationLabel(s: number): string {
  if (s < 3600) return `${Math.round(s / 60)} min`
  return `${+(s / 3600).toFixed(1)} h`
}

/** Mirrors the backend's escrow sizing (spotcommon.money.hold_amount / tick_charge). */
export const tickCharge = (priceMicro: number, qty: number, tickS: number) =>
  Math.ceil((priceMicro * qty * tickS) / 3600)
export const holdAmount = (maxMicro: number, qty: number, durationS: number, tickS: number) =>
  Math.ceil(durationS / tickS) * tickCharge(maxMicro, qty, tickS)
