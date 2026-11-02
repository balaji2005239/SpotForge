import { useStore } from './store'

export const GPUS = ['T4', 'A100', 'H100'] as const
export type Gpu = (typeof GPUS)[number]

export type JobStatus = 'QUEUED' | 'RUNNING' | 'PREEMPTED' | 'COMPLETED' | 'CANCELLED'

export interface Price { gpu_type: Gpu; clearing_price_micro: number; tick_no: number; capacity: number; filled_qty: number }
export interface TickPoint { tick_no: number; clearing_price_micro: number; capacity: number; filled_qty: number; at: string }
export interface BookLevel { price_micro: number; status: 'QUEUED' | 'RUNNING'; qty: number }
export interface Allocation {
  bid_id: string; user_id: string; gpu_type: Gpu; status: JobStatus; qty: number; alloc_qty: number
  ticks_run: number; duration_s: number; max_price_micro: number; grace_deadline_at: string | null
}
export interface Wallet { user_id: string; available_micro: number; held_micro: number }
export interface Hold {
  hold_id: string; user_id: string; amount_micro: number; captured_micro: number
  released_micro: number; remaining_micro: number; status: 'ACTIVE' | 'CLOSED'
}
export interface NodeInfo { node_id: string; gpu_type: Gpu; gpus: number; status: 'UP' | 'FAILED'; last_heartbeat: string }
export interface BidOut { bid_id: string; status: string; reject_reason: string | null; hold_amount_micro: number }
export interface SettlementStats { by_state: Record<string, { count: number; amount_micro: number; refunded_micro: number }>; total_captured_micro: number }
export interface Check { ok: boolean; problems: string[] }
export interface BidRequest {
  user_id: string; gpu_type: Gpu; qty: number; max_price_micro: number; duration_s: number; divisible: boolean
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function call<T>(url: string, init?: RequestInit): Promise<T> {
  const token = useStore.getState().session?.token
  const headers: Record<string, string> = { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) }
  const r = await fetch(url, { ...init, headers })
  if (r.status === 401 && token) useStore.getState().signOut('Your session ended. Sign in again.')
  if (!r.ok) {
    let detail = r.statusText
    try {
      const b = await r.json()
      detail = typeof b.detail === 'string' ? b.detail : JSON.stringify(b.detail ?? b)
    } catch { /* body wasn't JSON */ }
    throw new ApiError(r.status, detail)
  }
  return r.json() as Promise<T>
}

const post = <T,>(url: string, body?: unknown) =>
  call<T>(url, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) })

export interface AuthResult { access_token: string; username: string; role: 'user' | 'admin' }

export const api = {
  signup: (username: string, password: string) => post<AuthResult>('/auth/signup', { username, password }),
  login: (username: string, password: string) => post<AuthResult>('/auth/login', { username, password }),
  price: (g: Gpu) => call<Price>(`/api/market/v1/markets/${g}/price`),
  ticks: (g: Gpu, limit = 240) => call<TickPoint[]>(`/api/market/v1/markets/${g}/ticks?limit=${limit}`),
  book: (g: Gpu) => call<BookLevel[]>(`/api/market/v1/markets/${g}/book`),
  allocations: (user: string) => call<Allocation[]>(`/api/market/v1/allocations?user_id=${encodeURIComponent(user)}&limit=100`),
  reserves: () => call<{ gpu_type: Gpu; reserve_price_micro: number }[]>('/api/bid/v1/markets'),
  submitBid: (b: BidRequest) => post<BidOut>('/api/bid/v1/bids', { ...b, client_request_id: crypto.randomUUID?.() }),
  raiseBid: (id: string, max_price_micro: number) =>
    call<BidOut>(`/api/bid/v1/bids/${id}`, { method: 'PATCH', body: JSON.stringify({ max_price_micro }) }),
  cancelBid: (id: string) => call<BidOut>(`/api/bid/v1/bids/${id}`, { method: 'DELETE' }),
  wallet: (u: string) => call<Wallet>(`/api/wallet/v1/wallets/${encodeURIComponent(u)}`),
  holds: (u: string) => call<Hold[]>(`/api/wallet/v1/wallets/${encodeURIComponent(u)}/holds?limit=100`),
  deposit: (u: string, amountMicro: number, key: string) =>
    post<{ txn_id: number }>(`/api/wallet/v1/wallets/${encodeURIComponent(u)}/deposit`, { amount_micro: amountMicro, idempotency_key: key }),
  nodes: () => call<NodeInfo[]>('/api/capacity/v1/nodes'),
  crashNode: (id: string, downS = 20) => post<{ status: string }>(`/api/capacity/v1/nodes/${id}/fail?down_s=${downS}`),
  settlement: () => call<SettlementStats>('/api/settlement/v1/settlement/stats'),
  reconcile: () => post<Check>('/api/settlement/v1/admin/reconcile'),
  integrity: () => call<Check>('/api/wallet/v1/admin/integrity'),
}
