import { create } from 'zustand'
import type { Gpu } from './api'

export interface FeedItem { id: number; at: number; kind: 'preempted' | 'completed' | 'warned'; gpu: Gpu; bidId: string }
export interface Toast { id: number; tone: 'ok' | 'warn' | 'bad'; text: string }

export interface Session { token: string; username: string; role: 'user' | 'admin' }
const loadSession = (): Session | null => {
  try {
    const s = JSON.parse(localStorage.getItem('spot.session') ?? 'null') as Session | null
    return s?.token && s.username ? s : null
  } catch { return null }
}
const saveSession = (s: Session | null) => {
  try { if (s) localStorage.setItem('spot.session', JSON.stringify(s)); else localStorage.removeItem('spot.session') } catch { /* private mode */ }
}
const initial = loadSession()

interface State {
  session: Session | null
  userId: string
  signIn: (s: Session) => void
  signOut: (why?: string) => void
  gpu: Gpu
  setGpu: (g: Gpu) => void
  tab: 'jobs' | 'wallet' | 'ops'
  setTab: (t: State['tab']) => void
  tickSeconds: number
  lastTick: Partial<Record<Gpu, { no: number; at: number }>>
  live: boolean
  setLive: (v: boolean) => void
  onTick: (g: Gpu, no: number, tickSeconds?: number) => void
  feed: FeedItem[]
  pushFeed: (i: Omit<FeedItem, 'id' | 'at'>) => void
  toasts: Toast[]
  toast: (tone: Toast['tone'], text: string) => void
  dismiss: (id: number) => void
}

let seq = 0

export const useStore = create<State>((set, get) => ({
  session: initial,
  userId: initial?.username ?? '',
  signIn: (session) => { saveSession(session); set({ session, userId: session.username, tab: 'jobs' }) },
  signOut: (why) => {
    saveSession(null)
    set({ session: null, userId: '', feed: [], toasts: why ? [{ id: ++seq, tone: 'warn', text: why }] : [] })
  },
  gpu: 'A100',
  setGpu: (gpu) => set({ gpu }),
  tab: 'jobs',
  setTab: (tab) => set({ tab }),
  tickSeconds: 5,
  lastTick: {},
  live: false,
  setLive: (live) => set({ live }),
  onTick: (g, no, tickSeconds) =>
    set((s) => ({ lastTick: { ...s.lastTick, [g]: { no, at: Date.now() } }, tickSeconds: tickSeconds ?? s.tickSeconds })),
  feed: [],
  pushFeed: (i) => set((s) => ({ feed: [{ ...i, id: ++seq, at: Date.now() }, ...s.feed].slice(0, 30) })),
  toasts: [],
  toast: (tone, text) => {
    if (get().toasts.some((t) => t.text === text)) return  // the same notice twice in a row is noise
    const id = ++seq
    set((s) => ({ toasts: [...s.toasts, { id, tone, text }].slice(-4) }))
    setTimeout(() => get().dismiss(id), 5000)
  },
  dismiss: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}))
