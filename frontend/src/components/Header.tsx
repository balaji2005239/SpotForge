import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../api'
import { MICRO, money, uuid } from '../format'
import { useStore } from '../store'

export function Header() {
  const { userId, signOut, live, toast } = useStore()
  const qc = useQueryClient()
  const wallet = useQuery({ queryKey: ['wallet', userId], queryFn: () => api.wallet(userId) })
  const fund = useMutation({
    mutationFn: () => api.deposit(userId, 100 * MICRO, uuid()),
    onSuccess: () => { toast('ok', 'Added $100.00 of test funds.'); void qc.invalidateQueries({ queryKey: ['wallet'] }) },
    onError: (e: Error) => toast('bad', `Couldn't add funds: ${e.message}`),
  })
  const broke = wallet.data && wallet.data.available_micro + wallet.data.held_micro === 0

  return (
    <header className="top">
      <div className="brand">
        <svg width="26" height="26" viewBox="0 0 32 32" aria-hidden="true">
          <rect width="32" height="32" rx="7" fill="var(--panel)" />
          <path d="M5 21h5v-6h5v-5h5v7h7" fill="none" stroke="var(--warm)" strokeWidth="2.6" strokeLinejoin="round" />
        </svg>
        <div>
          <h1>GPU spot market</h1>
          <p className={live ? 'live on' : 'live'}>{live ? 'Live. Auctions clear every few seconds' : 'Reconnecting to the market…'}</p>
        </div>
      </div>

      <div className="acct">
        <div className="bal" aria-live="polite">
          <span className="lbl">Available</span>
          <b>{wallet.data ? money(wallet.data.available_micro) : '–'}</b>
        </div>
        <div className="bal">
          <span className="lbl">In escrow</span>
          <b>{wallet.data ? money(wallet.data.held_micro) : '–'}</b>
        </div>
        <button className={broke ? 'btn primary pulse' : 'btn'} onClick={() => fund.mutate()} disabled={fund.isPending}>
          Add $100 test funds
        </button>
        <span className="who chip" title="Signed in as">{userId}</span>
        <button className="btn" onClick={() => { signOut(); qc.clear() }}>Sign out</button>
      </div>
    </header>
  )
}
