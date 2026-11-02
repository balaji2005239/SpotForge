import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import { money, shortId } from '../format'
import { useStore } from '../store'

export function WalletPanel() {
  const userId = useStore((s) => s.userId)
  const wallet = useQuery({ queryKey: ['wallet', userId], queryFn: () => api.wallet(userId) }).data
  const holds = useQuery({ queryKey: ['holds', userId], queryFn: () => api.holds(userId) }).data ?? []
  const spent = holds.reduce((s, h) => s + h.captured_micro, 0)
  const refunded = holds.reduce((s, h) => s + h.released_micro, 0)

  return (
    <div>
      <dl className="totals">
        <div><dt>Available</dt><dd>{wallet ? money(wallet.available_micro) : '–'}</dd></div>
        <div><dt>In escrow</dt><dd>{wallet ? money(wallet.held_micro) : '–'}</dd></div>
        <div><dt>Spent on GPU time</dt><dd>{money(spent)}</dd></div>
        <div><dt>Refunded from escrow</dt><dd>{money(refunded)}</dd></div>
      </dl>
      {holds.length === 0 ? (
        <p className="empty pad">Nothing in escrow yet. Each bid holds its worst-case cost until the job ends.</p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr><th>Job</th><th className="num">Held at bid</th><th className="num">Charged</th><th className="num">Refunded</th><th>State</th></tr>
            </thead>
            <tbody>
              {holds.map((h) => (
                <tr key={h.hold_id}>
                  <td><span className="id">{shortId(h.hold_id)}</span></td>
                  <td className="num">{money(h.amount_micro)}</td>
                  <td className="num">{money(h.captured_micro)}</td>
                  <td className="num">{money(h.released_micro)}</td>
                  <td>{h.status === 'ACTIVE' ? <span className="pill s-running">In escrow</span> : <span className="pill s-completed">Settled</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
