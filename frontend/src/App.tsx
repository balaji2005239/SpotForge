import { BidTicket } from './components/BidTicket'
import { Feed } from './components/Feed'
import { Header } from './components/Header'
import { Jobs } from './components/Jobs'
import { Login } from './components/Login'
import { Ladder } from './components/Ladder'
import { MarketStrip, Sweep } from './components/MarketStrip'
import { Ops } from './components/Ops'
import { PriceChart } from './components/PriceChart'
import { Toasts } from './components/Toasts'
import { WalletPanel } from './components/WalletPanel'
import { useStore } from './store'
import { useMarketStream } from './useMarketStream'

const TABS = [['jobs', 'My jobs'], ['wallet', 'Wallet'], ['ops', 'Fleet and checks']] as const

export default function App() {
  const session = useStore((s) => s.session)
  return session ? <Market admin={session.role === 'admin'} /> : <Login />
}

function Market({ admin }: { admin: boolean }) {
  useMarketStream()
  const { tab, setTab } = useStore()
  const tabs = TABS.filter(([id]) => id !== 'ops' || admin)
  return (
    <div className="app">
      <Header />
      <Sweep />
      <MarketStrip />
      <main className="grid">
        <div className="main-col">
          <PriceChart />
          <Ladder />
        </div>
        <aside className="side-col">
          <BidTicket />
          <Feed />
        </aside>
        <section className="tabs-area">
          <div className="tabs" role="tablist">
            {tabs.map(([id, label]) => (
              <button key={id} role="tab" aria-selected={tab === id} className={tab === id ? 'on' : ''} onClick={() => setTab(id)}>{label}</button>
            ))}
          </div>
          <div role="tabpanel">
            {tab === 'jobs' && <Jobs />}
            {tab === 'wallet' && <WalletPanel />}
            {tab === 'ops' && admin && <Ops />}
          </div>
        </section>
      </main>
      <Toasts />
    </div>
  )
}
