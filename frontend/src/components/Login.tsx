import { useState } from 'react'
import { api } from '../api'
import { useStore } from '../store'

export function Login() {
  const { signIn, toasts } = useStore()
  const [mode, setMode] = useState<'in' | 'up'>('up')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const note = toasts[0]?.text

  const submit = async (e: React.FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      const r = await (mode === 'up' ? api.signup : api.login)(username.trim(), password)
      signIn({ token: r.access_token, username: r.username, role: r.role })
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="login">
      <form className="login-card" onSubmit={submit}>
        <h1>GPU spot market</h1>
        <p className="sub">Bid for GPU time. Prices clear every few seconds and you only pay the clearing price.</p>
        {note && <p className="note">{note}</p>}
        <label>
          Username
          <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" required minLength={3} maxLength={32} autoFocus />
        </label>
        <label>
          Password
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete={mode === 'up' ? 'new-password' : 'current-password'} required minLength={8} />
          {mode === 'up' && <span className="hint">At least 8 characters.</span>}
        </label>
        {error && <p className="err" role="alert">{error}</p>}
        <button className="btn primary" disabled={busy}>{mode === 'up' ? 'Create account' : 'Sign in'}</button>
        <button type="button" className="link" onClick={() => { setMode(mode === 'up' ? 'in' : 'up'); setError(null) }}>
          {mode === 'up' ? 'I already have an account' : 'Create a new account'}
        </button>
      </form>
    </main>
  )
}
