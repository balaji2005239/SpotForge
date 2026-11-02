import { useStore } from '../store'

export function Toasts() {
  const { toasts, dismiss } = useStore()
  return (
    <div className="toasts" role="status" aria-live="polite">
      {toasts.map((t) => (
        <button key={t.id} className={`toast ${t.tone}`} onClick={() => dismiss(t.id)}>{t.text}</button>
      ))}
    </div>
  )
}
