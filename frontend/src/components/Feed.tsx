import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import { shortId } from '../format'
import { useStore } from '../store'

export function Feed() {
  const { feed, userId } = useStore()
  const mine = new Set((useQuery({ queryKey: ['allocs', userId], queryFn: () => api.allocations(userId) }).data ?? []).map((a) => a.bid_id))
  return (
    <section className="block feed">
      <div className="block-head"><h2>Market events</h2></div>
      {feed.length === 0 ? (
        <p className="empty">Preemptions and finished jobs show up here as they happen.</p>
      ) : (
        <ul aria-live="polite">
          {feed.slice(0, 8).map((f) => (
            <li key={f.id} className={f.kind}>
              <span className="dot" aria-hidden="true" />
              <span>
                {f.gpu} job <span className="id">{shortId(f.bidId)}</span>{' '}
                {f.kind === 'preempted' ? 'was outbid and preempted' : f.kind === 'warned' ? 'was outbid and is on notice' : 'finished'}
                {mine.has(f.bidId) && <b className="mine"> (yours)</b>}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
