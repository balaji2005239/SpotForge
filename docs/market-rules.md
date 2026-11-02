# Market rules (v1)

- One independent market per GPU type (T4, A100, H100). One tick = 5 s.
- All money is integer micro-USD per GPU-hour. No floats anywhere.
- **Clearing** (`libs/clearing_core`): sort by price desc, then arrival (`seq`) asc. Incumbents keep
  their original `seq`, so they win ties. Fill greedily; an indivisible bid that does not fit is
  skipped and lower bids may still fill (maximises utilisation). Divisible bids can be part-filled.
- **Price** (default `HIGHEST_REJECTED`): the highest bid that lost at the margin (fully or partly),
  restricted to losers priced <= the lowest winner, floored at the reserve. Undersubscribed => reserve.
  Winners therefore never set their own price. `LOWEST_ACCEPTED` is available as an alternative.
- **Billing**: winners pay the clearing price per tick (rounded up per tick), never their bid.
- **Escrow**: on submit, hold `ceil(max_price * qty * duration / 3600)`. Per-tick charges are
  captured from the hold; the unused remainder is released on completion, cancel or preemption.
- **Determinism**: `clear()` is a pure function of a snapshot; `digest` lets replays be verified.
