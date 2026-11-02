from __future__ import annotations

from datetime import datetime, timezone


def iso_utc(dt: datetime) -> str:
    """ISO-8601 that always carries its zone. sqlite hands back naive datetimes (Postgres hands back aware ones);
    a naive string is parsed by browsers as LOCAL time, which skews countdowns by the user's UTC offset."""
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()
