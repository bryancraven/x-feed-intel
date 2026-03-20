#!/usr/bin/env python3
"""Run weekly cycle rollover for X Feed Intel."""
import json
import os
import sys
from datetime import datetime


from .database import get_db  # noqa: E402


def main() -> int:
    db = get_db()
    db.init_db()

    result = db.rollover_weekly_cycle_if_due(actor="system:timer")
    cycle = result.get("current_cycle") or db.get_current_weekly_cycle()

    payload = {
        "ok": True,
        "ran_at_utc": datetime.utcnow().isoformat(),
        "rolled_over": int(result.get("rolled_over") or 0),
        "current_cycle": {
            "id": (cycle or {}).get("id"),
            "week_key": (cycle or {}).get("week_key"),
            "starts_at": (cycle or {}).get("starts_at"),
            "ends_at": (cycle or {}).get("ends_at"),
            "timezone": (cycle or {}).get("timezone"),
        },
    }
    print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
