#!/usr/bin/env python3
"""One-time admin bootstrap for a fresh X Feed Intel deployment."""

from __future__ import annotations

import sys

from database import get_db


def bootstrap_admin(db, username: str, display_name: str, password: str) -> dict:
    """Create the initial admin user for a fresh database."""
    if db.has_users():
        raise RuntimeError("dashboard users already exist; refusing to bootstrap a second admin")
    return db.create_user(
        username=username,
        display_name=display_name,
        password=password,
        is_admin=True,
    )


def _prompt(label: str) -> str:
    return input(f"{label}: ").strip()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    username = args[0] if len(args) >= 1 else _prompt("Admin username")
    display_name = args[1] if len(args) >= 2 else _prompt("Admin display name")
    password = args[2] if len(args) >= 3 else _prompt("Admin password")

    if not username or not display_name or not password:
        print("Error: username, display name, and password are required.", file=sys.stderr)
        return 1

    db = get_db()
    db.init_db()

    try:
        created = bootstrap_admin(db, username, display_name, password)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Created bootstrap admin '{created['display_name']}' "
        f"with username '{created['username']}'."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
