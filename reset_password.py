#!/usr/bin/env python3
"""User management helper for X Feed Intel.

Usage:
    python3 reset_password.py --list
    python3 reset_password.py <username> <newpass>
    python3 reset_password.py --create <username> <display_name> <password> [--admin]
"""
import sys


from werkzeug.security import generate_password_hash
from database import get_db


def list_users():
    """Print all usernames and display names."""
    db = get_db()
    users = db.get_all_users()
    if not users:
        print("No users found.")
        return
    print("\n  Username          Display Name    Admin")
    print("  ────────────────  ──────────────  ─────")
    for u in users:
        admin_str = "yes" if u.get("is_admin") else "no"
        print(f"  {u['username']:18s}{u['display_name']:16s}{admin_str}")
    print()


def reset_password(username=None, new_pass=None):
    """Reset a user's password."""
    if username is None:
        username = input("Username: ").strip()
    if new_pass is None:
        new_pass = input("New password: ").strip()

    if not username or not new_pass:
        print("Error: username and password are required.")
        sys.exit(1)

    db = get_db()
    user = db.get_user_by_username(username)
    if not user:
        print(f"Error: No user with username '{username}'")
        print("Run with --list to see all usernames.")
        sys.exit(1)

    db.update_user_password(user["id"], generate_password_hash(new_pass))
    print(f"Password updated for {user['display_name']} ({username})")


def create_user(username: str, display_name: str, password: str, *, is_admin: bool = False):
    """Create a dashboard user with an explicit password."""
    db = get_db()
    created = db.create_user(
        username=username,
        display_name=display_name,
        password=password,
        is_admin=is_admin,
    )
    role = "admin" if created.get("is_admin") else "user"
    print(f"Created {role} {created['display_name']} ({created['username']})")


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args or "-l" in args:
        list_users()
    elif "--create" in args:
        create_idx = args.index("--create")
        create_args = args[create_idx + 1 :]
        is_admin = False
        if "--admin" in create_args:
            create_args.remove("--admin")
            is_admin = True
        if len(create_args) != 3:
            print("Usage: python3 reset_password.py --create <username> <display_name> <password> [--admin]")
            sys.exit(1)
        create_user(create_args[0], create_args[1], create_args[2], is_admin=is_admin)
    elif len(args) >= 2:
        reset_password(args[0], args[1])
    elif len(args) == 1:
        reset_password(args[0])
    else:
        reset_password()
