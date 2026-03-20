#!/usr/bin/env python3
"""Admin password reset — run via SSH on the Pi.

Usage:
    python3 -m x_feed_intel.reset_password                    # interactive
    python3 -m x_feed_intel.reset_password bryan_a3f1 newpass  # CLI args
    python3 -m x_feed_intel.reset_password --list              # show all users
"""
import sys
import os


from werkzeug.security import generate_password_hash
from .database import get_db


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


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args or "-l" in args:
        list_users()
    elif len(args) >= 2:
        reset_password(args[0], args[1])
    elif len(args) == 1:
        reset_password(args[0])
    else:
        reset_password()
