#!/usr/bin/env python3
"""Initialize the database schema and seed default users."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    init_db()
    print("Database initialized successfully.")
