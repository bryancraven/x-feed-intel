#!/usr/bin/env python3
"""Initialize the database schema and seed default users."""
import sys
import os

# Add repo root to path so modules can be imported directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import Database
import config

if __name__ == "__main__":
    os.makedirs(str(config.DATA_DIR), exist_ok=True)
    db = Database(str(config.DB_PATH))
    db.init_db()
    print(f"Database initialized at {config.DB_PATH}")
