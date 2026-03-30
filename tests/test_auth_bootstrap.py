import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


REPO_PARENT = Path(__file__).resolve().parents[1]
if str(REPO_PARENT) not in sys.path:
    sys.path.insert(0, str(REPO_PARENT))

from bootstrap_admin import bootstrap_admin  # noqa: E402
from database import Database  # noqa: E402


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "auth.db"
    test_db = Database(str(db_path))
    test_db.init_db()
    try:
        yield test_db
    finally:
        test_db.conn.close()


def test_bootstrap_admin_creates_initial_admin(db: Database):
    created = bootstrap_admin(db, "admin.user", "Admin User", "sup3r-secret")

    assert created["username"] == "admin_user"
    assert created["display_name"] == "Admin User"
    assert int(created["is_admin"]) == 1
    assert db.count_users() == 1


def test_bootstrap_admin_refuses_second_bootstrap(db: Database):
    bootstrap_admin(db, "admin.user", "Admin User", "sup3r-secret")

    with pytest.raises(RuntimeError):
        bootstrap_admin(db, "second", "Second Admin", "another-secret")


def test_create_session_round_trip_and_expiry_cleanup(db: Database):
    user = db.create_user("analyst", "Analyst", "pw-123", is_admin=False)
    token = db.create_session(int(user["id"]))

    session = db.get_session(token)
    assert session is not None
    assert session["display_name"] == "Analyst"

    expired = (datetime.utcnow() - timedelta(days=1)).isoformat()
    db.conn.execute("UPDATE sessions SET expires_at = ? WHERE token = ?", (expired, token))
    db.conn.commit()

    db.delete_expired_sessions()
    assert db.get_session(token) is None


def test_create_user_rejects_duplicate_username(db: Database):
    db.create_user("reviewer", "Reviewer", "pw-123", is_admin=False)

    with pytest.raises(ValueError):
        db.create_user("reviewer", "Reviewer Two", "pw-456", is_admin=False)
