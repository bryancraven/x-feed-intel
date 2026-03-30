# Contributing

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-test.txt
cp .env.example .env
python bootstrap_admin.py <username> <display_name> <password>
```

## Checks

```bash
python -m compileall .
SECRET_KEY=dev-secret pytest -q tests
```

## Expectations

- Do not introduce default credentials or auto-generated shared passwords.
- Keep session, callback, and API secrets outside version control.
- Update user-management and deployment docs when auth flows change.
