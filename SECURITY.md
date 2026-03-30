# Security

## Supported Use

- Configure `SECRET_KEY` explicitly for each deployment.
- Bootstrap the first admin account with `bootstrap_admin.py`; do not add default passwords to code or docs.
- Run the dashboard behind internal TLS or a reverse proxy when exposed beyond localhost.

## Reporting

- Do not publish live X API credentials, session secrets, or working exploit details in public issues.
- If a private reporting path is unavailable, open a minimal issue requesting secure follow-up.
