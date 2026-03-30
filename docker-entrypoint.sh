#!/bin/sh
set -eu

python scripts/init_db.py

exec waitress-serve --listen=0.0.0.0:5050 --threads=2 dashboard:app
