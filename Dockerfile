FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data logs

EXPOSE 5050

CMD ["sh", "-c", "python -c 'from database import init_db; init_db()' && python -m waitress --listen=0.0.0.0:5050 dashboard:app"]
