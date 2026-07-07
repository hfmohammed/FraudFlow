# python:3.12-slim is Debian-based (glibc). Do NOT switch to Alpine (musl libc) —
# confluent-kafka's binary wheels are glibc-linked and will fail to load on Alpine.
FROM python:3.12-slim

WORKDIR /app

# Copy requirements first so this layer is cached independently of source changes.
# Docker only re-runs pip install when requirements.txt actually changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Documents the port; actual binding is configured by METRICS_PORT env var.
EXPOSE 8000

# -u: unbuffered stdout/stderr so logs appear immediately in `docker compose logs`.
CMD ["python", "-u", "producer.py"]
