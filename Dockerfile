FROM python:3.11-slim

# ── Create non-root user (uid 1000) ──────────────────────────
RUN groupadd -r -g 1000 appuser && \
    useradd -r -u 1000 -g appuser -s /sbin/nologin -M appuser

WORKDIR /app

# ── Install dependencies as root, before switching user ──────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application code ─────────────────────────────────────
COPY --chown=appuser:appuser . .

# ── Drop to non-root ──────────────────────────────────────────
USER appuser

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# No default CMD — specified per service in docker-compose.yml
