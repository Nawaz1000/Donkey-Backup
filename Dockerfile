# ─── Stage 1: Python backend dependencies ───────────────────────────────────
FROM python:3.11-slim AS backend

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/ .

# ─── Stage 2: Final image (nginx + uvicorn together) ────────────────────────
FROM python:3.11-slim

# Install nginx, supervisor, postgresql-client, and mongodb tools
RUN apt-get update && apt-get install -y \
    nginx supervisor \
    postgresql-client \
    gnupg curl wget \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB Database Tools directly (no apt repo needed)
RUN wget -q https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.deb \
    && dpkg -i mongodb-database-tools-debian12-x86_64-100.10.0.deb \
    && rm mongodb-database-tools-debian12-x86_64-100.10.0.deb

# Copy installed Python packages and app from build stage
COPY --from=backend /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=backend /usr/local/bin /usr/local/bin
COPY --from=backend /app /app

# Copy frontend static files
COPY frontend/ /usr/share/nginx/html/

# Copy config files (separate files, no heredoc for full Docker compatibility)
COPY nginx.conf /etc/nginx/sites-available/default
COPY supervisord.conf /etc/supervisor/conf.d/backupvault.conf

# Create data directory
RUN mkdir -p /app/data

EXPOSE 80

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]