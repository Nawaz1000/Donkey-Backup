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

RUN mkdir -p /app/data

# Nginx config
RUN printf 'server {\n\
    listen 80;\n\
    location / {\n\
        root /usr/share/nginx/html;\n\
        index index.html;\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
    location /api/ {\n\
        proxy_pass http://127.0.0.1:8000;\n\
        proxy_set_header Host $host;\n\
        proxy_set_header X-Real-IP $remote_addr;\n\
    }\n\
}\n' > /etc/nginx/sites-available/default

# Supervisord config
RUN printf '[supervisord]\n\
nodaemon=true\n\
logfile=/dev/null\n\
logfile_maxbytes=0\n\
\n\
[program:nginx]\n\
command=nginx -g "daemon off;"\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n\
\n\
[program:uvicorn]\n\
command=uvicorn main:app --host 127.0.0.1 --port 8000\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n' > /etc/supervisor/conf.d/backupvault.conf

EXPOSE 80

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]