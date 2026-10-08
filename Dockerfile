FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY README.md LICENSE requirements.docker.lock ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps --require-hashes -r requirements.docker.lock \
    && useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin sentinel \
    && mkdir -p /data /config \
    && chown sentinel:sentinel /data

USER sentinel
VOLUME ["/data"]
EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8787/api/healthz',timeout=3).read()"]

CMD ["python", "-m", "crypto_sentinel", "--config", "/config/config.yaml", "run"]
