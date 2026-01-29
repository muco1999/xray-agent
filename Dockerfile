FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv

# базовые пакеты (нужны только для grpcurl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash tar \
    && rm -rf /var/lib/apt/lists/*

# grpcurl (amd64/arm64) через официальный tar.gz релиз
RUN set -eux; \
    ARCH="$(dpkg --print-architecture)"; \
    case "$ARCH" in \
      amd64) GRPCURL_ARCH="x86_64" ;; \
      arm64) GRPCURL_ARCH="arm64" ;; \
      *) echo "Unsupported arch: $ARCH"; exit 1 ;; \
    esac; \
    VERSION="1.9.3"; \
    curl -fsSL "https://github.com/fullstorydev/grpcurl/releases/download/v${VERSION}/grpcurl_${VERSION}_linux_${GRPCURL_ARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin grpcurl; \
    chmod +x /usr/local/bin/grpcurl; \
    grpcurl -version

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

# Твой код
COPY app /srv/app
COPY worker.py /srv/worker.py

# Сгенерённые proto-модули (в репозитории)
COPY xrayproto /srv/xrayproto

# Чтобы "import xrayproto...." работал
ENV PYTHONPATH="/srv:${PYTHONPATH}"

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
