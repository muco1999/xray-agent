FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash tar \
    && rm -rf /var/lib/apt/lists/*

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

ARG XRAY_PROTO_REF=main
RUN set -eux; \
    mkdir -p /srv/proto; \
    curl -fsSL "https://github.com/XTLS/Xray-core/archive/refs/heads/${XRAY_PROTO_REF}.tar.gz" \
      | tar -xz --strip-components=1 -C /srv/proto

RUN test -f /srv/proto/app/stats/command/command.proto && test -f /srv/proto/app/proxyman/command/command.proto

COPY app /srv/app
COPY worker.py /srv/worker.py
COPY xrayproto /srv/xrayproto

ENV PYTHONPATH="/srv:${PYTHONPATH}"

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
