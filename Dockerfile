FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv

# базовые пакеты
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash tar protobuf-compiler findutils \
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
    /usr/local/bin/grpcurl -version

# --- Xray proto (нужно для grpcurl без reflection) ---
ARG XRAY_PROTO_REF=main
RUN set -eux; \
    mkdir -p /srv/proto; \
    curl -fsSL "https://github.com/XTLS/Xray-core/archive/refs/heads/${XRAY_PROTO_REF}.tar.gz" \
      | tar -xz --strip-components=1 -C /srv/proto

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# generate python protobuf modules into /srv/gen
RUN mkdir -p /srv/gen && \
    python -m grpc_tools.protoc -I/srv/proto \
      --python_out=/srv/gen \
      --grpc_python_out=/srv/gen \
      $(find /srv/proto -name "*.proto")

ENV PYTHONPATH="/srv/gen:${PYTHONPATH}"

COPY app ./app
COPY worker.py ./worker.py

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
