FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv

RUN apt-get update && apt-get install -y \
    curl ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*

# grpcurl static binary
RUN curl -L https://github.com/fullstorydev/grpcurl/releases/latest/download/grpcurl_linux_x86_64 \
    -o /usr/local/bin/grpcurl \
    && chmod +x /usr/local/bin/grpcurl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# копируем новый пакет app/ и worker.py
COPY app ./app
COPY worker.py ./worker.py

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000"]
