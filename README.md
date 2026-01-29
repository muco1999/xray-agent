# Xray Agent

Xray Agent — это лёгкий HTTP API, который управляет Xray через gRPC API **без SSH**:
- status/health
- добавление/удаление VLESS пользователей в inbound (AlterInbound)
- получение списка пользователей, email, uuid
- асинхронные задачи через Redis очередь (optional)

## Security
Все эндпоинты защищены токеном:
- Header: `Authorization: Bearer <API_TOKEN>`

Рекомендации:
- генерируй длинный токен: `openssl rand -hex 32`
- ограничь доступ к порту firewall’ом (разрешить только свой IP/VPN)

## ENV (.env)
Пример:
```env
API_TOKEN=CHANGE_ME_TO_LONG_RANDOM_SECRET_64_CHARS
XRAY_API_ADDR=127.0.0.1:10085
XRAY_INBOUND_TAG=vless-in
XRAY_PROTO_ROOT=/srv/proto
REDIS_URL=redis://127.0.0.1:6379/0
