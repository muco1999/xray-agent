import time
import pytest
from httpx import AsyncClient

from app.main import xray_status as mod

# ⚠️ поменяй импорт на свой модуль, где лежат эндпоинты


@pytest.mark.asyncio
async def test_health_journal_ok(monkeypatch):
    async def fake_read_journalctl_json(unit: str, since_seconds: int, max_lines=200, timeout_sec=3.0):
        # Возвращаем хоть один journald entry
        return [{
            "__REALTIME_TIMESTAMP": str(int(time.time() * 1_000_000)),
            "MESSAGE": "dummy"
        }]

    monkeypatch.setattr(mod, "read_journalctl_json", fake_read_journalctl_json)

    async with AsyncClient(app=mod.app, base_url="http://test") as ac:
        r = await ac.get("/health/journal")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True


@pytest.mark.asyncio
async def test_status_clients_aggregates(monkeypatch):
    now = time.time()

    # 3 события: user1 с 2 IP, user2 с 1 IP
    journald_entries = [
        {
            "__REALTIME_TIMESTAMP": str(int((now - 10) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:31.289090 from 1.1.1.1:11111 accepted tcp:www.youtube.com:443 [vless-in -> direct] email: 1001"
        },
        {
            "__REALTIME_TIMESTAMP": str(int((now - 5) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:35.289090 from 2.2.2.2:22222 accepted tcp:www.google.com:443 [vless-in -> direct] email: 1001"
        },
        {
            "__REALTIME_TIMESTAMP": str(int((now - 20) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:10.289090 from 3.3.3.3:33333 accepted tcp:api.telegram.org:443 [vless-in -> direct] email: 2002"
        },
    ]

    async def fake_read_journalctl_json(unit: str, since_seconds: int, max_lines=25000, timeout_sec=8.0):
        return journald_entries

    async def fake_get_established_443_count():
        return 7

    # Мокаем I/O
    monkeypatch.setattr(mod, "read_journalctl_json", fake_read_journalctl_json)
    monkeypatch.setattr(mod, "get_established_443_count", fake_get_established_443_count)

    # Убираем влияние кэша
    mod._STATUS_CACHE.ts = 0.0
    mod._STATUS_CACHE.value = None

    async with AsyncClient(app=mod.app, base_url="http://test") as ac:
        r = await ac.get("/xray/status/clients")
        assert r.status_code == 200
        data = r.json()

    assert data["ok"] is True
    assert data["clients_total_seen"] == 2
    assert data["window_events"] == 3
    assert data["connections_established_443"] == 7

    # client 1001
    c1001 = next(x for x in data["clients"] if x["email"] == "1001")
    assert c1001["devices_estimate"] == 2
    assert sorted(c1001["unique_ips"]) == ["1.1.1.1", "2.2.2.2"]
    assert c1001["suspicious"] is False  # по умолчанию DEVICES_LIMIT=2 => 2 не подозрительно

    # client 2002
    c2002 = next(x for x in data["clients"] if x["email"] == "2002")
    assert c2002["devices_estimate"] == 1
    assert c2002["unique_ips"] == ["3.3.3.3"]


@pytest.mark.asyncio
async def test_suspicious_when_over_limit(monkeypatch):
    now = time.time()

    journald_entries = [
        {
            "__REALTIME_TIMESTAMP": str(int((now - 1) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:31.289090 from 1.1.1.1:11111 accepted tcp:a.com:443 [vless-in -> direct] email: 3003"
        },
        {
            "__REALTIME_TIMESTAMP": str(int((now - 1) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:32.289090 from 2.2.2.2:22222 accepted tcp:b.com:443 [vless-in -> direct] email: 3003"
        },
        {
            "__REALTIME_TIMESTAMP": str(int((now - 1) * 1_000_000)),
            "MESSAGE": "2026/02/06 11:52:33.289090 from 3.3.3.3:33333 accepted tcp:c.com:443 [vless-in -> direct] email: 3003"
        },
    ]

    async def fake_read_journalctl_json(unit: str, since_seconds: int, max_lines=25000, timeout_sec=8.0):
        return journald_entries

    async def fake_get_established_443_count():
        return 0

    monkeypatch.setattr(mod, "read_journalctl_json", fake_read_journalctl_json)
    monkeypatch.setattr(mod, "get_established_443_count", fake_get_established_443_count)

    # Ставим лимит 2, а IP будет 3 => suspicious True
    monkeypatch.setattr(mod, "DEVICES_LIMIT", 2)

    mod._STATUS_CACHE.ts = 0.0
    mod._STATUS_CACHE.value = None

    async with AsyncClient(app=mod.app, base_url="http://test") as ac:
        r = await ac.get("/xray/status/clients")
        data = r.json()

    c = next(x for x in data["clients"] if x["email"] == "3003")
    assert c["devices_estimate"] == 3
    assert c["suspicious"] is True


@pytest.mark.asyncio
async def test_health_journal_unavailable(monkeypatch):
    async def fake_read_journalctl_json(*args, **kwargs):
        raise RuntimeError("permission denied")

    monkeypatch.setattr(mod, "read_journalctl_json", fake_read_journalctl_json)

    async with AsyncClient(app=mod.app, base_url="http://test") as ac:
        r = await ac.get("/health/journal")
        assert r.status_code == 503
        assert "journald unavailable" in r.text