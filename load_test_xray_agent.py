import asyncio
import random
import string
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import httpx


@dataclass
class Stats:
    ok: int = 0
    fail: int = 0
    latencies_ms: List[float] = None

    def __post_init__(self):
        if self.latencies_ms is None:
            self.latencies_ms = []

    def add(self, ok: bool, latency_ms: float):
        if ok:
            self.ok += 1
        else:
            self.fail += 1
        self.latencies_ms.append(latency_ms)

    def summary(self) -> Dict[str, Any]:
        if not self.latencies_ms:
            return {"ok": self.ok, "fail": self.fail}
        l = sorted(self.latencies_ms)
        def pct(p: float) -> float:
            idx = int((p / 100.0) * (len(l) - 1))
            return l[idx]
        return {
            "ok": self.ok,
            "fail": self.fail,
            "avg_ms": sum(l) / len(l),
            "p50_ms": pct(50),
            "p90_ms": pct(90),
            "p99_ms": pct(99),
            "min_ms": l[0],
            "max_ms": l[-1],
        }


def rand_email(prefix: str = "lt") -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}-{suffix}"


class XrayAgentClient:
    def __init__(self, base_url: str, token: str, timeout: float = 20.0):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.timeout = timeout

    async def _req(self, client: httpx.AsyncClient, method: str, path: str, **kwargs) -> Tuple[bool, float, Any]:
        t0 = time.perf_counter()
        try:
            r = await client.request(
                method,
                f"{self.base_url}{path}",
                headers={**self.headers, **kwargs.pop("headers", {})},
                timeout=self.timeout,
                **kwargs,
            )
            dt = (time.perf_counter() - t0) * 1000
            if r.status_code >= 400:
                return False, dt, {"status": r.status_code, "body": r.text}
            return True, dt, r.json() if r.text else {}
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000
            return False, dt, {"exc": repr(e)}

    async def health_full(self, client: httpx.AsyncClient) -> Dict[str, Any]:
        ok, _, data = await self._req(client, "GET", "/health/full")
        if not ok:
            raise RuntimeError(f"/health/full failed: {data}")
        return data

    async def inbound_count(self, client: httpx.AsyncClient, tag: str) -> int:
        ok, _, data = await self._req(client, "GET", f"/inbounds/{tag}/users/count")
        print(ok, _, data)
        if not ok:
            raise RuntimeError(f"/inbounds/{tag}/users/count failed: {data}")
        # ожидаем {"result": {"count":"1"}} либо {"result":{"count":1}}
        count_val = data.get("result", {}).get("count")
        return int(count_val)

    async def inbound_emails(self, client: httpx.AsyncClient, tag: str) -> List[str]:
        ok, _, data = await self._req(client, "GET", f"/inbounds/{tag}/emails")
        if not ok:
            raise RuntimeError(f"/inbounds/{tag}/emails failed: {data}")
        return list(data.get("result") or [])

    async def inbound_uuids(self, client: httpx.AsyncClient, tag: str) -> List[str]:
        ok, _, data = await self._req(client, "GET", f"/inbounds/{tag}/uuids")
        if not ok:
            raise RuntimeError(f"/inbounds/{tag}/uuids failed: {data}")
        return list(data.get("result") or [])

    async def add_client(self, client: httpx.AsyncClient, inbound_tag: str, u: str, email: str, async_: bool) -> Any:
        payload = {"uuid": u, "email": email, "inbound_tag": inbound_tag}
        path = "/clients" + ("?async=true" if async_ else "")
        ok, _, data = await self._req(client, "POST", path, json=payload)
        if not ok:
            raise RuntimeError(f"add_client failed: {data}")
        return data

    async def remove_client(self, client: httpx.AsyncClient, inbound_tag: str, email: str, async_: bool) -> Any:
        path = f"/clients/{email}?inbound_tag={inbound_tag}" + ("&async=true" if async_ else "")
        ok, _, data = await self._req(client, "DELETE", path)
        if not ok:
            raise RuntimeError(f"remove_client failed: {data}")
        return data

    async def job_get(self, client: httpx.AsyncClient, job_id: str) -> Dict[str, Any]:
        ok, _, data = await self._req(client, "GET", f"/jobs/{job_id}")
        if not ok:
            raise RuntimeError(f"job_get failed: {data}")
        return data


async def wait_jobs_done(
    api: XrayAgentClient,
    client: httpx.AsyncClient,
    job_ids: List[str],
    *,
    poll_interval: float = 0.25,
    timeout_s: float = 60.0,
) -> Tuple[int, int]:
    """
    Ждём завершения списка job_id.
    Возвращает: (done, error)
    """
    pending = set(job_ids)
    done = 0
    err = 0
    t0 = time.perf_counter()

    while pending:
        if (time.perf_counter() - t0) > timeout_s:
            raise TimeoutError(f"jobs timeout; still pending={len(pending)}")

        # опрашиваем пачкой
        batch = list(pending)[:200]
        results = await asyncio.gather(*(api.job_get(client, jid) for jid in batch), return_exceptions=True)

        for jid, res in zip(batch, results):
            if isinstance(res, Exception):
                # временная ошибка — оставим pending
                continue
            state = res.get("state")
            if state in ("done", "error"):
                pending.remove(jid)
                if state == "done":
                    done += 1
                else:
                    err += 1

        if pending:
            await asyncio.sleep(poll_interval)

    return done, err


async def load_phase(
    api: XrayAgentClient,
    inbound_tag: str,
    *,
    total: int,
    concurrency: int,
    mode: str,  # "sync" or "async"
    action: str,  # "add" or "remove"
    emails: List[str],
) -> Dict[str, Any]:
    """
    Выполняет фазу add/remove в sync или async режиме.
    Возвращает статистику и job-результаты.
    """
    async_mode = (mode == "async")
    sem = asyncio.Semaphore(concurrency)
    stats = Stats()
    created_job_ids: List[str] = []

    async with httpx.AsyncClient() as client:

        async def one(i: int):
            async with sem:
                email = emails[i]
                u = str(uuid.uuid4())
                t0 = time.perf_counter()
                try:
                    if action == "add":
                        resp = await api.add_client(client, inbound_tag, u, email, async_mode)
                    else:
                        resp = await api.remove_client(client, inbound_tag, email, async_mode)

                    dt = (time.perf_counter() - t0) * 1000

                    if async_mode:
                        job_id = resp.get("job_id")
                        if not job_id:
                            raise RuntimeError(f"async response missing job_id: {resp}")
                        created_job_ids.append(job_id)

                    stats.add(True, dt)
                except Exception:
                    dt = (time.perf_counter() - t0) * 1000
                    stats.add(False, dt)

        tasks = [asyncio.create_task(one(i)) for i in range(total)]
        await asyncio.gather(*tasks)

        job_done = job_err = 0
        if async_mode and created_job_ids:
            job_done, job_err = await wait_jobs_done(api, client, created_job_ids, timeout_s=180.0)

        return {
            "action": action,
            "mode": mode,
            "total": total,
            "concurrency": concurrency,
            "request_stats": stats.summary(),
            "jobs": {"created": len(created_job_ids), "done": job_done, "error": job_err} if async_mode else None,
        }


async def main():
    # === НАСТРОЙКИ ===
    BASE_URL = "http://194.135.33.76:18000"
    TOKEN = "CHANGE_ME_TO_LONG_RANDOM_SECRET_64_CHARS"
    INBOUND_TAG = "vless-in"

    # нагрузка
    TOTAL = 200          # сколько клиентов добавить/удалить
    CONCURRENCY = 40     # параллельность
    MODE = "async"       # "sync" или "async"
    PREFIX = "lt"        # префикс email чтобы можно было фильтровать

    api = XrayAgentClient(BASE_URL, TOKEN)

    async with httpx.AsyncClient() as client:
        # 0) health
        health = await api.health_full(client)
        print("HEALTH:", health.get("ok"), health.get("xray", {}).get("xray_api_port_open"))

        before = await api.inbound_count(client, INBOUND_TAG)
        print("COUNT BEFORE:", before)

    # 1) подготовим emails
    emails = [rand_email(PREFIX) for _ in range(TOTAL)]

    # 2) ADD phase
    t0 = time.perf_counter()
    res_add = await load_phase(api, INBOUND_TAG, total=TOTAL, concurrency=CONCURRENCY, mode=MODE, action="add", emails=emails)
    t_add = time.perf_counter() - t0
    print("\nADD RESULT:", res_add)
    print(f"ADD wall time: {t_add:.2f}s")

    # 3) Verify
    async with httpx.AsyncClient() as client:
        after_add = await api.inbound_count(client, INBOUND_TAG)
        print("COUNT AFTER ADD:", after_add)

        # быстрый sanity: убедимся что часть emails/uuids есть
        in_emails = await api.inbound_emails(client, INBOUND_TAG)
        in_uuids = await api.inbound_uuids(client, INBOUND_TAG)
        print(f"EMAILS total on inbound: {len(in_emails)} (sample contains test prefix={PREFIX}: {sum(e.startswith(PREFIX+'-') for e in in_emails)})")
        print(f"UUIDS total on inbound: {len(in_uuids)}")

    # 4) REMOVE phase
    t0 = time.perf_counter()
    res_del = await load_phase(api, INBOUND_TAG, total=TOTAL, concurrency=CONCURRENCY, mode=MODE, action="remove", emails=emails)
    t_del = time.perf_counter() - t0
    print("\nDEL RESULT:", res_del)
    print(f"DEL wall time: {t_del:.2f}s")

    # 5) Verify end
    async with httpx.AsyncClient() as client:
        after_del = await api.inbound_count(client, INBOUND_TAG)
        print("COUNT AFTER DEL:", after_del)

    print("\nDONE.")


if __name__ == "__main__":
    asyncio.run(main())
