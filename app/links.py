import uuid


from app.config import settings
from app.xray import add_client


def build_vless_link(user_uuid: str, email: str, flow: str) -> str:
    # Проверки на env
    if not settings.public_host or not settings.reality_sni or not settings.reality_pbk or not settings.reality_sid:
        raise RuntimeError("Missing link params in env (PUBLIC_HOST/REALITY_*).")

    return (
        f"vless://{user_uuid}@{settings.public_host}:{settings.public_port}"
        f"?encryption=none"
        f"&flow={flow}"
        f"&security=reality"
        f"&sni={settings.reality_sni}"
        f"&fp={settings.reality_fp}"
        f"&pbk={settings.reality_pbk}"
        f"&sid={settings.reality_sid}"
        f"&type=tcp"
        f"#VPN-{email}"
    )

def handle_issue_client(job: dict) -> dict:
    p = job["payload"]
    email = str(p["telegram_id"]).strip()
    inbound_tag = p.get("inbound_tag") or settings.default_inbound_tag
    level = int(p.get("level", 0))
    flow = p.get("flow") or settings.default_flow

    user_uuid = str(uuid.uuid4())

    # add to xray
    try:
        add_client(user_uuid, email, inbound_tag, level, flow)
    except Exception as e:
        msg = str(e)
        # если хочешь upsert:
        # if "already exists" in msg: remove_client(email, inbound_tag); add_client(...)
        raise

    link = build_vless_link(user_uuid, email, flow)

    payload_to_notify = {
        "uuid": user_uuid,
        "email": email,
        "inbound_tag": inbound_tag,
        "link": link,
    }

    notify_external(payload_to_notify)  # POST на другой сервер /v1/notify

    return payload_to_notify  # сохраним в result job-а
