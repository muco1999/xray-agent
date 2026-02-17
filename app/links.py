from app.settings import settings



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

