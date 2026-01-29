import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    xray_api_addr: str = os.getenv("XRAY_API_ADDR", "127.0.0.1:10085")  # host:port
    xray_service: str = os.getenv("XRAY_SERVICE", "xray")
    default_inbound_tag: str = os.getenv("XRAY_INBOUND_TAG", "vless-in")
    api_token: str = os.getenv("API_TOKEN", "")

settings = Settings()

if not settings.api_token:
    raise RuntimeError("API_TOKEN is not set. Refusing to start without authentication.")
