from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field




class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,   # чтобы можно было и по имени, и по alias
    )

    api_token: str = Field(default="CHANGE_ME", alias="API_TOKEN")

    redis_url: str = Field(default="redis://127.0.0.1:6379/0", alias="REDIS_URL")
    xray_api_addr: str = Field(default="127.0.0.1:10085", alias="XRAY_API_ADDR")
    default_inbound_tag: str = Field(default="vless-in", alias="XRAY_INBOUND_TAG")

    proto_root: str = Field(default="/srv/proto", alias="XRAY_PROTO_ROOT")

    # Параметры для генерации ссылки (из .env)
    public_host: str | None = Field(default=None, alias="PUBLIC_HOST")
    public_port: int = Field(default=443, alias="PUBLIC_PORT")

    reality_sni: str | None = Field(default=None, alias="REALITY_SNI")
    reality_fp: str = Field(default="chrome", alias="REALITY_FP")
    reality_pbk: str | None = Field(default=None, alias="REALITY_PBK")
    reality_sid: str | None = Field(default=None, alias="REALITY_SID")

    default_flow: str = Field(default="", alias="DEFAULT_FLOW")

    notify_url: str | None = Field(default=None, alias="NOTIFY_URL")
    notify_api_key: str | None = Field(default=None, alias="NOTIFY_API_KEY")
    notify_timeout_sec: int = Field(default=10, alias="NOTIFY_TIMEOUT_SEC")
    notify_retries: int = Field(default=3, alias="NOTIFY_RETRIES")



























settings = Settings()
