from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_token: str = Field(default="CHANGE_ME", alias="API_TOKEN")

    redis_url: str = Field(default="redis://127.0.0.1:6379/0", alias="REDIS_URL")
    xray_api_addr: str = Field(default="127.0.0.1:10085", alias="XRAY_API_ADDR")
    default_inbound_tag: str = Field(default="vless-in", alias="XRAY_INBOUND_TAG")

    proto_root: str = Field(default="/srv/proto", alias="XRAY_PROTO_ROOT")


settings = Settings()
