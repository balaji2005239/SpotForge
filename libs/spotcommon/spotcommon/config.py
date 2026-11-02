from pydantic_settings import BaseSettings, SettingsConfigDict

GPU_TYPES = ("T4", "A100", "H100")

# Per-market reserve price floor (micro-USD per GPU-hour).
RESERVE_MICRO = {"T4": 100_000, "A100": 800_000, "H100": 1_500_000}


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    tick_seconds: int = 5
    lease_seconds: int = 15  # ticker leader lease; a dead leader is replaced after this long
    grace_seconds: int = 30  # notice before an outbid job is preempted (0 = immediate)
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap: str = "localhost:9092"
