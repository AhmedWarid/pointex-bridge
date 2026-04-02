from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    saveurs_path: str = r"\\CAISSE-PC\pointex21\LATELIER"
    api_port: int = 8470
    api_host: str = "0.0.0.0"
    api_key: str = "change-me-to-a-secret-key"
    cors_origins: list[str] = ["*"]  # Set specific origins in .env for production
    max_sales_range_days: int = 31  # Max date range for sales queries
    business_day_start_hour: int = 4
    business_day_end_hour: int = 22
    timezone: str = "Africa/Casablanca"
    log_level: str = "INFO"
    log_file: str = "bridge.log"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
