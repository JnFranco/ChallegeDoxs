from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/transactions_db"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"

    model_config = {"env_file": ".env"}


settings = Settings()
