import os

from dotenv import load_dotenv

load_dotenv()
if os.path.exists(".env.local"):
    load_dotenv(".env.local", override=True)


class Config:
    """
    Конфигурация приложения
    """

    APP_NAME = os.getenv("APP_NAME", "SQL RecSys")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    DEFAULT_MODE = os.getenv("DEFAULT_MODE", "http")
    DEFAULT_HTTP_PORT = int(os.getenv("DEFAULT_HTTP_PORT", 8000))
    DEFAULT_HTTP_HOST = os.getenv("DEFAULT_HTTP_HOST", "0.0.0.0")


config = Config()
