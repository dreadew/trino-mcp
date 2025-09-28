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

    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8005))


config = Config()
