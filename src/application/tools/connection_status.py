from typing import Any, Dict

from src.core.logging import get_logger
from src.infra import connection_manager

logger = get_logger(__name__)


async def connection_status(jdbc_url: str) -> Dict[str, Any]:
    """
    Проверяет статус подключения к Trino.

    :param jdbc_url: JDBC URL для подключения к Trino
    :return: Статус подключения и информация о сервере
    """
    try:
        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]

            cursor.execute("SELECT current_user")
            user = cursor.fetchone()[0]

            cursor.execute("SELECT current_catalog")
            catalog = cursor.fetchone()[0]

            cursor.execute("SELECT current_schema")
            schema = cursor.fetchone()[0]

            return {
                "status": "connected",
                "version": version,
                "user": user,
                "catalog": catalog,
                "schema": schema,
            }
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return {"status": "failed", "error": str(e)}
