from typing import Any, Dict

from trino.exceptions import TrinoUserError

from src.core.logging import get_logger
from src.infra import connection_manager

logger = get_logger(__name__)


async def list_catalogs(jdbc_url: str) -> Dict[str, Any]:
    """
    Возвращает список всех доступных каталогов.

    :param jdbc_url: JDBC URL для подключения к Trino
    :return: Список каталогов
    """
    try:
        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cursor.fetchall()]

            return {"catalogs": catalogs, "count": len(catalogs)}
    except TrinoUserError as e:
        logger.error(f"Error listing catalogs: {e}")
        return {"error": str(e), "catalogs": []}
