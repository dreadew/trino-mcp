from typing import Any, Dict, Optional

from trino.exceptions import TrinoUserError

from src.core.logging import get_logger
from src.infra import connection_manager

logger = get_logger(__name__)


async def list_schemas(jdbc_url: str, catalog: Optional[str] = None) -> Dict[str, Any]:
    """
    Возвращает список схем в указанном каталоге.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param catalog: Название каталога (опционально)
    :return: Список схем
    """
    try:
        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            if catalog:
                cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
            else:
                cursor.execute("SHOW SCHEMAS")

            schemas = [row[0] for row in cursor.fetchall()]

            return {"catalog": catalog, "schemas": schemas, "count": len(schemas)}
    except TrinoUserError as e:
        logger.error(f"Error listing schemas: {e}")
        return {"error": str(e), "catalog": catalog, "schemas": []}
