from typing import Any, Dict, Optional

from trino.exceptions import TrinoUserError

from src.core.logging import get_logger
from src.core.utils.validate import validate_identifier
from src.infra import connection_manager

logger = get_logger(__name__)


async def list_tables(
    jdbc_url: str, schema: str, catalog: Optional[str] = None
) -> Dict[str, Any]:
    """
    Возвращает список таблиц в указанной схеме.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param schema: Название схемы
    :param catalog: Название каталога (опционально)
    :return: Список таблиц
    """
    try:
        if not validate_identifier(schema):
            return {"error": "Invalid schema name", "tables": []}

        if catalog and not validate_identifier(catalog):
            return {"error": "Invalid catalog name", "tables": []}

        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            if catalog:
                cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}")
            else:
                cursor.execute(f"SHOW TABLES FROM {schema}")

            tables = []
            for row in cursor.fetchall():
                tables.append(
                    {"name": row[0], "type": row[1] if len(row) > 1 else "TABLE"}
                )

            return {
                "catalog": catalog,
                "schema": schema,
                "tables": tables,
                "count": len(tables),
            }
    except TrinoUserError as e:
        logger.error(f"Error listing tables: {e}")
        return {"error": str(e), "catalog": catalog, "schema": schema, "tables": []}
