from typing import Any, Dict, Optional

from trino.exceptions import TrinoUserError

from src.core.logging import get_logger
from src.core.utils.validate import validate_identifier
from src.infra import connection_manager

logger = get_logger(__name__)


async def describe_table(
    jdbc_url: str, table: str, schema: str, catalog: Optional[str] = None
) -> Dict[str, Any]:
    """
    Возвращает описание структуры таблицы.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param table: Название таблицы
    :param schema: Название схемы
    :param catalog: Название каталога (опционально)
    :return: Описание таблицы и ее колонок
    """
    try:
        if not all(validate_identifier(name) for name in [table, schema]):
            return {"error": "Invalid table or schema name", "columns": []}

        if catalog and not validate_identifier(catalog):
            return {"error": "Invalid catalog name", "columns": []}

        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            table_path = (
                f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"
            )
            cursor.execute(f"DESCRIBE {table_path}")

            columns = []
            for row in cursor.fetchall():
                columns.append(
                    {
                        "name": row[0],
                        "type": row[1],
                        "null": row[2] if len(row) > 2 else None,
                        "key": row[3] if len(row) > 3 else None,
                        "default": row[4] if len(row) > 4 else None,
                        "extra": row[5] if len(row) > 5 else None,
                    }
                )

            return {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "columns": columns,
                "column_count": len(columns),
            }
    except TrinoUserError as e:
        logger.error(f"Error describing table {table}: {e}")
        return {
            "error": str(e),
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "columns": [],
        }
