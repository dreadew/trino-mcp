from typing import Any, Dict, Optional

from src.core.logging import get_logger
from src.core.utils.validate import validate_identifier
from src.infra import connection_manager

logger = get_logger(__name__)


async def execute_query(
    jdbc_url: str,
    sql: str,
    limit: int = 100,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Выполняет SQL запрос с ограничением на количество строк.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param sql: SQL запрос для выполнения
    :param limit: Максимальное количество строк для возврата
    :param catalog: Каталог по умолчанию
    :param schema: Схема по умолчанию
    :return: Результат выполнения запроса
    """
    try:
        limit = min(limit, 1000)

        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            if catalog:
                if validate_identifier(catalog):
                    cursor.execute(f"USE {catalog}")
                else:
                    return {"error": "Invalid catalog name"}

            if schema:
                if validate_identifier(schema):
                    cursor.execute(
                        f"USE {catalog}.{schema}" if catalog else f"USE {schema}"
                    )
                else:
                    return {"error": "Invalid schema name"}

            cursor.execute(sql)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            rows = cursor.fetchmany(limit)

            return {
                "sql": sql,
                "columns": columns,
                "rows": [list(row) for row in rows],
                "row_count": len(rows),
                "limited": len(rows) == limit,
                "catalog": catalog,
                "schema": schema,
            }
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return {"error": str(e), "sql": sql}
