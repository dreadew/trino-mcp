from typing import Any, Dict, List, Optional

from src.core.logging import get_logger
from src.core.utils.validate import validate_identifier
from src.infra import connection_manager

logger = get_logger(__name__)


async def analyze_queries(
    jdbc_url: str,
    queries: List[str],
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Анализирует список SQL запросов без их выполнения.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param queries: Список SQL запросов для анализа
    :param catalog: Каталог по умолчанию
    :param schema: Схема по умолчанию
    :return: Результаты анализа запросов
    """
    try:
        results = []

        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            if catalog and validate_identifier(catalog):
                cursor.execute(f"USE {catalog}")
            if schema and validate_identifier(schema):
                cursor.execute(
                    f"USE {catalog}.{schema}" if catalog else f"USE {schema}"
                )

            for i, sql in enumerate(queries):
                try:
                    cursor.execute(f"EXPLAIN {sql}")
                    plan = cursor.fetchall()

                    results.append(
                        {
                            "query_index": i,
                            "sql": sql,
                            "status": "valid",
                            "plan": [row[0] for row in plan],
                        }
                    )
                except Exception as e:
                    results.append(
                        {
                            "query_index": i,
                            "sql": sql,
                            "status": "invalid",
                            "error": str(e),
                        }
                    )

        return {
            "total_queries": len(queries),
            "valid_queries": sum(1 for r in results if r["status"] == "valid"),
            "invalid_queries": sum(1 for r in results if r["status"] == "invalid"),
            "results": results,
        }

    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return {"error": str(e), "total_queries": len(queries)}
