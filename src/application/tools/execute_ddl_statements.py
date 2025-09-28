from typing import Any, Dict, List, Optional

from src.core import ddl_analyzer
from src.core.logging import get_logger
from src.core.utils.validate import validate_identifier
from src.infra import connection_manager

logger = get_logger(__name__)


async def execute_ddl_statements(
    jdbc_url: str,
    ddl_list: List[str],
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    validate_first: bool = True,
) -> Dict[str, Any]:
    """
    Выполняет список DDL выражений с предварительной валидацией.

    :param jdbc_url: JDBC URL для подключения к Trino
    :param ddl_list: Список DDL выражений для выполнения
    :param catalog: Каталог по умолчанию
    :param schema: Схема по умолчанию
    :param validate_first: Выполнить валидацию перед выполнением
    :return: Результаты выполнения DDL
    """
    try:
        results = {
            "total_statements": len(ddl_list),
            "validation": None,
            "execution_results": [],
            "success_count": 0,
            "error_count": 0,
        }

        if validate_first:
            validation_result = ddl_analyzer.analyze_ddl_list(ddl_list)
            results["validation"] = validation_result

            high_severity_issues = [
                issue
                for issue in validation_result.get("potential_issues", [])
                if issue.get("severity") == "high"
            ]

            if high_severity_issues:
                return {
                    **results,
                    "error": "Обнаружены критические проблемы в DDL. Выполнение остановлено.",
                    "critical_issues": high_severity_issues,
                }

        with connection_manager.get_connection(jdbc_url) as conn:
            cursor = conn.cursor()

            if catalog and validate_identifier(catalog):
                cursor.execute(f"USE {catalog}")
            if schema and validate_identifier(schema):
                schema_path = f"{catalog}.{schema}" if catalog else schema
                cursor.execute(f"USE {schema_path}")

            for i, ddl in enumerate(ddl_list):
                if not ddl or not ddl.strip():
                    continue

                try:
                    cursor.execute(ddl)
                    object_name = ddl_analyzer.extract_object_name(ddl)
                    ddl_type = ddl_analyzer.identify_ddl_type(ddl)

                    results["execution_results"].append(
                        {
                            "index": i,
                            "status": "success",
                            "ddl_type": ddl_type.value,
                            "object_name": object_name,
                            "ddl_preview": (
                                ddl[:100] + "..." if len(ddl) > 100 else ddl
                            ),
                        }
                    )
                    results["success_count"] += 1

                except Exception as e:
                    results["execution_results"].append(
                        {
                            "index": i,
                            "status": "error",
                            "error": str(e),
                            "ddl_preview": ddl[:100] + "..." if len(ddl) > 100 else ddl,
                        }
                    )
                    results["error_count"] += 1

        return results

    except Exception as e:
        logger.error(f"Error executing DDL statements: {e}")
        return {"error": str(e), "total_statements": len(ddl_list) if ddl_list else 0}
