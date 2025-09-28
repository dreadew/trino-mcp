from typing import Any, Dict, List

from src.core import ddl_analyzer
from src.core.logging import get_logger

logger = get_logger(__name__)


async def validate_ddl_statements(ddl_list: List[str]) -> Dict[str, Any]:
    """
    Анализирует и валидирует список DDL выражений.

    :param ddl_list: Список DDL выражений для анализа
    :return: Результаты анализа DDL
    """
    try:
        return ddl_analyzer.analyze_ddl_list(ddl_list)
    except Exception as e:
        logger.error(f"Error analyzing DDL statements: {e}")
        return {
            "error": str(e),
            "total_statements": len(ddl_list) if ddl_list else 0,
        }
