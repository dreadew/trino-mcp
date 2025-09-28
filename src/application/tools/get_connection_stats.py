from typing import Any, Dict

from src.infra import connection_manager


async def get_connection_stats() -> Dict[str, Any]:
    """
    Возвращает статистику активных подключений.

    :return: Статистика подключений
    """
    return connection_manager.get_stats()
