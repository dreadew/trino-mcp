import hashlib
import time
from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict

from trino.dbapi import connect

from src.core.logging import get_logger
from src.core.utils.parse import parse_trino_jdbc

logger = get_logger(__name__)


class ConnectionManager:
    """Менеджер для управления множественными подключениями к Trino."""

    def __init__(self, max_connections: int = 10, connection_ttl: int = 3600):
        self._connections: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        self._max_connections = max_connections
        self._connection_ttl = connection_ttl

    def _generate_connection_key(self, jdbc_url: str) -> str:
        """Генерирует уникальный ключ для JDBC URL."""
        return hashlib.sha256(jdbc_url.encode()).hexdigest()[:16]

    def _cleanup_expired_connections(self):
        """Удаляет истекшие соединения."""
        current_time = time.time()
        expired_keys = []

        for key, conn_info in self._connections.items():
            if current_time - conn_info["created_at"] > self._connection_ttl:
                expired_keys.append(key)

        for key in expired_keys:
            try:
                self._connections[key]["connection"].close()
            except Exception as e:
                logger.warning(f"Error closing expired connection {key}: {e}")
            finally:
                del self._connections[key]

    def _create_connection(self, jdbc_url: str):
        """
        Создает новое подключение к Trino.
        :param jdbc_url: JDBC URL для подключения к Trino
        :return: Объект подключения к Trino"""
        try:
            conn_params = parse_trino_jdbc(jdbc_url)

            connect_params = {
                "host": conn_params["host"],
                "port": conn_params["port"],
                "user": conn_params["user"],
            }

            if conn_params.get("password"):
                connect_params["password"] = conn_params["password"]
            if conn_params.get("catalog"):
                connect_params["catalog"] = conn_params["catalog"]
            if conn_params.get("schema"):
                connect_params["schema"] = conn_params["schema"]

            connection = connect(**connect_params)
            logger.info(
                f"Created new Trino connection to {conn_params['host']}:{conn_params['port']}"
            )
            return connection

        except Exception as e:
            logger.error(f"Failed to create connection with JDBC URL {jdbc_url}: {e}")
            raise

    @contextmanager
    def get_connection(self, jdbc_url: str):
        """
        Контекстный менеджер для получения подключения.

        :param jdbc_url: JDBC URL для подключения к Trino
        :yields: connection: Объект подключения к Trino
        """
        connection_key = self._generate_connection_key(jdbc_url)

        with self._lock:
            self._cleanup_expired_connections()

            if connection_key in self._connections:
                conn_info = self._connections[connection_key]
                try:
                    cursor = conn_info["connection"].cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchall()

                    yield conn_info["connection"]
                    return
                except Exception as e:
                    logger.warning(f"Cached connection {connection_key} is stale: {e}")
                    try:
                        conn_info["connection"].close()
                    except Exception:
                        pass
                    del self._connections[connection_key]

            if len(self._connections) >= self._max_connections:
                oldest_key = min(
                    self._connections.keys(),
                    key=lambda k: self._connections[k]["created_at"],
                )
                try:
                    self._connections[oldest_key]["connection"].close()
                except Exception:
                    pass
                del self._connections[oldest_key]

            connection = self._create_connection(jdbc_url)
            self._connections[connection_key] = {
                "connection": connection,
                "created_at": time.time(),
                "jdbc_url": jdbc_url,
            }

        try:
            yield connection
        except Exception as e:
            logger.error(f"Error using connection {connection_key}: {e}")
            raise

    def close_all(self):
        """Закрывает все кешированные соединения."""
        with self._lock:
            for conn_info in self._connections.values():
                try:
                    conn_info["connection"].close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            self._connections.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику по подключениям."""
        with self._lock:
            return {
                "active_connections": len(self._connections),
                "max_connections": self._max_connections,
                "connection_ttl": self._connection_ttl,
                "connections": [
                    {
                        "key": key[:8] + "...",
                        "created_at": info["created_at"],
                        "age_seconds": int(time.time() - info["created_at"]),
                        "host": parse_trino_jdbc(info["jdbc_url"])["host"],
                    }
                    for key, info in self._connections.items()
                ],
            }


connection_manager = ConnectionManager()
