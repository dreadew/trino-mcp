from typing import Optional

from src.application.tools import (
    connection_status,
    describe_table,
    execute_ddl_statements,
    execute_query,
    get_connection_stats,
    list_catalogs,
    list_schemas,
    list_tables,
    validate_ddl_statements,
)
from src.core.logging import get_logger

logger = get_logger(__name__)


def register_tools(mcp_server):
    """
    Регистрирует все инструменты для работы с Trino в FastMCP сервере.
    """

    @mcp_server.tool()
    async def connection_status_tool(jdbc_url: str) -> str:
        """Проверяет статус подключения к Trino."""
        try:
            result = connection_status(jdbc_url=jdbc_url)
            if hasattr(result, "__await__"):
                result = await result
            return str(result)
        except Exception as e:
            logger.error(f"Error in connection_status: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def list_catalogs_tool(jdbc_url: str) -> str:
        """Возвращает список всех доступных каталогов."""
        try:
            result = await list_catalogs(jdbc_url=jdbc_url)
            return str(result)
        except Exception as e:
            logger.error(f"Error in list_catalogs: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def list_schemas_tool(jdbc_url: str, catalog: Optional[str] = None) -> str:
        """Возвращает список схем в указанном каталоге."""
        try:
            kwargs = {"jdbc_url": jdbc_url}
            if catalog:
                kwargs["catalog"] = catalog
            result = await list_schemas(**kwargs)
            return str(result)
        except Exception as e:
            logger.error(f"Error in list_schemas: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def list_tables_tool(
        jdbc_url: str, schema: str, catalog: Optional[str] = None
    ) -> str:
        """Возвращает список таблиц в указанной схеме."""
        try:
            kwargs = {"jdbc_url": jdbc_url, "schema": schema}
            if catalog:
                kwargs["catalog"] = catalog
            result = await list_tables(**kwargs)
            return str(result)
        except Exception as e:
            logger.error(f"Error in list_tables: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def describe_table_tool(
        jdbc_url: str, table: str, schema: str, catalog: Optional[str] = None
    ) -> str:
        """Возвращает описание структуры таблицы."""
        try:
            kwargs = {"jdbc_url": jdbc_url, "table": table, "schema": schema}
            if catalog:
                kwargs["catalog"] = catalog
            result = await describe_table(**kwargs)
            return str(result)
        except Exception as e:
            logger.error(f"Error in describe_table: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def execute_query_tool(
        jdbc_url: str,
        sql: str,
        limit: int = 100,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """Выполняет SQL запрос с ограничением на количество строк."""
        try:
            kwargs = {"jdbc_url": jdbc_url, "sql": sql, "limit": limit}
            if catalog:
                kwargs["catalog"] = catalog
            if schema:
                kwargs["schema"] = schema
            result = await execute_query(**kwargs)
            return str(result)
        except Exception as e:
            logger.error(f"Error in execute_query: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def validate_ddl_statements_tool(ddl_list: list) -> str:
        """Анализирует и валидирует список DDL выражений."""
        try:
            result = await validate_ddl_statements(ddl_list=ddl_list)
            return str(result)
        except Exception as e:
            logger.error(f"Error in validate_ddl_statements: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def execute_ddl_statements_tool(
        jdbc_url: str,
        ddl_list: list,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        validate_first: bool = True,
    ) -> str:
        """Выполняет список DDL выражений с предварительной валидацией."""
        try:
            kwargs = {
                "jdbc_url": jdbc_url,
                "ddl_list": ddl_list,
                "validate_first": validate_first,
            }
            if catalog:
                kwargs["catalog"] = catalog
            if schema:
                kwargs["schema"] = schema
            result = await execute_ddl_statements(**kwargs)
            return str(result)
        except Exception as e:
            logger.error(f"Error in execute_ddl_statements: {e}")
            return f"Error: {str(e)}"

    @mcp_server.tool()
    async def get_connection_stats_tool() -> str:
        """Возвращает статистику активных подключений."""
        try:
            result = await get_connection_stats()
            return str(result)
        except Exception as e:
            logger.error(f"Error in get_connection_stats: {e}")
            return f"Error: {str(e)}"

    logger.info("Все инструменты Trino зарегистрированы в FastMCP сервере")
