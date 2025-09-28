from typing import List

from mcp.server import Server
from mcp.types import TextContent, Tool

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


def register_tools(server: Server):
    """
    Регистрирует все инструменты для работы с Trino.
    :param server: MCP сервер для регистрации инструментов
    """

    TOOLS = [
        Tool(
            name="connection_status",
            description="Проверяет статус подключения к Trino",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    }
                },
                "required": ["jdbc_url"],
            },
        ),
        Tool(
            name="list_catalogs",
            description="Возвращает список всех доступных каталогов",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    }
                },
                "required": ["jdbc_url"],
            },
        ),
        Tool(
            name="list_schemas",
            description="Возвращает список схем в указанном каталоге",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    },
                    "catalog": {
                        "type": "string",
                        "description": "Название каталога (опционально)",
                    },
                },
                "required": ["jdbc_url"],
            },
        ),
        Tool(
            name="list_tables",
            description="Возвращает список таблиц в указанной схеме",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    },
                    "schema": {"type": "string", "description": "Название схемы"},
                    "catalog": {
                        "type": "string",
                        "description": "Название каталога (опционально)",
                    },
                },
                "required": ["jdbc_url", "schema"],
            },
        ),
        Tool(
            name="describe_table",
            description="Возвращает описание структуры таблицы",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    },
                    "table": {"type": "string", "description": "Название таблицы"},
                    "schema": {"type": "string", "description": "Название схемы"},
                    "catalog": {
                        "type": "string",
                        "description": "Название каталога (опционально)",
                    },
                },
                "required": ["jdbc_url", "table", "schema"],
            },
        ),
        Tool(
            name="execute_query",
            description="Выполняет SQL запрос с ограничением на количество строк",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL запрос для выполнения",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Максимальное количество строк для возврата",
                        "default": 100,
                    },
                    "catalog": {
                        "type": "string",
                        "description": "Каталог по умолчанию",
                    },
                    "schema": {"type": "string", "description": "Схема по умолчанию"},
                },
                "required": ["jdbc_url", "sql"],
            },
        ),
        Tool(
            name="validate_ddl_statements",
            description="Анализирует и валидирует список DDL выражений",
            inputSchema={
                "type": "object",
                "properties": {
                    "ddl_list": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Список DDL выражений для анализа",
                    }
                },
                "required": ["ddl_list"],
            },
        ),
        Tool(
            name="execute_ddl_statements",
            description="Выполняет список DDL выражений с предварительной валидацией",
            inputSchema={
                "type": "object",
                "properties": {
                    "jdbc_url": {
                        "type": "string",
                        "description": "JDBC URL для подключения к Trino",
                    },
                    "ddl_list": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Список DDL выражений для выполнения",
                    },
                    "catalog": {
                        "type": "string",
                        "description": "Каталог по умолчанию",
                    },
                    "schema": {"type": "string", "description": "Схема по умолчанию"},
                    "validate_first": {
                        "type": "boolean",
                        "description": "Выполнить валидацию перед выполнением",
                        "default": True,
                    },
                },
                "required": ["jdbc_url", "ddl_list"],
            },
        ),
        Tool(
            name="get_connection_stats",
            description="Возвращает статистику активных подключений",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
    ]

    @server.list_tools()
    async def list_tools():
        """Возвращает список доступных инструментов."""
        return TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> List[TextContent]:
        """Вызывает указанный инструмент с переданными аргументами."""
        try:
            if name == "connection_status":
                result = await connection_status(**arguments)
            elif name == "list_catalogs":
                result = await list_catalogs(**arguments)
            elif name == "list_schemas":
                result = await list_schemas(**arguments)
            elif name == "list_tables":
                result = await list_tables(**arguments)
            elif name == "describe_table":
                result = await describe_table(**arguments)
            elif name == "execute_query":
                result = await execute_query(**arguments)
            elif name == "validate_ddl_statements":
                result = await validate_ddl_statements(**arguments)
            elif name == "execute_ddl_statements":
                result = await execute_ddl_statements(**arguments)
            elif name == "get_connection_stats":
                result = await get_connection_stats(**arguments)
            else:
                raise ValueError(f"Unknown tool: {name}")

            return [TextContent(type="text", text=str(result))]
        except Exception as e:
            logger.error(f"Error calling tool {name}: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]
