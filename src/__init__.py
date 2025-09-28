"""
Trino MCP Server - Model Context Protocol server for Trino database schema analysis.

Provides tools for analyzing Trino database schemas, validating DDL statements,
and executing queries with dynamic connection management.
"""

__version__ = "0.1.0"
__author__ = "dreadew"
__email__ = "p74ur@yandex.ru"

from src.api.server import main as run_server
from src.application.tools.trino_tools import register_tools
from src.infra.connection_manager import connection_manager
from src.core.ddl_analyzer import ddl_analyzer

__all__ = ["run_server", "register_tools", "connection_manager", "ddl_analyzer"]
