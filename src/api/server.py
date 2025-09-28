from mcp.server import Server
from mcp.server.stdio import stdio_server

from src.application.tools.trino_tools import register_tools
from src.core.config import config
from src.core.logging import get_logger
from src.infra.connection_manager import connection_manager

logger = get_logger(__name__)

server = Server(config.APP_NAME)

register_tools(server)


def main():
    """Запуск MCP сервера через stdio."""
    try:
        logger.info("Starting Trino MCP Server")
        logger.info("Running in stdio mode")
        stdio_server(server)
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        raise
    finally:
        connection_manager.close_all()


if __name__ == "__main__":
    main()
