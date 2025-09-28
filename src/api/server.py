from mcp.server.fastmcp import FastMCP

from src.application.tools.trino_tools import register_tools
from src.core.config import config

mcp = FastMCP(config.APP_NAME, host=config.HOST, port=config.PORT)

register_tools(mcp)

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
