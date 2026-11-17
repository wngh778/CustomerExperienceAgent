import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.mcp_util import get_mcp_executor

async def main():
    executor = await get_mcp_executor()
    query = """SELECT
   *
FROM inst.TSCCVMGC7"""
    res = await executor.execute_tool("mysql_query", {"query": query})
    print(res)

if __name__ == "__main__":
    asyncio.run(main())
