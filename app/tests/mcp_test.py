import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.mcp_util import get_mcp_executor

async def main():
    executor = await get_mcp_executor()
    query = """
    SELECT 문항응답내용
    FROM inst1.tsccvsv73 t
    WHERE t.문항구분 = '01' and t.설문조사방식구분 = '02' and t.설문조사대상구분 != '00' and t.설문응답종료년월일 between '20251001' and '20251030' and t.문항응답내용 is not null and TRIM(t.문항응답내용) != '';
    """
    res = await executor.execute_tool("mysql_query", {"query": query})
    print(res)

if __name__ == "__main__":
    asyncio.run(main())
