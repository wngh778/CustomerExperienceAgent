"""SQL 전처리 유틸리티.

report_generation_agent.py의 인스턴스 메서드들을 standalone 함수로 추출하여
tool 모듈에서도 재사용할 수 있게 한다.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from agent.report_generation.resources.catalog import TABLE_ALIAS_CODE_MAP

logger = logging.getLogger(__name__)

# 모듈 레벨 캐시 (테이블 → 컬럼 집합)
_table_columns_cache: dict[str, set[str]] = {}

_DEFAULT_LIMIT = 100


async def get_table_columns(
    table: str,
    mcp_executor: Any,
    user_id: str = "",
) -> set[str]:
    """INST1 스키마의 테이블 컬럼명 조회 (mcp_executor 사용, 캐싱)."""
    if table not in _table_columns_cache:
        query = (
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = 'INST1' AND TABLE_NAME = '{table}'"
        )
        try:
            result = await mcp_executor.execute_tool(
                "mysql_query", {"query": query}, emp_no=user_id,
            )
            data = result if isinstance(result, list) else result.get("data", [])
            _table_columns_cache[table] = {
                row.get("COLUMN_NAME", "") for row in data if row.get("COLUMN_NAME")
            }
        except Exception as e:
            logger.warning(f"Failed to get columns for {table}: {e}")
            return set()
    return _table_columns_cache[table]


async def strip_invalid_conditions(
    sql: str,
    mcp_executor: Any,
    user_id: str = "",
) -> str:
    """INST1 테이블에 없는 컬럼 조건을 WHERE/AND에서 제거."""
    stripped = sql.strip()
    if (stripped.upper().startswith("WITH")
            or sql.count("SELECT") > 1
            or re.search(r'\bOVER\s*\(', sql, re.IGNORECASE)):
        return sql

    m = re.search(r'FROM\s+INST1\.(\w+)', sql, re.IGNORECASE)
    if not m:
        return sql
    table = m.group(1)
    valid_cols = await get_table_columns(table, mcp_executor, user_id)
    if not valid_cols:
        return sql

    where_match = re.search(
        r'\bWHERE\b(.+?)(?=\bLIMIT\b|\bORDER\b|\bGROUP\b|;|\Z)',
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not where_match:
        return sql

    where_body = where_match.group(1)
    before_where = sql[:where_match.start()]
    after_where = sql[where_match.end():]

    conditions: list[str] = []
    rest = where_body
    while rest.strip():
        # BETWEEN ... AND '...' 패턴
        m2 = re.match(
            r"\s*(\w+)\s+BETWEEN\s+'[^']*'\s+AND\s+'[^']*'(.*)",
            rest, re.DOTALL,
        )
        if m2:
            cond_text = rest[:len(rest) - len(m2.group(2))] if m2.group(2) else rest
            conditions.append(cond_text.strip())
            rest = m2.group(2)
            rest = re.sub(r'^\s*AND\b', '', rest, count=1)
            continue
        # 일반 조건
        m2 = re.match(
            r"\s*(\w+)\s*(?:=|>=|<=|<>|!=|>|<|LIKE|IN)\s*(?:'[^']*'|\d+)(.*)$",
            rest, re.DOTALL | re.IGNORECASE,
        )
        if m2:
            cond_text = rest[:len(rest) - len(m2.group(2))] if m2.group(2) else rest
            conditions.append(cond_text.strip())
            rest = m2.group(2)
            rest = re.sub(r'^\s*AND\b', '', rest, count=1)
            continue
        conditions.append(rest.strip())
        break

    valid_conditions = []
    for cond in conditions:
        cond = cond.strip()
        if not cond:
            continue
        col_m = re.match(r'(\w+)', cond)
        if col_m and col_m.group(1) not in valid_cols:
            logger.debug(f"무효 컬럼 조건 제거: {cond}")
            continue
        valid_conditions.append(cond)

    if valid_conditions:
        new_where = " WHERE " + " AND ".join(valid_conditions) + " "
    else:
        new_where = " "

    return before_where + new_where + after_where


def ensure_limit(sql: str, default_limit: int = _DEFAULT_LIMIT) -> str:
    """LIMIT 절이 없으면 기본 LIMIT을 추가한다."""
    stripped = sql.strip().rstrip(";")
    if (stripped.upper().startswith("WITH")
            or re.search(r'\bUNION\b', stripped, re.IGNORECASE)
            or re.search(r'\bLIMIT\s+\d+', stripped, re.IGNORECASE)):
        return sql
    return stripped + f" LIMIT {default_limit};"


async def rewrite_sql(
    sql: str,
    mcp_executor: Any,
    user_id: str = "",
) -> str:
    """잔여 별칭 치환(fallback) + 무효 컬럼 조건 제거 + LIMIT 보장."""
    result = sql
    for alias, code in sorted(TABLE_ALIAS_CODE_MAP.items(), key=lambda x: -len(x[0])):
        result = re.sub(rf'\b{re.escape(alias)}\b', code, result)
    result = await strip_invalid_conditions(result, mcp_executor, user_id)
    result = ensure_limit(result)
    return result


async def execute_sql(
    sql: str,
    mcp_executor: Any,
    user_id: str = "",
) -> dict[str, Any]:
    """SQL을 전처리(별칭 치환 + 무효 컬럼 제거) 후 mcp_executor로 실행한다."""
    from agent.report_generation.resources.catalog import translate_sql
    translated = translate_sql(sql)
    preprocessed = await rewrite_sql(translated, mcp_executor, user_id)
    logger.info(f"최종 실행 SQL : {preprocessed}" )
    
    try:
        result = await mcp_executor.execute_tool(
            "mysql_query", {"query": preprocessed}, emp_no=user_id,
        )
        if result is None:
            raise ValueError("MYSQL 문법 오류")
        data = result if isinstance(result, list) else result.get("data", [])
        return {
            "success": True,
            "data": data,
            "row_count": len(data) if data else 0,
            "error": None,
        }
    except Exception as e:
        logger.exception(f"[execute_sql] Failed: {e}")
        return {"success": False, "data": None, "row_count": 0, "error": str(e)}
