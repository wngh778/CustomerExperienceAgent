"""NPS 데이터 조회 Tool 정의 — VIEW_REGISTRY 기반 24+1개.

각 tool은 VIEW_REGISTRY의 1개 view_id에 대응하며,
FilterCondition 기반 유연한 WHERE 조건을 받아 SQL을 결정적으로 구성·실행하고 결과를 반환한다.

LLM은 SQL을 직접 작성하지 않으며, tool 호출 파라미터만 지정한다.
"""

from __future__ import annotations

import json
import logging
import re
import functools
from typing import Any, Optional

from langchain_core.tools import tool

from agent.report_generation.resources.models import FilterCondition
from agent.report_generation.resources.schema import VIEW_REGISTRY
from agent.report_generation.tools.sql_utils import execute_sql

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# 일부 툴 금지
# ─────────────────────────────────────────────────────────────────────

def voc_tool_block(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        output = {
            "view_id": f"UNSUPPORTED_DATA-VOC_TYPE_SENTIMENT",
            "sql": "",
            "data": [],
            "row_count": 0,
            "error": None,
        }
        return json.dumps(output, ensure_ascii=False, default=str)
    return wrapper
        

# ─────────────────────────────────────────────────────────────────────
# Column Allowlist — VIEW_REGISTRY DDL + dim_columns 기반 (import 시 1회 빌드)
# ─────────────────────────────────────────────────────────────────────

_DDL_COLUMN_RE = re.compile(r"(\w+)\s+(?:TEXT|INTEGER|REAL)\b")


def _parse_ddl_columns(ddl: str) -> set[str]:
    """DDL CREATE TABLE 문에서 컬럼명을 추출한다. 같은 줄 쉼표 구분 컬럼도 처리."""
    cols = set()
    for line in ddl.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped.startswith("CREATE"):
            continue
        cols.update(m.group(1) for m in _DDL_COLUMN_RE.finditer(line))
    return cols

_VIEW_ALLOWED_COLUMNS: dict[str, set[str]] = {}
for _vid, _vdef in VIEW_REGISTRY.items():
    _ddl_cols = _parse_ddl_columns(_vdef.get("ddl", ""))
    _dim_cols = set(_vdef.get("dim_columns", []))
    _VIEW_ALLOWED_COLUMNS[_vid] = _ddl_cols | _dim_cols


# ─────────────────────────────────────────────────────────────────────
# 뷰별 자동 적용 조건 / 기본 정렬 / 커스텀 SELECT
# ─────────────────────────────────────────────────────────────────────

AUTO_CONDITIONS: dict[str, list[str]] = {
    "td_channel_ipa":       ["거래은행명 = 'KB국민은행'"],
    "td_cx_stage_ipa":      ["거래은행명 = 'KB국민은행'"],
    "td_voc_raw":           ["VOC필터링여부 = 0"],
    "bu_voc_raw":           ["VOC필터링여부 = 0"],
    "td_spectrum_voc":      ["VOC필터링여부 = 0"],
    "bu_spectrum_voc":      ["VOC필터링여부 = 0"],
    "improvement_by_dept":  ["배분여부 = 1", "VOC필터링여부 = 0"],
    "improvement_by_factor":["배분여부 = 1", "VOC필터링여부 = 0"],
}

DEFAULT_ORDER_BY: dict[str, str] = {
    # TD 사전집계
    "td_channel_nps":       "조사년도 DESC, 반기구분명 DESC",
    "td_cx_stage_nps":      "조사년도 DESC, 반기구분명 DESC",
    "td_channel_driver":    "조사년도 DESC, 반기구분명 DESC",
    "td_cx_stage_driver":   "조사년도 DESC, 반기구분명 DESC",
    "td_channel_ipa":       "조사년도 DESC, 반기구분명 DESC",
    "td_cx_stage_ipa":      "조사년도 DESC, 반기구분명 DESC",
    "td_voc_sentiment":     "조사년도 DESC, 반기구분명 DESC",
    # TD VOC 원문
    "td_voc_raw":           "조사년도 DESC, 반기구분명 DESC",
    # BU 일별
    "bu_channel_nps":       "기준년월일 DESC, 채널명 ASC",
    "bu_cx_stage_nps":      "기준년월일 DESC, 채널명 ASC, 고객경험단계명 ASC",
    "bu_channel_driver":    "기준년월일 DESC, 채널명 ASC, 고객경험단계명 ASC",
    "bu_stage_driver":      "기준년월일 DESC, 채널명 ASC, 고객경험단계명 ASC, 서비스품질명 ASC",
    "bu_cx_element_voc":    "기준년월일 DESC, 채널명 ASC, 고객경험단계명 ASC, 서비스품질명 ASC",
    "bu_voc_raw":           "기준년월일 DESC",
    # BU 월별
    "bu_channel_nps_trend":     "기준년월 DESC, 채널명 ASC",
    "bu_cx_stage_nps_trend":    "기준년월 DESC, 채널명 ASC, 고객경험단계명 ASC",
    "bu_channel_driver_trend":  "기준년월 DESC, 채널명 ASC, 고객경험단계명 ASC",
    "bu_stage_driver_trend":    "기준년월 DESC, 채널명 ASC, 고객경험단계명 ASC, 서비스품질명 ASC",
    "bu_cx_element_voc_monthly":"기준년월 DESC, 채널명 ASC, 고객경험단계명 ASC, 서비스품질명 ASC",
    # 개선조치
    "improvement_by_dept":   "개선사업그룹명, 개선부서명",
    "improvement_by_factor": "채널명, 고객경험단계명, 서비스품질명",
}

CUSTOM_SELECT: dict[str, str] = {
    "td_channel_nps": (
        "채널명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명, "
        "응답고객수, 추천고객수, 중립고객수, 비추천고객수, 추천비중점수, "
        "중립비중점수, 비추천비중점수, NPS점수"
    ),
    "td_cx_stage_nps": (
        "채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, "
        "반기구분명, 응답고객수, 추천고객수, 중립고객수, 비추천고객수, 추천비중점수,  "
        "중립비중점수, 비추천비중점수, NPS점수 "
    ),
    "td_channel_driver": (
        "채널명, 거래은행명, 영향요인구분명, 설문조사방식명, 조사년도, 반기구분명, "
        "응답고객수, 추천고객수, 중립고객수, 비추천고객수, 전체대비응답비중점수, "
        "전체대비추천비중점수, 전체대비중립비중점수, 전체대비비추천비중점수, NPS영향도점수,"
        "ROW_NUMBER() OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 영향요인구분명 ORDER BY NPS영향도점수 DESC) AS 영향도순위"
    ),
    "td_cx_stage_driver": (
        "채널명, 고객경험단계명, 거래은행명, 영향요인구분명, 설문조사방식명, 조사년도, "
        "반기구분명, 응답고객수, 추천고객수, 중립고객수, 비추천고객수, 전체대비응답비중점수, "
        "전체대비추천비중점수, 전체대비중립비중점수, 전체대비비추천비중점수, NPS영향도점수,"
        "ROW_NUMBER() OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명 ORDER BY NPS영향도점수 DESC) AS 영향도순위"
    ),
    "td_channel_ipa": (
        "채널명, 거래은행명, 영향요인구분명, 설문조사방식명, 조사년도, 반기구분명, "
        "문제영역명, 벤치마크은행명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, "
        "벤치마크NPS영향도점수, NPS영향도갭점수, NPS영향도갭평균점수"
    ),
    "td_cx_stage_ipa": (
        "채널명, 고객경험단계명, 거래은행명, 영향요인구분명, 설문조사방식명, 조사년도, "
        "반기구분명, 문제영역명, 벤치마크은행명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, "
        "벤치마크NPS영향도점수, NPS영향도갭점수, 벤치마크NPS점수갭평균점수"
    ),
    "td_voc_sentiment": (
        "채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명, "
        "응답고객수, 긍정고객수, 중립고객수, 부정고객수, 긍정비중점수, 중립비중점수, "
        "부정비중점수, NSS점수"
    ),
    "td_voc_type": (
        "채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 설문조사종류명, 조사년도, "
        "반기구분명, 응답고객수, 칭찬고객수, 불만고객수, 개선고객수, 기타고객수, "
        "칭찬비중점수, 불만비중점수, 개선비중점수, 기타비중점수, CCI점수"
    ),
    "td_voc_raw": (
        "VOC원문내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명, "
        "채널명, 고객경험단계명, 서비스품질명"
    ),
    "bu_channel_nps": (
        "기준년월일, 채널명, NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율"
    ),
    "bu_channel_nps_trend": (
        "기준년월, 기준년월일, 채널명, NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율"
    ),
    "bu_cx_stage_nps": (
        "기준년월일, 채널명, 고객경험단계명, NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율"
    ),
    "bu_cx_stage_nps_trend": (
        "기준년월, 기준년월일, 채널명, 고객경험단계명, NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율"
    ),
    "bu_channel_driver": (
        "기준년월일, 채널명, 고객경험단계명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ), 
    "bu_channel_driver_trend": (
        "기준년월, 기준년월일, 채널명, 고객경험단계명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_stage_driver": (
        "기준년월일, 채널명, 고객경험단계명, 서비스품질명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_stage_driver_trend": (
        "기준년월, 기준년월일, 채널명, 고객경험단계명, 서비스품질명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_cx_element_voc": (
        "기준년월일, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명, 전체건수, 긍정건수, 부정건수, 중립건수, 칭찬건수, 불만건수, 개선건수, 기타건수, 긍정비율, 칭찬비율, 불만비율, 개선비율, 기타비율, NSS점수, CCI점수"
    ),
    "bu_cx_element_voc_monthly": (
        "기준년월, 기준년월일, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명, 전체건수, 긍정건수, 부정건수, 중립건수, 칭찬건수, 불만건수, 개선건수, 기타건수, 긍정비율, 칭찬비율, 불만비율, 개선비율, 기타비율, NSS점수, CCI점수"
    ),
    "bu_voc_raw": (
        "VOC원문내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명, "
        "채널명, 고객경험단계명, 서비스품질명"
    ),
}


# ─────────────────────────────────────────────────────────────────────
# SQL Builder Helpers
# ─────────────────────────────────────────────────────────────────────

def _esc(value: str) -> str:
    """SQL 문자열 이스케이프 (single-quote)."""
    return value.replace("'", "''")


def _validate_column(column: str, view_id: str) -> None:
    """컬럼명이 해당 뷰의 허용 목록에 있는지 검증한다."""
    allowed = _VIEW_ALLOWED_COLUMNS.get(view_id, set())
    if allowed and column not in allowed:
        raise ValueError(
            f"컬럼 '{column}'은(는) 뷰 '{view_id}'에서 허용되지 않습니다. "
            f"허용 컬럼: {sorted(allowed)}"
        )


_ORDER_PART_RE = re.compile(r"^\s*(\w+)(?:\s+(?:ASC|DESC))?\s*$", re.IGNORECASE)


def _validate_order_by(order_by: str, view_id: str) -> str:
    """ORDER BY 값을 검증하고 안전한 SQL 절을 반환한다."""
    parts = order_by.split(",")
    for part in parts:
        m = _ORDER_PART_RE.match(part)
        if not m:
            raise ValueError(f"잘못된 order_by 형식: '{part.strip()}'")
        _validate_column(m.group(1), view_id)
    return order_by.strip()


def _filter_to_sql(f: FilterCondition) -> str:
    """단일 FilterCondition → SQL 조건문 문자열."""
    col = f.column
    op = f.op
    vals = f.values

    if op in ("=", "!=", ">", ">=", "<", "<=", "LIKE"):
        if len(vals) != 1:
            raise ValueError(f"연산자 '{op}'는 values에 1개의 값이 필요합니다. (받은 값: {vals})")
        return f"{col} {op} '{_esc(vals[0])}'"

    if op == "IN":
        if not vals:
            raise ValueError("연산자 'IN'은 1개 이상의 값이 필요합니다.")
        in_list = ", ".join(f"'{_esc(v)}'" for v in vals)
        return f"{col} IN ({in_list})"

    if op == "BETWEEN":
        if len(vals) != 2:
            raise ValueError(f"연산자 'BETWEEN'은 2개의 값이 필요합니다. (받은 값: {vals})")
        return f"{col} BETWEEN '{_esc(vals[0])}' AND '{_esc(vals[1])}'"

    raise ValueError(f"지원하지 않는 연산자: {op}")


def _build_filters_where(
    filters: list[FilterCondition] | None,
    view_id: str,
    auto_conditions: list[str] | None = None,
) -> str:
    """FilterCondition 리스트 + 자동 조건 → WHERE 절 문자열."""
    parts: list[str] = []
    if auto_conditions:
        parts.extend(auto_conditions)
    for f in (filters or []):
        _validate_column(f.column, view_id)
        parts.append(_filter_to_sql(f))
    if not parts:
        return ""
    return "WHERE " + " AND ".join(parts)


def _resolve_order(order_by: str | None, view_id: str) -> str:
    """사용자 지정 order_by 또는 기본 정렬을 반환."""
    if order_by:
        return f"ORDER BY {_validate_order_by(order_by, view_id)}"
    default = DEFAULT_ORDER_BY.get(view_id)
    return f"ORDER BY {default}" if default else ""


def _extract_where_col_names(filters: list[FilterCondition] | None, view_id: str) -> str:
    """filters(where 절에 들어갈 조건들) 내 컬럼명 추출"""
    col_names = []
    for f in (filters or []):
        _validate_column(f.column, view_id)
        col_names.append(f.column)
    return "," + ",".join(col_names) if col_names else ""


def _get_select_col_names(filters: list[FilterCondition] | None, spectrum_columns: list[str], view_id: str) -> str:
    """Filter columns과 group column을 합쳐서 사용"""
    col_names = []
    for f in (filters or []):
        _validate_column(f.column, view_id)
        col_names.append(f.column)
    select_cols = list(set(col_names).union(set(spectrum_columns)))
    return ",".join(select_cols) if select_cols else ""

def _replace_col_name(sql: str, view_id: str) -> str:
    if view_id in ["td_spectrum_nps", "td_spectrum_driver", "td_spectrum_channel_ipa", "td_spectrum_cx_stage_ipa", "bu_spectrum_nps"]:
        sql = sql.replace("설문고객연령5세내용", "연령5세내용").replace("설문고객연령5세내용", "연령10세내용")
    if view_id in ["bu_spectrum_voc", "td_voc_raw"]:
        sql = re.sub(r'(?<!설문고객)연령5세내용', '설문고객연령5세내용', sql)
    if view_id in ["td_channel_driver", "v_td_channel_ipa"]:
        sql = sql.replace("고객경험단계명", "영향요인구분명")
    if view_id in ["td_cx_stage_driver", "v_td_cx_stage_ipa"]:
        sql = sql.replace("서비스품질명", "영향요인구분명")
    sql = sql.replace("서비스품질요소명", "서비스품질명")

    return sql

# ─────────────────────────────────────────────────────────────────────
# 공통 실행 래퍼
# ─────────────────────────────────────────────────────────────────────

async def _run_query(
    sql: str,
    view_id: str,
    mcp_executor: Any,
    user_id: str = "",
) -> str:
    """SQL 실행 후 JSON 문자열로 반환 (ToolMessage content용)."""
    sql = _replace_col_name(sql, view_id)
    result = await execute_sql(sql, mcp_executor, user_id)
    output = {
        "view_id": view_id,
        "sql": sql,
        "data": result.get("data"),
        "row_count": result.get("row_count", 0),
        "error": result.get("error"),
    }
    return json.dumps(output, ensure_ascii=False, default=str)


# ═════════════════════════════════════════════════════════════════════
# Tool Factory — mcp_executor 주입 후 tool 리스트 생성
# ═════════════════════════════════════════════════════════════════════

def create_nps_tools(mcp_executor: Any, user_id: str = "") -> list:
    """24+1개 NPS 데이터 조회 tool을 생성한다.

    Args:
        mcp_executor: SQL 실행용 MCP executor 인스턴스
        user_id: 사용자 ID (rate limit 분산용)

    Returns:
        LangChain tool 리스트
    """

    # ─────────────────────────────────────────────────────────────
    # TD 사전집계 (7개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_td_channel_nps(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 채널별 NPS를 조회합니다. 타행비교(7개 은행), 반기별 데이터.
        사용 상황: 타행비교, 은행 순위, 경쟁사 비교, 시장평균 대비, TD NPS 현황.
        컬럼: 조사년도, 반기구분명, 거래은행명, 채널명(브랜드/플랫폼/대면채널/고객센터/상품), NPS점수, 추천비중점수, 비추천비중점수.
        filters 예시: [{"column":"조사년도","op":"<=","values":["2025"]}, {"column":"채널명","op":"IN","values":["플랫폼","대면채널"]}]"""
        view_id = "td_channel_nps"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"""
            SELECT 
                {select_cols}, 순위
            FROM v_td_channel_nps A
            LEFT JOIN (
                SELECT
                    채널명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명,
                    ROW_NUMBER() OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명
                        ORDER BY NPS점수 DESC
                    ) AS 순위
                FROM
                    v_td_channel_nps
                WHERE 거래은행명 <> '시장평균'
                ) B USING (채널명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명)
            {where}
            {order}
            LIMIT {limit}"""
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_cx_stage_nps(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 고객경험단계별 NPS를 조회합니다. 타행비교(7개 은행), 반기별 데이터.
        사용 상황: TD 고객경험단계 NPS 분석
        컬럼: 거래은행명, 채널명(브랜드/플랫폼/대면채널/고객센터/상품), 고객경험단계명, NPS점수, 추천비중점수, 비추천비중점수.
        filters 예시: [{"column":"조사년도","op":"=","values":["2025"]}, {"column":"채널명","op":"=","values":["플랫폼"]}]"""
        view_id = "td_cx_stage_nps"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"""
            SELECT 
                {select_cols}, 순위
            FROM v_td_cx_stage_nps A
            LEFT JOIN (
                SELECT
                    채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명,
                    ROW_NUMBER() OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명
                        ORDER BY NPS점수 DESC
                    ) AS 순위
                FROM
                    v_td_cx_stage_nps
                WHERE 거래은행명 <> '시장평균'
                ) B USING (채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명)
            {where}
            {order}
            LIMIT {limit}"""
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_channel_driver(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 채널 영향요인을 조회합니다. 어떤 고객경험단계가 채널 NPS에 기여하는지.
        사용 상황: TD 채널 영향요인, 어떤 경험단계가 NPS에 기여하는지.
        컬럼: 거래은행명, 채널명, 고객경험단계명, 영향도.
        filters 예시: [{"column":"거래은행명","op":"=","values":["KB국민은행"]}, {"column":"채널명","op":"=","values":["플랫폼"]}]"""
        view_id = "td_channel_driver"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_channel_driver\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_cx_stage_driver(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 경험단계 영향요인을 조회합니다. 어떤 서비스품질요소가 경험단계 NPS에 기여하는지.
        사용 상황: TD 서비스품질요소별 영향도, 개선 우선순위, 원인 분석.
        컬럼: 거래은행명, 채널명, 고객경험단계명, 서비스품질요소명, 영향도.
        filters 예시: [{"column":"채널명","op":"=","values":["플랫폼"]}, {"column":"고객경험단계명","op":"=","values":["접속/로그인"]}]"""
        view_id = "td_cx_stage_driver"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_cx_stage_driver\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_channel_ipa(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 채널 IPA 4사분면 분석을 조회합니다. 채널 NPS를 고객경험단계별로 분해.
        TD 전용. 거래은행명='KB국민은행' 조건 자동 적용.
        문제영역명: 현상유지, 유지개선, 중점개선, 점진개선.
        사용 상황: IPA 분석, 문제영역, 중점개선, 벤치마크 Gap, 강점/약점.
        컬럼: 채널명, 영향요인구분명(=고객경험단계), 문제영역명, NPS중요도점수, NPS영향도갭점수, 벤치마크은행명.
        filters 예시: [{"column":"채널명","op":"=","values":["플랫폼"]}, {"column":"문제영역명","op":"=","values":["중점개선"]}]"""
        view_id = "td_channel_ipa"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_channel_ipa\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_cx_stage_ipa(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 경험단계 IPA 4사분면 분석을 조회합니다. 경험단계 NPS를 서비스품질요소별로 분해.
        TD 전용. 거래은행명='KB국민은행' 조건 자동 적용.
        사용 상황: 경험단계별 IPA, 서비스품질요소별 문제영역, 상세 IPA.
        컬럼: 채널명, 고객경험단계명, 영향요인구분명(=서비스품질요소), 문제영역명, NPS중요도점수, NPS영향도갭점수.
        filters 예시: [{"column":"채널명","op":"=","values":["플랫폼"]}, {"column":"고객경험단계명","op":"=","values":["접속/로그인"]}]"""
        view_id = "td_cx_stage_ipa"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_cx_stage_ipa\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    @voc_tool_block
    async def query_td_voc_sentiment(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 고객경험단계별 VOC 유형 분포를 조회합니다. NSS 포함.
        사용 상황: TD VOC 감정분석, NSS, 긍정/부정 비율, 경험단계별 VOC.
        컬럼: 거래은행명, 채널명, 고객경험단계명, 전체건수, 긍정비율, 부정비율, NSS.
        filters 예시: [{"column":"거래은행명","op":"=","values":["KB국민은행"]}, {"column":"채널명","op":"=","values":["플랫폼"]}]"""
        view_id = "td_voc_sentiment"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_voc_sentiment\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    @voc_tool_block
    async def query_td_voc_type(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] TD 고객경험단계별 VOC 유형 분포를 조회합니다. CCI 포함.
        사용 상황: TD VOC 유형분석, CCI, 칭찬/불만/개선/기타 비율, 경험단계별 VOC.
        컬럼: 거래은행명, 채널명, 고객경험단계명, 응답고객수, 칭찬고객수, 불만고객수, 개선고객수, 기타고객수, 칭찬비중점수, 불만비중점수, 개선비중점수, 기타비중점수, CCI점수
        filters 예시: [{"column":"거래은행명","op":"=","values":["KB국민은행"]}, {"column":"채널명","op":"=","values":["플랫폼"]}]"""
        view_id = "td_voc_type"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"\
            SELECT {select_cols}\
            FROM v_td_voc_type\
            {where}\
            {order}\
            LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # TD VOC 원문 (1개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_td_voc_raw(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[개별 데이터 조회] TD VOC 원문을 조회합니다. 고객 목소리 실제 텍스트.
        VOC필터링여부=0 조건 자동 적용.
        사용 상황: TD VOC 원문, 고객 목소리, 실제 의견 확인 (TD).
        컬럼: VOC원문내용, 고객감정대분류명(긍정/부정/중립), 고객경험VOC유형명(칭찬/불만/개선/기타), 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명.
        filters 예시: [{"column":"조사년도","op":"=","values":["2025"]}, {"column":"고객감정대분류명","op":"=","values":["부정"]}]"""
        view_id = "td_voc_raw"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_td_voc_raw {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # TD 스펙트럼 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_td_spectrum_nps(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 NPS를 조회합니다.
        사용 상황: TD 연령대별, 고객등급별, 이용빈도별 NPS 조회 등
        spectrum_columns 예시: ["연령10세내용"], ["성별내용", "고객등급내용"] (교차 분석)
        가용 스펙트럼 컬럼: 연령10세내용, 연령5세내용, 성별내용, 고객등급내용, 이용거래기간내용, 플랫폼이용빈도내용, 고객센터이용빈도내용, 영업점이용빈도내용.
        filters 예시: [{"column":"조사년도","op":"=","values":["2025"]}, {"column":"거래은행명","op":"=","values":["KB국민은행"]}]
        주의사항: 채널에 대한 NPS 조회시 반드시 filters에 {"column":"고객경험단계명","op":"=","values":[""]}이 포함되어야 합니다.
        """
        view_id = "td_spectrum_nps"
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"

        select_cols = _get_select_col_names(filters, spectrum_columns, view_id)
        sql = (
            f"SELECT * FROM ("
            f"SELECT {select_cols}, COUNT(*) AS 전체건수, "
            f"ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) "
            f"- ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수 "
            f"FROM v_td_spectrum_nps {where} "
            f"GROUP BY {group_cols} "
            f") A {order} LIMIT {limit}"
        )
        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    async def query_td_spectrum_channel_driver(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 채널 영향요인 정보를 조회합니다.
        사용 상황: TD 연령대별, 고객등급별 채널 영향 요인 분석 등
        칼럼 : 반기구분명, 조사년도, 거래은행명, 채널명, 영향요인구분명, NPS중요도점수, NPS영향도점수
        spectrum_columns 예시: ["연령10세내용", "성별내용", "고객등급내용"].
        spectrum_ranges 예시 : [("20대"), ("여성"), ("VIP")]
        """
        view_id = "v_td_spectrum_channel_driver"
        spectrum_columns = [x for x in spectrum_columns if x not in ["거래은행명"]]
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
        order = order.replace("고객경험단계명", "영향요인구분명")
        if "고객경험단계명" not in where: # 채널 NPS 조회시 고객경험단계명='' 삽입
            where += " AND 고객경험단계명=''"
        cols = ", ".join(spectrum_columns)

        sql = f"""WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명, BM.벤치마크은행명 
		, {cols}
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 추천사유내용 AS 영향요인구분명
			,{cols}
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		{where} 
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용
			, {cols} 
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명
		, {cols}
)
SELECT
	조사년도, 반기구분명, 거래은행명, 채널명, 영향요인구분명
	, {cols} 
	, NPS중요도점수
	, NPS중요도평균점수
	, NPS영향도점수
FROM (
	SELECT 
		조사년도, 반기구분명, 채널명, 영향요인구분명, 거래은행명
		, {cols}
		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols}))/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols})*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명) AS NPS중요도평균점수
	FROM AGGR) A
{order} LIMIT {limit}"""
        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    async def query_td_spectrum_channel_ipa(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 채널 IPA 문제영역 정보를 조회합니다.
        사용 상황: TD 연령대별, 고객등급별 채널 IPA 문제영역 등
        spectrum_columns 예시: ["연령10세내용", "성별내용", "고객등급내용"].
        spectrum_ranges 예시 : [("20대"), ("여성"), ("VIP")]
        주의사항: 채널에 대한 NPS 조회시 반드시 filters에 {"column":"고객경험단계명","op":"=","values":[""]}이 포함되어야 합니다.
        """
        view_id = "v_td_spectrum_channel_ipa"
        spectrum_columns = [x for x in spectrum_columns if x not in ["거래은행명"]]
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
        order = order.replace("고객경험단계명", "영향요인구분명")
        cols = ", ".join(spectrum_columns)
        kb_cols = ", ".join(["KB." + col for col in spectrum_columns])

        sql = f"""WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명, BM.벤치마크은행명 
		, {cols}
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 추천사유내용 AS 영향요인구분명
			,{cols}
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		{where} 
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용
			, {cols} 
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명
		, {cols}
)
SELECT
	KB.반기구분명, KB.조사년도, 
	채널명, 영향요인구분명
	, {kb_cols} 
	, NPS중요도점수
	, NPS중요도평균점수
	, NPS영향도점수
	, 벤치마크은행명
	, 벤치마크NPS영향도점수
	, (NPS영향도점수 - 벤치마크NPS영향도점수) AS NPS영향도갭점수
	, sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명) AS NPS영향도갭평균점수
	, CASE WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "현상유지"
		WHEN NPS중요도점수 <= NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "유지관리" 
		WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			<= sum(NPS영향도점수 -벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "중점개선" 
		ELSE '점진개선' END AS 문제영역명
FROM (
	SELECT 
		조사년도, 반기구분명, 채널명, 영향요인구분명
		, {cols}
		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols}))/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols})*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명) AS NPS중요도평균점수
	FROM AGGR
	WHERE 거래은행명='KB국민은행') KB
 LEFT JOIN (
	SELECT
		조사년도, 반기구분명, 채널명, 영향요인구분명
		, {cols}
		, 거래은행명 AS 벤치마크은행명
		,(추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {cols})*100 AS 벤치마크NPS영향도점수
	FROM AGGR 
	WHERE 거래은행명=벤치마크은행명) BM
USING (조사년도, 반기구분명, 채널명, 영향요인구분명, {cols})
{order} LIMIT {limit}"""
        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    async def query_td_spectrum_cx_stage_driver(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 고객경험단계 영향요인 정보를 조회합니다.
        사용 상황: TD 연령대별, 고객등급별 고객경험단계 영향요인 분석등
        칼럼 : 반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명, NPS중요도점수, NPS영향도점수
        spectrum_columns 예시: ["연령10세내용", "성별내용", "고객등급내용"].
        spectrum_ranges 예시 : [("20대"), ("여성"), ("VIP")]
        """
        view_id = "v_td_spectrum_cx_stage_driver"
        spectrum_columns = [x for x in spectrum_columns if x not in ["거래은행명"]]
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
        order = order.replace("서비스품질명", "영향요인구분명")

        cols = ", ".join(spectrum_columns)
        
        sql = f"""WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명, BM.벤치마크은행명 
		, {cols}
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 고객경험단계명, 추천사유내용 AS 영향요인구분명
			,{cols}
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		{where} 
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명, 추천사유내용
			, {cols} 
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명
		, {cols}
)
SELECT
	조사년도, 반기구분명,  거래은행명, 채널명, 고객경험단계명, 영향요인구분명
	, {cols}
	, NPS중요도점수
	, NPS중요도평균점수
	, NPS영향도점수
FROM (
	SELECT 
		반기구분명, 조사년도, 채널명, 고객경험단계명, 영향요인구분명, 거래은행명
		, {cols}
		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols}))/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols})*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명) AS NPS중요도평균점수
	FROM AGGR) A
{order} LIMIT {limit}"""

        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    async def query_td_spectrum_cx_stage_ipa(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 고객경험단계 IPA 문제영역 정보를 조회합니다.
        사용 상황: TD 연령대별, 고객등급별 고객경험단계 IPA 문제영역 등
        spectrum_columns 예시: ["연령10세내용", "성별내용", "고객등급내용"].
        spectrum_ranges 예시 : [("20대"), ("여성"), ("VIP")]
        """
        view_id = "v_td_spectrum_cx_stage_ipa"
        spectrum_columns = [x for x in spectrum_columns if x not in ["거래은행명"]]
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
        order = order.replace("서비스품질명", "영향요인구분명")
        cols = ", ".join(spectrum_columns)
        kb_cols = ", ".join(["KB." + col for col in spectrum_columns])
        
        sql = f"""WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명, BM.벤치마크은행명 
		, {cols}
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 고객경험단계명, 추천사유내용 AS 영향요인구분명
			,{cols}
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		{where} 
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명, 추천사유내용
			, {cols} 
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명
		, {cols}
)
SELECT
	KB.반기구분명, KB.조사년도, 
	채널명, 고객경험단계명, 영향요인구분명
	, {kb_cols} 
	, NPS중요도점수
	, NPS중요도평균점수
	, NPS영향도점수
	, 벤치마크은행명
	, 벤치마크NPS영향도점수
	, (NPS영향도점수 - 벤치마크NPS영향도점수) AS NPS영향도갭점수
	, sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명) AS NPS영향도갭평균점수
	, CASE WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "현상유지"
		WHEN NPS중요도점수 <= NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "유지관리" 
		WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			<= sum(NPS영향도점수 -벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
		THEN "중점개선" 
		ELSE '점진개선' END AS 문제영역명
FROM (
	SELECT 
		반기구분명, 조사년도, 채널명, 고객경험단계명, 영향요인구분명
		, {cols}
		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols}))/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols})*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명) AS NPS중요도평균점수
	FROM AGGR
	WHERE 거래은행명='KB국민은행') KB
 LEFT JOIN (
	SELECT
		채널명, 고객경험단계명, 영향요인구분명, 거래은행명 AS 벤치마크은행명
		, {cols}
		,(추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {cols})*100 AS 벤치마크NPS영향도점수
	FROM AGGR 
	WHERE 거래은행명=벤치마크은행명) BM
USING (채널명, 영향요인구분명, 고객경험단계명, {cols})
{order} LIMIT {limit}"""

        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    @voc_tool_block
    async def query_td_spectrum_voc(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] TD 고객 세그먼트(스펙트럼)별 VOC 감정 또는 유형 집계에 대한 정보를 조회합니다.
        VOC필터링여부=0 조건 자동 적용.
        사용 상황: TD 세그먼트/스펙트럼(연령대별, 고객등급별 등등) VOC 감정(긍정/부정/중립)별 또는 유형(칭찬/불만/개선/기타)별 건수 및 비율 조회, NSS(==긍정비율-부정비율), CCI(=불만비율) 점수 조회
        칼럼이름 : 전체건수, 긍정건수, 부정건수, NSS, CCI
        spectrum_columns 예시: CX체계("채널명", "고객경험단계명", "서비스품질명", "고객경험요소명"), 세그먼트("연령10세내용", "성별내용", "고객등급내용")
        filters 예시: [{"column":"조사년도","op":"=","values":["2025"]}, {"column":"거래은행명","op":"=","values":["KB국민은행"]}]"""
        view_id = "td_spectrum_voc"
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"

        select_cols = _get_select_col_names(filters, spectrum_columns, view_id)
        sql = (
            f"SELECT * FROM ( "
            f"SELECT {select_cols}, COUNT(*) AS 전체건수, "
            f"ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 긍정비율, "
            f"ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 부정비율, "
            f"ROUND(SUM(CASE WHEN 고객감정대분류명='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비율, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='칭찬' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 칭찬비율, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='개선' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 개선비율, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 불만비율, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='기타' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 기타비율, "
            f"- ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI "
            f"FROM v_td_spectrum_voc {where} "
            f"GROUP BY {group_cols}"
            f") A {order} LIMIT {limit}"
        )
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU 사전집계 NPS (4개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_channel_nps(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 채널별 NPS 일별 누적(연초~당일)을 조회합니다.
        사용 상황: 채널 NPS 현황, 전체 채널 비교, BU NPS 조회.
        컬럼: 기준년월일(YYYYMMDD), 채널명(KB 스타뱅킹/영업점/고객센터/상품), NPS점수, 전체건수, 추천비율, 비추천비율.
        filters 예시: [{"column":"기준년월일","op":"=","values":["20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_channel_nps"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_channel_nps {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_channel_nps_trend(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 채널 NPS 월말 스냅샷(월별 추이)을 조회합니다.
        사용 상황: NPS 추이, 기간별 변화, 트렌드, 전월 대비.
        컬럼: 기준년월(YYYYMM), 기준년월일, 채널명, NPS점수, 전체건수.
        filters 예시: [{"column":"기준년월","op":"BETWEEN","values":["202507","202602"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_channel_nps_trend"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_channel_nps_trend {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_cx_stage_nps(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 고객경험단계별 NPS 일별 누적(연초~당일)을 조회합니다.
        사용 상황: BU 경험단계 분석, 상세 드릴다운, 어느 단계가 약한지.
        컬럼: 기준년월일, 채널명, 고객경험단계명, NPS점수, 전체건수.
        filters 예시: [{"column":"기준년월일","op":"=","values":["20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_cx_stage_nps"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_cx_stage_nps {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_cx_stage_nps_trend(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 고객경험단계별 NPS 월말 스냅샷(월별 추이)을 조회합니다.
        사용 상황: BU 경험단계 NPS 추이, 월별 비교.
        컬럼: 기준년월, 기준년월일, 채널명, 고객경험단계명, NPS점수, 전체건수.
        filters 예시: [{"column":"기준년월","op":"BETWEEN","values":["202601","202602"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_cx_stage_nps_trend"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_cx_stage_nps_trend {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU 사전집계 영향요인 (4개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_channel_driver(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 채널 영향요인 일별 누적을 조회합니다. 어떤 고객경험단계가 채널 NPS에 영향.
        사용 상황: BU 채널 영향요인, 어떤 경험단계가 채널 NPS에 영향.
        컬럼: 기준년월일, 채널명, 고객경험단계명, 영향도점수.
        filters 예시: [{"column":"기준년월일","op":"<=","values":["20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_channel_driver"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_channel_driver {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_channel_driver_trend(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 채널 영향요인 월말 스냅샷을 조회합니다. 전월/전전월 비교 포함.
        사용 상황: BU 채널 영향도 추이, 전월 대비.
        컬럼: 기준년월, 기준년월일, 채널명, 고객경험단계명, 영향도점수, 전월영향도점수, 전전월영향도점수.
        filters 예시: [{"column":"기준년월","op":"<=","values":["202602"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_channel_driver_trend"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_channel_driver_trend {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_stage_driver(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 경험단계 영향요인 일별 누적을 조회합니다. 어떤 서비스품질요소가 경험단계 NPS에 영향.
        사용 상황: BU 서비스품질요소별 영향도, 어떤 요인이 경험단계 NPS에 영향.
        컬럼: 기준년월일, 채널명, 고객경험단계명, 서비스품질요소명, 영향도점수.
        filters 예시: [{"column":"기준년월일","op":"<=","values":["20260225"]}, {"column":"고객경험단계명","op":"=","values":["접속/로그인"]}]"""
        view_id = "bu_stage_driver"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_stage_driver {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_stage_driver_trend(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 경험단계 영향요인 월말 스냅샷을 조회합니다. 전월/전전월 비교 포함.
        사용 상황: BU 서비스품질요소별 영향도 추이, 전월 대비.
        컬럼: 기준년월, 채널명, 고객경험단계명, 서비스품질요소명, 영향도점수, 전월영향도점수, 전전월영향도점수.
        filters 예시: [{"column":"기준년월","op":"<=","values":["202602"]}, {"column":"고객경험단계명","op":"=","values":["접속/로그인"]}]"""
        view_id = "bu_stage_driver_trend"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_stage_driver_trend {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU VOC 사전집계 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_cx_element_voc(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 고객경험요소별 VOC 감정·유형 일별 누적을 조회합니다.
        사용 상황: BU 고객경험요소별 VOC 현황, NSS, CCI, 감정분석, 불만 많은 영역.
        컬럼: 기준년월일, 채널명, 고객경험단계명, 서비스품질요소명, 고객경험요소명, 전체건수, NSS, CCI, 불만건수.
        filters 예시: [{"column":"기준년월일","op":"<=","values":["20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_cx_element_voc"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_cx_element_voc {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    @voc_tool_block
    async def query_bu_cx_element_voc_monthly(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU 고객경험요소별 VOC 월별 단독 집계를 조회합니다.
        사용 상황: BU 고객경험요소별 VOC 월별 비교.
        컬럼: 기준년월, 기준년월일, 채널명, 고객경험단계명, 서비스품질요소명, 고객경험요소명, 전체건수, NSS, CCI.
        filters 예시: [{"column":"기준년월","op":"<=","values":["202602"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_cx_element_voc_monthly"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_cx_element_voc_monthly {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU VOC 원문 (1개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_voc_raw(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 50,
        check_reason: str = "",
    ) -> str:
        """[기본조회] BU VOC 원문을 조회합니다. 고객 목소리 실제 텍스트.
        VOC필터링여부=0 조건 자동 적용.
        사용 상황: BU VOC 원문, 고객 목소리, 실제 의견 확인 (BU).
        컬럼: VOC원문내용, 고객감정대분류명, 고객경험VOC유형명, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명.
        filters 예시: [{"column":"기준년월일","op":"BETWEEN","values":["20260201","20260225"]}, {"column":"고객감정대분류명","op":"=","values":["부정"]}]"""
        view_id = "bu_voc_raw"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        select_cols = CUSTOM_SELECT.get(view_id, "*")
        sql = f"SELECT {select_cols} FROM v_bu_voc_raw {where} {order} LIMIT {limit}"
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU 스펙트럼 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_spectrum_nps(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] BU 고객 세그먼트(스펙트럼)별 NPS를 조회합니다. 마스터 기반 동적 GROUP BY.
        추천점수(0~10) 기반 CASE WHEN으로 NPS 직접 계산. 전체건수 30건 미만 시 해석 주의.
        사용 상황: BU 연령대별, 성별, 에피소드별, 고객등급별 NPS, BU 스펙트럼.
        spectrum_columns 예시: ["연령5세내용"], ["성별내용", "실질고객내용"] (교차 분석)
        가용 스펙트럼 컬럼: 연령5세내용, 성별내용, 실질고객내용, 에피소드유형내용, 에피소드상세내용, 주직무구분명, 직군구분명, 영업점명, 지역본부명, 지역영업그룹명.
        filters 예시: [{"column":"기준년월일","op":"BETWEEN","values":["20260101","20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "bu_spectrum_nps"
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        where_cols = ", ".join([c.column for c in filters])
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
        sql = (
            f"SELECT {where_cols}, {group_cols}, COUNT(*) AS 전체건수, "
            f"ROUND(SUM(CASE WHEN 추천점수>=9 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) "
            f"- ROUND(SUM(CASE WHEN 추천점수<=6 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수 "
            f"FROM v_bu_spectrum_nps {where} "
            f"GROUP BY {group_cols} {order} LIMIT {limit}"
        )

        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    @voc_tool_block
    async def query_bu_spectrum_voc(
        spectrum_columns: list[str],
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[스펙트럼 분석] BU 고객 세그먼트(스펙트럼)별 VOC 감정·유형 집계를 조회합니다.
        VOC필터링여부=0 조건 자동 적용.
        사용 상황: BU 연령대별, 성별, 고객등급별 VOC 분포, BU 스펙트럼 VOC.
        spectrum_columns 예시: ["설문고객연령5세내용"], ["성별내용", "실질고객내용"] (교차 분석)
        가용 스펙트럼 컬럼: 설문고객연령5세내용, 설문고객연령10세내용, 성별내용, 실질고객내용, 에피소드유형내용, 에피소드상세내용, 주직무구분명, 직군구분명, 영업점명, 지역본부명, 지역영업그룹명.
        filters 예시: [{"column":"기준년월일","op":"BETWEEN","values":["20260101","20260225"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""

        view_id = "bu_spectrum_voc"
        for col in spectrum_columns:
            _validate_column(col, view_id)
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        where_cols = ", ".join([c.column for c in filters])
        group_cols = ", ".join(spectrum_columns)
        if order_by:
            order = f"ORDER BY {_validate_order_by(order_by, view_id)}"
        else:
            order = f"ORDER BY {group_cols}"
            
        sql = (
            f"SELECT {where_cols}, {group_cols}, COUNT(*) AS 전체건수, "
            f"SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END) AS 긍정건수, "
            f"SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END) AS 부정건수, "
            f"ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) "
            f"- ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS, "
            f"ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI "
            f"FROM v_bu_spectrum_voc {where} "
            f"GROUP BY {group_cols} {order} LIMIT {limit}"
        )
        
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # 개선조치 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_improvement_by_dept(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[기본조회] 부서별 개선조치 배분/처리 현황을 조회합니다. BU 전용.
        배분여부=1 AND VOC필터링여부=0 조건 자동 적용.
        사용 상황: 개선부서별 배분, 처리율, 검토완료, 미처리, 개선예정 건수.
        컬럼: 개선사업그룹명, 개선부서명, 배분건수, 검토완료건수, 처리율, 미처리건수, 검토기한만료건수.
        filters 예시: [{"column":"기준년월일","op":"LIKE","values":["202602%"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]
        기간 지정: LIKE "202602%" 또는 BETWEEN ["20260201","20260228"]"""
        view_id = "improvement_by_dept"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        sql = (
            f"SELECT 개선사업그룹명, 개선부서명, "
            f"SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수, "
            f"SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END) AS 검토건수, "
            f"ROUND(SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END)*100.0"
            f" / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 검토율, "
            f"SUM(CASE WHEN 과제검토명='현행유지' THEN 1 ELSE 0 END) AS 현행유지건수, "
            f"SUM(CASE WHEN 과제검토명='개선예정' THEN 1 ELSE 0 END) AS 개선예정건수, "
            f"SUM(CASE WHEN 과제검토명='개선불가' THEN 1 ELSE 0 END) AS 개선불가건수 "
            f"FROM v_bu_spectrum_voc {where} "
            f"GROUP BY 개선사업그룹명, 개선부서명 "
            f"{order} LIMIT {limit}"
        )
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_improvement_by_factor(
        filters: Optional[list[FilterCondition]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        check_reason: str = "",
    ) -> str:
        """[기본조회] 서비스품질요소별 개선조치 처리 현황을 조회합니다. BU 전용.
        배분여부=1 AND VOC필터링여부=0 조건 자동 적용.
        사용 상황: 서비스품질요소별 배분, 처리율, 검토 현황.
        컬럼: 채널명, 고객경험단계명, 서비스품질명, 배분건수, 검토완료건수, 처리율.
        filters 예시: [{"column":"기준년월일","op":"BETWEEN","values":["20260201","20260228"]}, {"column":"채널명","op":"=","values":["KB 스타뱅킹"]}]"""
        view_id = "improvement_by_factor"
        where = _build_filters_where(filters, view_id, AUTO_CONDITIONS.get(view_id))
        order = _resolve_order(order_by, view_id)
        sql = (
            f"SELECT 채널명, 고객경험단계명, 서비스품질명, "
            f"SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수, "
            f"SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END) AS 검토건수, "
            f"ROUND(SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END)*100.0"
            f" / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 검토율 "
            f"FROM v_bu_spectrum_voc {where} "
            f"GROUP BY 채널명, 고객경험단계명, 서비스품질명 "
            f"{order} LIMIT {limit}"
        )
        return await _run_query(sql, view_id, mcp_executor, user_id)



    # ─────────────────────────────────────────────────────────────
    # 유틸리티 (1개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def get_latest_data_info() -> str:
        """TD/BU 최신 데이터 날짜 정보를 조회합니다.
        기간 파라미터를 결정하기 전에 이 도구를 먼저 호출하여 가용 데이터 범위를 확인하세요."""
        td_query = (
            "SELECT DISTINCT 조사년도, 반기구분명 FROM INST1.TSCCVMGC1 "
            "ORDER BY 조사년도 DESC, 반기구분명 DESC LIMIT 1"
        )
        bu_query = (
            "SELECT 채널명, MAX(기준년월일) AS 최신일자 FROM INST1.TSCCVMGD5 "
            "GROUP BY 채널명 ORDER BY 채널명"
        )
        result = {"td": None, "bu": []}
        try:
            td_result = await mcp_executor.execute_tool(
                "mysql_query", {"query": td_query}, emp_no=user_id,
            )
            td_data = td_result if isinstance(td_result, list) else td_result.get("data", [])
            if td_data:
                result["td"] = td_data[0]
        except Exception:
            pass
        try:
            bu_result = await mcp_executor.execute_tool(
                "mysql_query", {"query": bu_query}, emp_no=user_id,
            )
            bu_data = bu_result if isinstance(bu_result, list) else bu_result.get("data", [])
            result["bu"] = bu_data
        except Exception:
            pass
        return json.dumps(result, ensure_ascii=False, default=str)

    # ─────────────────────────────────────────────────────────────
    # 스킵 도구 (도구 호출 생략 사유 기록)
    # ─────────────────────────────────────────────────────────────

    @tool
    def report_skip_reason(
        reason: str,
        considered_tools: list[str],
    ) -> str:
        """데이터 조회 도구를 호출하지 않을 때 반드시 이 도구를 호출하여 사유를 기록한다.
        재진입 시 수집된 데이터가 충분하여 추가 조회가 불필요한 경우에도 이 도구를 호출한다.
        reason: 도구를 호출하지 않는 구체적 사유 (예: "이미 충분한 데이터 확보", "해당 조사유형에 뷰 미존재").
        considered_tools: 검토했으나 호출하지 않기로 한 도구명 리스트."""
        return json.dumps(
            {"skipped": True, "reason": reason, "considered_tools": considered_tools},
            ensure_ascii=False,
        )

    # ─────────────────────────────────────────────────────────────
    # Tool 리스트 반환
    # ─────────────────────────────────────────────────────────────

    return [
        # TD 사전집계
        query_td_channel_nps,
        query_td_cx_stage_nps,
        query_td_channel_driver,
        query_td_cx_stage_driver,
        query_td_channel_ipa,
        query_td_cx_stage_ipa,
        query_td_voc_sentiment,
        query_td_voc_type,
        # TD VOC 원문
        query_td_voc_raw,
        # TD 스펙트럼
        query_td_spectrum_nps,
        query_td_spectrum_voc,
        query_td_spectrum_channel_driver,
        query_td_spectrum_channel_ipa,
        query_td_spectrum_cx_stage_driver,
        query_td_spectrum_cx_stage_ipa,
        # BU NPS
        query_bu_channel_nps,
        query_bu_channel_nps_trend,
        query_bu_cx_stage_nps,
        query_bu_cx_stage_nps_trend,
        # BU 영향요인
        query_bu_channel_driver,
        query_bu_channel_driver_trend,
        query_bu_stage_driver,
        query_bu_stage_driver_trend,
        # BU VOC
        query_bu_cx_element_voc,
        query_bu_cx_element_voc_monthly,
        query_bu_voc_raw,
        # BU 스펙트럼
        query_bu_spectrum_nps,
        query_bu_spectrum_voc,
        # 개선조치
        query_improvement_by_dept,
        query_improvement_by_factor,
        # 유틸리티
        # get_latest_data_info,
        # report_skip_reason,
    ]
