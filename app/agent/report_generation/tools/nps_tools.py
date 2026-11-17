"""NPS 데이터 조회 Tool 정의 — VIEW_REGISTRY 기반 24+1개.

각 tool은 VIEW_REGISTRY의 1개 view_id에 대응하며,

LLM은 SQL을 직접 작성하지 않으며, tool 호출 파라미터만 지정한다.
"""

from __future__ import annotations

import json
import logging
import re
import functools
import inspect

from typing import Any, Optional, Literal

from langchain_core.tools import tool

from agent.report_generation.resources.schema import VIEW_ALLOWED_COLUMNS
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
# 커스텀 SELECT
# ─────────────────────────────────────────────────────────────────────

CUSTOM_SELECT: dict[str, str] = {
    "td_channel_nps": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 응답고객수, 추천비중점수, "
        "중립비중점수, 비추천비중점수, NPS점수"
    ),
    "td_cx_stage_nps": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 고객경험단계명, 응답고객수, 추천비중점수,  "
        "중립비중점수, 비추천비중점수, NPS점수"
    ),
    "td_channel_driver": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 영향요인구분명, 응답고객수, 전체대비응답비중점수, "
        "전체대비추천비중점수, 전체대비중립비중점수, 전체대비비추천비중점수, NPS영향도점수,"
        "ROW_NUMBER() OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 영향요인구분명 ORDER BY NPS영향도점수 DESC) AS 영향도순위"
    ),
    "td_cx_stage_driver": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 고객경험단계명, 영향요인구분명, 응답고객수, 전체대비응답비중점수, "
        "전체대비추천비중점수, 전체대비중립비중점수, 전체대비비추천비중점수, NPS영향도점수,"
        "ROW_NUMBER() OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명 ORDER BY NPS영향도점수 DESC) AS 영향도순위"
    ),
    "td_channel_ipa": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 영향요인구분명, 문제영역명, "
        "벤치마크은행명, NPS중요도점수, NPS영향도점수, "
        "벤치마크NPS영향도점수, NPS영향도갭점수"
    ),
    "td_cx_stage_ipa": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, "
        "채널명, 고객경험단계명, 영향요인구분명,  문제영역명, "
        "벤치마크은행명, NPS중요도점수, NPS영향도점수, "
        "벤치마크NPS영향도점수, NPS영향도갭점수"
    ),
    "td_voc_type": (
        "설문조사방식명, 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, "
        "응답고객수, 긍정비중점수, 중립비중점수, 부정비중점수, NSS점수, "
        "칭찬비중점수, 불만비중점수, 개선비중점수, 기타비중점수, CCI점수"
    ),
    "td_voc_raw": (
        "설문조사방식명, 조사년도, 반기구분명, 채널명, 고객경험단계명, 고객경험요소명, "
        "VOC원문내용, 추천의향내용, 고객감정대분류명, 고객경험VOC유형명 "
    ),
    "bu_channel_nps": (
        "기준년월일, 기준년월, 채널명, NPS점수, 전체건수, 추천비율, 중립비율, 비추천비율"
    ),
    "bu_channel_nps_trend": (
        "기준년월일, 기준년월, 채널명, NPS점수, 전체건수, 추천비율, 중립비율, 비추천비율"
    ),
    "bu_cx_stage_nps": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, NPS점수, 전체건수, 추천비율, 중립비율, 비추천비율"
    ),
    "bu_cx_stage_nps_trend": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, NPS점수, 전체건수, 추천비율, 중립비율, 비추천비율"
    ),
    "bu_channel_driver": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ), 
    "bu_channel_driver_trend": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_cx_stage_driver": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 서비스품질명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_cx_stage_driver_trend": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 서비스품질명, 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전월대비영향도점수, 전전월영향도점수, 전전월대비영향도점수"
    ),
    "bu_cx_element_voc": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명, 전체건수, 긍정비율, 중립비율, 부정비율, NSS점수, 칭찬비율, 불만비율, 개선비율, 기타비율, CCI점수"
    ),
    "bu_cx_element_voc_monthly": (
        "기준년월일, 기준년월, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명, 전체건수, 긍정비율, 중립비율, 부정비율, NSS점수, 칭찬비율, 불만비율, 개선비율, 기타비율, CCI점수"
    ),
    "bu_voc_raw": (
        "설문조사방식명, 기준년월일, 채널명, 고객경험단계명, 고객경험요소명, "
        "VOC원문내용, 추천의향내용, 고객감정대분류명, 고객경험VOC유형명"
    ),
}


# ─────────────────────────────────────────────────────────────────────
# SQL Builder Helpers
# ─────────────────────────────────────────────────────────────────────

def _replace_col_name(sql: str, view_id: str) -> str:
    if view_id in ["bu_spectrum_nps", "bu_spectrum_voc"]:
        sql = sql.replace("고객등급내용", "실질고객내용")
    if view_id in ["bu_spectrum_voc"]:
        # F4(bu_spectrum_voc)만 연령 컬럼명이 설문고객연령xx세내용 형태
        # F3(bu_spectrum_nps)는 연령5세내용, 연령10세내용 그대로 사용
        sql = sql.replace("고객연령5세내용", "설문고객연령5세내용")
        sql = sql.replace("고객연령10세내용", "설문고객연령10세내용")
        sql = re.sub(r'(?<!설문고객)연령10세내용', '설문고객연령10세내용', sql)  # 이중치환 방지
    if view_id in ["td_channel_driver", "td_channel_ipa"]:
        sql = sql.replace("고객경험단계명", "영향요인구분명")
    if view_id in ["td_cx_stage_driver", "td_cx_stage_ipa"]:
        sql = sql.replace("서비스품질명", "영향요인구분명")
    return sql

def ym_to_date_range(ym: str) -> tuple[str, str]:
    """YYYYMM → (월초 YYYYMMDD, 다음달 첫날 YYYYMMDD) 반환. MAX 서브쿼리 범위 조건에 사용."""
    year, month = int(ym[:4]), int(ym[4:])
    start = f"{ym}01"
    end = f"{year + 1}0101" if month == 12 else f"{year}{month + 1:02d}01"
    return start, end

def make_dates_cte(view_id: str, 기준년월: list, 기준년월일: list) -> str:
    """dates CTE 본문 생성. 기준년월은 실제 데이터에 존재하는 기준년월일로 변환한다.

    - 1개월: 해당 월 범위 내 실제 존재하는 모든 일자 (DISTINCT)
    - 2개월 이상: 월별 MAX(기준년월일) (월말 최대일자)
    임의(미래) 일자를 생성하지 않으며, 기준년월일 인덱스 레인지 스캔으로 조회해 속도를 보장한다.
    """
    parts = [f"SELECT '{d}' AS 기준년월일" for d in (기준년월일 or [])]
    if 기준년월:
        if len(기준년월) < 2:
            s, e = ym_to_date_range(기준년월[0])
            parts.append(
                f"SELECT DISTINCT 기준년월일 FROM {view_id} "
                f"WHERE 기준년월일 >= '{s}' AND 기준년월일 < '{e}'"
            )
        else:
            range_conds = " OR ".join(
                f"(기준년월일 >= '{s}' AND 기준년월일 < '{e}')"
                for ym in 기준년월
                for s, e in [ym_to_date_range(ym)]
            )
            parts.append(
                f"SELECT MAX(기준년월일) AS 기준년월일 FROM {view_id} "
                f"WHERE ({range_conds}) GROUP BY SUBSTRING(기준년월일,1,6)"
            )
    return " UNION ALL ".join(parts)

def make_bu_where_and_extra_select(view_id: str, args: list, values: dict):
    """
    BU sql의 where 절을 구성합니다 (not spectrum sql)
    """
    where = "WHERE"
    for arg in args:
        if values[arg] and arg not in ('누적방식', 'reason', '상세조회'):
            where_values = ','.join([f"'{v}'" for v in values[arg]])

            if arg == "기준년월":
                if "trend" in view_id or "monthly" in view_id:
                    # 월별 테이블 (E5, D6, D8 등): 기준년월 인덱스 직접 활용
                    sub_where = f"기준년월일 IN (SELECT MAX(기준년월일) FROM {view_id} WHERE 기준년월 IN ({where_values}) GROUP BY 기준년월)"
                else:
                    # 일별 누적 테이블 (E4, D5, D7 등): 기준년월일 인덱스로 월 범위 내 MAX 조회
                    range_conds = " OR ".join([
                        f"(기준년월일 >= '{s}' AND 기준년월일 < '{e}')"
                        for ym in values[arg]
                        for s, e in [ym_to_date_range(ym)]
                    ])
                    sub_where = f"기준년월일 IN (SELECT MAX(기준년월일) FROM {view_id} WHERE ({range_conds}) GROUP BY SUBSTRING(기준년월일,1,6))"
            else:
                sub_where = f"{arg} IN ({where_values})"

            if where != "WHERE":
                where += f" AND {sub_where}"
            else:
                where += f" {sub_where}"
    return where

def make_bu_spectrum_where_and_extra_select(args: list, values: dict, spectrum_columns: list[str]=['성별내용', '연령10세내용', '고객등급내용', '지역영업그룹명'], view_id: str=''):
    """
    BU spectrum sql의 where 절과 추가 select 절을 구성합니다.
    기준년월은 월말 최대일자 서브쿼리로 변환합니다 (make_bu_where_and_extra_select와 동일).
    """
    select = []
    where = "WHERE"
    for arg in args:
        if values[arg] and arg not in ('누적방식', 'reason'):
            if arg in spectrum_columns:
                select.append(arg)
            where_values = ','.join([f"'{v}'" for v in values[arg]])
            if arg == '기준년월' and view_id:
                range_conds = " OR ".join([
                    f"(기준년월일 >= '{s}' AND 기준년월일 < '{e}')"
                    for ym in values[arg]
                    for s, e in [ym_to_date_range(ym)]
                ])
                sub_where = f"기준년월일 IN (SELECT MAX(기준년월일) FROM {view_id} WHERE ({range_conds}) GROUP BY SUBSTRING(기준년월일,1,6))"
            else:
                sub_where = f"{arg} IN ({where_values})"
            if where != "WHERE":
                where += f" AND {sub_where}"
            else:
                where += f" {sub_where}"
    extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
    return where, extra_select

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
    """NPS 데이터 조회 tool을 생성한다.

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
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """TD 채널별 NPS를 조회합니다. 타행비교(7개 은행), 반기별 데이터.
        사용 상황: 타행비교, 은행 순위, 경쟁사 비교, 시장평균 대비, TD 채널 NPS 현황
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_channel_nps"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
    
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
                SELECT 
                    {select_cols}, 순위
                FROM {view_id} A
                LEFT JOIN (
                    SELECT
                        채널명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명,
                        ROW_NUMBER() OVER (
                            PARTITION BY 조사년도, 반기구분명, 채널명
                            ORDER BY NPS점수 DESC
                        ) AS 순위,
                        count(*) OVER (
                            PARTITION BY 조사년도, 반기구분명, 채널명
                        ) AS 전체은행수
                    FROM
                        {view_id}
                    WHERE 거래은행명 <> '시장평균'
                    ) B USING (채널명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명)
                {where}
                ORDER BY 조사년도 DESC, 반기구분명 DESC
                LIMIT 500"""
        else:
            view_id = "td_spectrum_nps"
            # select & where 절 만들기
            select = []
            inner_where = "WHERE"
            bank_where = ""
            # where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if arg == '거래은행명':
                        # 거래은행명은 외부 쿼리에서 필터링(순위보존)
                        bank_where = f"WHERE 거래은행명 IN ({where_values})"
                        continue
                    if inner_where != "WHERE":
                        inner_where += f" AND {arg} IN ({where_values})"
                    else:
                        inner_where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            if 채널명 != ['상품']:
                inner_where += " AND 고객경험단계명=''"
            sql = f"""
            SELECT * FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명, {extra_select}
                        ORDER BY NPS점수 DESC
                    ) AS 순위,
                    count(*) OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명, {extra_select}
                    ) AS 전체은행수
                FROM (
                    SELECT 거래은행명, 조사년도, 반기구분명, 채널명, {extra_select}, COUNT(*) AS 응답고객수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 추천비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 비추천비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수 
                    FROM {view_id} {inner_where}
                    GROUP BY 거래은행명, 조사년도, 반기구분명, 채널명, {extra_select}
                ) A
            ) ranked {bank_where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC, {extra_select}, 순위
            LIMIT 500
            """
            
        return await _run_query(sql, view_id, mcp_executor, user_id)


    @tool
    async def query_td_cx_stage_nps(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """TD 고객경험단계별 NPS를 조회합니다. 타행비교(7개 은행), 반기별 데이터.
        사용 상황: 타행비교, 은행 순위, 경쟁사 비교, 시장평균 대비, TD 고객경험단계 NPS 현황
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_cx_stage_nps"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"

            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
                SELECT 
                    {select_cols}, 순위, 전체은행수
                FROM {view_id} A
                LEFT JOIN (
                    SELECT
                        채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명,
                        ROW_NUMBER() OVER (
                            PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명
                            ORDER BY NPS점수 DESC
                        ) AS 순위,
                        count(*) OVER (
                            PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명
                        ) AS 전체은행수
                    FROM
                        {view_id}
                    WHERE 거래은행명 <> '시장평균'
                    ) B USING (채널명, 고객경험단계명, 거래은행명, 설문조사방식명, 조사년도, 반기구분명)
                {where}
                ORDER BY 조사년도 DESC, 반기구분명 DESC
                LIMIT 500"""
        else:
            view_id = "td_spectrum_nps"
            # select & where 절 만들기
            select = []
            inner_where = "WHERE"
            bank_where = ""
            # where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if arg == '거래은행명':
                        # 거래은행명은 외부 쿼리에서 필터링 (순위 보존)
                        bank_where = f"WHERE 거래은행명 IN ({where_values})"
                        continue
                    if inner_where != "WHERE":
                        inner_where += f" AND {arg} IN ({where_values})"
                    else:
                        inner_where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            if 채널명 != ['상품']:
                inner_where += " AND 고객경험단계명!=''"
            sql = f"""
            SELECT * FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, {extra_select}
                        ORDER BY NPS점수 DESC
                    ) AS 순위,
                    count(*) OVER (
                        PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, {extra_select}
                    ) AS 전체은행수
                FROM (
                    SELECT 거래은행명, 조사년도, 반기구분명, 채널명, 고객경험단계명, {extra_select}, COUNT(*) AS 응답고객수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 추천비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 비추천비중점수, 
                    ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수 
                    FROM {view_id} {inner_where}
                    GROUP BY 거래은행명, 조사년도, 반기구분명, 채널명, 고객경험단계명, {extra_select}
                ) A
            ) ranked {bank_where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC, {extra_select}, 순위
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_channel_driver(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """TD 채널 NPS의 영향요인(=원인)을 조회합니다. 어떤 고객경험단계가 채널 NPS에 기여하는지.
        사용 상황: TD 채널 영향요인, 원인 분석.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_channel_driver"
             # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"

            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}
            FROM {view_id}
            {where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        else:
            view_id = "td_spectrum_driver"
            select = []
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
            where += " AND 고객경험단계명=''"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            sql = f"""
            WITH AGGR AS (
            	SELECT 
            		X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명, BM.벤치마크은행명 
            		, {extra_select}
            		, sum(갯수) AS 전체건수
            		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
            		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
            		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
            	FROM (
            		SELECT 거래은행명, 채널명, 추천사유내용 AS 영향요인구분명
            			,{extra_select}
            			, 추천의향내용, count(*) AS 갯수
            			, 조사년도, 반기구분명
            			FROM {view_id}
            		{where} 
            		GROUP BY 
            			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용
            			, {extra_select} 
            			, 추천의향내용
            	) X
            	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
            	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명, {extra_select}
            )
            SELECT
            	조사년도, 반기구분명, 거래은행명, 채널명, 영향요인구분명
            	, {extra_select}
            	, NPS중요도점수
            	, NPS영향도점수
            FROM (
            	SELECT 
            		조사년도, 반기구분명, 채널명, 영향요인구분명, 거래은행명
            		, {extra_select}
            		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}))/ 2 * 100 AS NPS중요도점수
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select})*100 AS NPS영향도점수
            	FROM AGGR) A
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_cx_stage_driver(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """TD 고객경험단계 NPS의 영향요인(=원인)을 조회합니다. 어떤 서비스품질요소가 경험단계 NPS에 기여하는지.
        사용 상황: TD 고객경험단계 영향요인, 원인 분석.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_channel_driver"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"

            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}
            FROM td_cx_stage_driver
            {where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        else:
            view_id = "td_spectrum_driver"
            # select & where 절 만들기
            select = []
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            where += " AND 고객경험단계명!=''"
            sql = f"""
            WITH AGGR AS (
            	SELECT 
            		X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명, BM.벤치마크은행명 
            		, {extra_select}
            		, sum(갯수) AS 전체건수
            		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
            		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
            		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
            	FROM (
            		SELECT 거래은행명, 채널명, 고객경험단계명, 추천사유내용 AS 영향요인구분명
            			,{extra_select}
            			, 추천의향내용, count(*) AS 갯수
            			, 조사년도, 반기구분명
            			FROM {view_id}
            		{where} 
            		GROUP BY 
            			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용
            			, {extra_select} 
            			, 추천의향내용
            	) X
            	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
            	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명
            		, {extra_select}
            )
            SELECT
            	조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명
            	, {extra_select} 
            	, NPS중요도점수
            	, NPS영향도점수
            FROM (
            	SELECT 
            		조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명, 거래은행명
            		, {extra_select}
            		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}))/ 2 * 100 AS NPS중요도점수
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select})*100 AS NPS영향도점수
            	FROM AGGR) A
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_td_channel_ipa(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """당행의 TD 채널의 IPA 문제영역(현상유지, 유지관리, 점진개선, 중점개선) 정보를 조회합니다. (타행 기준 조회 불가)
        사용 상황: IPA 분석, 문제영역 분석, 개선영역, 강점/약점.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_channel_ipa"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"

            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}
            FROM td_channel_ipa
            {where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        else:
            view_id = "td_spectrum_channel_ipa"
            # select & where 절 만들기
            select = []
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            kb_cols = ", ".join(["KB." + col for col in select])
            where += " AND 고객경험단계명=''"
            sql = f"""
            WITH AGGR AS (
            	SELECT 
            		X.반기구분명, X.조사년도, X.거래은행명, X.채널명, 영향요인구분명, BM.벤치마크은행명 
            		, {extra_select}
            		, sum(갯수) AS 전체건수
            		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
            		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
            		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
            	FROM (
            		SELECT 거래은행명, 채널명, 추천사유내용 AS 영향요인구분명
            			, {extra_select}
            			, 추천의향내용, count(*) AS 갯수
            			, 조사년도, 반기구분명
                    FROM {view_id}
                    {where}
            		GROUP BY 
            			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용, {extra_select}, 추천의향내용
            	) X
            	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
            	GROUP BY X.반기구분명, X.조사년도, X.거래은행명, X.채널명, 영향요인구분명, {extra_select}
            )
            SELECT
            	KB.반기구분명, KB.조사년도, 채널명, 영향요인구분명
            	, {kb_cols} 
                , ROUND(NPS중요도점수, 1) AS NPS중요도점수
                , ROUND(NPS영향도점수, 1) AS NPS영향도점수
                , 벤치마크은행명
                , ROUND(벤치마크NPS영향도점수, 1) AS 벤치마크NPS영향도점수
                , ROUND(NPS영향도점수 - 벤치마크NPS영향도점수, 1) AS NPS영향도갭점수
            	, CASE WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) > sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "현상유지"
            		WHEN NPS중요도점수 <= NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) > sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "유지관리" 
            		WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) <= sum(NPS영향도점수 -벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "중점개선" 
            		ELSE '점진개선' END AS 문제영역명
            FROM (
            	SELECT 
            		조사년도, 반기구분명, 채널명, 영향요인구분명
            		, {extra_select}
            		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select}))/ 2 * 100 AS NPS중요도점수
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select})*100 AS NPS영향도점수
            		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명) AS NPS중요도평균점수
            	FROM AGGR
            	WHERE 거래은행명='KB국민은행') KB
             LEFT JOIN (
            	SELECT
            		조사년도, 반기구분명, 채널명, 영향요인구분명
            		, {extra_select}
            		, 거래은행명 AS 벤치마크은행명
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 거래은행명, {extra_select})*100 AS 벤치마크NPS영향도점수
            	FROM AGGR 
            	WHERE 거래은행명=벤치마크은행명) BM USING (조사년도, 반기구분명, 채널명, 영향요인구분명, {extra_select})
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)    

    @tool
    async def query_td_cx_stage_ipa(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """당행의 TD 고객경험단계의 IPA 문제영역(현상유지, 유지관리, 점진개선, 중점개선) 정보를 조회합니다. (타행 기준 조회 불가)
        사용 상황: 고객경험단계 IPA 분석, 문제영역 분석, 개선영역, 강점/약점.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_cx_stage_ipa"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"

            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}
            FROM {view_id}
            {where}
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        else:
            view_id = "td_spectrum_cx_stage_ipa"
            # select & where 절 만들기
            select = []
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select) # 스펙트럼 컬럼도 select 추가
            kb_cols = ", ".join(["KB." + col for col in select])
            where += " AND 고객경험단계명!=''"
            sql = f"""
            WITH AGGR AS (
            	SELECT 
            		X.반기구분명, X.조사년도, X.거래은행명, X.채널명, X.고객경험단계명, 영향요인구분명, BM.벤치마크은행명 
            		, {extra_select}
            		, sum(갯수) AS 전체건수
            		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
            		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
            		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
            	FROM (
            		SELECT 거래은행명, 채널명, 고객경험단계명, 추천사유내용 AS 영향요인구분명
            			, {extra_select}
            			, 추천의향내용, count(*) AS 갯수
            			, 조사년도, 반기구분명
                    FROM {view_id}
                    {where}
            		GROUP BY 
            			반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명, 추천사유내용, {extra_select}, 추천의향내용
            	) X
            	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 고객경험단계명, 벤치마크은행명 FROM inst1.tsccvmgc8 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명, 고객경험단계명) 
            	GROUP BY X.반기구분명, X.조사년도, X.거래은행명, X.채널명, X.고객경험단계명, 영향요인구분명, {extra_select}
            )
            SELECT
            	KB.반기구분명, KB.조사년도, 채널명, 고객경험단계명, 영향요인구분명
            	, {kb_cols} 
                , ROUND(NPS중요도점수, 1) AS NPS중요도점수
                , ROUND(NPS영향도점수, 1) AS NPS영향도점수
                , 벤치마크은행명
                , ROUND(벤치마크NPS영향도점수, 1) AS 벤치마크NPS영향도점수
                , ROUND(NPS영향도점수 - 벤치마크NPS영향도점수, 1) AS NPS영향도갭점수
            	, CASE WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) > sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "현상유지"
            		WHEN NPS중요도점수 <= NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) > sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "유지관리" 
            		WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) <= sum(NPS영향도점수 -벤치마크NPS영향도점수) OVER (PARTITION BY 채널명) / count(*) OVER (PARTITION BY 채널명)
            		THEN "중점개선" 
            		ELSE '점진개선' END AS 문제영역명
            FROM (
            	SELECT 
            		조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명
            		, {extra_select}
            		, (전체건수/sum(전체건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {extra_select}) + 비추천건수/sum(비추천건수) over (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {extra_select}))/ 2 * 100 AS NPS중요도점수
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {extra_select})*100 AS NPS영향도점수
            		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명) AS NPS중요도평균점수
            	FROM AGGR
            	WHERE 거래은행명='KB국민은행') KB
             LEFT JOIN (
            	SELECT
            		조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명
            		, {extra_select}
            		, 거래은행명 AS 벤치마크은행명
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 조사년도, 반기구분명, 채널명, 고객경험단계명, 거래은행명, {extra_select})*100 AS 벤치마크NPS영향도점수
            	FROM AGGR 
            	WHERE 거래은행명=벤치마크은행명) BM USING (조사년도, 반기구분명, 채널명, 고객경험단계명, 영향요인구분명, {extra_select})
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    @voc_tool_block
    async def query_td_voc_type(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        고객감정대분류명: Optional[list] = [],
        고객경험VOC유형명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
    ) -> str:
        """TD 채널 또는 고객경험단계 별로 수집된 VOC의 감정(긍정/부정) 또는 유형(칭찬/불만/개선/기타) 분포를 조회합니다. NSS(감정지수), CCI(고객불만지수) 포함. (VOC 내용(=원문)은 미포함).
        사용 상황: TD VOC 감정 분석, VOC 유형 분석, 긍정/부정/칭찬/불만 등 비율, VOC 유형 분류
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 고객감정대분류명: 긍정,부정
            - 고객경험VOC유형명: 칭찬,불만,개선,기타
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용):
            view_id = "td_voc_type"
            # where 절 만들기
            where = "WHERE"
            for arg in args:
                if values[arg]:
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"        
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}
            FROM (
                SELECT c9.설문조사방식명, c9.조사년도, c9.반기구분명, c9.거래은행명, c9.채널명, c9.고객경험단계명, c9.응답고객수, c9.긍정비중점수, c9.중립비중점수, c9.부정비중점수, c9.NSS점수, d1.칭찬비중점수, d1.불만비중점수, d1.개선비중점수, d1.기타비중점수, d1.CCI점수 FROM INST1.TSCCVMGC9 c9
                JOIN {view_id} d1 ON c9.그룹회사코드 = d1.그룹회사코드 AND c9.설문ID = d1.설문ID AND c9.채널명 = d1.채널명 AND c9.고객경험단계명 = d1.고객경험단계명 AND c9.거래은행명 = d1.거래은행명
            ) a
            {where}
            LIMIT 500
            """
        else:
            view_id = "td_spectrum_voc"
            # select & where 절 만들기
            select = []
            where = "WHERE"
            base_where = "WHERE"
            spectrum_dim = {'성별내용', '연령10세내용', '고객등급내용'}
            for arg in args:
                if values[arg]:
                    if arg in spectrum_dim:
                        select.append(arg)
                    where_values = ','.join([f"'{v}'" for v in values[arg]])
                    if where != "WHERE":
                        where += f" AND {arg} IN ({where_values})"
                    else:
                        where += f" {arg} IN ({where_values})"
                    if arg not in spectrum_dim:
                        if base_where != "WHERE":
                            base_where += f" AND {arg} IN ({where_values})"
                        else:
                            base_where += f" {arg} IN ({where_values})"
            extra_select = ','.join(select)
            total_select = ', '.join([f"'전체' AS {col.strip()}" for col in extra_select.split(',')])
            sql = f"""
            SELECT * FROM (
                SELECT 설문조사방식명, 조사년도, 반기구분명, 거래은행명, 채널명,
                       '전체' AS 고객경험단계명, {extra_select},
                       COUNT(*) AS 응답고객수,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 긍정비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 부정비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='칭찬' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 칭찬비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 불만비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='개선' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 개선비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='기타' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 기타비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI,
                       0 AS 정렬순서
                FROM {view_id}
                {where} AND VOC필터링여부=0
                GROUP BY 설문조사방식명, 조사년도, 반기구분명, 거래은행명, 채널명, {extra_select}
                UNION ALL
                SELECT 설문조사방식명, 조사년도, 반기구분명, 거래은행명, 채널명,
                       고객경험단계명, {extra_select},
                       COUNT(*) AS 응답고객수,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 긍정비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 부정비율,
                       ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='칭찬' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 칭찬비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 불만비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='개선' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 개선비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='기타' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 기타비율,
                       ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI,
                       2 AS 정렬순서
                FROM {view_id}
                {where} AND VOC필터링여부=0
                GROUP BY 설문조사방식명, 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, {extra_select}
            ) T
            ORDER BY 조사년도 DESC, 반기구분명 DESC, 채널명, 정렬순서, 고객경험단계명, {extra_select}
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)


    # ─────────────────────────────────────────────────────────────
    # TD VOC 원문 (1개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_td_voc_raw(
        reason: str,
        조사년도: Optional[list] = [],
        반기구분명: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        거래은행명: Optional[list] = ["KB국민은행", "신한은행", "하나은행", "NH농협은행", "우리은행", "토스뱅크", "카카오뱅크"],
        고객감정대분류명: Optional[list] = [],
        고객경험VOC유형명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        VOC원문내용: Optional[list] = []
    ) -> str:
        """TD VOC 내용(=원문)을 조회합니다.
        사용 상황: TD VOC 원문, VOC 내용 분석, VOC 분석, 실제 의견 확인.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 조사년도: YYYY
            - 반기구분명: 상반기,하반기
            - 채널명: 은행,브랜드,플랫폼,대면채널,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 거래은행명: KB국민은행,신한은행,하나은행,NH농협은행,우리은행,토스뱅크,카카오뱅크
            - 고객감정대분류명: 긍정,부정
            - 고객경험VOC유형명: 칭찬,불만,개선,기타
            - 성별내용: 남자,여자
            - 연령10세내용: 20대,30대,40대,50대,60대
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - VOC원문내용: REGEXP로 특정 키워드를 포함한 VOC를 찾고싶을때 사용하는 키워드 목록"""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        view_id = "td_voc_raw"
        # where 절 만들기
        extra_select = []
        where = "WHERE"
        for arg in args:
            if values[arg]:
                if arg in ['성별내용', '연령10세내용', '고객등급내용']:
                    extra_select.append(arg)
                where_values = ','.join([f"'{v}'" for v in values[arg]])
                if where != "WHERE":
                    if arg == 'VOC원문내용':
                        where_values = where_values.replace("','", "|")
                        where += f" AND {arg} REGEXP {where_values}"
                    else:
                        where += f" AND {arg} IN ({where_values})"
                else:
                    if arg == 'VOC원문내용':
                        where_values = where_values.replace("','", "|")
                        where += f" {arg} REGEXP {where_values}"
                    else:
                        where += f" {arg} IN ({where_values})"
        select_cols = CUSTOM_SELECT.get(view_id, "*")    
        if extra_select: # 스펙트럼 컬럼도 select 추가
            select_cols += ',' + ','.join(extra_select) 
        sql = f"""
        SELECT {select_cols}
        FROM {view_id}
        {where} AND VOC필터링여부=0
        ORDER BY 조사년도 DESC, 반기구분명 DESC
        LIMIT 300
        """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU 사전집계 NPS (4개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_channel_nps(
        reason: str,
        기준년월일: Optional[list] = [],
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        누적방식: Literal['연간누적', '월누적'] = '연간누적',
    ) -> str:
        """BU 채널별 NPS 점수를 조회합니다.
        사용 상황: BU 채널 NPS 현황, BU 전체 채널 비교, BU NPS 조회.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - 누적방식: NPS계산에 사용되는 데이터의 최초 누적 시점을 연초로 계산한 방법과 월초로 계산한 방법 중 1개를 선택."""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용 or 지역영업그룹명):
            if 누적방식 == "연간누적":
                view_id = "bu_channel_nps"
                from_clause = f"FROM (SELECT *, SUBSTRING(기준년월일, 1, 6) AS 기준년월 FROM {view_id}) A"
            else: # 월누적
                view_id = "bu_channel_nps_trend"
                from_clause = f"FROM {view_id}"
            # where 절 만들기
            where = make_bu_where_and_extra_select(view_id, args, values)
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
            {from_clause}
            {where}
            ORDER BY 기준년월일 DESC, 채널명 ASC
            LIMIT 500
            """
        else:
            view_id = "bu_spectrum_nps"
            # select & where 절 만들기
            where, extra_select = make_bu_spectrum_where_and_extra_select(args, values)
            # 스펙트럼 차원 제외한 기본 WHERE (전체 집계용)
            spectrum_dim = {'성별내용', '연령10세내용', '고객등급내용', '지역영업그룹명'}
            base_args = [a for a in args if a not in spectrum_dim]
            base_where, _ = make_bu_spectrum_where_and_extra_select(base_args, values)
            total_select = ', '.join([f"'전체' AS {col.strip()}" for col in extra_select.split(',')])

            if 누적방식 == "연간누적":
                start_date_format = "%Y0101"
            else: # 월누적
                start_date_format = "%Y%m01"

            # 기준년월 → 실제 데이터 기반 기준년월일 변환 (인덱스 레인지 스캔)
            dates = make_dates_cte(view_id, 기준년월, 기준년월일)
            if not dates:
                return "기준년월 또는 기준년월일 지정이 필요합니다."

            where = where.replace("기준년월일", "d.기준년월일")
            base_where = base_where.replace("기준년월일", "d.기준년월일")

            if '상품' in 채널명:
                # 기존 where절의 채널명 in ('상품') 조건 제거
                where = re.sub(
                    r"(채널명\s*IN\s*\()([^)]*)(\))",
                    lambda m: m.group(1) +
                            re.sub(r"(?:^\s*'상품'\s*,\s*|\s*,\s*'상품'\s*(?=,|$))", "", m.group(2)).strip() +
                            m.group(3),
                    where
                )
                product_where = re.sub(r"(채널명\s*IN\s*\()([^)]*)(\))", "채널명 IN ('상품')", where)
                # 상품 채널 NPS의 경우, 고객경험단계명 = '해당무' 없어야함.
                product_query = f"""
                UNION ALL
                SELECT
                    d.기준년월일,
                    t.채널명,
                    {extra_select},
                    COUNT(*) AS 전체건수,
                    SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 추천비율,
                    SUM(CASE WHEN t.고객충성도내용='중립' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 중립비율,
                    SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 비추천비율,
                    ROUND(
                    (SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END)
                        - SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END))
                    / NULLIF(COUNT(*), 0) * 100.0, 2
                    ) AS NPS점수
                FROM {view_id} t
                JOIN dates d
                    ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
                {product_where}
                GROUP BY d.기준년월일, t.채널명, {extra_select}
                """
            else:
                product_query = ''

            sql = f"""
            WITH dates AS (
              {dates}
            ), AGGR AS (
              SELECT
                d.기준년월일,
                t.채널명,
                {extra_select},
                COUNT(*) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 추천비율,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 중립비율,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 비추천비율,
                ROUND(
                  (SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END)
                    - SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END))
                  / NULLIF(COUNT(*), 0) * 100.0, 2
                ) AS NPS점수
              FROM {view_id} t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              {where} AND t.고객경험단계명 = '해당무'
              GROUP BY d.기준년월일, t.채널명, {extra_select}
              {product_query}
            )
            SELECT * FROM (
              SELECT DISTINCT *, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식, 1 AS 정렬순서 FROM AGGR
              UNION ALL
              SELECT DISTINCT d.기준년월일, t.채널명, {total_select},
                COUNT(*) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 추천비율,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 중립비율,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 비추천비율,
                ROUND(
                  (SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END)
                    - SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END))
                  / NULLIF(COUNT(*), 0) * 100.0, 2
                ) AS NPS점수,
                'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식, 0 AS 정렬순서
              FROM {view_id} t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              {base_where} AND t.고객경험단계명 = '해당무'
              GROUP BY d.기준년월일, t.채널명
            ) T
            ORDER BY 기준년월일 DESC, 채널명 ASC, 정렬순서, {extra_select}
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_cx_stage_nps(
        reason: str,
        기준년월일: Optional[list] = [],
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        누적방식: Literal['연간누적', '월누적'] = '연간누적',
    ) -> str:
        """BU 고객경험단계별 NPS점수를 조회합니다.
        사용 상황: BU 고객경험단계 NPS 분석, 현황, 어느 단계가 약한지.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - 누적방식: NPS계산에 사용되는 데이터의 최초 누적 시점을 연초로 계산한 방법과 월초로 계산한 방법 중 1개를 선택."""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용 or 지역영업그룹명):
            if 누적방식 == "연간누적":
                view_id = "bu_cx_stage_nps"
            else: # 월누적
                view_id = "bu_cx_stage_nps_trend"
            # where 절 만들기
            where = make_bu_where_and_extra_select(view_id, args, values)
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
            FROM {view_id}
            {where}
            ORDER BY 기준년월일 DESC, 채널명 ASC
            LIMIT 500
            """
        else:
            view_id = "bu_spectrum_nps"
            # select & where 절 만들기
            where, extra_select = make_bu_spectrum_where_and_extra_select(args, values)
            # 스펙트럼 차원 제외한 기본 WHERE (전체/단계 집계용)
            spectrum_dim = {'성별내용', '연령10세내용', '고객등급내용', '지역영업그룹명'}
            base_args = [a for a in args if a not in spectrum_dim]
            base_where, _ = make_bu_spectrum_where_and_extra_select(base_args, values)
            total_select = ', '.join([f"'전체' AS {col.strip()}" for col in extra_select.split(',')])

            if 누적방식 == "연간누적":
                start_date_format = "%Y0101"
            else: # 월누적
                start_date_format = "%Y%m01"

            # 기준년월 → 실제 데이터 기반 기준년월일 변환 (인덱스 레인지 스캔)
            dates = make_dates_cte(view_id, 기준년월, 기준년월일)
            if not dates:
                return "기준년월 또는 기준년월일 지정이 필요합니다."
            where = where.replace("기준년월일", "d.기준년월일")
            base_where = base_where.replace("기준년월일", "d.기준년월일")
            sql = f"""
            WITH dates AS (
              {dates}
            ), AGGR AS (
              SELECT
                d.기준년월일,
                t.채널명,
                t.고객경험단계명, {extra_select},
                COUNT(*) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 추천비율,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 중립비율,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 비추천비율,
                ROUND(
                  (SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END)
                    - SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END))
                  / NULLIF(COUNT(*), 0) * 100.0, 2
                ) AS NPS점수
              FROM {view_id} t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              {where} AND t.고객경험단계명 != '해당무'
              GROUP BY d.기준년월일, t.채널명, t.고객경험단계명, {extra_select}
            )
            SELECT * FROM (
              SELECT DISTINCT *, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식, 1 AS 정렬순서 FROM AGGR
              UNION ALL
              SELECT DISTINCT d.기준년월일, t.채널명,
                '전체' AS 고객경험단계명, {extra_select},
                COUNT(*) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 추천비율,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 중립비율,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100.0 AS 비추천비율,
                ROUND(
                  (SUM(CASE WHEN t.고객충성도내용='추천' THEN 1 ELSE 0 END)
                    - SUM(CASE WHEN t.고객충성도내용='비추천' THEN 1 ELSE 0 END))
                  / NULLIF(COUNT(*), 0) * 100.0, 2
                ) AS NPS점수,
                'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식, 0 AS 정렬순서
              FROM {view_id} t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              {where} AND t.고객경험단계명 != '해당무'
              GROUP BY d.기준년월일, t.채널명, {extra_select}
            ) T
            ORDER BY 기준년월일 DESC, 채널명 ASC, 정렬순서, 고객경험단계명, {extra_select}
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU 사전집계 영향요인 (4개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_channel_driver(
        reason: str,
        기준년월일: Optional[list] = [],
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        누적방식: Literal['연간누적', '월누적'] = '연간누적',
    ) -> str:
        """BU 채널 NPS의 영향요인(=원인)을 조회합니다. 어떤 고객경험단계가 채널 NPS에 기여하는지.
        사용 상황: BU 채널 영향요인.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - 누적방식: NPS계산에 사용되는 데이터의 최초 누적 시점을 연초로 계산한 방법과 월초로 계산한 방법 중 1개를 선택."""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용 or 지역영업그룹명):
            if 누적방식 == "연간누적":
                view_id = "bu_channel_driver"
                from_clause = f"FROM (SELECT *, SUBSTRING(기준년월일, 1, 6) AS 기준년월 FROM {view_id}) A"
            else: # 월누적
                view_id = "bu_channel_driver_trend"
                from_clause = f"FROM {view_id}"
            # where 절 만들기
            where = make_bu_where_and_extra_select(view_id, args, values)
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
            {from_clause}
            {where}
            ORDER BY 기준년월일 DESC, 채널명 DESC
            LIMIT 500
            """
        else:
            view_id = "bu_spectrum_nps"
            # select & where 절 만들기
            where, extra_select = make_bu_spectrum_where_and_extra_select(args, values)

            if 누적방식 == "연간누적":
                start_date_format = "%Y0101"
            else: # 월누적
                start_date_format = "%Y%m01"

            # 기준년월 → 실제 데이터 기반 기준년월일 변환 (인덱스 레인지 스캔)
            dates = make_dates_cte(view_id, 기준년월, 기준년월일)
            if not dates:
                return "기준년월 또는 기준년월일 지정이 필요합니다."

            if '상품' in 채널명:
                # 기존 where절의 채널명 in ('상품') 조건 제거
                where = re.sub(
                    r"(채널명\s*IN\s*\()([^)]*)(\))",
                    lambda m: m.group(1) +
                            re.sub(r"(?:^\s*'상품'\s*,\s*|\s*,\s*'상품'\s*(?=,|$))", "", m.group(2)).strip() +
                            m.group(3),
                    where
                )
                product_where = re.sub(r"(채널명\s*IN\s*\()([^)]*)(\))", "채널명 IN ('상품')", where)
                # 상품 채널 NPS의 경우, 고객경험단계명 = '해당무' 없어야함.
                product_query = f"""
                UNION ALL 
                SELECT
                d.기준년월일,
                t.채널명,
                고객경험단계명,
                {extra_select},
                sum(갯수) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
                FROM (
                  select 기준년월일, 기준년월, 채널명, 고객경험단계명, {extra_select}, 고객충성도내용, count(*) as 갯수 
                  from {view_id}
                  {product_where}
                  group by 기준년월일, 기준년월, 채널명, 고객경험단계명, {extra_select}, 고객충성도내용
                  ) t
                JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
                GROUP BY d.기준년월일, 기준년월, t.채널명, {extra_select}, 고객경험단계명
                """
            else:
                product_query = ''

            sql = f"""
            WITH dates AS (
              {dates}
            ), AGGR AS (
              SELECT
                d.기준년월일,
                t.채널명,
                영향요인구분명,
                {extra_select},
                sum(갯수) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
              FROM (
            	  select 기준년월일, 기준년월, 채널명, 영향요인구분명, {extra_select}, 고객충성도내용, count(*) as 갯수 
            	  from {view_id}
            	  {where} AND 고객경험단계명 = '해당무'
            	  group by 기준년월일, 기준년월, 채널명, 영향요인구분명, {extra_select}, 고객충성도내용
            	  ) t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              GROUP BY d.기준년월일, 기준년월, t.채널명, {extra_select}, 영향요인구분명
              {product_query}
            )
            select distinct *, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
            from (
            	SELECT
            		기준년월일, 채널명, 영향요인구분명
            		, {extra_select}
            		, 추천건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 추천비율
            		, 중립건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 중립비율
            		, 비추천건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 비추천비율
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 AS NPS영향도점수
            	FROM AGGR) A
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_bu_cx_stage_driver(
        reason: str,
        기준년월일: Optional[list] = [],
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        누적방식: Literal['연간누적', '월누적'] = '연간누적',
    ) -> str:
        """BU 고객경험단계 NPS의 영향요인(=원인)을 조회합니다. 어떤 서비스품질요소가 경험단계 NPS에 기여하는지.
        사용 상황: BU 고객경험단계 영향요인, 원인 분석.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - 누적방식: NPS계산에 사용되는 데이터의 최초 누적 시점을 연초로 계산한 방법과 월초로 계산한 방법 중 1개를 선택."""
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용 or 지역영업그룹명):
            if 누적방식 == "연간누적":
                view_id = "bu_cx_stage_driver"
                from_clause = f"FROM (SELECT *, SUBSTRING(기준년월일, 1, 6) AS 기준년월 FROM {view_id}) A"
            else: # 월누적
                view_id = "bu_cx_stage_driver_trend"
                from_clause = f"FROM {view_id}"
            # where 절 만들기
            where = make_bu_where_and_extra_select(view_id, args, values)
            select_cols = CUSTOM_SELECT.get(view_id, "*")
            sql = f"""
            SELECT {select_cols}, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
            {from_clause}
            {where}
            ORDER BY 기준년월일 DESC, 채널명 ASC
            LIMIT 500
            """
        else:
            view_id = "bu_spectrum_nps"
            # select & where 절 만들기
            where, extra_select = make_bu_spectrum_where_and_extra_select(args, values)

            if 누적방식 == "연간누적":
                start_date_format = "%Y0101"
            else: # 월누적
                start_date_format = "%Y%m01"

            # 기준년월 → 실제 데이터 기반 기준년월일 변환 (인덱스 레인지 스캔)
            dates = make_dates_cte(view_id, 기준년월, 기준년월일)
            if not dates:
                return "기준년월 또는 기준년월일 지정이 필요합니다."
            sql = f"""
            WITH dates AS (
              {dates}
            ), AGGR AS (
              SELECT
                d.기준년월일,
                t.채널명,
                t.고객경험단계명,
                t.영향요인구분명,
                {extra_select},
                sum(갯수) AS 전체건수,
                SUM(CASE WHEN t.고객충성도내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수,
                SUM(CASE WHEN t.고객충성도내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수,
                SUM(CASE WHEN t.고객충성도내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
              FROM (
            	  select 기준년월일, 기준년월, 채널명, 고객경험단계명, 영향요인구분명, {extra_select}, 고객충성도내용, count(*) as 갯수 
            	  from {view_id}
            	  {where} AND 고객경험단계명 != '해당무'
            	  group by 기준년월일, 기준년월, 채널명, 고객경험단계명, 영향요인구분명, {extra_select}, 고객충성도내용
            	  ) t
              JOIN dates d
                ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
              GROUP BY d.기준년월일, 기준년월, t.채널명, 고객경험단계명, {extra_select}, 영향요인구분명
              )
              select distinct *, 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식
              from (
                SELECT
            		기준년월일, 채널명, 고객경험단계명, 영향요인구분명
            		, {extra_select}
            		, 추천건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 추천비율
            		, 중립건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 중립비율
            		, 비추천건수 / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 as 비추천비율
            		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY  기준년월일, 채널명, {extra_select})*100 AS NPS영향도점수
            	FROM AGGR) A
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # BU VOC 사전집계 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_cx_element_voc(
        reason: str,
        기준년월일: Optional[list] = [],
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        성별내용: Optional[list] = [],
        연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        누적방식: Literal['연간누적', '월누적'] = '연간누적',
        상세조회: bool = False,
    ) -> str:
        """BU 채널 또는 고객경험단계 별로 수집된 VOC의 감정(긍정/부정) 또는 유형(칭찬/불만/개선/기타) 분포를 조회합니다. NSS(감정지수), CCI(고객불만지수) 포함. (VOC 내용(=원문)은 미포함).
        사용 상황: BU VOC 감정분석, VOC 유형 분석, 긍정/부정 비율. 불만 비율.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - 누적방식: NPS계산에 사용되는 데이터의 최초 누적 시점을 연초로 계산한 방법과 월초로 계산한 방법 중 1개를 선택.
            - 상세조회: False(기본)=채널 전체합산+고객경험단계별 집계 반환. True=사용자가 서비스품질명 또는 고객경험요소명 단위까지 명시적으로 요청한 경우에만 사용."""
        frame = inspect.currentframe()
        assert frame is not None
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        if not (성별내용 or 연령10세내용 or 고객등급내용 or 지역영업그룹명):
            if 누적방식 == "연간누적":
                view_id = "bu_cx_element_voc"
                base_from = f"(SELECT *, SUBSTRING(기준년월일, 1, 6) AS 기준년월 FROM {view_id})"
            else: # 월누적
                view_id = "bu_cx_element_voc_monthly"
                base_from = view_id
            # where 절 만들기
            where = make_bu_where_and_extra_select(view_id, args, values)
            # 날짜 조건 없으면 FILTERED 내 최신 날짜를 LATEST CTE로 자동 결정
            # (글로벌 MAX 대신 채널명 등 조회 조건 내 MAX를 사용)
            if not 기준년월일 and not 기준년월:
                latest_cte = ",\n            LATEST AS (SELECT MAX(기준년월일) AS max_dt FROM FILTERED)"
                latest_dt_filter = "AND 기준년월일 = (SELECT max_dt FROM LATEST)"
            else:
                latest_cte = ""
                latest_dt_filter = ""
            if 상세조회:
                # 서비스품질명/고객경험요소명 단위 상세 (사용자가 명시 요청 시)
                detail_sql = f"""
                SELECT 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식,
                       기준년월일, 기준년월, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명,
                       전체건수, 긍정비율, 중립비율, 부정비율, NSS점수,
                       칭찬비율, 불만비율, 개선비율, 기타비율, CCI점수,
                       1 AS 정렬순서
                FROM FILTERED WHERE 1=1 {latest_dt_filter}"""
            else:
                # 기본: 고객경험단계명까지만 집계 (행 수 최소화)
                detail_sql = f"""
                SELECT 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식,
                       기준년월일, 기준년월, 채널명, 고객경험단계명,
                       '전체' AS 서비스품질명, '전체' AS 고객경험요소명,
                       SUM(전체건수) AS 전체건수,
                       ROUND(SUM(긍정건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 긍정비율,
                       ROUND(SUM(중립건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 중립비율,
                       ROUND(SUM(부정건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 부정비율,
                       ROUND((SUM(긍정건수)-SUM(부정건수))*100.0/NULLIF(SUM(전체건수),0),2) AS NSS점수,
                       ROUND(SUM(칭찬건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 칭찬비율,
                       ROUND(SUM(불만건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 불만비율,
                       ROUND(SUM(개선건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 개선비율,
                       ROUND(SUM(기타건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 기타비율,
                       ROUND(SUM(불만건수)*100.0/NULLIF(SUM(전체건수),0),2) AS CCI점수,
                       1 AS 정렬순서
                FROM FILTERED WHERE 1=1 {latest_dt_filter}
                GROUP BY 기준년월일, 기준년월, 채널명, 고객경험단계명"""
            sql = f"""
            WITH FILTERED AS (
                SELECT * FROM {base_from} A
                {where}
            ){latest_cte}
            SELECT * FROM (
                SELECT 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식,
                       기준년월일, 기준년월, 채널명,
                       '전체' AS 고객경험단계명, '전체' AS 서비스품질명, '전체' AS 고객경험요소명,
                       SUM(전체건수) AS 전체건수,
                       ROUND(SUM(긍정건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 긍정비율,
                       ROUND(SUM(중립건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 중립비율,
                       ROUND(SUM(부정건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 부정비율,
                       ROUND((SUM(긍정건수)-SUM(부정건수))*100.0/NULLIF(SUM(전체건수),0),2) AS NSS점수,
                       ROUND(SUM(칭찬건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 칭찬비율,
                       ROUND(SUM(불만건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 불만비율,
                       ROUND(SUM(개선건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 개선비율,
                       ROUND(SUM(기타건수)*100.0/NULLIF(SUM(전체건수),0),2) AS 기타비율,
                       ROUND(SUM(불만건수)*100.0/NULLIF(SUM(전체건수),0),2) AS CCI점수,
                       0 AS 정렬순서
                FROM FILTERED WHERE 1=1 {latest_dt_filter}
                GROUP BY 기준년월일, 기준년월, 채널명
                UNION ALL
                {detail_sql}
            ) T
            ORDER BY 기준년월일 DESC, 채널명 DESC, 정렬순서, 고객경험단계명
            LIMIT 500
            """
        else:
            view_id = "bu_spectrum_voc"

            if 누적방식 == "연간누적":
                start_date_format = "%Y0101"
            else:
                start_date_format = "%Y%m01"

            # 날짜 조건 제외한 where 절 (날짜는 BETWEEN JOIN으로 처리)
            date_args = {'기준년월', '기준년월일'}
            args_no_date = [a for a in args if a not in date_args]
            where_no_date, extra_select = make_bu_spectrum_where_and_extra_select(args_no_date, values, view_id=view_id)

            # 기준년월 → 실제 데이터 기반 기준년월일 변환 (인덱스 레인지 스캔)
            dates = make_dates_cte(view_id, 기준년월, 기준년월일)
            if not dates:
                # 날짜 미지정 → 조건 내 최신 날짜 자동 결정
                dates = f"SELECT MAX(기준년월일) AS 기준년월일 FROM {view_id} {where_no_date} AND VOC필터링여부=0"

            sql = f"""
            WITH dates AS (
              {dates}
            )
            SELECT * FROM (
                SELECT 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식,
                       d.기준년월일, SUBSTRING(d.기준년월일,1,6) AS 기준년월, t.채널명,
                       '전체' AS 고객경험단계명, {extra_select},
                       COUNT(*) AS 응답고객수,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 긍정비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 부정비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN t.고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='칭찬' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 칭찬비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 불만비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='개선' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 개선비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='기타' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 기타비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI,
                       0 AS 정렬순서
                FROM {view_id} t
                JOIN dates d
                  ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
                {where_no_date} AND t.VOC필터링여부=0
                GROUP BY d.기준년월일, t.채널명, {extra_select}
                UNION ALL
                SELECT 'BU' AS 설문조사방식명, '{누적방식}' AS 누적방식,
                       d.기준년월일, SUBSTRING(d.기준년월일,1,6) AS 기준년월, t.채널명,
                       t.고객경험단계명, {extra_select},
                       COUNT(*) AS 응답고객수,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 긍정비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='중립' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 중립비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 부정비율,
                       ROUND(SUM(CASE WHEN t.고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) - ROUND(SUM(CASE WHEN t.고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='칭찬' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 칭찬비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 불만비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='개선' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 개선비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='기타' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS 기타비율,
                       ROUND(SUM(CASE WHEN t.고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI,
                       2 AS 정렬순서
                FROM {view_id} t
                JOIN dates d
                  ON t.기준년월일 BETWEEN DATE_FORMAT(STR_TO_DATE(d.기준년월일, '%Y%m%d'), '{start_date_format}') AND d.기준년월일
                {where_no_date} AND t.VOC필터링여부=0
                GROUP BY d.기준년월일, t.채널명, t.고객경험단계명, {extra_select}
            ) T
            ORDER BY 기준년월일 DESC, 채널명 DESC, 정렬순서, 고객경험단계명, {extra_select}
            LIMIT 500
            """
        return await _run_query(sql, view_id, mcp_executor, user_id)


    # ─────────────────────────────────────────────────────────────
    # BU VOC 원문 (1개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_bu_voc_raw(
        reason: str,
        기준년월일: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        고객감정대분류명: Optional[list] = [],
        고객경험VOC유형명: Optional[list] = [],
        성별내용: Optional[list] = [],
        설문고객연령10세내용: Optional[list] = [],
        고객등급내용: Optional[list] = [],
        지역영업그룹명: Optional[list] = [],
        VOC원문내용: Optional[list] = []
    ) -> str:
        """BU VOC 내용(=원문)을 조회합니다. 토큰을 고려하여 최대 추출 가능한 voc 개수 300개 제한이 있으니 이를 고려하여 기준년월일을 잘 설정하세요.
        사용 상황: BU VOC 원문, VOC 내용 분석, VOC 분석, 실제 의견 확인.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월일: YYYYMMDD
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 고객감정대분류명: 긍정,부정
            - 고객경험VOC유형명: 칭찬,불만,개선,기타
            - 성별내용: 남자,여자
            - 연령10세내용: 20,30,40,50,60
            - 고객등급내용: 베스트,패밀리,그랜드,VIP,VVIP
            - 지역영업그룹명: 충청호남영업추진그룹,수도권영업추진그룹,강남영업추진그룹,강북영업추진그룹,영남영업추진그룹
            - VOC원문내용: REGEXP로 특정 키워드를 포함한 VOC를 찾고싶을때 사용하는 키워드 목록"""
        frame = inspect.currentframe()
        assert frame is not None
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        view_id = "bu_voc_raw"
        # where 절 만들기
        extra_select = []
        where = "WHERE"
        for arg in args:
            if values[arg]:
                if arg in ['성별내용', '설문고객연령10세내용', '고객등급내용', '지역영업그룹명']:
                    extra_select.append(arg)
                where_values = ','.join([f"'{v}'" for v in values[arg]])
                if where != "WHERE":
                    if arg == 'VOC원문내용':
                        where_values = where_values.replace("','", "|")
                        where += f" AND {arg} REGEXP {where_values}"
                    else:
                        where += f" AND {arg} IN ({where_values})"
                else:
                    if arg == 'VOC원문내용':
                        where_values = where_values.replace("','", "|")
                        where += f" AND {arg} REGEXP {where_values}"
                    else:
                        where += f" {arg} IN ({where_values})"
        select_cols = CUSTOM_SELECT.get(view_id, "*")    
        if extra_select: # 스펙트럼 컬럼도 select 추가
            select_cols += ',' + ','.join(extra_select) 
        sql = f"""
        SELECT {select_cols}, 'BU' AS 설문조사방식명
        FROM {view_id}
        {where} AND VOC필터링여부=0
        ORDER BY 기준년월일 DESC
        LIMIT 300
        """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    # ─────────────────────────────────────────────────────────────
    # 개선조치 (2개)
    # ─────────────────────────────────────────────────────────────

    @tool
    async def query_improvement_by_dept(
        reason: str,
        기준년월: Optional[list] = [],
        개선사업그룹명: Optional[list] = [],
        개선부서명: Optional[list] = [],
    ) -> str:
        """부서 또는 그룹별 배분된 VOC에 대한 조치 현황을 조회합니다. BU 전용. 특정 기간에 대한 언급이 없다면 기본 옵션은 이번달.
        사용 상황: BU 배분 VOC 조치 현황, 개선부서 또는 그룹별 배분/처리/검토완료/미처리/개선예정 건수.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월: YYYYMM
            - 개선사업그룹명: 개선사업그룹명
            - 개선부서명: 개선부서명"""
        frame = inspect.currentframe()
        assert frame is not None
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        view_id = "improvement_by_dept"
        # where 절 만들기
        where = "WHERE"
        for arg in args:
            if values[arg]:
                where_values = ','.join([f"'{v}'" for v in values[arg]])
                if where != "WHERE":
                    where += f" AND {arg} IN ({where_values})"
                else:
                    where += f" {arg} IN ({where_values})"
        sql = f"""
        SELECT 기준년월,
        개선사업그룹명, 개선부서명,
        SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수,
        SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END) AS 검토건수,
        ROUND(SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END)*100.0 / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 검토율,
        SUM(CASE WHEN 과제검토명='현행유지' THEN 1 ELSE 0 END) AS 현행유지건수,
        SUM(CASE WHEN 과제검토명='개선예정' THEN 1 ELSE 0 END) AS 개선예정건수,
        SUM(CASE WHEN 과제검토명='개선불가' THEN 1 ELSE 0 END) AS 개선불가건수
        FROM {view_id}
        {where} AND 배분여부=1
        GROUP BY 기준년월, 개선사업그룹명, 개선부서명
        ORDER BY 기준년월 DESC, 개선사업그룹명, 개선부서명
        LIMIT 500
        """
        return await _run_query(sql, view_id, mcp_executor, user_id)

    @tool
    async def query_improvement_by_factor(
        reason: str,
        기준년월: Optional[list] = [],
        채널명: Optional[list] = [],
        고객경험단계명: Optional[list] = [],
        서비스품질명: Optional[list] = [],
        고객경험요소명: Optional[list] = [],
    ) -> str:
        """채널/고객경험단계/서비스품질요소/고객경험요소 별 배분된 VOC에 대한 조치 현황을 조회합니다. BU 전용. 특정 기간에 대한 언급이 없다면 기본 옵션은 이번달.
        사용 상황: BU 배분 VOC 조치 현황, 채널/고객경험단계/서비스품질요소/고객경험요소 별 배분/처리/검토완료/미처리/개선예정 건수.
        파라미터 설명:
            - reason: 사용자에게 보여질 진행 상황 요약 (해당 툴 선택 사유 등)
            - 기준년월: YYYYMM
            - 채널명: KB 스타뱅킹,영업점,고객센터,상품
            - 고객경험단계명: 고객경험단계명
            - 서비스품질명: 서비스품질요소명
            - 고객경험요소명: 고객경험요소명"""
        frame = inspect.currentframe()
        assert frame is not None
        args, _, _, values = inspect.getargvalues(frame)
        args.pop(0) # reason 버리기 (함수 내에서는 필요 없음)

        view_id = "improvement_by_dept"
        # where 절 만들기
        where = "WHERE"
        for arg in args:
            if values[arg]:
                where_values = ','.join([f"'{v}'" for v in values[arg]])
                if where != "WHERE":
                    where += f" AND {arg} IN ({where_values})"
                else:
                    where += f" {arg} IN ({where_values})"
        sql = f"""
        SELECT 기준년월, 
        채널명, 고객경험단계명, 서비스품질명, 고객경험요소명,
        SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수,
        SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END) AS 검토건수,
        ROUND(SUM(CASE WHEN 과제진행상태명='승인완료' THEN 1 ELSE 0 END)*100.0 / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 검토율,
        SUM(CASE WHEN 과제검토명='현행유지' THEN 1 ELSE 0 END) AS 현행유지건수,
        SUM(CASE WHEN 과제검토명='개선예정' THEN 1 ELSE 0 END) AS 개선예정건수,
        SUM(CASE WHEN 과제검토명='개선불가' THEN 1 ELSE 0 END) AS 개선불가건수
        FROM {view_id}
        {where} AND 배분여부=1
        GROUP BY 기준년월, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명
        ORDER BY 기준년월 DESC, 채널명, 고객경험단계명, 서비스품질명
        LIMIT 500
        """
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
    def tool_skip(
        reason: str,
    ) -> str:
        """툴 호출이 필요하지 않은 경우 해당 툴 호출.
        재진입 시 수집된 데이터가 충분하여 추가 조회가 불필요한 경우에도 해당 툴 호출.
        파라미터 설명:
            - reason: 툴을 호출하지 않는 구체적 사유 (예: "이미 충분한 데이터 확보", "해당 조사유형에 뷰 미존재")."""
        return json.dumps(
            {"skipped": True, "reason": reason},
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
        # # TD VOC
        query_td_voc_type,
        query_td_voc_raw,
        # # BU NPS
        query_bu_channel_nps,
        query_bu_cx_stage_nps,
        query_bu_channel_driver,
        query_bu_cx_stage_driver,
        # # BU VOC
        query_bu_cx_element_voc,
        query_bu_voc_raw,
        # # 개선조치
        query_improvement_by_dept,
        query_improvement_by_factor,
        # 유틸리티
        # get_latest_data_info,
        # tool_skip,
    ]
