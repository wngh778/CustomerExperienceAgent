"""InsightAgent 스키마 정의 — Single Source of Truth.

24개 뷰의 메타데이터(카탈로그) + DDL + SQL 템플릿 + 주의사항을
``VIEW_REGISTRY`` dict 하나로 통합 관리한다. 스키마 변경 시 이 파일만 수정하면 된다.

LLM 프롬프트에는 가독성 좋은 별칭(v_xxx)을 사용하고,
실행 시 :mod:`catalog` 모듈의 ``translate_sql()`` 로 INST1.TSCCVMG* 테이블명으로 변환한다.

주요 공개 심볼:
    - ``VIEW_REGISTRY``: 뷰 정의 딕셔너리 (ViewDef 타입)
    - ``QUERY_CATALOG``: query_planner용 카탈로그 리스트
    - ``SCHEMA_OVERVIEW``: 전체 스키마 요약 텍스트
    - ``format_view_details_for_prompt()``: DDL+템플릿을 프롬프트용 텍스트로 변환
"""

from pathlib import Path
from typing import Any, TypedDict

import pandas as pd


# =============================================================================
# ViewDef 타입
# =============================================================================

class ViewDef(TypedDict, total=False):
    """VIEW_REGISTRY에 저장되는 뷰 정의 타입.

    Layer 1 (카탈로그) 필드는 query_planner가 뷰 선택에 사용하고,
    Layer 2 (상세) 필드는 sql_generator가 SQL 작성에 사용한다.
    """

    # --- 카탈로그 (Layer 1: Query Planner) ---
    view: str                    # 뷰 별칭 (v_xxx) — LLM이 SQL에 사용
    description: str             # 뷰 설명
    survey_type: str             # TD / BU / TD/BU
    use_when: str                # 이 뷰를 선택해야 하는 상황
    dim_columns: list[str]       # WHERE 조건 가능한 DIM 컬럼
    brief_columns: str           # Layer 1 플래너용 컬럼 요약

    # --- 상세 (Layer 2: SQL Generator) ---
    ddl: str                     # CREATE TABLE DDL
    templates: list[str]         # SQL 예제 쿼리
    notes: str                   # 주의사항


# =============================================================================
# Schema Overview (query_planner 프롬프트에 내장)
# =============================================================================

def _load_cx_hierarchy_text() -> str:
    """cx_hierarchy.txt에서 CX 계층을 읽어 조사방식 → 채널 → 경험단계(서비스품질요소) 텍스트로 변환."""
    hierarchy_path = Path(__file__).parent / "cx_hierarchy.txt"
    try:
        df = pd.read_csv(hierarchy_path, sep="\t", dtype=str)
    except Exception:
        return "  (CX 계층 파일 로드 실패)"

    lines: list[str] = []
    for survey_type, st_group in df.groupby("조사방식", sort=False):
        lines.append(f"  {survey_type} 채널:")
        for channel, ch_group in st_group.groupby("채널", sort=False):
            stages: list[str] = []
            for stage, sg_group in ch_group.groupby("고객경험단계", sort=False):
                factors = sg_group["서비스품질요소"].dropna().unique().tolist()
                if factors:
                    stages.append(f"{stage}({', '.join(factors)})")
                else:
                    stages.append(stage)
            lines.append(f"    {channel} → {', '.join(stages)}")
    return "\n".join(lines)


SCHEMA_OVERVIEW = """\

◆ 원본 데이터 주요 칼럼

  공통 수치: 추천점수(0~10, 9-10추천/7-8중립/0-6비추천, F1 제외), 추천의향내용(추천/중립/비추천), 고객충성도내용(추천/중립/비추천)
  공통 VOC: VOC원문내용, 고객감정대분류명(긍정/부정/중립), 고객경험VOC유형명(칭찬/개선/불만/기타)
  공통 분류: 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명(동적 추출)
  공통 필터: VOC필터링여부(0=정상처리, 1=필터링-무의미/짧은응답 등 추출불가), 고객경험요소분류성공여부(0/1)
  참고: 서술형 마스터의 칼럼명은 서비스품질명 (서비스품질요소명과 의미 동일)

  TD 전용: 거래은행명(KB국민은행,신한은행,우리은행,하나은행,NH농협은행,카카오뱅크,토스뱅크), 반기구분명(상반기/하반기), 고객경험정답감정명(만족/아쉬움/실망/감정없음/짜증/감동/기대)
  BU 전용: 에피소드유형내용, 개선부서명, 배분여부(0/1), 과제진행상태명(검토완료/미처리/검토기한만료), 검토구분(현행유지/개선예정/개선불가)
  TD/BU 서술형 공통: 추천영향요인내용 — 고객경험단계='해당무'이면 고객경험단계가, 그외에는 서비스품질요소가 들어감

  TD 스펙트럼 (MGF1/F2 칼럼):
    성별내용: 남자, 여자
    연령5세내용: 20세이상 24세이하, 25세이상 29세이하, ..., 65세이상 69세이하
    연령10세내용: 20대, 30대, 40대, 50대, 60대
    이용거래기간내용: 1년미만, 1년이상 2년미만, 2년이상 3년미만, 3년이상 5년미만, 5년이상 10년미만, 10년이상
    플랫폼이용빈도내용: 거의 매일, 2~3일에 한 번, 일주일에 한 번, 격주에 한 번, 한달에 한 번
    고객센터이용빈도내용: 1회, 2회, 3~4회, 5회 이상
    영업점이용빈도내용: 1회, 2회, 3~4회, 5~6회, 7~10회, 10회 초과
    고객등급내용: VVIP, VIP, 패밀리, 그랜드, 베스트
    고객충성도내용/추천의향내용: 추천, 중립, 비추천
  BU 스펙트럼 (MGF3/F4 칼럼):
    설문고객연령5세내용: 20, 25, 30, ..., 65, 70
    설문고객연령10세내용: 20, 30,.., 70
    성별내용: 남자, 여자
    거래기간수: 숫자(년 단위, 현재 데이터 부족으로 0이 대부분)
    실질고객내용(TD의 고객등급내용과 동일 값범위): VVIP, VIP, 패밀리, 그랜드, 베스트
    에피소드유형내용: 예금, 대출, 펀드, 해외송금, 외화환전, 주택도시기금, 신탁, 보험, 계리발생, 계좌이체
    에피소드상세내용: 적금, 정기예금, 신용대출, 입출금, 예적금담보대출, 펀드, 해외송금, 외화환전, 주택도시기금대출-전세(버팀목), 신탁, 주택담보대출, 보험, 전월세대출, 주택도시기금대출-구입(디딤돌), 여신상담, 계좌이체, ...
    고객충성도내용/추천의향내용: 추천, 중립, 비추천
    주직무구분명: 비대면 PB고객 담당, 비대면 개인고객 담당, 개인고객담당, PB고객담당, 기업고객담당, ...
    직군구분명: 일반(일반), 사무(PT), 일반(임금피크), 기능, 사무(텔러), 일반직원
    지역영업그룹명:'강남영업추진그룹', '강북영업추진그룹', '수도권영업추진그룹', '영남영업추진그룹', '충청호남영업추진그룹'

◆ 조건값 참조

  거래은행명(TD): KB국민은행, 신한은행, 우리은행, 하나은행, NH농협은행, 카카오뱅크, 토스뱅크
  문제영역명(IPA): 현상유지, 유지관리, 중점개선, 점진개선
  과제진행상태명(BU): 검토완료, 미처리, 검토기한만료
  검토구분: 현행유지, 개선예정, 개선불가 (검토완료 건만)
  배분여부(BU): 0, 1
  고객감정대분류명: 긍정, 부정, 중립
  고객경험VOC유형명: 칭찬, 개선, 불만, 기타
  고객경험정답감정명: 만족, 아쉬움, 실망, 감정 없음, 짜증, 감동, 기대
  개선사업그룹명: 영업기획그룹, 개인고객그룹, 기업고객그룹, 고객컨택영업그룹, 디지털영업그룹, 경영지원그룹, WM고객그룹, 소비자보호그룹
"""




# =============================================================================
# VIEW_REGISTRY — 뷰 통합 정의
# =============================================================================

VIEW_REGISTRY: dict[str, ViewDef] = {
    # =========================================================================
    # TD NPS (사전집계)
    # =========================================================================
    "td_channel_nps": {
        "view": "v_td_channel_nps",
        "description": "TD 채널별 NPS (타행 비교 포함)",
        "survey_type": "TD",
        "use_when": "타행비교, 은행 순위, 경쟁사 비교, 시장평균 대비, TD NPS 현황",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명(7개), 채널명(TD 5채널) | METRIC: 추천비율, 중립비율, 비추천비율, NPS점수",
        "ddl": """\
-- TD 채널별 NPS (타행 비교 포함, 반기별)
CREATE TABLE v_td_channel_nps (
    반기구분명 TEXT,    -- '상반기' | '하반기'
    조사년도 TEXT,      -- '2024', '2025'
    거래은행명 TEXT,    -- 'KB국민은행','신한은행','우리은행','하나은행','NH농협은행','카카오뱅크','토스뱅크'
    채널명 TEXT,       -- TD 5개: '브랜드','플랫폼','대면채널','고객센터','상품'
    추천비중점수 REAL,     -- 추천자(9-10점) %
    중립비중점수 REAL,     -- 중립자(7-8점) %
    비추천비중점수 REAL,   -- 비추천자(0-6점) %
    NPS점수 REAL       -- = 추천비중점수 - 비추천비중점수
);""",
        "templates": [
            "SELECT 거래은행명, 채널명, NPS점수, 추천비중점수, 비추천비중점수\nFROM v_td_channel_nps\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기';",
            "SELECT 거래은행명, NPS점수\nFROM v_td_channel_nps\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 채널명 = '플랫폼'\nORDER BY NPS점수 DESC;",
        ],
        "notes": "타행 비교 시 거래은행명 조건을 생략하면 7개 은행 전체 반환.",
    },
    "td_cx_stage_nps": {
        "view": "v_td_cx_stage_nps",
        "description": "TD 고객경험단계별 NPS (타행 비교 포함)",
        "survey_type": "TD",
        "use_when": "TD 경험단계 분석, 상세 드릴다운, 어느 단계가 약한지",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, 벤치마크은행명 | METRIC: NPS점수, 벤치마크NPS점수, 벤치마크NPS점수갭점수",
        "ddl": """\
-- TD 고객경험단계별 NPS (타행 비교 포함)
CREATE TABLE v_td_cx_stage_nps (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    고객경험단계명 TEXT, 벤치마크은행명 TEXT, 
    NPS점수 REAL, 벤치마크NPS점수 REAL, 벤치마크NPS점수갭점수 REAL
);""",
        "templates": [
            "SELECT 거래은행명, 고객경험단계명, NPS점수\nFROM v_td_cx_stage_nps\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 채널명 = '플랫폼';",
        ],
        "notes": "",
    },
    "td_channel_driver": {
        "view": "v_td_channel_driver",
        "description": "TD 채널 영향요인 (채널 하위 고객경험단계 별 영향도)",
        "survey_type": "TD",
        "use_when": "TD 채널 영향요인, 어떤 고객경험단계가 NPS에 기여하는지",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 영향요인구분명(해당 뷰에서는 고객경험단계를 의미) | METRIC: NPS영향도점수",
        "ddl": """\
-- TD 채널 영향요인 (어떤 경험단계가 채널 NPS에 영향)
CREATE TABLE v_td_channel_driver (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    영향요인구분명 TEXT,  -- 영향요인 = 고객경험단계
    NPS영향도점수 REAL    -- 해당 고객경험단계가 채널 NPS에 기여하는 정도
);""",
        "templates": [
            "SELECT 영향요인구분명, NPS영향도점수\nFROM v_td_channel_driver\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 채널명 = '플랫폼' AND 거래은행명 = 'KB국민은행'\nORDER BY NPS영향도점수 DESC;",
        ],
        "notes": "영향도 합산 = 해당 채널 NPS. 양수는 기여, 음수는 감소.",
    },
    "td_cx_stage_driver": {
        "view": "v_td_cx_stage_driver",
        "description": "TD 고객경험단계 영향요인 (고객경험단계 하위 서비스품질요소 별 영향도)",
        "survey_type": "TD",
        "use_when": "TD 고객경험단계 영향요인, TD 서비스품질요소별 영향도, 개선 우선순위, 원인 분석",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명(해당 뷰에서는 서비스품질요소를 의미)) | METRIC: NPS영향도점수",
        "ddl": """\
-- TD 경험단계 영향요인 (어떤 서비스품질요소가 고객경험단계 NPS에 영향)
CREATE TABLE v_td_cx_stage_driver (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    고객경험단계명 TEXT,
    영향요인구분명 TEXT,  -- 영향요인 = 서비스품질요소
    NPS영향도점수 REAL   -- 해당 서비스품질요소가 고객경험단계 NPS에 기여하는 정도
);""",
        "templates": [
            "SELECT 영향요인구분명, NPS영향도점수\nFROM v_td_cx_stage_driver\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 채널명 = '플랫폼' AND 고객경험단계명 = '접속/로그인'\n  AND 거래은행명 = 'KB국민은행';",
        ],
        "notes": "",
    },
    "td_channel_ipa": {
        "view": "v_td_channel_ipa",
        "description": "TD 채널 IPA 문제원인 분석 (4사분면: 현상유지/유지관리/중점개선/점진개선). 채널 NPS를 고객경험단계별로 분해한 영향도 기반 분석.",
        "survey_type": "TD",
        "use_when": "IPA 분석, 문제영역, 중점개선, 벤치마크 Gap, 강점/약점",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 영향요인구분명(=고객경험단계) | METRIC: 문제영역명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, 벤치마크은행명, 벤치마크NPS영향도점수, NPS영향도갭점수, NPS영향도갭평균점수",
        "ddl": """\
-- TD 채널 IPA (중요도-성과갭 4사분면 분석)
-- X축=NPS중요도점수(영향도, 전체은행 합산), Y축=NPS영향도갭(KB-벤치마크)
-- 문제영역명: '현상유지','유지관리','중점개선'(최우선 개선),'점진개선'
-- 영향요인구분명: 채널 NPS를 분해하는 하위 요인으로, 이 뷰에서는 고객경험단계명 값이 들어감
--   (예: 접속/로그인, 조회, 이체/송금, 상품가입, 탐색, 비교, 호감, 인지, ...)
CREATE TABLE v_td_channel_ipa (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    영향요인구분명 TEXT,        -- 고객경험단계명 값 (채널→경험단계 분해)
    문제영역명 TEXT,           -- 4사분면 분류
    NPS중요도점수 REAL,        -- X축: 전체은행 합산 중요도
    NPS중요도평균점수 REAL,    -- X축 기준선
    NPS영향도점수 REAL,        -- KB 해당 단계 영향도
    벤치마크은행명 TEXT,       -- KB 직상위 기관
    벤치마크NPS영향도점수 REAL,
    NPS영향도갭점수 REAL,          -- Y축: KB - 벤치마크 영향도 차이
    NPS영향도갭평균점수 REAL   -- Y축 기준선
);""",
        "templates": [
            "SELECT 영향요인구분명, 문제영역명, NPS중요도점수, NPS영향도갭점수, 벤치마크은행명\nFROM v_td_channel_ipa\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 채널명 = '플랫폼' AND 거래은행명 = 'KB국민은행';",
            "SELECT 영향요인구분명, 문제영역명, NPS중요도점수, NPS영향도갭점수\nFROM v_td_channel_ipa\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 채널명 = '플랫폼' AND 거래은행명 = 'KB국민은행'\n  AND 문제영역명 = '중점개선';",
        ],
        "notes": "IPA는 TD 전용. 거래은행명='KB국민은행' 조건 필수. 영향요인구분명에는 고객경험단계명 값이 들어감.",
    },
    "td_cx_stage_ipa": {
        "view": "v_td_cx_stage_ipa",
        "description": "TD 경험단계 IPA 문제원인 분석 (서비스품질요소별 4사분면). 경험단계 NPS를 서비스품질요소별로 분해한 영향도 기반 분석.",
        "survey_type": "TD",
        "use_when": "고객경험단계별 IPA, 서비스품질요소별 문제영역, 상세 IPA",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명(=서비스품질요소) | METRIC: 문제영역명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, 벤치마크은행명, 벤치마크NPS영향도점수, NPS영향도갭점수, 벤치마크NPS점수갭평균점수",
        "ddl": """\
-- TD 경험단계 IPA (서비스품질요소 수준 4사분면 분석)
-- 영향요인구분명: 경험단계 NPS를 분해하는 하위 요인으로, 이 뷰에서는 서비스품질요소명 값이 들어감
--   (예: 편리성, 신속성, 정확성, 안정성, 유용성, 디자인, 수수료, 정보정확성, 브랜드인지도, 광고효과, ...)
CREATE TABLE v_td_cx_stage_ipa (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    고객경험단계명 TEXT,
    영향요인구분명 TEXT,       -- 서비스품질요소명 값 (경험단계→서비스품질요소 분해)
    문제영역명 TEXT, NPS중요도점수 REAL, NPS중요도평균점수 REAL,
    NPS영향도점수 REAL, 벤치마크은행명 TEXT, 벤치마크NPS영향도점수 REAL,
    NPS영향도갭점수 REAL, 벤치마크NPS점수갭평균점수 REAL
);""",
        "templates": [
            "SELECT 영향요인구분명, 문제영역명, NPS중요도점수, NPS영향도갭점수\nFROM v_td_cx_stage_ipa\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 채널명 = '플랫폼' AND 고객경험단계명 = '접속/로그인'\n  AND 거래은행명 = 'KB국민은행';",
        ],
        "notes": "IPA는 TD 전용. 거래은행명='KB국민은행' 조건 필수. 영향요인구분명에는 서비스품질요소명 값이 들어감.",
    },

    # =========================================================================
    # TD VOC (사전집계)
    # =========================================================================
    "td_voc_type": {
        "view": "v_td_voc_type",
        "description": "TD 고객경험단계별 VOC 유형 분포 (CCI 포함)",
        "survey_type": "TD",
        "use_when": "TD VOC CCI(고객불만지수), 칭찬/불만/개선/기타 비율",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명 | METRIC: 응답고객수, 칭찬고객수, 불만고객수, 개선고객수, 기타고객수, 칭찬비중점수, 불만비중점수, 개선비중점수, 기타비중점수, CCI점수",
        "ddl": """\
-- TD 고객경험단계별 VOC 유형 분석 (사전집계)
CREATE TABLE v_td_voc_type (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    고객경험단계명 TEXT,
    응답고객수 INTEGER,
    칭찬고객수 INTEGER, 불만고객수 INTEGER, 개선고객수 INTEGER, 기타고객수 INTEGER,
    칭찬비중점수 REAL, 불만비중점수 REAL, 개선비중점수 REAL, 기타비중점수 REAL,
    CCI점수 REAL    -- 고객불만지수
);""",
        "templates": [
            "SELECT 채널명, 고객경험단계명, 응답고객수, CCI점수, 불만비중점수\nFROM v_td_voc_type\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 거래은행명 = 'KB국민은행' AND 채널명 = '플랫폼';",
        ],
        "notes": "사전집계 뷰. 별도 필터 불필요.",
    },
    "td_voc_sentiment": {
        "view": "v_td_voc_sentiment",
        "description": "TD 고객경험단계별 VOC 감정 분포 (CCI 포함)",
        "survey_type": "TD",
        "use_when": "TD VOC 감정분석, NSS(고객감정지수), 칭찬/불만/개선/기타 비율",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명 | METRIC: 응답고객수, 긍정고객수, 중립고객수, 부정고객수, 긍정비중점수, 중립비중점수, 부정비중점수, NSS점수",
        "ddl": """\
-- TD 고객경험단계별 VOC 감정 분석 (사전집계)
CREATE TABLE v_td_voc_sentiment (
    반기구분명 TEXT, 조사년도 TEXT, 거래은행명 TEXT, 채널명 TEXT,
    고객경험단계명 TEXT,
    응답고객수 INTEGER,
    긍정고객수 INTEGER, 중립고객수 INTEGER, 부정고객수 INTEGER,
    긍정비중점수 REAL, 중립비중점수 REAL, 부정비중점수 REAL,
    NSS점수 REAL    -- 고객불만지수
);""",
        "templates": [
            "SELECT 채널명, 고객경험단계명, 응답고객수, NSS점수, 부정비중점수\nFROM v_td_voc_sentiment\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기'\n  AND 거래은행명 = 'KB국민은행' AND 채널명 = '플랫폼';",
        ],
        "notes": "사전집계 뷰. 별도 필터 불필요.",
    },

    # =========================================================================
    # TD VOC 원문
    # =========================================================================
    "td_voc_raw": {
        "view": "v_td_voc_raw",
        "description": "TD VOC 원문 조회 (서술형 마스터 기반)",
        "survey_type": "TD",
        "use_when": "TD VOC 원문, 고객 목소리, 실제 의견 확인 (TD)",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명", "서비스품질명"],
        "brief_columns": "DIM: 조사년도, 반기구분명, 거래은행명, 채널명, 고객경험단계명 | TEXT: VOC원문내용, 상품서비스용어내용, 성능품질용어내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명, 추천영향요인내용 | FILTER: VOC필터링여부",
        "ddl": """\
-- TD VOC 원문 (TD 서술형 마스터 뷰, MGF2)
-- 주의: TD는 배분 대상이 아니므로 배분여부/과제진행상태명/검토구분 칼럼 없음
CREATE TABLE v_td_voc_raw (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    VOC원문내용 TEXT,
    VOC필터링여부 INTEGER,            -- 0=정상처리, 1=필터링(무의미/짧은응답 등 추출불가)
    고객경험요소분류성공여부 INTEGER, -- 0/1
    고객경험요소명 TEXT,
    서비스품질명 TEXT,               -- 서비스품질요소명과 동일 의미
    상품서비스용어내용 TEXT, 성능품질용어내용 TEXT,
    고객감정대분류명 TEXT,           -- '긍정','부정','중립'
    고객경험VOC유형명 TEXT,          -- '칭찬','불만','개선','기타'
    고객경험정답감정명 TEXT,         -- '만족','아쉬움','실망','감정 없음','짜증','감동','기대'
    추천점수 INTEGER,               -- 0~10
    추천의향내용 TEXT,              -- '추천','중립','비추천'
    성별내용 TEXT, 연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT,
    설문ID TEXT, 설문참여대상자고유ID TEXT, 문항ID TEXT,
    고객경험상품명 TEXT,             -- BU 에피소드내용과 유사
    추천영향요인내용 TEXT           -- 고객경험단계='해당무'→고객경험단계의 개체값이 들어가고, 고객경험단계<>'해당무'→서비스품질요소의 개체어값이 들어감.
);""",
        "templates": [
            "SELECT VOC원문내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명\nFROM v_td_voc_raw\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 거래은행명 = 'KB국민은행'\n  AND 채널명 = '플랫폼' AND VOC필터링여부 = 0\nLIMIT 50;",
        ],
        "notes": "VOC필터링여부=0 조건 필수.",
    },

    # =========================================================================
    # TD 스펙트럼 (마스터 기반 동적 쿼리)
    # =========================================================================
    "td_spectrum_nps": {
        "view": "v_td_spectrum_nps",
        "description": "TD 고객 세그먼트(스펙트럼)별 NPS (마스터 기반 동적 GROUP BY)",
        "survey_type": "TD",
        "use_when": "TD 연령대별, 고객등급별, 이용빈도별 NPS, TD 스펙트럼",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 연령10세내용, 이용거래기간내용, 플랫폼이용빈도내용, 고객센터이용빈도내용, 영업점이용빈도내용, 고객등급내용 | COMPUTED: 추천의향내용 기반 NPS 직접 계산 (추천점수 없음)",
        "ddl": """\
-- TD 점수형 마스터 (스펙트럼 NPS 분석 기반, MGF1)
-- 선택 가능한 스펙트럼 칼럼:
--   성별내용: '남자','여자'
--   연령5세내용: '20세이상 24세이하','25세이상 29세이하',...,'65세이상 69세이하'
--   연령10세내용: '20대','30대','40대','50대','60대'
--   이용거래기간내용: '1년미만','1년이상 2년미만','2년이상 3년미만','3년이상 5년미만','5년이상 10년미만','10년이상'
--   플랫폼이용빈도내용: '거의 매일','2~3일에 한 번','일주일에 한 번','격주에 한 번','한달에 한 번'
--   고객센터이용빈도내용: '1회','2회','3~4회','5회 이상'
--   영업점이용빈도내용: '1회','2회','3~4회','5~6회','7~10회','10회 초과'
--   고객등급내용: 'VVIP','VIP','패밀리','그랜드','베스트'
--   고객충성도내용/추천의향내용: '추천','중립','비추천'
CREATE TABLE v_td_spectrum_nps (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    추천사유내용 TEXT,   -- 추천/비추천 사유 자유응답
    성별내용 TEXT,
    연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT,
    고객충성도내용 TEXT,  -- '추천','중립','비추천'
    추천의향내용 TEXT,     -- '추천','중립','비추천'
    NPS점수 TEXT     -- NPS점수
);""",
        "templates": [
            "SELECT 연령10세내용 AS 스펙트럼, COUNT(*) AS 전체건수,\n  ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2)\n  - ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수\nFROM v_td_spectrum_nps\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 거래은행명 = 'KB국민은행'\nGROUP BY 연령10세내용\nORDER BY 연령10세내용;",
            "SELECT 고객등급내용 AS 스펙트럼, COUNT(*) AS 전체건수,\n  ROUND(SUM(CASE WHEN 추천의향내용='추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2)\n  - ROUND(SUM(CASE WHEN 추천의향내용='비추천' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수\nFROM v_td_spectrum_nps\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 거래은행명 = 'KB국민은행'\nGROUP BY 고객등급내용;",
        ],
        "notes": "스펙트럼 뷰는 마스터 테이블 기반. 추천점수 없으므로 추천의향내용 기반 CASE WHEN으로 NPS 계산. 전체건수 30건 미만 시 해석 주의.",
    },
    "td_spectrum_driver": {
        "view": "v_td_spectrum_driver",
        "description": "TD 고객 세그먼트(스펙트럼)별 영향요인 분석 (마스터 기반 동적 GROUP BY)",
        "survey_type": "TD",
        "use_when": "TD 연령대별, 고객등급별, 이용빈도별 영향요인 분석 등",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 연령10세내용, 이용거래기간내용, 플랫폼이용빈도내용, 고객센터이용빈도내용, 영업점이용빈도내용, 고객등급내용 | COMPUTED: 추천의향내용, 추천사유내용 기반 영향요인별 영향도 직접 계산 (추천점수 없음)",
        "ddl": """\
-- TD 점수형 마스터 (스펙트럼 NPS 분석 기반, MGF1)
-- 선택 가능한 스펙트럼 칼럼:
--   성별내용: '남자','여자'
--   연령5세내용: '20세이상 24세이하','25세이상 29세이하',...,'65세이상 69세이하'
--   연령10세내용: '20대','30대','40대','50대','60대'
--   이용거래기간내용: '1년미만','1년이상 2년미만','2년이상 3년미만','3년이상 5년미만','5년이상 10년미만','10년이상'
--   플랫폼이용빈도내용: '거의 매일','2~3일에 한 번','일주일에 한 번','격주에 한 번','한달에 한 번'
--   고객센터이용빈도내용: '1회','2회','3~4회','5회 이상'
--   영업점이용빈도내용: '1회','2회','3~4회','5~6회','7~10회','10회 초과'
--   고객등급내용: 'VVIP','VIP','패밀리','그랜드','베스트'
--   고객충성도내용/추천의향내용: '추천','중립','비추천'
CREATE TABLE v_td_spectrum_nps (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    추천사유내용 TEXT,   -- 추천/비추천 사유 자유응답
    성별내용 TEXT,
    연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT,
    고객충성도내용 TEXT,  -- '추천','중립','비추천'
    추천의향내용 TEXT,     -- '추천','중립','비추천'
    추천사유내용 TEXT,     -- 채널 또는 고객경험단계에 대한 추천 사유(=영향요인)
);""",
        "templates": [
            "select A.조사년도, A.반기구분명, A.채널명, A.거래은행명, A.영향요인구분, A.연령10세내용, A.응답고객수, A.추천고객수, A.중립고객수, A.비추천고객수\n, ROUND((A.응답고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비응답비중\n    , ROUND((A.추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비추천비중\n    , ROUND((A.중립고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비중립비중\n    , ROUND((A.비추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비비추천비중\n    , ROUND((A.추천고객수 * 1.0)  /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채 널명,연령10세내용)*100, 1)\n      - ROUND((A.비추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은 행명,A.채널명,연령10세내용)*100, 1) AS NPS영향도\nFROM (\n    SELECT  A.설문ID\n        , MAX(A.설문조사방식명) AS 설문 조사방식명\n        , MAX(A.설문조사종류명) AS 설문조사종류명\n        , MAX(A.조사년도        ) AS 조사년도\n        , MAX(A.반기구분명        ) AS 반기구분명\n        , A.채널명\n        , A.거래은행명\n        , COALESCE(A.영향요인구분,'채널전체') AS 영향요인구분\n        , 연령10세내용\n        , COUNT(*)                                                   AS 응답고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '추천'   THEN 1 ELSE 0 END) AS 추천고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '중립'   THEN 1 ELSE 0 END) AS 중립고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '비추천' THEN 1 ELSE 0 END) AS 비추천고객수\n    FROM (\n        SELECT  A.설문ID\n            , A.설문조사방식명\n            , A.설문조사종류명\n            , A.조사년도\n            , A.반기구분명\n            , A.거래은행명\n            , A.채널명\n            , A.추천의향내용\n            , CASE WHEN A.채널명 <> '상품' THEN A.추천사유내용\n                     ELSE                           A.고객경험단계명 \n                END AS 영향요인구분\n            , A.연령10세내용\n        FROM  v_td_spectrum_driver A\n        WHERE  1=1\n        AND 조사년도 = '2025' \n        AND 반기구분명 = '하반기'\n        AND 거래은행명 IN ('KB국민은행') \n        AND 채널명 IN ('상품')\n        ) A\n    GROUP BY \n        A.설문ID\n        , A.거래은행명\n        , A.채널명\n        , A.영향요인구분\n        , 연령10세내용\n    ) A;",
            "select A.조사년도, A.반기구분명, A.채널명, A.고객경험단계명, A.거래은행명, A.영향요인구분, A.연령10세내용, A.응답고객수, A.추천고객수, A.중립고객수, A.비추천고객수\n, ROUND((A.응답고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비응답비중\n    , ROUND((A.추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비추천비중\n    , ROUND((A.중립고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비중립비중\n    , ROUND((A.비추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채널명,연령10세내용)*100, 1) AS 전체대비비추천비중\n    , ROUND((A.추천고객수 * 1.0)  /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은행명,A.채 널명,연령10세내용)*100, 1)\n      - ROUND((A.비추천고객수 * 1.0) /MAX(A.응답고객수) OVER(PARTITION BY A.설문ID,A.거래은 행명,A.채널명,연령10세내용)*100, 1) AS NPS영향도\nFROM (\n    SELECT  A.설문ID\n        , MAX(A.설문조사방식명) AS 설문 조사방식명\n        , MAX(A.설문조사종류명) AS 설문조사종류명\n        , MAX(A.조사년도        ) AS 조사년도\n        , MAX(A.반기구분명        ) AS 반기구분명\n        , A.채널명\n        , A.고객경험단계명\n        , A.거래은행명\n        , COALESCE(A.영향요인구분,'채널전체') AS 영향요인구분\n        , 연령10세내용\n        , COUNT(*)                                                   AS 응답고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '추천'   THEN 1 ELSE 0 END) AS 추천고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '중립'   THEN 1 ELSE 0 END) AS 중립고객수  \n        , SUM(CASE WHEN A.추천의향내용 = '비추천' THEN 1 ELSE 0 END) AS 비추천고객수\n    FROM (\n        SELECT  A.설문ID\n            , A.설문조사방식명\n            , A.설문조사종류명\n            , A.조사년도\n            , A.반기구분명\n            , A.거래은행명\n            , A.채널명\n            , A.고객경험단계명\n            , A.추천의향내용\n            , CASE WHEN A.채널명 <> '상품' THEN A.추천사유내용\n                     ELSE                           A.고객경험단계명 \n                END AS 영향요인구분\n            , A.연령10세내용\n        FROM  v_td_spectrum_driver A\n        WHERE  1=1\n        AND 조사년도 = '2025' \n        AND 반기구분명 = '하반기'\n        AND 거래은행명 IN ('KB국민은행') \n        AND 채널명 IN ('플랫폼')\n        AND 고객경험단계명 IN ('로그인/인증')\n        ) A\n    GROUP BY \n        A.설문ID\n        , A.거래은행명\n        , A.채널명\n        , A.영향요인구분\n        , 연령10세내용\n    ) A;",
        ],
        "notes": "상품 채널의 경우 영향요인을 추출하는 로직이 다르니 주의. (CASE WHEN A.채널명 <> '상품' THEN A.추천사유내용\n                     ELSE                           A.고객경험단계명 \n                END AS 영향요인구분)",
    },
    "td_spectrum_channel_ipa": {
        "view": "v_td_spectrum_channel_ipa",
        "description": "TD 고객 세그먼트(스펙트럼)별 채널 IPA 분석",
        "survey_type": "TD",
        "use_when": "TD 연령대별, 고객등급별, 이용빈도별 채널 IPA, 스펙트럼/세그먼트 채널 IPA",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "영향요인구분명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 연령10세내용, 이용거래기간내용, 플랫폼이용빈도내용, 고객센터이용빈도내용, 영업점이용빈도내용, 고객등급내용 | COMPUTED: 문제영역명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, 벤치마크은행명, 벤치마크NPS영향도점수, NPS영향도갭점수, NPS영향도갭평균점수",
        "ddl": """\
-- TD 스펙트럼 채널 IPA 분석 (스펙트럼 NPS 분석 기반, MGF1)
-- 선택 가능한 스펙트럼 칼럼:
--   성별내용: '남자','여자'
--   연령5세내용: '20세이상 24세이하','25세이상 29세이하',...,'65세이상 69세이하'
--   연령10세내용: '20대','30대','40대','50대','60대'
--   이용거래기간내용: '1년미만','1년이상 2년미만','2년이상 3년미만','3년이상 5년미만','5년이상 10년미만','10년이상'
--   플랫폼이용빈도내용: '거의 매일','2~3일에 한 번','일주일에 한 번','격주에 한 번','한달에 한 번'
--   고객센터이용빈도내용: '1회','2회','3~4회','5회 이상'
--   영업점이용빈도내용: '1회','2회','3~4회','5~6회','7~10회','10회 초과'
--   고객등급내용: 'VVIP','VIP','패밀리','그랜드','베스트'
--   고객충성도내용/추천의향내용: '추천','중립','비추천'
CREATE TABLE v_td_spectrum_channel_ipa (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 
    고객경험단계명 TEXT, -- 채널분석인 경우 값이 고객경험단계명=''인 경우만 추출해야 함. 
    추천사유내용 TEXT,   -- 영향요인구분명(추천사유가 되는 고객경험단계명)
    성별내용 TEXT,
    연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT,
    고객충성도내용 TEXT,  -- '추천','중립','비추천'
    추천의향내용 TEXT     -- '추천','중립','비추천'
);""",
        "templates": [
            """WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명, BM.벤치마크은행명 
		, 연령10세내용
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 추천사유내용 AS 영향요인구분명
			,연령10세내용 -- 수정가능(스펙트럼)
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		WHERE 조사년도='2025' AND 반기구분명='하반기' -- 수정가능(기간)
			AND 채널명 IN ('브랜드') AND 고객경험단계명='' -- 수정가능(채널 및 고객경험단계)
			AND 연령10세내용 IN ('60대') -- 수정가능(스펙트럼)
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 추천사유내용
			, 연령10세내용 -- 수정가능(스펙트럼)
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 영향요인구분명
		, 연령10세내용 -- 수정가능(스펙트럼)
)
SELECT
	KB.반기구분명, KB.조사년도, 
	채널명, 영향요인구분명
	, KB.연령10세내용 -- 수정가능(스펙트럼)
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
		반기구분명, 조사년도, 채널명, 영향요인구분명
		, 연령10세내용 -- 수정가능(스펙트럼)
		, (전체건수/sum(전체건수) over () + 비추천건수/sum(비추천건수) over ())/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 거래은행명, 채널명)*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명) AS NPS중요도평균점수
	FROM AGGR
	WHERE 거래은행명='KB국민은행') KB
 LEFT JOIN (
	SELECT
		채널명, 영향요인구분명, 거래은행명 AS 벤치마크은행명
		, 연령10세내용 -- 수정가능(스펙트럼)
		,(추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 거래은행명, 채널명)*100 AS 벤치마크NPS영향도점수
	FROM AGGR 
	WHERE 거래은행명=벤치마크은행명) BM
USING (채널명, 영향요인구분명)
ORDER BY KB.조사년도 DESC, KB.반기구분명 DESC
LIMIT 200"""],
        "notes": "반드시 주석으로 `수정가능`으로 표시된 부분만 질의에 맞게 수정하고 그 외의 템플릿은 수정하지 말 것.",
    },
    "td_spectrum_cx_stage_ipa": {
        "view": "v_td_spectrum_cx_stage_ipa",
        "description": "TD 고객 세그먼트(스펙트럼)별 고객경험단계 IPA 분석",
        "survey_type": "TD",
        "use_when": "TD 연령대별, 고객등급별, 이용빈도별 채널 IPA, 스펙트럼/세그먼트 채널 IPA",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명", "영향요인구분명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 연령10세내용, 이용거래기간내용, 플랫폼이용빈도내용, 고객센터이용빈도내용, 영업점이용빈도내용, 고객등급내용 | COMPUTED: 문제영역명, NPS중요도점수, NPS중요도평균점수, NPS영향도점수, 벤치마크은행명, 벤치마크NPS영향도점수, NPS영향도갭점수, NPS영향도갭평균점수",
        "ddl": """\
-- TD 스펙트럼 채널 IPA 분석 (스펙트럼 NPS 분석 기반, MGF1)
-- 선택 가능한 스펙트럼 칼럼:
--   성별내용: '남자','여자'
--   연령5세내용: '20세이상 24세이하','25세이상 29세이하',...,'65세이상 69세이하'
--   연령10세내용: '20대','30대','40대','50대','60대'
--   이용거래기간내용: '1년미만','1년이상 2년미만','2년이상 3년미만','3년이상 5년미만','5년이상 10년미만','10년이상'
--   플랫폼이용빈도내용: '거의 매일','2~3일에 한 번','일주일에 한 번','격주에 한 번','한달에 한 번'
--   고객센터이용빈도내용: '1회','2회','3~4회','5회 이상'
--   영업점이용빈도내용: '1회','2회','3~4회','5~6회','7~10회','10회 초과'
--   고객등급내용: 'VVIP','VIP','패밀리','그랜드','베스트'
--   고객충성도내용/추천의향내용: '추천','중립','비추천'
CREATE TABLE v_td_spectrum_channel_ipa (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 
    고객경험단계명 TEXT, 
    추천사유내용 TEXT,   -- 영향요인구분명(추천사유가 되는 서비스품질요소)
    성별내용 TEXT,
    연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT,
    고객충성도내용 TEXT,  -- '추천','중립','비추천'
    추천의향내용 TEXT     -- '추천','중립','비추천'
);""",
        "templates": [
            """WITH AGGR AS (
	SELECT 
		X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명, BM.벤치마크은행명 
		, 연령10세내용
		, sum(갯수) AS 전체건수
		, sum(CASE WHEN 추천의향내용='추천' THEN 갯수 ELSE 0 END) AS 추천건수
		, sum(CASE WHEN 추천의향내용='중립' THEN 갯수 ELSE 0 END) AS 중립건수
		, sum(CASE WHEN 추천의향내용='비추천' THEN 갯수 ELSE 0 END) AS 비추천건수
	FROM (
		SELECT 거래은행명, 채널명, 고객경험단계명, 추천사유내용 AS 영향요인구분명
			,연령10세내용 -- 수정가능(스펙트럼)
			, 추천의향내용, count(*) AS 갯수
			, 조사년도, 반기구분명
			FROM INST1.TSCCVMGF1
		WHERE 조사년도='2025' AND 반기구분명='하반기' -- 수정가능(기간)
			AND 채널명 IN ('브랜드') AND 고객경험단계명 IN ('고객중심') -- 수정가능(채널 및 고객경험단계)
			AND 연령10세내용 IN ('60대') -- 수정가능(스펙트럼)
		GROUP BY 
			반기구분명, 조사년도, 거래은행명, 채널명, 고객경험단계명, 추천사유내용
			, 연령10세내용 -- 수정가능(스펙트럼)
			, 추천의향내용
	) X
	LEFT JOIN (SELECT DISTINCT 조사년도, 반기구분명, 채널명, 벤치마크은행명 FROM inst1.tsccvmgc3 WHERE 벤치마크은행명 IS NOT null) BM USING (조사년도, 반기구분명, 채널명) 
	GROUP BY X.반기구분명, X.조사년도, 거래은행명, 채널명, 고객경험단계명, 영향요인구분명
		, 연령10세내용 -- 수정가능(스펙트럼)
)
SELECT
	KB.반기구분명, KB.조사년도, 
	채널명, 고객경험단계명, 영향요인구분명
	, KB.연령10세내용 -- 수정가능(스펙트럼)
	, NPS중요도점수
	, NPS중요도평균점수
	, NPS영향도점수
	, 벤치마크은행명
	, 벤치마크NPS영향도점수
	, (NPS영향도점수 - 벤치마크NPS영향도점수) AS NPS영향도갭점수
	, sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명, 고객경험단계명) / count(*) OVER (PARTITION BY 채널명, 고객경험단계명) AS NPS영향도갭평균점수
	, CASE WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명, 고객경험단계명) / count(*) OVER (PARTITION BY 채널명, 고객경험단계명)
		THEN "현상유지"
		WHEN NPS중요도점수 <= NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			> sum(NPS영향도점수 - 벤치마크NPS영향도점수) OVER (PARTITION BY 채널명, 고객경험단계명) / count(*) OVER (PARTITION BY 채널명, 고객경험단계명)
		THEN "유지관리" 
		WHEN NPS중요도점수 > NPS중요도평균점수 AND (NPS영향도점수 - 벤치마크NPS영향도점수) 
			<= sum(NPS영향도점수 -벤치마크NPS영향도점수) OVER (PARTITION BY 채널명, 고객경험단계명) / count(*) OVER (PARTITION BY 채널명, 고객경험단계명)
		THEN "중점개선" 
		ELSE '점진개선' END AS 문제영역명
FROM (
	SELECT 
		반기구분명, 조사년도, 채널명, 고객경험단계명, 영향요인구분명
		, 연령10세내용 -- 수정가능(스펙트럼)
		, (전체건수/sum(전체건수) over () + 비추천건수/sum(비추천건수) over ())/ 2 * 100 AS NPS중요도점수
		, (추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명)*100 AS NPS영향도점수
		, 100 / count(*) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명) AS NPS중요도평균점수
	FROM AGGR
	WHERE 거래은행명='KB국민은행') KB
 LEFT JOIN (
	SELECT
		채널명, 고객경험단계명, 영향요인구분명, 거래은행명 AS 벤치마크은행명
		, 연령10세내용 -- 수정가능(스펙트럼)
		,(추천건수-비추천건수) / sum(전체건수) OVER (PARTITION BY 거래은행명, 채널명, 고객경험단계명)*100 AS 벤치마크NPS영향도점수
	FROM AGGR 
	WHERE 거래은행명=벤치마크은행명) BM
USING (채널명, 고객경험단계명, 영향요인구분명)
ORDER BY KB.조사년도 DESC, KB.반기구분명 DESC
LIMIT 200"""],
        "notes": "반드시 주석으로 `수정가능`으로 표시된 부분만 질의에 맞게 수정하고 그 외의 템플릿은 수정하지 말 것.",
    },
    "td_spectrum_voc": {
        "view": "v_td_spectrum_voc",
        "description": "TD 고객 세그먼트(스펙트럼)별 VOC 감정·유형 집계 (마스터 기반 동적 GROUP BY)",
        "survey_type": "TD",
        "use_when": "TD 연령대별, 고객등급별 VOC 분포, TD 스펙트럼 VOC",
        "dim_columns": ["조사년도", "반기구분명", "거래은행명", "채널명", "고객경험단계명"],
        "brief_columns": "GROUP BY 선택 가능: (td_spectrum_nps와 동일) | COMPUTED: VOC감정/VOC유형 기반 NSS, CCI, 건수·비율 직접 계산 | FILTER: VOC필터링여부=0 필수",
        "ddl": """\
-- TD 서술형 마스터 (스펙트럼 VOC 분석 기반, MGF2)
-- 전체 칼럼: td_voc_raw 참조 (동일 뷰)
-- 스펙트럼 칼럼: td_spectrum_nps 참조
-- 주의: TD는 배분 관련 칼럼 없음
CREATE TABLE v_td_spectrum_voc (
    조사년도 TEXT, 반기구분명 TEXT, 거래은행명 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    VOC필터링여부 INTEGER,
    고객감정대분류명 TEXT, 고객경험VOC유형명 TEXT,
    성별내용 TEXT, 연령5세내용 TEXT, 연령10세내용 TEXT,
    이용거래기간내용 TEXT,
    플랫폼이용빈도내용 TEXT, 고객센터이용빈도내용 TEXT, 영업점이용빈도내용 TEXT,
    고객등급내용 TEXT, 긍정비율 INTEGER, 부정비율 INTEGER, 중립비율 INTEGER, 칭찬비율 INTEGER, 
    불만비율 INTEGER, 개선비율 INTEGER, 기타비율 INTEGER, NSS REAL, CCI REAL
);""",
        "templates": [
            "SELECT 연령10세내용 AS 스펙트럼, COUNT(*) AS 전체건수,\n  SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END) AS 칭찬고객수,\n  SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END) AS 불만고객수,\n  ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2)\n  - ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,\n  ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI\nFROM v_td_spectrum_voc\nWHERE 조사년도 = '2025' AND 반기구분명 = '상반기' AND 거래은행명 = 'KB국민은행'\n  AND VOC필터링여부 = 0\nGROUP BY 연령10세내용\nORDER BY 연령10세내용;",
        ],
        "notes": "VOC필터링여부=0 조건 필수. NPS 뷰가 아닌 VOC 집계용.",
    },

    # =========================================================================
    # BU NPS (사전집계)
    # =========================================================================
    "bu_channel_nps": {
        "view": "v_bu_channel_nps",
        "description": "BU 채널별 NPS 일별 누적 [연초~당일]",
        "survey_type": "BU",
        "use_when": "채널 NPS 현황, 전체 채널 비교, BU NPS 조회",
        "dim_columns": ["기준년월일", "채널명"],
        "brief_columns": "DIM: 기준년월일(YYYYMMDD), 채널명(BU 4채널) | METRIC: NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율",
        "ddl": """\
-- BU 채널별 NPS 일별 누적 [연초~당일]
CREATE TABLE v_bu_channel_nps (
    기준년월일 TEXT,  -- 'YYYYMMDD' (연초~당일 누적)
    채널명 TEXT,     -- BU 4개: 'KB 스타뱅킹','영업점','고객센터','상품'
    NPS점수 REAL, 전체건수 INTEGER,
    추천건수 INTEGER, 추천비율 REAL,
    중립건수 INTEGER, 중립비율 REAL,
    비추천건수 INTEGER, 비추천비율 REAL
);""",
        "templates": [
            "SELECT 채널명, NPS점수, 전체건수\nFROM v_bu_channel_nps\nWHERE 기준년월일 = '20260225';",
        ],
        "notes": "기준년월일은 연초~당일 누적치.",
    },
    "bu_channel_nps_trend": {
        "view": "v_bu_channel_nps_trend",
        "description": "BU 채널 NPS 월말 스냅샷 [월별 추이]",
        "survey_type": "BU",
        "use_when": "NPS 추이, 기간별 변화, 트렌드, 전월 대비",
        "dim_columns": ["기준년월", "채널명"],
        "brief_columns": "DIM: 기준년월(YYYYMM), 채널명 | METRIC: NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율",
        "ddl": """\
-- BU 채널 NPS 월말 스냅샷 (월별 추이 분석용)
CREATE TABLE v_bu_channel_nps_trend (
    기준년월 TEXT,   -- 'YYYYMM' (월말 스냅샷)
    채널명 TEXT, -- BU 4개: '스타뱅킹','영업점','고객센터','상품'
    NPS점수 REAL, 전체건수 INTEGER,
    추천건수 INTEGER, 추천비율 REAL,
    중립건수 INTEGER, 중립비율 REAL,
    비추천건수 INTEGER, 비추천비율 REAL
);""",
        "templates": [
            "SELECT 기준년월, 채널명, NPS점수\nFROM v_bu_channel_nps_trend\nWHERE 기준년월 BETWEEN '202507' AND '202602';",
        ],
        "notes": "월별 추이 분석에 사용. 기간 비교 시 BETWEEN 사용.",
    },
    "bu_cx_stage_nps": {
        "view": "v_bu_cx_stage_nps",
        "description": "BU 경험단계별 NPS 일별 누적 [연초~당일]",
        "survey_type": "BU",
        "use_when": "BU 경험단계 분석, 상세 드릴다운, 어느 단계가 약한지",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명 | METRIC: NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율",
        "ddl": """\
-- BU 고객경험단계별 NPS 일별 누적 [연초~당일]
CREATE TABLE v_bu_cx_stage_nps (
    기준년월일 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    NPS점수 REAL, 전체건수 INTEGER,
    추천건수 INTEGER, 추천비율 REAL,
    중립건수 INTEGER, 중립비율 REAL,
    비추천건수 INTEGER, 비추천비율 REAL
);""",
        "templates": [
            "SELECT 고객경험단계명, NPS점수, 전체건수\nFROM v_bu_cx_stage_nps\nWHERE 기준년월일 = '20260225' AND 채널명 = 'KB 스타뱅킹';",
        ],
        "notes": "",
    },
    "bu_cx_stage_nps_trend": {
        "view": "v_bu_cx_stage_nps_trend",
        "description": "BU 경험단계별 NPS 월말 스냅샷 [월별 추이]",
        "survey_type": "BU",
        "use_when": "BU 경험단계 NPS 추이, 월별 비교",
        "dim_columns": ["기준년월", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 기준년월, 채널명, 고객경험단계명 | METRIC: NPS점수, 전체건수, 추천건수, 추천비율, 중립건수, 중립비율, 비추천건수, 비추천비율",
        "ddl": """\
-- BU 고객경험단계별 NPS 월말 스냅샷 [월별 추이]
CREATE TABLE v_bu_cx_stage_nps_trend (
    기준년월 TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    NPS점수 REAL, 전체건수 INTEGER,
    추천건수 INTEGER, 추천비율 REAL,
    중립건수 INTEGER, 중립비율 REAL,
    비추천건수 INTEGER, 비추천비율 REAL
);""",
        "templates": [
            "SELECT 기준년월, 고객경험단계명, NPS점수\nFROM v_bu_cx_stage_nps_trend\nWHERE 기준년월 BETWEEN '202601' AND '202602' AND 채널명 = 'KB 스타뱅킹';",
        ],
        "notes": "",
    },

    # =========================================================================
    # BU 영향요인 (사전집계)
    # =========================================================================
    "bu_channel_driver": {
        "view": "v_bu_channel_driver",
        "description": "BU 채널 영향요인 일별 누적 (채널→고객경험단계 영향도) [연초~당일]",
        "survey_type": "BU",
        "use_when": "BU 채널 영향요인, 어떤 경험단계가 채널 NPS에 영향",
        "dim_columns": ["기준년월일", "채널명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명 | METRIC: 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수",
        "ddl": """\
-- BU 채널 영향요인 일별 누적 [연초~당일]
CREATE TABLE v_bu_channel_driver (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT,
    전체건수 INTEGER, 추천비율 REAL, 중립비율 REAL, 비추천비율 REAL,
    영향도점수 REAL
);""",
        "templates": [
            "SELECT 고객경험단계명, 영향도점수\nFROM v_bu_channel_driver\nWHERE 기준년월일 = '20260225' AND 채널명 = 'KB 스타뱅킹'\nORDER BY 영향도점수 DESC;",
        ],
        "notes": "",
    },
    "bu_channel_driver_trend": {
        "view": "v_bu_channel_driver_trend",
        "description": "BU 채널 영향요인 월말 스냅샷 (전월/전전월 비교 포함) [월말]",
        "survey_type": "BU",
        "use_when": "BU 채널 영향도 추이, 전월 대비",
        "dim_columns": ["기준년월", "채널명"],
        "brief_columns": "DIM: 기준년월, 채널명, 고객경험단계명 | METRIC: 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전전월영향도점수",
        "ddl": """\
-- BU 채널 영향요인 월말 스냅샷 [전월/전전월 비교]
CREATE TABLE v_bu_channel_driver_trend (
    기준년월 TEXT, 채널명 TEXT, 고객경험단계명 TEXT,
    전체건수 INTEGER, 추천비율 REAL, 중립비율 REAL, 비추천비율 REAL,
    영향도점수 REAL, 전월영향도점수 REAL, 전전월영향도점수 REAL
);""",
        "templates": [
            "SELECT 고객경험단계명, 영향도점수, 전월영향도점수, 전전월영향도점수\nFROM v_bu_channel_driver_trend\nWHERE 기준년월 = '202602' AND 채널명 = 'KB 스타뱅킹';",
        ],
        "notes": "",
    },
    "bu_stage_driver": {
        "view": "v_bu_stage_driver",
        "description": "BU 경험단계 영향요인 일별 누적 (경험단계→서비스품질요소 영향도) [연초~당일]",
        "survey_type": "BU",
        "use_when": "BU 서비스품질요소별 영향도, 어떤 요인이 경험단계 NPS에 영향",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명, 서비스품질명 | METRIC: 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수",
        "ddl": """\
-- BU 경험단계 영향요인 일별 누적 [연초~당일]
CREATE TABLE v_bu_stage_driver (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT, 서비스품질명 TEXT,
    전체건수 INTEGER, 추천비율 REAL, 중립비율 REAL, 비추천비율 REAL,
    영향도점수 REAL
);""",
        "templates": [
            "SELECT 서비스품질명, 영향도점수\nFROM v_bu_stage_driver\nWHERE 기준년월일 = '20260225' AND 채널명 = 'KB 스타뱅킹'\n  AND 고객경험단계명 = '접속/로그인'\nORDER BY 영향도점수 DESC;",
        ],
        "notes": "",
    },
    "bu_stage_driver_trend": {
        "view": "v_bu_stage_driver_trend",
        "description": "BU 경험단계 영향요인 월말 스냅샷 (전월/전전월 비교 포함) [월말]",
        "survey_type": "BU",
        "use_when": "BU 서비스품질요소별 영향도 추이, 전월 대비",
        "dim_columns": ["기준년월", "채널명", "고객경험단계명"],
        "brief_columns": "DIM: 기준년월, 채널명, 고객경험단계명, 서비스품질요소명 | METRIC: 전체건수, 추천비율, 중립비율, 비추천비율, 영향도점수, 전월영향도점수, 전전월영향도점수",
        "ddl": """\
-- BU 경험단계 영향요인 월말 스냅샷 [전월/전전월 비교]
CREATE TABLE v_bu_stage_driver_trend (
    기준년월 TEXT, 채널명 TEXT, 고객경험단계명 TEXT, 서비스품질요소명 TEXT,
    전체건수 INTEGER, 추천비율 REAL, 중립비율 REAL, 비추천비율 REAL,
    영향도점수 REAL, 전월영향도점수 REAL, 전전월영향도점수 REAL
);""",
        "templates": [
            "SELECT 서비스품질요소명, 영향도점수, 전월영향도점수, 전전월영향도점수\nFROM v_bu_stage_driver_trend\nWHERE 기준년월 = '202602' AND 채널명 = 'KB 스타뱅킹'\n  AND 고객경험단계명 = '접속/로그인';",
        ],
        "notes": "",
    },

    # =========================================================================
    # BU VOC (사전집계)
    # =========================================================================
    "bu_cx_element_voc": {
        "view": "v_bu_cx_element_voc",
        "description": "BU 고객경험요소별 VOC 감정·유형 일별 누적 [연초~당일]",
        "survey_type": "BU",
        "use_when": "BU 고객경험요소별 VOC 현황, NSS, CCI, 감정분석, 불만 많은 영역",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명", "서비스품질명", "고객경험요소명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명 | METRIC: 전체건수, 긍정건수, 부정건수, 중립건수, 칭찬건수, 불만건수, 개선건수, 기타건수, 긍정비율, 부정비율, 중립비율, 칭찬비율, 불만비율, 개선비율, 기타비율, NSS점수, CCI점수",
        "ddl": """\
-- BU 고객경험요소별 VOC 감정·유형 일별 누적 [연초~당일]
CREATE TABLE v_bu_cx_element_voc (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT,
    서비스품질명 TEXT, 고객경험요소명 TEXT,
    전체건수 INTEGER,
    긍정건수 INTEGER, 부정건수 INTEGER, 중립건수 INTEGER,
    칭찬건수 INTEGER, 불만건수 INTEGER, 개선건수 INTEGER, 기타건수 INTEGER,
    긍정비율 REAL, 부정비율 REAL, 중립비율 REAL,
    칭찬비율 REAL, 불만비율 REAL, 개선비율 REAL, 기타비율 REAL,
    NSS점수 REAL, CCI점수 REAL
);""",
        "templates": [
            "SELECT 고객경험요소명, 전체건수, NSS점수, CCI점수, 불만건수\nFROM v_bu_cx_element_voc\nWHERE 기준년월일 = '20260225' AND 채널명 = 'KB 스타뱅킹'\nORDER BY CCI점수 DESC\nLIMIT 20;",
        ],
        "notes": "NSS점수 = 긍정비율 - 부정비율. CCI점수 = 불만비율.",
    },
    "bu_cx_element_voc_monthly": {
        "view": "v_bu_cx_element_voc_monthly",
        "description": "BU 고객경험요소별 VOC 감정·유형 월별 단독 집계 [월 단독]",
        "survey_type": "BU",
        "use_when": "BU 고객경험요소별 VOC 월별 비교",
        "dim_columns": ["기준년월", "채널명", "고객경험단계명", "서비스품질명", "고객경험요소명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명, 서비스품질명, 고객경험요소명 | METRIC: (v_bu_cx_element_voc와 동일)",
        "ddl": """\
-- BU 고객경험요소별 VOC 월별 단독 집계 [월 단독]
CREATE TABLE v_bu_cx_element_voc_monthly (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT,
    서비스품질명 TEXT,
    전체건수 INTEGER,
    긍정건수 INTEGER, 부정건수 INTEGER, 중립건수 INTEGER,
    칭찬건수 INTEGER, 불만건수 INTEGER, 개선건수 INTEGER, 기타건수 INTEGER,
    긍정비율 REAL, 부정비율 REAL, 중립비율 REAL,
    칭찬비율 REAL, 불만비율 REAL, 개선비율 REAL, 기타비율 REAL,
    NSS점수 REAL, CCI점수 REAL
);""",
        "templates": [
            "SELECT 전체건수, NSS점수, CCI점수\nFROM v_bu_cx_element_voc_monthly\nWHERE 기준년월일 = '20260225' AND 채널명 = 'KB 스타뱅킹'\nORDER BY 전체건수 DESC;",
        ],
        "notes": "",
    },

    # =========================================================================
    # BU VOC 원문
    # =========================================================================
    "bu_voc_raw": {
        "view": "v_bu_voc_raw",
        "description": "BU VOC 원문 조회 (서술형 마스터 기반)",
        "survey_type": "BU",
        "use_when": "BU VOC 원문, 고객 목소리, 실제 의견 확인 (BU)",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명", "서비스품질명"],
        "brief_columns": "DIM: 기준년월일, 채널명, 고객경험단계명 | TEXT: VOC원문내용, 상품서비스용어내용, 성능품질용어내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명, 추천영향요인내용 | FILTER: VOC필터링여부",
        "ddl": """\
-- BU VOC 원문 (BU 서술형 마스터 뷰, MGF4)
CREATE TABLE v_bu_voc_raw (
    기준년월일 TEXT,                  -- YYYYMMDD
    설문ID TEXT, 설문참여대상자고유ID TEXT, 문항ID TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    VOC원문내용 TEXT,
    VOC필터링여부 INTEGER,            -- 0=정상처리, 1=필터링(무의미/짧은응답 등 추출불가)
    고객경험요소분류성공여부 INTEGER, -- 0/1
    고객경험요소명 TEXT,
    서비스품질명 TEXT,               -- 서비스품질요소명과 동일 의미
    상품서비스용어내용 TEXT, 성능품질용어내용 TEXT,
    고객감정대분류명 TEXT,           -- '긍정','부정','중립'
    고객경험VOC유형명 TEXT,          -- '칭찬','불만','개선','기타'
    배분여부 INTEGER,               -- 0/1
    과제진행상태명 TEXT,             -- '검토완료','미처리','검토기한만료'
    검토구분 TEXT,                   -- '현행유지','개선예정','개선불가'
    검토년월일 TEXT,
    과제검토명 TEXT, 과제검토의견내용 TEXT,
    설문고객연령 INTEGER,
    연령5세내용 TEXT, 성별내용 TEXT,
    거래기간수 INTEGER, 실질고객내용 TEXT,  -- TD 고객등급내용과 동일 값범위
    에피소드유형내용 TEXT, 에피소드상세내용 TEXT,
    주직무구분명 TEXT, 직군구분명 TEXT,
    지역영업그룹명 TEXT,
    고객경험요소 TEXT,               -- 고객경험요소명과 별도 (원본 추출값)
    개선부서명 TEXT, 개선사업그룹명 TEXT,
    기준년월 TEXT,                    -- YYYYMM
    기준주시작일 TEXT,               -- YYYYMMDD, 해당 주 월요일
    과제추진사업내용 TEXT,
    VOC문제원인내용 TEXT,
    추천영향요인내용 TEXT,           -- 해당무→고객경험단계, 그외→서비스품질요소
    VOC필터링사유내용 TEXT,
    설문고객연령5세내용 INTEGER,      -- 15, 20, 25, ..., 70
    설문고객연령10세내용 INTEGER      -- 10, 20, 30, 40, 50, 60, 70
);""",
        "templates": [
            "SELECT VOC원문내용, 고객감정대분류명, 고객경험VOC유형명, 고객경험요소명\nFROM v_bu_voc_raw\nWHERE 기준년월일 BETWEEN '20260201' AND '20260225'\n  AND 채널명 = 'KB 스타뱅킹' AND VOC필터링여부 = 0\nLIMIT 50;",
            "SELECT VOC원문내용, 고객경험요소명, 서비스품질명\nFROM v_bu_voc_raw\nWHERE 기준년월일 BETWEEN '20260201' AND '20260225'\n  AND 채널명 = 'KB 스타뱅킹' AND 고객경험VOC유형명 = '불만'\n  AND VOC필터링여부 = 0\nLIMIT 30;",
        ],
        "notes": "VOC필터링여부=0 조건 필수.",
    },

    # =========================================================================
    # BU 스펙트럼 (마스터 기반 동적 쿼리)
    # =========================================================================
    "bu_spectrum_nps": {
        "view": "v_bu_spectrum_nps",
        "description": "BU 고객 세그먼트(스펙트럼)별 NPS (마스터 기반 동적 GROUP BY)",
        "survey_type": "BU",
        "use_when": "BU 연령대별, 성별, 고객등급별, 에피소드별 NPS, BU 스펙트럼",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 성별내용, 거래기간수, 실질고객내용, 에피소드유형내용, 에피소드상세내용, 주직무구분명, 직군구분명, 지역영업그룹명 | COMPUTED: 추천점수 기반 NPS 직접 계산",
        "ddl": """\
-- BU 점수형 마스터 (스펙트럼 NPS 분석 기반, MGF3)
-- 선택 가능한 스펙트럼 칼럼:
--   설문고객연령5세내용: 20, 25, 30, 35, 40, 45, 50, 55, 60, 65
--   설문고객연령10세내용: 20, 25, 30, 35, 40, 45, 50, 55, 60, 65
--   성별내용: '남자','여자'
--   거래기간수: 숫자(년 단위, 현재 데이터 부족으로 0이 대부분)
--   실질고객내용(TD의 고객등급내용과 동일 값범위): 'VVIP','VIP','패밀리','그랜드','베스트'
--   고객충성도내용: '추천','중립','비추천'
--   에피소드유형내용: '예금','대출','펀드','해외송금','외화환전','주택도시기금','신탁','보험','계리발생','계좌이체'
--   에피소드상세내용: '적금','정기예금','신용대출','입출금','예적금담보대출','펀드','해외송금','외화환전','주택도시기금대출-전세(버팀목)','신탁','주택담보대출','보험','전월세대출','여신상담','계좌이체',...
--   주직무구분명: '비대면 PB고객 담당','비대면 개인고객 담당','개인고객담당','PB고객담당','기업고객담당',...
--   직군구분명: '일반(일반)','사무(PT)','일반(임금피크)','기능','사무(텔러)','일반직원'
--   지역영업그룹명: '강남영업추진그룹', '강북영업추진그룹', '수도권영업추진그룹', '영남영업추진그룹', '충청호남영업추진그룹'
CREATE TABLE v_bu_spectrum_nps (
    기준년월일 TEXT, 설문ID TEXT, 설문참여대상자고유ID TEXT, 문항ID TEXT,
    채널명 TEXT, 고객경험단계명 TEXT,
    추천점수 INTEGER,
    영향요인구분명 TEXT,  -- 추천/비추천 영향요인 구분
    설문고객연령5세내용 INTEGER, 설문고객연령5세내용 INTEGER, 성별내용 TEXT, 거래기간수 INTEGER, 실질고객내용 TEXT,
    고객충성도내용 TEXT,
    에피소드유형내용 TEXT, 에피소드상세내용 TEXT,
    주직무구분명 TEXT, 직군구분명 TEXT,
    지역영업그룹명 TEXT
);""",
        "templates": [
            "-- 연령대별 NPS\nSELECT 연령5세내용 AS 스펙트럼, COUNT(*) AS 전체건수,\n  ROUND(SUM(CASE WHEN 추천점수>=9 THEN 1 ELSE 0 END)*100.0/COUNT(*),2)\n  - ROUND(SUM(CASE WHEN 추천점수<=6 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NPS점수\nFROM v_bu_spectrum_nps\nWHERE 기준년월일 BETWEEN SUBSTR('20260225',1,4)||'0101' AND '20260225'\n  AND 채널명 = 'KB 스타뱅킹'\nGROUP BY 연령5세내용\nORDER BY 연령5세내용;",
        ],
        "notes": "스펙트럼 뷰는 마스터 테이블 기반. NPS를 추천점수 기반 CASE WHEN으로 직접 계산. 전체건수 30건 미만 시 해석 주의. 채널 nps를 알려면 고객경험단계명='해당무' 필요.",
    },
    "bu_spectrum_voc": {
        "view": "v_bu_spectrum_voc",
        "description": "BU 고객 세그먼트(스펙트럼)별 VOC 감정·유형 집계 (마스터 기반 동적 GROUP BY)",
        "survey_type": "BU",
        "use_when": "BU 연령대별, 성별, 고객등급별 VOC 분포, BU 스펙트럼 VOC",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명"],
        "brief_columns": "GROUP BY 선택 가능: 연령5세내용, 성별내용, 거래기간수, 실질고객내용, 에피소드유형내용, 에피소드상세내용, 주직무구분명, 직군구분명, 지역영업그룹명 | COMPUTED: VOC감정/VOC유형 기반 NSS, CCI 직접 계산 | FILTER: VOC필터링여부=0 필수",
        "ddl": """\
-- BU 서술형 마스터 (스펙트럼 VOC 분석 기반, MGF4)
-- 전체 칼럼: bu_voc_raw 참조 (동일 뷰)
-- 선택 가능한 스펙트럼 칼럼:
--   설문고객연령5세내용: 20, 25, 30, 35, 40, 45, 50, 55, 60, 65
--   설문고객연령10세내용: 20, 25, 30, 35, 40, 45, 50, 55, 60, 65
--   성별내용: '남자','여자'
--   거래기간수: 숫자(년 단위, 현재 데이터 부족으로 0이 대부분)
--   실질고객내용(TD의 고객등급내용과 동일 값범위): 'VVIP','VIP','패밀리','그랜드','베스트'
--   에피소드유형내용: '예금','대출','펀드','해외송금','외화환전','주택도시기금','신탁','보험','계리발생','계좌이체'
--   에피소드상세내용: '적금','정기예금','신용대출','입출금','예적금담보대출','펀드','해외송금','외화환전','주택도시기금대출-전세(버팀목)','신탁','주택담보대출','보험','전월세대출','여신상담','계좌이체',...
--   주직무구분명: '비대면 PB고객 담당','비대면 개인고객 담당','개인고객담당','PB고객담당','기업고객담당',...
--   직군구분명: '일반(일반)','사무(PT)','일반(임금피크)','기능','사무(텔러)','일반직원'
--   지역영업그룹명: '강남영업추진그룹', '강북영업추진그룹', '수도권영업추진그룹', '영남영업추진그룹', '충청호남영업추진그룹'
CREATE TABLE v_bu_spectrum_voc (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT,
    VOC필터링여부 INTEGER,
    고객감정대분류명 TEXT, 고객경험VOC유형명 TEXT, 설문고객연령5세내용 INTEGER, 설문고객연령10세내용 INTEGER, 성별내용 TEXT, 거래기간수 INTEGER, 실질고객내용 TEXT, 에피소드유형내용 TEXT, 에피소드상세내용 TEXT, 직원번호 TEXT, 직원한글성명 TEXT, 주직무구분명 TEXT, 직군구분명 TEXT, 영업점명 TEXT, 지역본부명 TEXT, 지역영업그룹명 TEXT
);""",
        "templates": [
            "SELECT 연령5세내용 AS 스펙트럼, COUNT(*) AS 전체건수,\n  ROUND(SUM(CASE WHEN 고객감정대분류명='긍정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2)\n  - ROUND(SUM(CASE WHEN 고객감정대분류명='부정' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS NSS,\n  ROUND(SUM(CASE WHEN 고객경험VOC유형명='불만' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS CCI\nFROM v_bu_spectrum_voc\nWHERE 기준년월일 BETWEEN SUBSTR('20260225',1,4)||'0101' AND '20260225'\n  AND 채널명 = 'KB 스타뱅킹' AND VOC필터링여부 = 0\nGROUP BY 연령5세내용\nORDER BY 연령5세내용;",
        ],
        "notes": "VOC필터링여부=0 조건 필수. VOC 집계용.",
    },

    # =========================================================================
    # 개선조치 (BU 전용, 서술형 마스터 기반)
    # =========================================================================
    "improvement_by_dept": {
        "view": "v_bu_spectrum_voc",
        "description": "부서별 개선조치 배분/처리 현황 (BU 서술형 마스터 기반 집계 쿼리)",
        "survey_type": "BU",
        "use_when": "개선부서별 배분, 처리율, 검토완료, 미처리, 개선예정 건수",
        "dim_columns": ["기준년월일", "채널명"],
        "brief_columns": "GROUP BY: 개선사업그룹명, 개선부서명 | COMPUTED: 배분건수, 검토완료건수, 처리율, 미처리건수, 검토기한만료건수, 현행유지건수, 개선예정건수, 개선불가건수 | FILTER: 배분여부=1 AND VOC필터링여부=0 필수",
        "ddl": """\
-- 부서별 개선조치 현황 (BU 서술형 마스터에서 집계, MGF4)
-- 과제진행상태명: '검토완료','미처리','검토기한만료'
-- 검토구분: '현행유지','개선예정','개선불가' (검토완료 건만)
-- 배분여부=1 AND VOC필터링여부=0 조건 필수
CREATE TABLE v_bu_spectrum_voc (
    기준년월일 TEXT,
    배분여부 INTEGER, 과제진행상태명 TEXT, 검토구분 TEXT,
    개선사업그룹명 TEXT, 개선부서명 TEXT,
    VOC필터링여부 INTEGER
);""",
        "templates": [
            "SELECT 개선사업그룹명, 개선부서명,\n  SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수,\n  SUM(CASE WHEN 과제진행상태명='검토완료' THEN 1 ELSE 0 END) AS 검토완료건수,\n  ROUND(SUM(CASE WHEN 과제진행상태명='검토완료' THEN 1 ELSE 0 END)*100.0\n    / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 처리율,\n  SUM(CASE WHEN 과제진행상태명='미처리' THEN 1 ELSE 0 END) AS 미처리건수,\n  SUM(CASE WHEN 과제진행상태명='검토기한만료' THEN 1 ELSE 0 END) AS 검토기한만료건수,\n  SUM(CASE WHEN 검토구분='현행유지' THEN 1 ELSE 0 END) AS 현행유지건수,\n  SUM(CASE WHEN 검토구분='개선예정' THEN 1 ELSE 0 END) AS 개선예정건수,\n  SUM(CASE WHEN 검토구분='개선불가' THEN 1 ELSE 0 END) AS 개선불가건수\nFROM v_bu_spectrum_voc\nWHERE SUBSTR(기준년월일,1,6) = '202602'\n  AND 배분여부 = 1 AND VOC필터링여부 = 0\nGROUP BY 개선사업그룹명, 개선부서명\nORDER BY 개선사업그룹명, 개선부서명;",
        ],
        "notes": "배분여부=1 AND VOC필터링여부=0 조건 필수.",
    },
    "improvement_by_factor": {
        "view": "v_bu_spectrum_voc",
        "description": "서비스품질요소별 개선조치 처리 현황 (BU 서술형 마스터 기반 집계 쿼리)",
        "survey_type": "BU",
        "use_when": "서비스품질요소별 배분, 처리율, 검토 현황",
        "dim_columns": ["기준년월일", "채널명", "고객경험단계명", "서비스품질명"],
        "brief_columns": "GROUP BY: 채널명, 고객경험단계명, 서비스품질명 | COMPUTED: (improvement_by_dept와 동일 지표)",
        "ddl": """\
-- 서비스품질요소별 개선조치 현황 (BU 서술형 마스터에서 집계, MGF4)
CREATE TABLE v_bu_spectrum_voc (
    기준년월일 TEXT, 채널명 TEXT, 고객경험단계명 TEXT, 서비스품질명 TEXT,
    배분여부 INTEGER, 과제진행상태명 TEXT, 검토구분 TEXT,
    VOC필터링여부 INTEGER
);""",
        "templates": [
            "SELECT 채널명, 고객경험단계명, 서비스품질명,\n  SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END) AS 배분건수,\n  SUM(CASE WHEN 과제진행상태명='검토완료' THEN 1 ELSE 0 END) AS 검토완료건수,\n  ROUND(SUM(CASE WHEN 과제진행상태명='검토완료' THEN 1 ELSE 0 END)*100.0\n    / NULLIF(SUM(CASE WHEN 배분여부=1 THEN 1 ELSE 0 END),0), 2) AS 처리율\nFROM v_bu_spectrum_voc\nWHERE SUBSTR(기준년월일,1,6) = '202602'\n  AND 배분여부 = 1 AND VOC필터링여부 = 0\nGROUP BY 채널명, 고객경험단계명, 서비스품질명\nORDER BY 채널명, 고객경험단계명, 서비스품질명;",
        ],
        "notes": "배분여부=1 AND VOC필터링여부=0 조건 필수.",
    },
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_catalog() -> list[dict[str, Any]]:
    """VIEW_REGISTRY에서 query_planner용 카탈로그 리스트를 생성한다."""
    catalog = []
    for view_id, vdef in VIEW_REGISTRY.items():
        catalog.append({
            "id": view_id,
            "view": vdef.get("view", ""),
            "description": vdef.get("description", ""),
            "survey_type": vdef.get("survey_type", ""),
            "use_when": vdef.get("use_when", ""),
            "dim_columns": vdef.get("dim_columns", []),
            "brief_columns": vdef.get("brief_columns", ""),
        })
    return catalog


# 하위 호환: 기존 코드에서 QUERY_CATALOG을 직접 참조하는 경우
QUERY_CATALOG = get_catalog()


def get_view_detail(view_id: str) -> dict | None:
    """view_id에 해당하는 상세 정보(ddl, templates, notes)를 반환."""
    vdef = VIEW_REGISTRY.get(view_id)
    if not vdef:
        return None
    return {
        "ddl": vdef.get("ddl", ""),
        "templates": vdef.get("templates", []),
        "notes": vdef.get("notes", ""),
    }


def format_view_details_for_prompt(view_ids: list[str]) -> str:
    """선택된 view_id 목록에 대한 DDL + templates를 프롬프트용 텍스트로 포매팅."""
    parts = []
    for vid in view_ids:
        detail = get_view_detail(vid)
        if not detail:
            parts.append(f"-- [WARNING] '{vid}': 상세 정보 없음")
            continue

        section = f"### {vid}\n\n{detail['ddl']}\n"

        if detail.get("notes"):
            section += f"\n-- 주의: {detail['notes']}\n"

        templates = detail.get("templates", [])
        if templates:
            section += "\n-- 쿼리 예제:\n"
            for i, tmpl in enumerate(templates, 1):
                section += f"\n-- 예제 {i}:\n{tmpl}\n"

        parts.append(section)

    return "\n\n".join(parts)
