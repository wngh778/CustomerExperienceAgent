"""뷰 별칭 ↔ 실제 INST1 테이블 코드 매핑 및 SQL 변환.

LLM이 생성한 SQL에는 가독성 좋은 별칭(v_xxx)이 포함되어 있으며,
``translate_sql()`` 함수로 실제 DB 테이블명(INST1.TSCCVMG*)으로 치환한 뒤 실행한다.

주요 공개 심볼:
    - ``TABLE_ALIAS_CODE_MAP``: 별칭 → INST1 테이블 코드 매핑 딕셔너리
    - ``translate_sql()``: SQL 내 별칭을 실제 테이블명으로 치환
"""

import re


# =============================================================================
# 별칭 → INST1 테이블 코드 매핑
# =============================================================================

TABLE_ALIAS_CODE_MAP: dict[str, str] = {
    # --- TD 사전집계 ---
    "v_td_channel_nps":        "INST1.TSCCVMGC1",
    "v_td_cx_stage_nps":       "INST1.TSCCVMGC6",
    "v_td_channel_driver":     "INST1.TSCCVMGC2",
    "v_td_cx_stage_driver":    "INST1.TSCCVMGC7",
    "v_td_channel_ipa":        "INST1.TSCCVMGC3",
    "v_td_cx_stage_ipa":       "INST1.TSCCVMGC8",
    "v_td_voc_type":           "INST1.TSCCVMGD1",
    "v_td_voc_sentiment":      "INST1.TSCCVMGC9",

    # --- TD 마스터 ---
    "v_td_spectrum_nps":       "INST1.TSCCVMGF1",
    "v_td_spectrum_driver":    "INST1.TSCCVMGF1",
    "v_td_spectrum_channel_ipa":    "INST1.TSCCVMGF1",
    "v_td_spectrum_cx_stage_ipa":    "INST1.TSCCVMGF1",
    "v_td_spectrum_voc":       "INST1.TSCCVMGF2",
    "v_td_voc_raw":            "INST1.TSCCVMGF2",

    # --- BU 사전집계 (일별 누적) ---
    "v_bu_channel_nps":        "INST1.TSCCVMGD5",
    "v_bu_cx_stage_nps":       "INST1.TSCCVMGD7",
    "v_bu_channel_driver":     "INST1.TSCCVMGD9",
    "v_bu_stage_driver":       "INST1.TSCCVMGE2",
    "v_bu_cx_element_voc":     "INST1.TSCCVMGE4",

    # --- BU 사전집계 (월말/추이) ---
    "v_bu_channel_nps_trend":        "INST1.TSCCVMGD6",
    "v_bu_cx_stage_nps_trend":       "INST1.TSCCVMGD8",
    "v_bu_channel_driver_trend":     "INST1.TSCCVMGE1",
    "v_bu_stage_driver_trend":       "INST1.TSCCVMGE3",
    "v_bu_cx_element_voc_monthly":   "INST1.TSCCVMGE5",

    # --- BU 마스터 ---
    "v_bu_spectrum_nps":       "INST1.TSCCVMGF3",
    "v_bu_spectrum_voc":       "INST1.TSCCVMGF4",
    "v_bu_voc_raw":            "INST1.TSCCVMGF4",
}


def translate_sql(sql: str) -> str:
    """SQL 내 뷰 별칭(v_xxx)을 실제 INST1 테이블명으로 치환한다.

    긴 별칭부터 먼저 매칭하여 부분 치환 문제를 방지한다.

    Args:
        sql: 별칭이 포함된 SQL 문자열

    Returns:
        INST1.TSCCVMG* 테이블명으로 치환된 SQL
    """
    result = sql
    for alias, code in sorted(TABLE_ALIAS_CODE_MAP.items(), key=lambda x: -len(x[0])):
        result = re.sub(rf'\b{re.escape(alias)}\b', code, result)
    return result
