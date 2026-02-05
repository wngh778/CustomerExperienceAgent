import asyncio
import pandas as pd
from pathlib import Path
from datetime import timedelta
from typing import List, Dict, Tuple, Union
from .utils import (
    ### 고정값 설정
    PROMPT_PATH,
    SQL_PATH,
    REPORT_TEMPLATE_PATH,
    OUTPUT_DIR,
    REPORT_NAME,
    CATEGORIES,
    POSTPROCESS_MAP,
    ### 보고서 생성 관련 함수
    build_query_index,
    fetch_all_queries,
    postprocess_bank,
    apply_postprocess,
    generate_report_for_chunk,
    save_category_reports,
    md_to_docx,
    df_to_xml,
    ### 날짜 처리 함수
    make_td_nps_report_date_params
)

# -------------------------------------------------
# 메인 엔트리 – 전체 흐름
# -------------------------------------------------
async def generate_td_nps_report(mcp_executor, user_id, llm, prompts:dict, today_date) -> Path:

    CATEGORY_MAP = {
        "은행": "bank",
        "브랜드": "brand",
        "플랫폼": "platform",
        "대면채널": "face",
        "고객센터": "center",
        "상품": "product",
    }

    QUERY_INDEX = build_query_index(CATEGORIES) # 전역에서 재사용

    date_params = await make_td_nps_report_date_params(mcp_executor)

    yyyy        = date_params.get('yyyy')
    yyyyhf      = date_params.get('yyyyhf')
    yyyy_b1hf   = date_params.get('yyyy_b1hf')
    yyyyhf_b1hf = date_params.get('yyyyhf_b1hf')

    BASE_PLACEHOLDER_MAP = {
        "{yyyy}"       : yyyy,
        "{yyyyhf}"     : yyyyhf,
        "{yyyy_b1hf}"  : yyyy_b1hf,
        "{yyyyhf_b1hf}": yyyyhf_b1hf,
    }

    # 0️ 전체 쿼리 실행
    query_results = await fetch_all_queries(SQL_PATH, BASE_PLACEHOLDER_MAP, QUERY_INDEX, mcp_executor)

    # 1️ 은행 전용 후처리 (NPS 현황, NPS 영향요인)
    bm_info, pstv_fac, weak_fac, weak1_fac = postprocess_bank(
        query_results[("은행", "TD_은행_01")],
        query_results[("은행", "TD_은행_02")]
    )

    # 2️ 공통 placeholder
    common_placeholders = {
        "{bm_info}"  : df_to_xml(bm_info),
        "{pstv_fac}" : df_to_xml(pstv_fac),
        "{weak_fac}" : df_to_xml(weak_fac),
        "{weak1_fac}": df_to_xml(weak1_fac),
    }
    group_map: Dict[Tuple[str, str, str], List[pd.DataFrame]] = {}
    for (cat, qid), (depth, chunk) in QUERY_INDEX.items():
        df_raw = query_results[(cat, qid)]
        # 후처리 적용
        df_processed = apply_postprocess(POSTPROCESS_MAP, cat, qid, df_raw, bm_info)
        key = (cat, depth, chunk)
        group_map.setdefault(key, []).append(df_processed)

    # 3️ 카테고리·depth·청크 순회
    results = []
    for (cat, depth, chunk), df_list in group_map.items(): 
        res = await generate_report_for_chunk(CATEGORIES, BASE_PLACEHOLDER_MAP, user_id, llm, OUTPUT_DIR, prompts, cat, depth, chunk, df_list, common_placeholders) 
        results.append(res)

    md_parts: List[str] = []
    cat_md_map: Dict[str, List[str]] = {c: [] for c in CATEGORIES.keys()} # 카테고리별 md생성

    # 5️ 결과 합치기
    for res in results:
        if isinstance(res, Exception):
            continue
        cat, md = res
        md_parts.append(md + "\n")
        cat_md_map[cat].append(md + "\n")

    await save_category_reports(BASE_PLACEHOLDER_MAP, REPORT_NAME, CATEGORY_MAP, OUTPUT_DIR, cat_md_map, False)

    # 6️ 최종 markdown → docx 변환
    final_md = "\n".join(md_parts)
    final_md_path = OUTPUT_DIR / f"{REPORT_NAME.replace(' ', '_')}_{yyyy}_{yyyyhf}_all.md"
    final_md_path.parent.mkdir(parents=True, exist_ok=True)
    final_md_path.write_text(final_md, encoding="utf-8")

    final_docx_path = OUTPUT_DIR / f"{REPORT_NAME.replace(' ', '_')}_{yyyy}_{yyyyhf}_all.docx"
    md_to_docx(final_md_path, final_docx_path)

    await save_category_reports(BASE_PLACEHOLDER_MAP, REPORT_NAME, CATEGORY_MAP, OUTPUT_DIR, cat_md_map, False)
    return final_docx_path

# -------------------------------------------------
# 8️⃣ 직접 실행용
# -------------------------------------------------
if __name__ == "__main__":
    asyncio.run(generate_td_nps_report())