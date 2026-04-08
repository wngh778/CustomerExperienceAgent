import os
import json
from typing import Any, Dict, Callable, List
from datetime import datetime
from .utils import (
    JINJA_ENV,
    _RENDERER_REGISTRY,
    register_renderer,
    render_template,
    _to_dict,
    extract_data_by_type,
    _round_diff,
    safe_float,
    diff_and_flag,
    has_batchim
)


##############################################################
#                        TD 랜더러                           #
##############################################################

# TD_요약_NPS진단
@register_renderer("TD_요약_NPS진단")
def render_TD_요약_NPS진단(data: Any) -> str:
    """
    td_nps 와 td_cx_nps_summary 두 쿼리 결과를 하나의 템플릿에 전달
    - data : list[dict] / pandas.DataFrame / list[list[dict]] 등
    - 반환값 : Jinja2 템플릿이 렌더링된 문자열

    사용 쿼리
        - 1-1. 채널 NPS 진단 - 이하 td_nps
        - 2-0. 고객경험단계 NPS 진단 - 이하 td_cx_nps_summary
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    #   td_nps   → "td_nps"
    #   td_cx_nps_summary → "td_cx_nps_summary"
    data_by_type = extract_data_by_type(records)

    td_nps_records = data_by_type.get("td_nps", [])
    td_cx_records = data_by_type.get("td_cx_nps_summary", [])

    # 레코드 - KB국민은행
    kb_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 시장평균 제외
    filter_td_nps_records = [
        r for r in td_nps_records
        if r.get("거래은행구분") != "시장평균"
    ]
        
    # 레코드 - NPS 순위 (내림차순)
    sorted_by_nps = sorted(
        filter_td_nps_records,
        key=lambda x: float(x.get("NPS점수", 0)),
        reverse=True,
    )
    rank = next(
        (idx + 1 for idx, r in enumerate(sorted_by_nps) if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 벤치마크
    benchmark_name = kb_rec.get("벤치마크은행구분")
    benchmark_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == benchmark_name),
        None,
    )

    # 레코드 - 시장 평균
    market_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == "시장평균"),
        None,
    )

    # NPS, 비추천비중 차이·플래그
    kb_nps = float(kb_rec["NPS점수"])
    bench_nps = float(benchmark_rec["NPS점수"])
    market_nps = float(market_rec["NPS점수"])

    bench_nps_diff, bench_nps_flag = diff_and_flag(kb_nps, bench_nps, True)
    market_nps_diff, market_nps_flag = diff_and_flag(kb_nps, market_nps, False)

    kb_detractor = float(kb_rec["비추천비중"])
    bench_detractor = float(benchmark_rec["비추천비중"])
    detractor_diff, detractor_flag = diff_and_flag(bench_detractor, kb_detractor, True)

    # 채널명 (은행이면 빈 문자열)
    channel_raw = kb_rec.get("채널구분", "")
    channel_name = "" if channel_raw == "은행" else channel_raw

    # 템플릿 컨텍스트 - td_nps
    nps_context = {
        "채널명": channel_name,
        "NPS": f"{kb_nps:.1f}",
        "순위": rank,
        "벤치마크사명": benchmark_name,
        "벤치마크사_NPS_차이": f"{abs(bench_nps_diff):.1f}",
        "벤치마크사_NPS_플래그": bench_nps_flag,
        "시장평균_NPS_차이": f"{abs(market_nps_diff):.1f}",
        "시장평균_NPS_플래그": market_nps_flag,
        "비추천비중": f"{kb_detractor:.1f}",
        "벤치마크사_비추천비중_차이": f"{abs(detractor_diff):.1f}",
        "비추천비중_플래그": detractor_flag,
    }

    # td_cx_nps_summary 로직
    # 레코드 - 벤치마크사
    benchmark_cx_rec = next(
        (r for r in td_cx_records if r.get("거래은행구분") == r.get("벤치마크은행구분")),
        None,
    )
    if benchmark_cx_rec is None:
        raise ValueError("벤치마크 레코드를 찾을 수 없습니다.")

    benchmark_cx_name = benchmark_cx_rec["벤치마크은행구분"]
    benchmark_cx_nps = float(benchmark_cx_rec["벤치마크NPS점수"])

    # 레코드 - KB국민은행 컬럼만
    kb_cx_records = [r for r in td_cx_records if r.get("거래은행구분") == "KB국민은행"]

    # 가장 큰 열위 CX (benchmark NPS - KB NPS 가 가장 큰 양수)
    gap_cx = None
    gap_cx_nps = None
    max_gap = float("-inf")          # 양수 차이가 작을수록 “가장 큰 열위”

    for r in kb_cx_records:
        try:
            gap = float(r["벤치마크NPS점수갭"])
        except (KeyError, ValueError, TypeError):
            continue
        if gap > max_gap:
            max_gap = gap
            gap_cx = r["고객경험단계구분"]
            gap_cx_nps = float(r["NPS점수"])

    gap_diff = max_gap

    # NPS ≤ 0 인 CX 리스트
    negative_cxs: List[tuple[str, str]] = []
    for r in kb_cx_records:
        try:
            nps_val = float(r["NPS점수"])
        except (KeyError, ValueError, TypeError):
            continue
        if nps_val <= 0:
            negative_cxs.append((r["고객경험단계구분"], f"{nps_val:.1f}"))

    # td_cx_nps_summary 컨텍스트
    cx_context = {
        "GAP_CX": gap_cx,                         # GAP 1위 CX
        "GAP_CX_NPS": f"{gap_cx_nps:.1f}",        # GAP 1위 CX NPS
        "벤치마크사명": benchmark_cx_name,       # 벤치마크사명 (두 쿼리 모두 동일하다고 가정)
        "벤치마크사_차이": f"{abs(gap_diff):.1f}",     # 벤치마크 대비 차이
        "CX_NPS_0점이하": negative_cxs,           # [(CX, NPS), …]
    }

    # 템플릿 컨텍스트 - 최종 병합
    context = {**nps_context, **cx_context}

    # 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_요약_NPS진단.j2")
    return template.render(**context)


# TD_요약_문제영역
@register_renderer("TD_요약_문제영역")
def render_TD_요약_문제영역(data: Any) -> str:
    """
    td_ipa 전용 렌더러
    - data : list[dict] 혹은 pandas.DataFrame
    - 문제영역구분이 '중점개선'·'점진개선'인 레코드를 각각 리스트로 만든 뒤
      Jinja2 템플릿에 전달한다.

    사용 쿼리
        - 1-3. 채널 IPA 분석 - 이하 td_ipa
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 레코드 - 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    # 쿼리 타입별로 레코드 분리
    #   td_ipa → "td_ipa"
    td_ipa_records = data_by_type.get("td_ipa", [])

    # 레코드 - 벤치마크 은행명 (모든 레코드가 동일하다고 가정)
    benchmark_name = td_ipa_records[0].get("벤치마크은행구분", "")

    # 레코드 - 문제영역별 분리
    mid_improve: List[Dict[str, Any]] = []      # 중점개선
    gradual_improve: List[Dict[str, Any]] = []  # 점진개선

    for r in td_ipa_records:
        area = r.get("문제영역구분")
        # 템플릿에서 바로 사용할 키 이름을 정규화
        item = {
            "영향요인구분": r.get("영향요인구분", ""),
            "NPS중요도": r.get("NPS중요도", ""),
            "NPS영향도GAP": r.get("NPS영향도GAP", ""),
        }
        if area == "중점개선":
            mid_improve.append(item)
        elif area == "점진개선":
            gradual_improve.append(item)

    # 템플릿 컨텍스트
    context = {
        "벤치마크사명": benchmark_name,
        "중점개선": mid_improve,
        "점진개선": gradual_improve,
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_요약_문제영역.j2")
    return template.render(**context)


# TD_NPS진단_NPS진단
@register_renderer("TD_NPS진단_NPS진단")
def render_TD_NPS진단_NPS진단(data: Any) -> str:
    """
    td_nps 전용 렌더러
    - data : list[dict] 혹은 pandas.DataFrame

    사용 쿼리
        - 1-1. 채널 NPS 진단 - 이하 td_nps
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 레코드 - 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)
    td_nps_records = data_by_type.get("td_nps", [])

    # 레코드 - KB국민은행
    kb_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 시장평균 제외
    filter_td_nps_records = [
        r for r in td_nps_records
        if r.get("거래은행구분") != "시장평균"
    ]
        
    # 레코드 - NPS 순위 (내림차순)
    sorted_by_nps = sorted(
        filter_td_nps_records,
        key=lambda x: float(x.get("NPS점수", 0)),
        reverse=True,
    )
    rank = next(
        (idx + 1 for idx, r in enumerate(sorted_by_nps) if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

     # 레코드 - 벤치마크
    benchmark_name = kb_rec.get("벤치마크은행구분")
    benchmark_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == benchmark_name),
        None,
    )

    # 레코드 - 시장 평균
    market_rec = next(
        (r for r in td_nps_records if r.get("거래은행구분") == "시장평균"),
        None,
    )

    # NPS, 비추천비중 차이·플래그
    kb_nps = float(kb_rec["NPS점수"])
    bench_nps = float(benchmark_rec["NPS점수"])
    market_nps = float(market_rec["NPS점수"])

    bench_nps_diff, bench_nps_flag = diff_and_flag(kb_nps, bench_nps, True)
    market_nps_diff, market_nps_flag = diff_and_flag(kb_nps, market_nps, False)

    kb_detractor = float(kb_rec["비추천비중"])
    bench_detractor = float(benchmark_rec["비추천비중"])
    detractor_diff, detractor_flag = diff_and_flag(kb_detractor, bench_detractor, False)

    # 채널명 (은행이면 빈 문자열)
    channel_raw = kb_rec.get("채널구분", "")
    channel_name = "" if channel_raw == "은행" else channel_raw

    # 템플릿 컨텍스트
    context = {
        "채널명": channel_name,
        "NPS": f"{kb_nps:.1f}",
        "순위": rank,
        "벤치마크사명": benchmark_name,
        "벤치마크사_NPS_차이": f"{abs(bench_nps_diff):.1f}",
        "벤치마크사_NPS_플래그": bench_nps_flag,
        "시장평균_NPS_차이": f"{abs(market_nps_diff):.1f}",
        "시장평균_NPS_플래그": market_nps_flag,
        "비추천비중": f"{kb_detractor:.1f}",
        "벤치마크사_비추천비중_차이": f"{abs(detractor_diff):.1f}",
        "비추천비중_플래그": detractor_flag,
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_NPS진단_NPS진단.j2")
    rendered = template.render(**context)
    return rendered


# TD_NPS진단_영향요인
@register_renderer("TD_NPS진단_NPS영향요인")
def render_TD_NPS진단_NPS영향요인(data: Any) -> str:
    """
    td_factor 와 td_ipa 두 쿼리 결과를 하나의 템플릿에 전달

    사용 쿼리
        - 1-2. 채널 NPS 영향요인 - 이하 td_factor
        - 1-3. 채널 IPA 분석 - 이하 td_ipa
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    data_by_type = extract_data_by_type(records)

    td_factor_records = data_by_type.get("td_factor", [])
    td_ipa_records = data_by_type.get("td_ipa", [])

    # td_factor 로직 
    # 레코드 - KB국민은행
    kb_factor_recs = [r for r in td_factor_records if r.get("거래은행구분") == "KB국민은행"]
    kb_factor = kb_factor_recs[0]

    # 레코드 - 채널명 (은행이면 빈 문자열)
    channel_raw = kb_factor.get("채널구분", "")
    channel_name = "" if channel_raw == "은행" else channel_raw

    # 레코드 - KB‑NPS‑최상위 CX (응답률 기준, 채널전체 제외)
    kb_cx_candidates = [
        r for r in kb_factor_recs if r.get("영향요인구분") != "채널전체"
    ]

    kb_top_cx_rec = max(
        kb_cx_candidates,
        key=lambda r: float(r.get("전체대비응답비중", 0)),
    )
    kb_top_cx_name = kb_top_cx_rec.get("영향요인구분", "")
    kb_top_cx_rate = float(kb_top_cx_rec.get("전체대비응답비중", 0))

    # 레코드 - NPS 최상위기관 (채널전체 CX 에서 NPS영향도가 가장 높은 은행)
    channel_all = [
        r for r in td_factor_records if r.get("영향요인구분") == "채널전체"
    ]

    top_bank_rec = max(
        channel_all,
        key=lambda r: float(r.get("NPS영향도", 0)),
    )
    top_bank_name = top_bank_rec.get("거래은행구분", "")

    # 레코드 - 같은 CX(=KB 최상위 CX) 에 대한 응답률
    top_bank_cx_rec = next(
        (
            r
            for r in td_factor_records
            if r.get("거래은행구분") == top_bank_name
            and r.get("영향요인구분") == kb_top_cx_name
        ),
        None,
    )
    top_bank_cx_rate = float(top_bank_cx_rec.get("전체대비응답비중", 0))

    # 레코드 - 벤치마크(KB 레코드의 벤치마크은행) CX 응답률
    benchmark_name = kb_factor.get("벤치마크은행구분", "")
    bench_cx_rec = next(
        (
            r
            for r in td_factor_records
            if r.get("거래은행구분") == benchmark_name
            and r.get("영향요인구분") == kb_top_cx_name
        ),
        None,
    )
    bench_cx_rate = float(bench_cx_rec.get("전체대비응답비중", 0))

    # 플래그·차이 계산
    top_bank_cx_diff = _round_diff(kb_top_cx_rate, top_bank_cx_rate)
    bench_cx_diff = _round_diff(kb_top_cx_rate, bench_cx_rate)
        
    top_cx_flag = diff_and_flag(kb_top_cx_rate, top_bank_cx_rate, True)
    bench_cx_flag = diff_and_flag(kb_top_cx_rate, bench_cx_rate, False)

    # 레코드 - 추천·비추천 비중이 가장 높은 CX (KB 기준, 채널전체 제외)
    # 추천 비중
    best_rec_cx_rec = max(
        kb_cx_candidates,
        key=lambda r: float(r.get("전체대비추천비중", 0)),
    )
    best_rec_cx = best_rec_cx_rec.get("영향요인구분", "")
    best_rec_ratio = float(best_rec_cx_rec.get("전체대비추천비중", 0))

    # 비추천 비중
    worst_rec_cx_rec = max(
        kb_cx_candidates,
        key=lambda r: float(r.get("전체대비비추천비중", 0)),
    )
    worst_rec_cx = worst_rec_cx_rec.get("영향요인구분", "")
    worst_rec_ratio = float(worst_rec_cx_rec.get("전체대비비추천비중", 0))

    # 템플릿 컨텍스트 - td_factor
    factor_context = {
        "채널명": channel_name,
        "응답률_최상위_CX": kb_top_cx_name,
        "응답률": f"{kb_top_cx_rate:.1f}",
        "최상위은행명": top_bank_name,
        "최상위은행_CX_차이": f"{abs(top_bank_cx_diff):.1f}",
        "최상위은행_CX_플래그": top_cx_flag,
        "벤치마크사명": benchmark_name,
        "벤치마크사_CX_차이": f"{abs(bench_cx_diff):.1f}",
        "벤치마크사_CX_플래그": bench_cx_flag,
        "추천이유_1위_CX": best_rec_cx,
        "추천비율": f"{best_rec_ratio:.1f}",
        "비추천이유_1위_CX": worst_rec_cx,
        "비추천비율": f"{worst_rec_ratio:.1f}",
    }


    # td_ipa 로직
    # 문제영역별 레코드 분리
    mid_improve: List[Dict[str, Any]] = []      # 중점개선
    gradual_improve: List[Dict[str, Any]] = []  # 점진개선

    for r in td_ipa_records:
        area = r.get("문제영역구분")
        item = {
            "영향요인구분": r.get("영향요인구분", ""),
            "NPS중요도": r.get("NPS중요도", ""),
            "NPS영향도GAP": r.get("NPS영향도GAP", ""),
        }
        if area == "중점개선":
            mid_improve.append(item)
        elif area == "점진개선":
            gradual_improve.append(item)

    ipa_context = {
        "중점개선": mid_improve,
        "점진개선": gradual_improve,
    }

    # 템플릿 컨텍스트 - 최종 병합
    context = {**ipa_context, **factor_context}

    template = JINJA_ENV.get_template("TD_NPS진단_NPS영향요인.j2")
    rendered = template.render(**context)
    return rendered


# TD_고객경험_NPS진단
@register_renderer("TD_고객경험_NPS진단")
def render_TD_고객경험_NPS진단(data: Any) -> str:
    """
    td_cx_nps_summary 쿼리 결과를 템플릿에 전달

    사용 쿼리
        - 2-0. 고객경험단계 NPS 진단 - 이하 td_cx_nps_summary
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    data_by_type = extract_data_by_type(records)

    summary_records = data_by_type.get("td_cx_nps_summary", [])

    # 레코드 - KB국민은행
    kb_cx_records = [
        r for r in summary_records if r.get("거래은행구분") == "KB국민은행"
    ]
        
    # 레코드 - 벤치마크(신한은행)
    benchmark_rec = next(
        (r for r in summary_records if r.get("거래은행구분") == r.get("벤치마크은행구분")),
        None,
    )

    benchmark_name = benchmark_rec["벤치마크은행구분"]
    benchmark_nps = float(benchmark_rec["벤치마크NPS점수"])

    # 레코드 - 가장 큰 열위 CX
    gap_cx = None
    gap_cx_nps = None
    max_gap = float("-inf")
    for r in kb_cx_records:
        try:
            gap = float(r["벤치마크NPS점수갭"])
        except (KeyError, ValueError, TypeError):
            continue
        if gap > max_gap:
            max_gap = gap
            gap_cx = r["고객경험단계구분"]
            gap_cx_nps = float(r["NPS점수"])

    gap_diff = max_gap

    # 레코드 - NPS ≤ 0 인 CX 리스트
    negative_cxs: List[tuple[str, str]] = []
    for r in kb_cx_records:
        try:
            nps_val = float(r["NPS점수"])
        except (KeyError, ValueError, TypeError):
            continue
        if nps_val <= 0:
            negative_cxs.append((r["고객경험단계구분"], f"{nps_val:.1f}"))

    # 템플릿 컨텍스트
    context = {
        "GAP_CX": gap_cx,                         # GAP 1위 CX
        "GAP_CX_NPS": f"{gap_cx_nps:.1f}",        # GAP 1위 CX NPS
        "벤치마크사명": benchmark_name,       # 벤치마크사명
        "벤치마크사_차이": f"{gap_diff:.1f}",     # 벤치마크 대비 차이
        "CX_NPS_0점이하": negative_cxs,           # [(CX, NPS), …]
    }

    # 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_고객경험_NPS진단.j2")
    return template.render(**context)


# TD_고객경험_고객경험_NPS진단
@register_renderer("TD_고객경험_고객경험_NPS진단")
def render_TD_고객경험_고객경험_NPS진단(data: Any) -> str:
    """
    td_cx_nps 쿼리 결과를 템플릿에 전달

    사용 쿼리
        - 2-1. 고객경험단계 별 NPS 진단 - 이하 td_cx_nps
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    data_by_type = extract_data_by_type(records)

    td_cx_nps_records = data_by_type.get("td_cx_nps", [])

    # 레코드 - KB국민은행
    kb_rec = next(
        (r for r in td_cx_nps_records if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - CX(고객경험단계) : 쿼리 파라미터에 따라 이미 필터링돼 있으므로 레코드에서 그대로 사용
    cx_stage = kb_rec.get("고객경험단계구분", "")

    # 레코드 - NPS 순위 (시장평균 제외, 내림차순)
    banks = [r for r in td_cx_nps_records if r.get("거래은행구분") != "시장평균"]
    sorted_by_nps = sorted(banks, key=lambda x: float(x.get("NPS점수", 0)), reverse=True)

    # 레코드 - 순위
    rank = next(
        (idx + 1 for idx, r in enumerate(sorted_by_nps) if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 직상위기관 (바로 위에 있는 기관)
    if rank is None or rank == 1:
        # KB가 1위라면 직상위기관은 자신과 동일하게 처리 (차이는 0)
        direct_separator = "직하위기관"
        direct_rec = sorted_by_nps[rank + 1]
        direct_name = direct_rec.get("거래은행구분")
    else:
        direct_separator = "직상위기관"
        direct_rec = sorted_by_nps[rank - 2]   # 0‑index 보정
        direct_name = direct_rec.get("거래은행구분")

    # 레코드 - 시장평균
    market_rec = next((r for r in td_cx_nps_records if r.get("거래은행구분") == "시장평균"), None)

    # 레코드 - 최상위기관 (rank 1)
    top_rec = sorted_by_nps[0]
    preprocessing_top_name = top_rec.get("거래은행구분")

    # NPS 값
    kb_nps = float(kb_rec["NPS점수"])
    direct_nps = float(direct_rec["NPS점수"])
    market_nps = float(market_rec["NPS점수"])

    # NPS 차이·플래그
    direct_nps_diff, direct_nps_flag = diff_and_flag(kb_nps, direct_nps, True)

    # ⑧ 비추천비중 값
    kb_det = float(kb_rec["비추천비중"])
    top_det = float(top_rec["비추천비중"])
    direct_det = float(direct_rec["비추천비중"])

    # 비추천비중 차이·플래그
    preprocess_top_det_diff, top_det_flag = diff_and_flag(kb_det, top_det, False)          # 높음/낮음

    direct_det_diff, direct_det_flag = diff_and_flag(kb_det, direct_det, False)
    market_nps_diff, market_nps_flag = diff_and_flag(kb_nps, market_nps, False)

    if preprocessing_top_name != "KB국민은행":
        top_name = f"{preprocessing_top_name}보다"
        # 높음/낮음
        top_det_diff = f"{abs(preprocess_top_det_diff):.1f}%p"

    else:
        top_name = f"{preprocessing_top_name}으로 나타났고,"
        top_det_diff = ""
        top_det_flag = ""

    # ⑨ 템플릿 컨텍스트
    context = {
        "CX": cx_stage,
        "CX_NPS": f"{kb_nps:.1f}",
        "순위": rank,
        "직상위기관": direct_separator,
        "직상위기관명": direct_name,
        "직상위기관_NPS_차이": f"{abs(direct_nps_diff):.1f}",
        "직상위기관_NPS_플래그": direct_nps_flag,          # “높고/낮고”
        "시장평균_NPS_차이": f"{abs(market_nps_diff):.1f}",
        "시장평균_NPS_플래그": market_nps_flag,            # “높고/낮고”
        "비추천비중": f"{kb_det:.1f}",
        "최상위기관명": top_name,
        "최상위기관_비추천비중_차이": top_det_diff,
        "최상위기관_비추천비중_플래그": top_det_flag,      # “높고/낮고”
        "직상위기관_비추천비중_차이": f"{abs(direct_det_diff):.1f}",
        "직상위기관_비추천비중_플래그": direct_det_flag,   # “높음/낮음”
    }

    # ⑩ 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_고객경험_고객경험_NPS진단.j2")
    return template.render(**context)

# TD_고객경험_고객경험_영향요인
@register_renderer("TD_고객경험_고객경험_영향요인")
def render_TD_고객경험_고객경험_영향요인(data: Any) -> str:
    """
    td_cx_factor 와 td_cx_ipa 두 쿼리 결과를 하나의 템플릿에 전달

    사용 쿼리
        - 2-2. 고객경험단계 NPS 영향요인 - 이하 td_cx_factor
        - 2-3. 고객경험단계 IPA 분석 - 이하 td_cx_ipa
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    data_by_type = extract_data_by_type(records)

    factor_records: List[Dict[str, Any]] = data_by_type.get("td_cx_factor", [])
    ipa_records:    List[Dict[str, Any]] = data_by_type.get("td_cx_ipa", [])

    # td_cx_factor 로직
    # 레코드 - KB국민은행 (CX 필터, ‘고객경험단계전체’ 제외)
    kb_records = [
        r for r in factor_records
        if r.get("거래은행구분") == "KB국민은행"
        and r.get("고객경험단계구분") != "고객경험단계전체"
        and r.get("영향요인구분") != "고객경험단계전체"
    ]

    # 레코드 - CX 단계 (파라미터에 따라 하나만 존재)
    cx_stage = kb_records[0].get("고객경험단계구분", "")

    # 레코드 - KB응답률 1위 SQ (전체대비응답비중)
    top_resp_sq = max(kb_records, key=lambda x: float(x.get("전체대비응답비중", 0)))
    top_resp_name = top_resp_sq.get("영향요인구분", "")
    top_resp_rate = float(top_resp_sq.get("전체대비응답비중", 0))

    # 레코드 - KB‑추천·비추천 1위 SQ
    top_reco_sq = max(kb_records, key=lambda x: float(x.get("전체대비추천비중", 0)))
    top_reco_name = top_reco_sq.get("영향요인구분", "")
    top_reco_rate = float(top_reco_sq.get("전체대비추천비중", 0))

    top_det_sq = max(kb_records, key=lambda x: float(x.get("전체대비비추천비중", 0)))
    top_det_name = top_det_sq.get("영향요인구분", "")
    top_det_rate = float(top_det_sq.get("전체대비비추천비중", 0))

    # 레코드 - NPS 영향도 최상위기관 찾기 (‘고객경험단계전체’ 행)
    nps_total_rows = [
        r for r in factor_records if r.get("영향요인구분") == "고객경험단계전체"
    ]

    nps_top_bank = max(nps_total_rows, key=lambda x: float(x.get("NPS영향도", 0)))
    top_bank_name = nps_top_bank.get("거래은행구분", "")

    # 최상위기관·벤치마크(신한은행) 동일 SQ 응답비중 구하기
    def _find_resp_rate(bank: str) -> float:
        """같은 영향요인(=top_resp_name) 에 대한 응답비중 반환"""
        rec = next(
            (
                r
                for r in factor_records
                if r.get("거래은행구분") == bank
                and r.get("영향요인구분") == top_resp_name
            ),
            None,
        )
        return float(rec.get("전체대비응답비중", 0)) if rec else 0.0

    top_bank_resp_rate = _find_resp_rate(top_bank_name)

    benchmark_name = kb_records[0].get("벤치마크은행구분", "")
    has_benchmark = bool(benchmark_name and benchmark_name.strip())
    bench_resp_rate = _find_resp_rate(benchmark_name) if has_benchmark else 0.0

    # 차이·플래그 계산
    top_bank_diff, top_bank_flag = diff_and_flag(top_resp_rate, top_bank_resp_rate, True)
    bench_diff, bench_flag = (
        diff_and_flag(top_resp_rate, bench_resp_rate, False)
        if has_benchmark
        else (None, None)
    )

    # factor 컨텍스트
    factor_context = {
        "CX": cx_stage,
        "응답률_최상위_SQ": top_resp_name,
        "응답률": f"{top_resp_rate:.1f}",
        "최상위은행명": top_bank_name,
        "is_top_bank_kb": top_bank_name == "KB국민은행",
        "최상위은행_SQ_차이": f"{abs(top_bank_diff):.1f}",
        "최상위은행_SQ_플래그": top_bank_flag,
        "벤치마크사명": benchmark_name,
        "has_benchmark": has_benchmark,
        "벤치마크사_SQ_차이": f"{abs(bench_diff):.1f}" if has_benchmark else None,
        "벤치마크사_SQ_플래그": bench_flag,
        "추천이유_1위_SQ": top_reco_name,
        "추천비율": f"{top_reco_rate:.1f}",
        "비추천이유_1위_SQ": top_det_name,
        "비추천비율": f"{top_det_rate:.1f}",
    }

    # 레코드 - 문제영역별 분리
    mid_improve: List[Dict[str, Any]] = []      # 중점개선
    gradual_improve: List[Dict[str, Any]] = []  # 점진개선

    for r in ipa_records:
        area = r.get("문제영역구분")
        item = {
            "영향요인구분": r.get("영향요인구분", ""),
            "NPS중요도": r.get("NPS중요도", ""),
            "NPS영향도GAP": r.get("NPS영향도GAP", ""),
        }
        if area == "중점개선":
            mid_improve.append(item)
        elif area == "점진개선":
            gradual_improve.append(item)

    ipa_context = {
        "중점개선": mid_improve,
        "점진개선": gradual_improve,
    }

    # 템플릿 컨텍스트 - 최종 병합
    context = {**factor_context, **ipa_context}

    # 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_고객경험_고객경험_영향요인.j2")
    rendered = template.render(**context)
    return rendered


# TD_고객경험_고객경험_고객감정
@register_renderer("TD_고객경험_고객경험_고객감정")
def render_TD_고객경험_고객경험_고객감정(data: Any) -> str:
    """
    td_cx_emotion_nss 와 td_cx_emotion_ccs 두 쿼리 결과를 하나의 템플릿에 전달한다.
    - data : list[dict] / pandas.DataFrame / list[list[dict]] 등
    - 반환값 : Jinja2 템플릿이 렌더링된 문자열
    """
    # 입력 데이터를 list[dict] 로 정규화
    records = _to_dict(data)

    # 쿼리 타입별 레코드 분리
    data_by_type = extract_data_by_type(records)

    nss_records = data_by_type.get("td_cx_emotion_nss", [])
    ccs_records = data_by_type.get("td_cx_emotion_ccs", [])

    # td_cx_emotion_nss 로직 (NSS, 부정비중)
    # 레코드 - KB국민은행
    kb_nss_rec = next(
        (r for r in nss_records if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 벤치마크 (KB 레코드의 벤치마크은행구분 사용)
    benchmark_name = kb_nss_rec.get("벤치마크은행구분")
    bench_nss_rec = next(
        (r for r in nss_records if r.get("거래은행구분") == benchmark_name),
        None,
    )
        
    # 차이·플래그 계산
    kb_nss = float(kb_nss_rec.get("NSS점수", 0))
    bench_nss = float(bench_nss_rec.get("NSS점수", 0))
    nss_diff, nss_flag = diff_and_flag(kb_nss, bench_nss, True)

    kb_neg = float(kb_nss_rec.get("부정비중", 0))
    bench_neg = float(bench_nss_rec.get("부정비중", 0))
    neg_diff, neg_flag = diff_and_flag(kb_neg, bench_neg, False)

    # td_cx_emotion_ccs 로직
    # 레코드 - KB국민은행
    kb_ccs_rec = next(
        (r for r in ccs_records if r.get("거래은행구분") == "KB국민은행"),
        None,
    )

    # 레코드 - 벤치마크
    benchmark_name_ccs = kb_ccs_rec.get("벤치마크은행구분")
    bench_ccs_rec = next(
        (r for r in ccs_records if r.get("거래은행구분") == benchmark_name_ccs),
        None,
    )

    # 차이·플래그 계산 (CCS)
    kb_ccs = float(kb_ccs_rec.get("CCS점수", 0))
    bench_ccs = float(bench_ccs_rec.get("CCS점수", 0))
    ccs_diff, ccs_flag = diff_and_flag(kb_ccs, bench_ccs, True)

    # 개선비중 차이·플래그
    kb_imp = float(kb_ccs_rec.get("개선비중", 0))
    bench_imp = float(bench_ccs_rec.get("개선비중", 0))
    imp_diff, imp_flag = diff_and_flag(kb_imp, bench_imp, False)

    # 템플릿 컨텍스트
    context: Dict[str, Any] = {
        # 공통
        "고객경험단계구분": kb_nss_rec.get("고객경험단계구분", ""),
        "벤치마크사": benchmark_name,                     # 벤치마크
        # NSS 영역
        "KB_NSS": f"{kb_nss:.1f}",
        "NSS점수_차이": f"{abs(nss_diff):.1f}",
        "NSS_플래그": nss_flag,
        "부정비중_차이": f"{abs(neg_diff):.1f}",
        "부정비중_플래그": neg_flag,
        # CCS 영역
        "KB_CCS": f"{kb_ccs:.1f}",
        "CCS점수_차이": f"{abs(ccs_diff):.1f}",
        "CCS_플래그": ccs_flag,
        "개선비중_차이": f"{abs(imp_diff):.1f}",
        "개선비중_플래그": imp_flag,
    }

    # 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_고객경험_고객경험_고객감정.j2")
    rendered = template.render(**context)
    return rendered


# TD_문제영역
@register_renderer("TD_문제영역")
def render_TD_문제영역(data: Any) -> str:
    """
    td_ipa 쿼리 결과를 템플릿에 전달

    사용 쿼리
        - 1-3. 채널 IPA 분석 - 이하 td_ipa
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # 레코드 - 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    # td_ipa 데이터만 가져오기
    td_ipa_records = data_by_type.get("td_ipa", [])

    # 레코드 - 벤치마크 은행명 (모든 레코드가 동일하다고 가정)
    benchmark_name = td_ipa_records[0].get("벤치마크은행구분", "")

    # 레코드 - 문제영역별 분리
    mid_improve: List[Dict[str, Any]] = []      # 중점개선
    gradual_improve: List[Dict[str, Any]] = []  # 점진개선

    for r in td_ipa_records:
        area = r.get("문제영역구분")
        # 템플릿에서 바로 사용할 키 이름을 정규화
        item = {
            "영향요인구분": r.get("영향요인구분", ""),
            "NPS중요도": r.get("NPS중요도", ""),
            "NPS영향도GAP": r.get("NPS영향도GAP", ""),
        }
        if area == "중점개선":
            mid_improve.append(item)
        elif area == "점진개선":
            gradual_improve.append(item)

    # 템플릿 컨텍스트
    context = {
        "벤치마크사명": benchmark_name,
        "중점개선": mid_improve,
        "점진개선": gradual_improve,
    }

    # 템플릿 렌더링
    template = JINJA_ENV.get_template("TD_문제영역.j2")
    return template.render(**context)


##############################################################
#                        BU 템플릿                           #
##############################################################
# BU_요약_NPS진단
@register_renderer("BU_요약_NPS진단")
def render_BU_요약_NPS진단(data: Any) -> str:
    """
    사용 쿼리
        - *bu_cha_nps_total
        - *bu_cx_nps_total
        - *bu_voc_total
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    if not records:
        return ""

    # records에서 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    nps_total = data_by_type.get("bu_cha_nps_total", [])
    cx_total = data_by_type.get("bu_cx_nps_total", [])
    voc_total = data_by_type.get("bu_voc_total", [])

    # 채널
    channel_name = nps_total[0].get("채널")

    # 누적(NPS total) → bu_cha_nps_total 에서 같은 채널의 NPS
    total_row = next((r for r in nps_total), None)
    누적_채널_NPS = safe_float(total_row.get("NPS", 0))

    cur_month_row = nps_total[0]
    prev_month_row = nps_total[1] if len(nps_total) > 1 else None

    cur_nps = safe_float(cur_month_row.get("NPS", 0))
    prev_nps = safe_float(prev_month_row.get("NPS", 0))

    전월_대비_차이 = round(cur_nps - prev_nps, 1) if prev_nps is not None else 0.0
    상승_하락 = (
        "상승" if 전월_대비_차이 > 0
        else ("하락" if 전월_대비_차이 < 0 else "동일")
    )

    # YY.MM 형태 추출 (기준년월은 문자열 “YYYYMM”)
    기준년월일 = cur_month_row.get("기준년월일")
    YY = 기준년월일[:4]
    MM = 기준년월일[4:6]

    # 두 번째 문장 : CX NPS 최고·최저
    nps_total_sorted = sorted(
        cx_total,
        key=lambda r: int(r.get("기준년월", 0)),
        reverse=True,
    )
    sorted_by_nps = sorted(nps_total_sorted, key=lambda x: float(x.get("NPS", 0)), reverse=True)
    sorted_by_nps = [r for r in sorted_by_nps if r.get("기준년월일") == 기준년월일] # 최근일 데이터만 남김

    # 순위
    # 가장 높은 레코드 → 첫 번째
    max_row = sorted_by_nps[0]

    # 가장 낮은 레코드 → 마지막
    min_row = sorted_by_nps[-1]

    NPS_점수_1위_CX명 = max_row.get("고객경험단계", "")
    NPS_점수_1위_CX_NPS = safe_float(max_row.get("NPS", 0))

    NPS_점수_최저_CX명 = min_row.get("고객경험단계", "")
    NPS_점수_최저_CX_NPS = safe_float(min_row.get("NPS", 0))

    # nss 총 긍정 건수 - 총 부정건수 / 총건수
    # cci는 총 불만 비율 -> 총 불만 / 총건수
    누적_긍정 = 0.0
    누적_부정 = 0.0
    누적_불만 = 0.0
    누적_총_건수 = 0.0
    for r in voc_total:
        누적_긍정 += safe_float(r.get("긍정건수", 0))
        누적_부정 += safe_float(r.get("부정건수", 0))
        누적_불만 += safe_float(r.get("불만건수", 0))
        누적_총_건수 += safe_float(r.get("전체건수", 0))

    if 누적_총_건수 != 0:
        누적_NSS = (round((누적_긍정 * 100 / 누적_총_건수), 1) - round((누적_부정 * 100 / 누적_총_건수), 1))
        누적_CCI = round((누적_불만 * 100 / 누적_총_건수), 1)
    else:
        누적_NSS = 0.0
        누적_CCI = 0.0

    # 템플릿 컨텍스트 구성
    context: Dict[str, Any] = {
        "YY": YY,
        "MM": MM,
        "채널명": channel_name,
        "누적_채널_NPS": f"{누적_채널_NPS:.1f}",
        "전월_대비_차이": f"{abs(전월_대비_차이):.1f}",
        "상승_하락": 상승_하락,
        "NPS_점수_1위_CX명": NPS_점수_1위_CX명,
        "NPS_점수_1위_CX_NPS": f"{NPS_점수_1위_CX_NPS:.1f}",
        "NPS_점수_최저_CX명": NPS_점수_최저_CX명,
        "NPS_점수_최저_CX_NPS": f"{NPS_점수_최저_CX_NPS:.1f}",
        "누적_NSS": f"{누적_NSS:.1f}",
        "누적_CCI": f"{누적_CCI:.1f}",
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("BU_요약_NPS진단.j2")
    return template.render(**context)

# BU_채널NPS_NPS분석
@register_renderer("BU_채널NPS_NPS분석")
def render_BU_채널NPS_NPS분석(data: Any) -> str:
    """
    사용 쿼리
        - *bu_cha_nps_total
        - *bu_cha_factor_total
        - bu_cha_factor_month
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # records에서 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    nps_total = data_by_type.get("bu_cha_nps_total", [])
    factor_total = data_by_type.get("bu_cha_factor_total", [])

    # 채널·누적 NPS·추천·비추천 비율
    channel_name = nps_total[0].get("채널")

    # 누적(NPS total) – bu_cha_nps_total
    total_row = next(
        (r for r in nps_total), None
    )
    누적_채널_NPS = safe_float(total_row.get("NPS", 0))
    누적_추천비율 = safe_float(total_row.get("추천비율", 0))
    누적_비추천비율 = safe_float(total_row.get("비추천비율", 0))
    # 전월 대비 NPS / 추천비율 / 비추천비율

    cur_row = nps_total[0] # 가장 최신
    prev_row = nps_total[1] # 이전월

    # 현재·전월 NPS
    cur_nps = safe_float(cur_row.get("NPS", 0))
    prev_nps = safe_float(prev_row.get("NPS", 0))
    preprocessing_전월_대비_차이 = round(cur_nps - prev_nps, 1)
    if preprocessing_전월_대비_차이 > 0:
        전월_대비_차이 = f"전월누적 대비 ({abs(preprocessing_전월_대비_차이):.1f})%p"
        상승_하락 = "상승"
    elif preprocessing_전월_대비_차이 == 0:
        전월_대비_차이 = f"전월누적과 "
        상승_하락 = "동일"
    else:     # preprocessing_전월_대비_차이 < 0
        전월_대비_차이 = f"전월누적 대비 ({abs(preprocessing_전월_대비_차이):.1f})%p"
        상승_하락 = "하락"

    # 현재·전월 추천비율 / 비추천비율
    cur_reco = safe_float(cur_row.get("추천비율", 0))
    prev_reco = safe_float(prev_row.get("추천비율", 0))
    cur_det = safe_float(cur_row.get("비추천비율", 0))
    prev_det = safe_float(prev_row.get("비추천비율", 0))


    preprocessing_전월_추천비율_차이 = round(cur_reco - prev_reco, 1)
    if preprocessing_전월_추천비율_차이 > 0:
        전월_추천비율_차이 = f"전월 대비 ({abs(preprocessing_전월_추천비율_차이):.1f})%p"
        추천_상승_하락 = "상승"
    elif preprocessing_전월_추천비율_차이 == 0:
        전월_추천비율_차이 = f"전월과"
        추천_상승_하락 = "동일"
    else:        # preprocessing_전월_추천비율_차이 < 0:
        전월_추천비율_차이 = f"전월 대비 ({abs(preprocessing_전월_추천비율_차이):.1f})%p"
        추천_상승_하락 = "하락"

    preprocessing_전월_비추천비율_차이 = round(cur_det - prev_det, 1)
    if preprocessing_전월_비추천비율_차이 > 0:
        전월_비추천비율_차이 = f"전월 대비 ({abs(preprocessing_전월_비추천비율_차이):.1f})%p"
        비추천_상승_하락 = "상승"
    elif preprocessing_전월_비추천비율_차이 == 0:
        전월_비추천비율_차이 = f"전월과"
        비추천_상승_하락 = "동일"
    else:        # preprocessing_전월_비추천비율_차이 < 0:
        전월_비추천비율_차이 = f"전월 대비 ({abs(preprocessing_전월_비추천비율_차이):.1f})%p"
        비추천_상승_하락 = "하락"

    # 영향도 1위·꼴찌 (긍정·부정) – factor_total 사용
    # factor_total 에서 같은 채널만 추출
    factor_rows = [r for r in factor_total if r.get("채널") == channel_name]

    # 영향도(positive) 최댓값, 최솟값 찾기
    # 컬럼명: "영향도"
    max_row = max(factor_rows, key=lambda r: float(r.get("영향도", 0)))
    min_row = min(factor_rows, key=lambda r: float(r.get("영향도", 0)))

    영향도_1위_영향요인 = max_row.get("고객경험단계", "")
    영향도_1위_값 = round(float(max_row.get("영향도", 0)), 1)

    영향도_꼴찌_영향요인 = min_row.get("고객경험단계", "")
    영향도_꼴찌_값 = round(float(min_row.get("영향도", 0)), 1)

    # 년도·월 추출 (YY.MM)
    # 현재 월 행(cur_row)의 기준년월을 사용한다.
    기준년월일 = cur_row.get("기준년월일")
    if isinstance(기준년월일, str):
        기준년월일 = int(기준년월일)

    YY = 기준년월일 // 10000
    MM = (기준년월일 % 10000) // 100

    # 템플릿 컨텍스트 구성
    context: Dict[str, Any] = {
        # 날짜·채널
        "YY": YY,
        "MM": MM,
        "채널명": channel_name,
        # 누적 NPS·추천·비추천
        "누적_채널_NPS": f"{누적_채널_NPS:.1f}",
        "누적_추천비율": f"{누적_추천비율:.1f}",
        "누적_비추천비율": f"{누적_비추천비율:.1f}",
        # 전월 대비
        "전월_대비_차이": 전월_대비_차이,
        "상승_하락": 상승_하락,
        "전월_추천비율_차이": 전월_추천비율_차이,
        "추천_상승_하락": 추천_상승_하락,
        "전월_비추천비율_차이": 전월_비추천비율_차이,
        "비추천_상승_하락": 비추천_상승_하락,
        # 영향도
        "영향도_1위_영향요인": 영향도_1위_영향요인,
        "영향도_1위_값": f"{영향도_1위_값:.1f}",
        "영향도_꼴찌_영향요인": 영향도_꼴찌_영향요인,
        "영향도_꼴찌_값": f"{영향도_꼴찌_값:.1f}",
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("BU_채널NPS_NPS분석.j2")
    return template.render(**context)

# BU_고객경험_고객경험_NPS분석
@register_renderer("BU_고객경험_고객경험_NPS분석")
def render_BU_고객경험_고객경험_NPS분석(data: Any) -> str:
    """
    사용 쿼리
        - *bu_cx_nps_total
        - *bu_cx_factor_total
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # records에서 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    nps_total: List[Dict[str, Any]] = data_by_type.get("bu_cx_nps_total", [])
    factor_total: List[Dict[str, Any]] = data_by_type.get("bu_cx_factor_total", [])

    # NPS‑total (누적) & 월간 데이터
    # 기준년월(YYYYMM) 기준 내림차순 정렬 → 최신 → 전월
    nps_total_sorted = sorted(
        nps_total,
        key=lambda r: int(r.get("기준년월", 0)),
        reverse=True,
    )
    cur_row = nps_total_sorted[0]                     # 최신
    prev_row = nps_total_sorted[1] if len(nps_total_sorted) > 1 else None # 이전월

    # 채널·고객경험단계(예: “계좌조회/이체 NPS”) → 템플릿에 CX 로 사용
    cx_name = cur_row.get("고객경험단계", "")
    channel_name = cur_row.get("채널", "")

    # 누적 NPS, 추천·비추천 비율
    누적_NPS점수 = safe_float(cur_row.get("NPS", 0))
    누적_추천비율 = safe_float(cur_row.get("추천비율", 0))
    누적_비추천비율 = safe_float(cur_row.get("비추천비율", 0))

    # 월간(현재) NPS와 전월 대비 차이
    if prev_row:
        전월_NPS = safe_float(prev_row.get("NPS", 0))
        전월_추천비율 = safe_float(prev_row.get("추천비율", 0))
        전월_비추천비율 = safe_float(prev_row.get("비추천비율", 0))
    else:
        전월_NPS = 전월_추천비율 = 전월_비추천비율 = 0.0

    전월_대비_차이 = 누적_NPS점수 - 전월_NPS
    상승_하락 = (
        "상승" if 전월_대비_차이 > 0
        else ("하락" if 전월_대비_차이 < 0 else "동일")
    )

    # 추천·비추천 비율 전월 대비 차이
    전월_대비_추천비율_차이 = 누적_추천비율 - 전월_추천비율
    전월_대비_비추천비율_차이 = 누적_비추천비율 - 전월_비추천비율

    추천_상승_하락 = (
        "상승" if 전월_대비_추천비율_차이 > 0
        else ("하락" if 전월_대비_추천비율_차이 < 0 else "동일")
    )
    비추천_상승_하락 = (
        "상승" if 전월_대비_비추천비율_차이 > 0
        else ("하락" if 전월_대비_비추천비율_차이 < 0 else "동일")
    )

    # 월(MM) 추출 (YYYYMM → MM)
    기준년월 = cur_row.get("기준년월", 0)
    try:
        YYYYMM = f"{기준년월[:4]}.{int(기준년월[4:])}"
    except Exception:
        YYYYMM = ""

    # 서비스품질요소(영향도) – 1위·최저
    factor_sorted = sorted(
        factor_total,
        key=lambda r: float(r.get("영향도", 0)),
        reverse=True,
    )
    # 가장 높은 영향도(긍정)
    pos_row = factor_sorted[0]
    # 가장 낮은 영향도(부정)
    neg_row = factor_sorted[-1]

    누적_서비스품질요소_1위_긍정요소명 = pos_row.get("서비스품질요소", "")
    누적_서비스품질요소_1위_긍정요소NPS점수 = safe_float(pos_row.get("영향도", 0))

    누적_가장_낮은_NPS영향도_서비스품질요소명 = neg_row.get("서비스품질요소", "")
    누적_가장_낮은_NPS영향도_서비스품질요소_NPS점수 = safe_float(neg_row.get("영향도", 0))

    # 템플릿 컨텍스트 구성
    context: Dict[str, Any] = {
        # 누적 NPS
        "CX": cx_name + "은" if has_batchim(cx_name[-1]) else cx_name + "는",
        "누적_NPS점수": f"{누적_NPS점수:.1f}",
        "YYYYMM": YYYYMM,
        "전월_대비_차이": f"{abs(전월_대비_차이):.1f}",
        "상승_하락": 상승_하락,
        "누적_추천비율": f"{누적_추천비율:.1f}",
        "누적_비추천비율": f"{누적_비추천비율:.1f}",
        "전월_대비_추천비율_차이": f"{abs(전월_대비_추천비율_차이):.1f}",
        "전월_대비_비추천비율_차이": f"{abs(전월_대비_비추천비율_차이):.1f}",
        "추천_상승_하락": 추천_상승_하락,
        "비추천_상승_하락": 비추천_상승_하락,
        # 서비스품질요소
        "누적_서비스품질요소_1위_긍정요소명": 누적_서비스품질요소_1위_긍정요소명,
        "누적_서비스품질요소_1위_긍정요소NPS점수": f"{누적_서비스품질요소_1위_긍정요소NPS점수:.1f}",
        "누적_가장_낮은_NPS영향도_서비스품질요소명": 누적_가장_낮은_NPS영향도_서비스품질요소명,
        "누적_가장_낮은_NPS영향도_서비스품질요소_NPS점수": f"{누적_가장_낮은_NPS영향도_서비스품질요소_NPS점수:.1f}",
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("BU_고객경험_고객경험_NPS분석.j2")
    return template.render(**context)

# BU_고객경험_고객경험_품질요소
@register_renderer("BU_고객경험_고객경험_품질요소")
def render_BU_고객경험_고객경험_품질요소(data: Any) -> str:
    """
    사용 쿼리
        - *bu_sq_total
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # records에서 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    factor_total = data_by_type.get("bu_sq_total", [])

    # 고객경험단계
    cx_stage = factor_total[0].get("고객경험단계명", "")

    # NPS 계산 (월누적NPS 기준으로 정렬)
    sorted_by_rec = sorted(factor_total, key=lambda x: float(x.get("월누적NPS", 0)), reverse=True)
    max_row = sorted_by_rec[0] if sorted_by_rec else {}
    min_row = sorted_by_rec[-1] if sorted_by_rec else {}

    # 전월누적NPS 값(숫자형) 추출
    preprocessing_max_monthly_nps = safe_float(max_row.get("월누적NPS", 0)) - safe_float(max_row.get("전월누적NPS", 0))
    preprocessing_min_monthly_nps = safe_float(min_row.get("월누적NPS", 0)) - safe_float(min_row.get("전월누적NPS", 0))

    # 상승/하락 여부 계산
    if preprocessing_max_monthly_nps > 0:
        max_monthly_impact = f"전월대비 {abs(round(preprocessing_max_monthly_nps, 1))}점"
        상승폭 = "상승"
    elif preprocessing_max_monthly_nps == 0:
        max_monthly_impact = f"전월과"
        상승폭 = "동일"
    else:   # preprocessing_max_monthly_nps < 0 인 경우
        max_monthly_impact = f"전월대비 {abs(round(preprocessing_max_monthly_nps, 1))}점"
        상승폭 = "하락"
    
    # 하락폭 계산
    if preprocessing_min_monthly_nps < 0:
        min_monthly_impact = f"전월대비 {abs(round(preprocessing_min_monthly_nps, 1))}점"
        하락폭 = "하락"
    elif preprocessing_min_monthly_nps == 0:
        min_monthly_impact = f"전월과"
        하락폭 = "동일"
    else:   # min_monthly_impact > 0 인 경우
        min_monthly_impact = f"전월대비 {abs(round(preprocessing_min_monthly_nps, 1))}점"
        하락폭 = "상승"

    # 템플릿 컨텍스트 구성
    context: Dict[str, Any] = {
        # CX 단계
        "CX": cx_stage,
        # 월누적 가장 높은 NPS 품질요소
        "월누적_가장높은_추천_품질요소명": max_row.get("서비스품질명", ""),
        "월누적_가장높은_추천_품질요소점수": f"{safe_float(max_row.get('월누적NPS', 0)):.1f}",
        # 월누적 가장 낮은 NPS 품질요소
        "월누적_가장낮은_추천_품질요소명": min_row.get("서비스품질명", ""),
        "월누적_가장낮은_추천_품질요소점수": f"{safe_float(min_row.get('월누적NPS', 0)):.1f}",
        # 전월대비 상승·하락 폭
        "전월대비_상승한_품질요소_상승폭": max_monthly_impact,
        "전월대비_하락한_품질요소_하락폭": min_monthly_impact,
        # 상승/하락 여부 표시
        "상승폭_상승_하락": 상승폭,
        "하락폭_상승_하락": 하락폭,
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("BU_고객경험_고객경험_품질요소.j2")
    return template.render(**context)


# BU_문제원인
@register_renderer("BU_문제원인")
def render_BU_문제원인(data: Any) -> str:
    """
    사용 쿼리
        - *bu_factor
    """
    # Data → list[dict] 로 정규화
    records = _to_dict(data)

    # records에서 쿼리ID 유형별로 분리
    data_by_type = extract_data_by_type(records)

    factor_total = data_by_type.get("bu_factor", [])

    # 채널명
    channel_name = factor_total[0].get("채널", "")

    # CX 별 전체건수 합계 계산 → 응답률 1위 CX
    cx_sum: Dict[str, float] = {}
    for r in factor_total:
        cx = r.get("고객경험단계")
        cnt = safe_float(r.get("전체건수", 0))
        cx_sum[cx] = cx_sum.get(cx, 0) + cnt

    # 전체건수 1위인 고객경험단계
    cx_max = max(cx_sum.items(), key=lambda kv: kv[1])[0]

    # 전체건수 1위인 서비스품질요소
    records_in_cx = [r for r in factor_total if r.get("고객경험단계") == cx_max]
    service_max = max(records_in_cx, key=lambda r: float(r.get("전체건수", 0)))
    service_max_name = service_max.get("서비스품질요소", "")

    # 영향도 가장 상위인 고객경험단계 / 서비스품질요소
    neg_record = max([d for d in factor_total if int(d['긍정건수']) > 0], key=lambda r: float(r.get("부정비율", 0)))
    neg_cx = neg_record.get("고객경험단계", "")
    neg_service = neg_record.get("서비스품질요소", "")

    # 영향도 가장 하위인 고객경험단계 / 서비스품질요소
    pos_record = max([d for d in factor_total if int(d['부정건수']) > 0], key=lambda r: float(r.get("긍정비율", 0)))
    pos_cx = pos_record.get("고객경험단계", "")
    pos_service = pos_record.get("서비스품질요소", "")

    # 템플릿 컨텍스트 구성
    context: Dict[str, Any] = {
        "채널명": channel_name,
        "누적_서비스_품질요소_응답률_1위_CX": cx_max,
        "누적_서비스_품질요소_응답률_1위": service_max_name,
        "누적_부정_1위_서비스품질요소_CX": neg_cx,
        "누적_부정_1위_서비스품질요소": neg_service,
        "누적_긍정_1위_서비스품질요소_CX": pos_cx,
        "누적_긍정_1위_서비스품질요소": pos_service,
    }

    # Jinja2 템플릿 렌더링
    template = JINJA_ENV.get_template("BU_문제원인.j2")
    return template.render(**context)
