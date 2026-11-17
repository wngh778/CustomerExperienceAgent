"""
combined_testset.csv 기반 에이전트 통합 평가 러너

평가 항목:
  - survey_type (S): 에이전트가 TD/BU 중 올바른 조사 유형의 뷰를 사용했는지
  - view (V):        기대 뷰(td_spectrum_nps 등)를 실제로 호출했는지
  - yn (Y):          가능여부(가능/불가/정책위반 등) 기대에 맞게 처리했는지

사용법:
    LLM_PROVIDER=openai python -m app.tests.run_combined_testset
    LLM_PROVIDER=openai python -m app.tests.run_combined_testset --concurrency 3
    LLM_PROVIDER=openai python -m app.tests.run_combined_testset --ids G1-1 G1-2 A1
    LLM_PROVIDER=openai python -m app.tests.run_combined_testset --type 스펙트럼
    LLM_PROVIDER=openai python -m app.tests.run_combined_testset --no-langfuse
"""

from __future__ import annotations

import asyncio
import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# app/ 디렉토리를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_path = LOG_DIR / f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

_fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
_file_handler = logging.FileHandler(_log_path, encoding="utf-8")
_file_handler.setFormatter(_fmt)

# 루트 로거: WARNING 이상만 (httpx, openai, langgraph 등 노이즈 차단)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logging.getLogger().addHandler(_file_handler)

# 평가 로거 + 에이전트 로거만 INFO로 파일에 기록
# propagate=False: 루트 로거로 전파 차단 (루트에도 _file_handler가 있어 중복 방지)
for _name in ("eval", "ReportGenerationAgent"):
    _lg = logging.getLogger(_name)
    _lg.setLevel(logging.INFO)
    _lg.addHandler(_file_handler)
    _lg.propagate = False

logger = logging.getLogger("eval")

TEST_CSV = Path(__file__).parent.parent.parent / "docs" / "cx_agent_testset.txt"
RESULTS_DIR = Path(__file__).parent.parent.parent / "docs" / "eval_results"


# ─────────────────────────────────────────────────────────────────────
# 평가 로직
# ─────────────────────────────────────────────────────────────────────

def infer_survey_type(tools_used: list[str]) -> list[str]:
    """사용된 tool 이름(query_td_*/query_bu_* 프리픽스)으로 TD/BU 유추."""
    has_td = any("_td_" in str(v) for v in tools_used)
    has_bu = any("_bu_" in str(v) for v in tools_used)
    if has_td and has_bu:
        return ["TD", "BU"]
    elif has_td:
        return ["TD"]
    elif has_bu:
        return ["BU"]
    return ["없음"]


def evaluate_case(row: dict, state: dict) -> dict:
    """에이전트 실행 결과를 기대값과 비교해 평가 결과 딕셔너리를 반환.

    체크 항목:
      survey_check (S): 사용된 뷰 프리픽스로 유추한 TD/BU가 기대값과 일치하는지
      view_check   (V): tools_used에 기대 뷰 ID가 포함되는지 (스펙트럼 행만)
      yn_check     (Y): 가능여부 기대에 맞는 처리가 이루어졌는지

    None 값은 "-"(체크 불필요)를 의미하며 전체 판정에서 제외됨.
    """
    expected_survey = str(row.get("survey_type", "")).strip()
    expected_views_raw = str(row.get("툴", "")).strip()
    expected_yn = str(row.get("가능여부", "")).strip()

    tools_used: list[str] = state.get("tools_used") or []
    intent: str = state.get("intent", "")
    policy_violated: bool = bool(state.get("policy_violated", False))
    final_answer: str = state.get("final_answer", "")
    query_reasons: list[str] = state.get("query_reasons") or []

    actual_survey = infer_survey_type(tools_used)

    # 1. Survey type check
    survey_ok: bool | None
    if expected_survey in ("TD", "BU"):
        survey_ok = expected_survey in actual_survey
    elif "+" in expected_survey:
        parts = [p.strip() for p in expected_survey.split("+")]
        survey_ok = all(p in actual_survey for p in parts)
    elif expected_survey in ("정책위반", "조건부허용", "해당없음", ""):
        survey_ok = None  # N/A
    elif "(" in expected_survey:
        # "BU(근사)" 등 — 괄호 앞 부분만 비교
        survey_ok = expected_survey.split("(")[0].strip() in actual_survey
    else:
        survey_ok = None

    # 2. View check
    view_ok: bool | None = None
    if expected_views_raw and expected_views_raw not in ("해당없음", ""):
        tokens = re.split(r"[,+/ ]+", expected_views_raw)
        expected_views = [
            t.strip() for t in tokens
            if t.strip() and t.strip() not in ("또는", "각각", "td", "bu", "각각")
        ]
        if expected_views:
            view_ok = any(v in tools_used for v in expected_views)

    # 3. 가능여부 check
    yn_ok: bool | None = None
    if expected_yn:
        if "정책위반" in expected_yn:
            yn_ok = policy_violated or "정책" in final_answer
        elif expected_yn == "불가":
            refusal_kws = ["불가", "지원하지 않", "미지원", "해당 없음", "제공되지", "지원되지", "없습니다"]
            yn_ok = len(tools_used) == 0 or any(kw in final_answer for kw in refusal_kws)
        elif "데이터없음" in expected_yn:
            yn_ok = True  # 에러 없이 응답했으면 통과
        elif expected_yn.startswith("가능") or "조건부허용" in expected_yn:
            yn_ok = len(tools_used) > 0

    # 4. 종합 판정 (None인 항목 제외)
    checks = [(k, v) for k, v in [("S", survey_ok), ("V", view_ok), ("Y", yn_ok)] if v is not None]
    if checks:
        overall = "PASS" if all(v for _, v in checks) else "FAIL"
    else:
        overall = "N/A"

    def sym(v: bool | None) -> str:
        return "-" if v is None else ("✓" if v else "✗")

    return {
        "actual_survey": actual_survey,
        "tools_used": ",".join(tools_used),
        "intent": intent,
        "policy_violated": str(policy_violated),
        "survey_check": sym(survey_ok),
        "view_check": sym(view_ok),
        "yn_check": sym(yn_ok),
        "overall": overall,
        "query_reasons": " | ".join(query_reasons),
        "answer_preview": final_answer[:400],
    }


# ─────────────────────────────────────────────────────────────────────
# 단일 케이스 실행
# ─────────────────────────────────────────────────────────────────────

async def run_single(
    agent,
    semaphore: asyncio.Semaphore,
    row: dict,
    langfuse_cb=None,
) -> dict:
    query = str(row.get("질의", "")).strip()
    case_id = str(row.get("번호", "?"))

    async with semaphore:
        try:
            state = await agent.execute_eval(query, user_id="eval", langfuse_callback=langfuse_cb)
            result = evaluate_case(row, state)
        except Exception as e:
            logger.exception("[%s] 실행 중 예외 발생 — query: %s", case_id, query[:80])
            result = {
                "actual_survey": "ERROR",
                "tools_used": "",
                "intent": "ERROR",
                "policy_violated": "False",
                "survey_check": "✗",
                "view_check": "✗",
                "yn_check": "✗",
                "overall": "ERROR",
                "answer_preview": str(e)[:300],
            }

        tag = result["overall"]
        line = (
            f"  [{tag:^5}] [{case_id}] "
            f"survey={result['actual_survey']}, "
            f"views={result['tools_used'][:40]}"
        )
        if tag in ("FAIL", "ERROR"):
            expected_survey = str(row.get("survey_type", "")).strip()
            expected_views = str(row.get("툴", "")).strip()
            line += (
                f" | S={result['survey_check']} V={result['view_check']} Y={result['yn_check']}"
                f" | expected: survey={expected_survey}, views={expected_views}"
            )

        logger.info(line)

    return {**row, **result}


# ─────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="combined_testset.csv 에이전트 평가")
    parser.add_argument("--concurrency", type=int, default=3, help="동시 실행 수 (기본 3)")
    parser.add_argument("--ids", nargs="*", help="실행할 케이스 번호 목록 (예: G1-1 A1)")
    parser.add_argument(
        "--type",
        choices=["스펙트럼", "개체명인식"],
        help="테스트 유형 필터",
    )
    parser.add_argument("--no-langfuse", action="store_true", help="Langfuse 비활성화")
    args = parser.parse_args()

    from agent.report_generation.report_generation_agent import ReportGenerationAgent
    from core.config import settings
    from core.mcp_util import get_mcp_executor

    mcp_executor = await get_mcp_executor()
    agent = ReportGenerationAgent(mcp_executor=mcp_executor)

    # ── Langfuse 콜백 설정 ──
    langfuse_cb = None
    if not args.no_langfuse and settings.langfuse_public_key and settings.langfuse_secret_key:
        try:
            from langfuse.callback import CallbackHandler

            session_id = f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            langfuse_cb = CallbackHandler(
                public_key=settings.langfuse_public_key,
                secret_key=settings.langfuse_secret_key,
                host=settings.langfuse_host,
                session_id=session_id,
            )
            logger.info(f"Langfuse 활성화: {settings.langfuse_host} | session={session_id}")
        except ImportError:
            logger.warning("langfuse 미설치. pip install langfuse 후 재시도")

    # ── CSV 로드 및 필터 ──
    df = pd.read_csv(TEST_CSV, dtype=str, delimiter='\t').fillna("")
    rows = df.to_dict("records")

    if args.type:
        rows = [r for r in rows if r.get("test_type", "") == args.type]
    if args.ids:
        rows = [r for r in rows if r.get("번호", "") in args.ids]

    logger.info(f"실행 대상: {len(rows)}개 케이스  (concurrency={args.concurrency})")
    logger.info(f"로그 파일:  {_log_path}")
    logger.info("─" * 65)

    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = [run_single(agent, semaphore, row, langfuse_cb) for row in rows]
    results = await asyncio.gather(*tasks)

    # ── 요약 통계 ──
    total = len(results)
    passes = sum(1 for r in results if r.get("overall") == "PASS")
    fails  = sum(1 for r in results if r.get("overall") == "FAIL")
    errors = sum(1 for r in results if r.get("overall") == "ERROR")
    na     = total - passes - fails - errors

    logger.info("=" * 65)
    logger.info(f"전체: {total}  |  PASS: {passes}  FAIL: {fails}  ERROR: {errors}  N/A: {na}")
    if passes + fails > 0:
        logger.info(f"정확도(PASS/PASS+FAIL): {passes / (passes + fails) * 100:.1f}%")

    # 유형별 집계
    by_type: dict[str, dict[str, int]] = {}
    for r in results:
        t = r.get("test_type", "기타")
        by_type.setdefault(t, {"PASS": 0, "FAIL": 0, "ERROR": 0, "N/A": 0})
        key = r.get("overall", "N/A")
        by_type[t][key] = by_type[t].get(key, 0) + 1
    for t, c in sorted(by_type.items()):
        p, f = c.get("PASS", 0), c.get("FAIL", 0)
        acc = f"{p/(p+f)*100:.0f}%" if p + f > 0 else "N/A"
        logger.info(f"  [{t}] PASS={p} FAIL={f} ERROR={c.get('ERROR',0)} | 정확도={acc}")

    # ── 상세 테이블 ──
    logger.info(f"{'번호':<8} {'유형':<10} {'예상조사':<9} {'실제조사':<9} S  V  Y  결과")
    logger.info("─" * 60)
    for r in results:
        logger.info(
            f"{r.get('번호',''):<8} "
            f"{r.get('test_type','')[:9]:<10} "
            f"{r.get('survey_type',''):<9} "
            f"{str(r.get('actual_survey','')):<9} "
            f"{r.get('survey_check',''):<3}"
            f"{r.get('view_check',''):<3}"
            f"{r.get('yn_check',''):<3}"
            f"{r.get('overall','')}"
        )

    # ── FAIL / ERROR 상세 ──
    failures = [r for r in results if r.get("overall") in ("FAIL", "ERROR")]
    if failures:
        logger.info("─" * 65)
        logger.info("FAIL / ERROR 상세:")
        for r in failures:
            logger.info(
                f"  [{r.get('번호','')}] {r.get('질의','')[:60]}\n"
                f"    예상: survey={r.get('survey_type','')}  툴={r.get('툴','')}\n"
                f"    실제: survey={r.get('actual_survey','')}  views={r.get('tools_used','')}\n"
                f"    체크: S={r.get('survey_check','')} V={r.get('view_check','')} Y={r.get('yn_check','')}\n"
                f"    답변: {r.get('answer_preview','')[:180]}"
            )

    # ── CSV 저장 ──
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"eval_{ts}.csv"
    pd.DataFrame(results).to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(f"결과 저장: {out_path}")

    if langfuse_cb:
        langfuse_cb.flush()
        logger.info(f"Langfuse 트레이스 전송 완료: {settings.langfuse_host}")


if __name__ == "__main__":
    asyncio.run(main())
