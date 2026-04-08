from __future__ import annotations
import re
import os
import json
import requests
import random
import string
import httpx
import shutil
from datetime import datetime

from collections import defaultdict
from typing import Any

from pathlib import Path
from langgraph.graph import END
from watchdog.observers import Observer
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from core.config import settings
from core.pii_masking import check_pii
from core.custom_aoai import AzureChatOpenAIWithDynamicHeaders
from langchain_openai import AzureChatOpenAI

from .logger import get_logger

logger = get_logger(__name__)

def create_simple_agent(llm, system_prompt="", output_structure=None):
        prompt_template = ChatPromptTemplate.from_messages(
                [
                    ("system", system_prompt),
                ]
            )

        if output_structure is None:
            agent = prompt_template | llm | StrOutputParser()
        else:
            structured_output = llm.with_structured_output(output_structure)
            agent = prompt_template | structured_output
        return agent

def load_resource_file(file_path, read_type="r"):
    if file_path is not None and os.path.exists(file_path):            
        with open(file_path, read_type) as f:
            if ".json" in file_path:
                return json.load(f)
            else:
                return f.read()
    else:
        assert False, f"파일 경로가 올바르지 않습니다. env 파일을 확인하세요.\n현재 경로: {file_path}"

def append_to_system_prompt(messages, extra_instruction):
    if messages and messages[0]["role"] == "system":
        if extra_instruction not in messages[0]["content"]:
            messages[0]["content"] += f"\n\n**{extra_instruction}**"
    else:
        messages.insert(0, {"role": "system", "content": extra_instruction})
    return messages

def add_random_char(user_id):
    random_str = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(5))
    return f"{user_id}-{random_str}"

def create_azurechatopenai(headers:dict={"kb-key": settings.AZURE_OPENAI_API_KEY}, user_id:str=settings.MCP_USER_ID, model_name:str="gpt-5", reasoning_effort:str="minimal"):
    def random_user_header():
        nonlocal user_id
        if not hasattr(random_user_header, "is_first"):
            x_client_user = user_id
        else:
            x_client_user = add_random_char(user_id)
        if not hasattr(random_user_header, "is_first"):
            random_user_header.is_first = False
        return {"x-client-user": x_client_user}

def create_azurechatopenai(headers:dict={"kb-key": settings.AZURE_OPENAI_API_KEY}, user_id:str="cxagent", model_name:str="gpt-5", reasoning_effort:str="minimal"):
    if model_name == "gpt-5":
        endpoint = settings.AZURE_OPENAI_ENDPOINT + "/gpt-5"
    elif model_name == "gpt-5-mini":
        endpoint = settings.AZURE_OPENAI_ENDPOINT + "/gpt-5-mini"

    llm = AzureChatOpenAI(
        model_name=model_name,
        openai_api_version=settings.OPENAI_API_VERSION,
        api_version=settings.API_VERSION,
        default_headers=headers,
        max_retries=settings.MAX_RETRIES,
        timeout=settings.TIMEOUT,
        azure_endpoint=endpoint,
        reasoning_effort=reasoning_effort,
        api_key=settings.AZURE_OPENAI_API_KEY,
    )
    return llm

    return llm

def convert_input_messages(messages:list[dict]):
    result = []
    for m in messages:
        if m["role"] != "user":
            result.append(("assistant", m["content"]))
        else:
            result.append(("user", m["content"]))
    return result

def keep_latest_files(folder_path, keep=90, is_dir=False):
    folder = Path(folder_path)
    files = [f for f in folder.iterdir() if f.is_dir()] if is_dir else [f for f in folder.iterdir() if f.is_file()]
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

    for file in files[keep:]:
        try:
            if is_dir:
                shutil.rmtree(file)
            else:
                file.unlink()
            logger.info(f"{file.name} 삭제됨")
        except Exception as e:
            logger.error(f"{file.name} 삭제 실패, {e}")

def check_pii_in_chat_history(messages:list[dict]):
    for m in messages:
        if check_pii(m.get("content")):
            return True
    return False



# 채널·경험단계 고정 상태에서의 요소 그룹 키
_CXE_GROUP_COLS = ("서비스품질요소", "고객경험요소", "근거")


def format_discover_cxe_message(
    channel_name, cx_stage_name, start_date, end_date, total_df
) -> str:
    """신규 고객경험요소 발굴 결과를 Markdown 보고서로 변환한다.

    Args:
        df: CSV.
        start_date: 분석 시작일 (예: ``"20260101"``)
        end_date: 분석 종료일 (예: ``"20260228"``)

    Returns:
        Markdown 형식 보고서 문자열
    """

    df = total_df[(total_df["채널"] == channel_name) & (total_df["고객경험단계"] == cx_stage_name)].copy()
    df['설문응답종료년월일'] = df['설문응답종료년월일'].apply(lambda x : datetime.strptime(str(x), "%Y%m%d").strftime("%Y-%m-%d"))
    rows = df.to_dict(orient="records")
    if not rows:
        return "분석 대상 데이터가 없습니다."

    # ── 채널·경험단계 추출 (전 행 동일) ──
    channel = str(rows[0].get("채널", ""))
    cx_stage = str(rows[0].get("고객경험단계", ""))

    # ── 그룹핑: (서비스품질요소, 고객경험요소, 근거) 단위 ──
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(c, "")) for c in _CXE_GROUP_COLS)
        groups[key].append(row)

    # VOC 건수 내림차순 정렬
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)

    total_voc = len(rows)
    total_elements = len(groups)

    lines: list[str] = []
    start_date = datetime.strptime(str(start_date), "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(str(end_date), "%Y%m%d").strftime("%Y-%m-%d")
    
    # ── 헤더 ──
    lines.append("# 신규 고객경험요소 발굴 보고서")
    lines.append("")
    lines.append(
        f"- **분석 기간**: {start_date} ~ {end_date}"
        f"\n"
        f"- **채널**: {channel}"
        f"\n"
        f"- **고객경험단계**: {cx_stage}"
        f"\n"
        f"- **신규 고객경험요소 후보 갯수**: {total_elements}개"
    )
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── 1. 고객경험요소란? ──
    _append_concept_section(lines)

    # ── 2. 후보 요약 테이블 ──
    _append_summary_table(lines, sorted_groups)

    # ── 3. 후보별 상세 ──
    _append_detail_sections(lines, sorted_groups)

    # ── 4. CX 계층 반영 제안 ──
    # _append_hierarchy_tree(lines, sorted_groups, channel, cx_stage)

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# 내부 섹션 빌더
# ─────────────────────────────────────────────────────────────────────


def _append_concept_section(lines: list[str]) -> None:
    """섹션 1: 고객경험요소 개념 설명."""
    lines.append("### 1. 고객경험요소 개요")
    lines.append("")
    lines.append("고객경험요소는 **고객이 서비스를 이용하며 실제로 접촉하는 구체적 대상**입니다.")
    lines.append("")
    lines.append("(채널 → 고객경험단계 → 서비스품질요소 → \"**고객경험요소**\")")
    lines.append("")
    lines.append("| 고객경험관리 계층 | 예시 |")
    lines.append("|:----------:|:------|")
    lines.append("| 채널 | KB 스타뱅킹, 영업점 |")
    lines.append("| 고객경험단계     | 로그인/인증, 대기 |")
    lines.append("| 서비스품질요소    | 로그인/인증 화면의 시인성, 대기공간의 쾌적성 |")
    lines.append("| **고객경험요소**  | **명확한 가이드 및 레이아웃, 대기 의자의 수량 및 안락함** |")
    lines.append("")
    lines.append(
        "**고객경험요소**는 VOC(고객의 목소리)에서 추출되며, "
        "고객경험관리 시스템의 **분석 단위**로 활용됩니다."
    )
    lines.append("")
    lines.append("---")
    lines.append("")


def _append_summary_table(
    lines: list[str],
    sorted_groups: list[tuple[tuple[str, ...], list[dict[str, Any]]]],
) -> None:
    """섹션 2: 후보 요약 테이블."""
    lines.append("### 2. 신규 고객경험요소 후보 요약")
    lines.append("")

    for i, (key, voc_list) in enumerate(sorted_groups, 1):
        sq_factor, element, rationale = key
        lines.append(
            f"- **{_esc_pipe(element)}** "
        )

    lines.append("")
    lines.append("---")
    lines.append("")


def _append_detail_sections(
    lines: list[str],
    sorted_groups: list[tuple[tuple[str, ...], list[dict[str, Any]]]],
) -> None:
    """섹션 3: 후보별 상세 (서비스품질요소 + 근거 + VOC 원문 목록)."""
    lines.append("### 3. 고객경험요소 추출근거 상세")
    lines.append("")

    for i, (key, voc_list) in enumerate(sorted_groups, 1):
        sq_factor, element, rationale = key

        lines.append(f"#### 후보 {i}: {element}")
        lines.append("")
        lines.append(f"- **서비스품질요소**: {sq_factor}")
        lines.append(f"- **근거**: {rationale}")
        lines.append("")

        # VOC 원문 테이블
        lines.append("| # | 근거 VOC |")
        lines.append("|:-----------------:|---------|")
        for j, voc in enumerate(voc_list, 1):
            date = voc.get("설문응답종료년월일", "")
            text = _esc_pipe(str(voc.get("VOC원문", "")))
            lines.append(f"| {j} | {text} |")

        lines.append("")
        if i < len(sorted_groups):
            lines.append("---")
            lines.append("")


def _append_hierarchy_tree(
    lines: list[str],
    sorted_groups: list[tuple[tuple[str, ...], list[dict[str, Any]]]],
    channel: str,
    cx_stage: str,
) -> None:
    """섹션 4: CX 계층 트리."""
    lines.append("---")
    lines.append("")
    lines.append("### 4. CX 계층 반영 제안")
    lines.append("")

    # 품질요소 → [요소명] 트리 구성 (삽입 순서 유지)
    tree: dict[str, list[str]] = {}
    for key, _ in sorted_groups:
        sq_factor, element, _ = key
        tree.setdefault(sq_factor, [])
        if element not in tree[sq_factor]:
            tree[sq_factor].append(element)

    lines.append(channel)
    quality_list = list(tree.items())
    lines.append(f" └─ {cx_stage}")
    for q_idx, (sq_factor, elements) in enumerate(quality_list):
        is_last_q = q_idx == len(quality_list) - 1
        q_branch = "└─" if is_last_q else "├─"
        q_cont = "   " if is_last_q else "│  "
        lines.append(f"     {q_branch} {sq_factor}")
        for e_idx, element in enumerate(elements):
            is_last_e = e_idx == len(elements) - 1
            e_branch = "└─" if is_last_e else "├─"
            lines.append(f"     {q_cont}  {e_branch} ★ {element}")

    lines.append("")


# ─────────────────────────────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────────────────────────────


def _esc_pipe(text: str) -> str:
    """Markdown 테이블 내 파이프 문자 이스케이프."""
    return text.replace("|", "\\|").replace("\n", "")


def format_date(date_txt: str) -> str:
    """'YYYYMMDD' 텍스트를 'YY년 M월 D일'로 변환합니다."""
    yy = date_txt[2:4]
    m = int(date_txt[4:6])
    d = int(date_txt[6:8])
    return f'{yy}년 {m}월 {d}일'