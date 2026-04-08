"""
NPS 데이터 분석 에이전트 - LangGraph 클래스 기반 구현

Flow:
    START → intent_classifier
        ├─ "unsafe"       → unsafe_responder → END
        ├─ "nps_analysis" → policy_guard ─┬─ pass → query_planner ⇄ tool_executor → nps_analyst → END
        │                                 └─ blocked → policy_violation_responder → END
        ├─ "manual"       → manual_qa → END
        └─ "general_chat" → general_responder → END

GPT-5 최적화:
    - XML 태그 기반 프롬프트 구조
    - 명시적 마크다운 출력 지시
    - AIMessage.content 리스트 형태 처리
"""

from __future__ import annotations

import asyncio
import logging
import re
import random
import string
import json
import pandas as pd

from datetime import date, datetime
from pathlib import Path
from typing import Any, Annotated, TypedDict

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, AIMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langfuse import Langfuse, get_client
from langfuse.langchain import CallbackHandler

from core.config import settings

from agent.agent_template import Agent
from agent.report_generation.resources.prompts import (
    INTENT_CLASSIFIER_PROMPT,
    POLICY_GUARD_PROMPT,
    QUERY_PLANNER_PROMPT,
    QUERY_PLANNER_REENTRY_PROMPT,
    NPS_ANALYST_PROMPT,
    MANUAL_QA_PROMPT,
    GENERAL_RESPONDER_PROMPT,
)
from agent.report_generation.resources.schema import SCHEMA_OVERVIEW
from agent.report_generation.resources.catalog import TABLE_ALIAS_CODE_MAP, translate_sql
from agent.report_generation.resources.models import (
    IntentClassification,
    PolicyGuardResult,
)
from agent.report_generation.tools import create_nps_tools

import warnings

warnings.filterwarnings(
    "ignore",
    message="Pydantic serializer warnings",
    category=UserWarning,
)

LANGFUSE = Langfuse(
    public_key=settings.LANGFUSE_PUBLIC_KEY,
    secret_key=settings.LANGFUSE_SECRET_KEY,
    host=settings.LANGFUSE_URL,
    tracing_enabled=settings.LANGFUSE_ENABLED
)


# =============================================================================
# 0. DataFrame → XML 계층 변환
# =============================================================================

def convert_hierarchy_markdown(input_path: str = "", survey_type: str = "", path_sep: str = '|', leaf_sep: str = ';'):
    """
    주어진 TSV(탭 구분) 텍스트를 LLM 친화적인 경로:리스트 형태로 압축 변환합니다.
    - 형식: level1|level2|level3|level4:leaf1;leaf2;...
    - 같은 경로(level1~4)가 반복되는 경우 leaf들을 한 줄로 합치고 중복 leaf는 제거합니다.
    - 공백/불필요한 문자 정리로 토큰량을 줄입니다.

    매개변수:
      input_path (str): 원본 .txt/.tsv 파일 경로
      output_path (str|None): 결과를 저장할 경로. None이면 문자열로만 반환
      path_sep (str): 계층 레벨 구분자 (기본 '|')
      leaf_sep (str): leaf(마지막 항목) 구분자 (기본 ';')

    반환:
      str: 압축된 텍스트 문자열
    """
    import re

    grouped = {}  # {(l1,l2,l3,l4): [leaf,...]}
    delim = re.compile(r'\t+')  # 기본은 탭 구분, 필요시 추가 분리 적용

    with open(input_path, 'r', encoding='utf-8-sig') as f:
        first_line_checked = False
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('---'):
                continue

            parts = [p.strip() for p in delim.split(line) if p.strip()]

            # 탭 구분이 불완전하면 2개 이상 공백도 구분자로 시도
            if len(parts) != 5:
                parts = [p.strip() for p in re.split(r'\t+|\s{2,}', line) if p.strip()]

            if len(parts) != 5:
                # 형식 불일치 라인은 건너뜀
                continue

            l1, l2, l3, l4, leaf = parts

            # 경로/leaf 내 불필요한 내부 공백 정규화(연속 공백 -> 단일 공백)
            norm = lambda s: re.sub(r'\s{2,}', ' ', s)
            l1, l2, l3, l4, leaf = map(norm, (l1, l2, l3, l4, leaf))

            key = (l1, l2, l3, l4)
            cur = grouped.get(key)
            if cur is None:
                grouped[key] = [leaf]
            else:
                # 중복 leaf 방지
                if leaf not in cur:
                    cur.append(leaf)

    # 출력 조립: level1|level2|level3|level4:leaf1;leaf2;...
    lines = []
    for key, leaves in grouped.items():
        if not first_line_checked:
            first_line_checked = True
            line = f"|{path_sep.join(key)}:{leaf_sep.join(leaves)}|"
            col_sep = "|" + "---|" * (len(parts) - 1)
            lines.append(line)
            lines.append(col_sep)
            continue

        # 특정 설문 타입만 추출
        if "TD" == survey_type.upper():
            if key[0].upper() == "BU":
                continue
        elif "BU" == survey_type.upper():
            if key[0].upper() == "TD":
                continue

        line = f"|{path_sep.join(key)}:{leaf_sep.join(leaves)}|"
        lines.append(line)

    result = '\n'.join(lines)
    return result

def extract_text_content(content: Any) -> str:
    """
    AIMessage content에서 텍스트 추출

    GPT-5 시리즈는 content가 리스트 형태로 반환됨:
    [{'type': 'text', 'text': '...', 'annotations': [], 'id': '...'}]

    기존 모델은 문자열로 반환됨

    Args:
        content: AIMessage.content (str 또는 list)

    Returns:
        추출된 텍스트 문자열
    """
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        texts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                texts.append(item.get("text", ""))
            elif isinstance(item, str):
                texts.append(item)
        return "".join(texts)

    return str(content)



# =============================================================================
# 1. State 정의
# =============================================================================

class NPSAgentState(TypedDict, total=False):
    """NPS 에이전트 그래프 상태"""

    # 입력 (둘 중 하나 이상 필수)
    conversation_history: list[tuple[str, str]]  # [("user","..."), ("assistant","...")]
    current_query: str
    original_query: str
    user_id: str

    # 의도 분류 결과
    intent: str                       # unsafe / nps_analysis / manual / general_chat
    intent_reason: str

    # 정책 검사 결과
    policy_violated: bool
    policy_reason: str

    # SQL 실행 결과 (data_retriever → nps_analyst)
    query_results: list[dict]         # [{view_id, sql, data, row_count, error}]

    # 내부 메시지 (data_retriever ToolNode 루프용)
    messages: Annotated[list[BaseMessage], add_messages]

    # Tool 호출 횟수 (안전 캡)
    tool_call_count: int

    # 출력
    final_answer: str
    tools_used: list[str]
    queries_executed: list[str]
    query_reasons: list[str]          # query_planner tool call별 reason





# =============================================================================
# 5. 도구 정의
# =============================================================================

def read_manual() -> dict[str, Any]:
    """CXM 시스템 매뉴얼 파일을 읽어 반환합니다."""
    manual_path = Path(__file__).parent / "resources" / "manual.md"
    try:
        content = manual_path.read_text(encoding="utf-8")
        return {"success": True, "content": content, "error": None}
    except Exception as e:
        return {"success": False, "content": None, "error": str(e)}






# =============================================================================
# 7. ReportGenerationAgent
# =============================================================================

class ReportGenerationAgent(Agent):
    """
    NPS 데이터 분석 에이전트.

    Flow:
        START → intent_classifier
            ├─ "nps_analysis" → policy_guard → query_planner ⇄ tool_executor → nps_analyst → END
            ├─ "manual"       → manual_qa                                                   → END
            └─ "general_chat" → general_responder                                           → END
    """

    def __init__(self, prompt_path: str=None, tool_description_path:str=None, mcp_executor=None):
        """에이전트를 초기화하고 LangGraph 그래프를 빌드한다.

        Args:
            prompt_path: 프롬프트 파일 경로 (미지정 시 기본 경로)
            tool_description_path: 도구 설명 파일 경로 (미지정 시 기본 경로)
        """
        default_resource_path = Path(__file__).parent.parent.parent / "resources"

        super().__init__(prompt_path, tool_description_path)

        # 인스턴스 레벨 캐시
        self._department_list_cache: str | None = None
        self._latest_data_info_cache: str | None = None

        # NPS 데이터 조회 도구 생성
        self.mcp_executor = mcp_executor
        self._nps_tools = create_nps_tools(self.mcp_executor)

        # 설문체계 hirarchy markdown
        self._cx_hierarchy = self._load_cx_hierarchy()

        # 그래프 빌드
        self.graph = self._build_graph()

        

    def load_resources(self):
        # MIGRATION 시 ABC 규격을 위한 내용
        return

    # -----------------------------------------------------------------
    # LLM 호출 래퍼
    # -----------------------------------------------------------------

    async def _call_llm(
        self,
        llm: BaseChatModel,
        messages: list[BaseMessage],
        state: NPSAgentState,
        is_first: bool = False,
    ) -> Any:
        """x-client-user 헤더를 주입하여 LLM을 호출한다.

        - is_first=True: user_id 원본 (사용자별 이용량 측정)
        - is_first=False: user_id + 랜덤 5자 접미사 (rate limit 분산)
        """
        user_id = state.get("user_id", "")
        if is_first or not user_id:
            client_id = user_id
        else:
            suffix = "".join(random.choices(string.ascii_lowercase, k=5))
            client_id = f"{user_id}_{suffix}"

        extra_headers = {"x-client-user": client_id} if client_id else {}
        return await llm.ainvoke(messages, extra_headers=extra_headers)

    # -----------------------------------------------------------------
    # execute: 외부 진입점
    # -----------------------------------------------------------------
    async def execute_eval(
        self,
        query: str,
        user_id: str = "eval",
        langfuse_callback=None,
    ) -> dict:
        """평가용 실행 — 전체 NPSAgentState를 반환한다.

        Args:
            query: 사용자 질의 문자열
            user_id: 사용자 ID
            langfuse_callback: Langfuse CallbackHandler 인스턴스 (없으면 None)

        Returns:
            전체 NPSAgentState dict (tools_used, intent, policy_violated, final_answer 등 포함)
        """
        init_state: dict = {"current_query": query}
        if user_id:
            init_state["user_id"] = user_id

        config = {}
        if langfuse_callback is not None:
            config["callbacks"] = [langfuse_callback]

        state = await self.graph.ainvoke(init_state, config=config if config else None)
        return state

    async def execute(self, user_id, messages):
        """
        에이전트를 실행하고 최종 state를 반환한다.

        Args:
            current_query: 현재 사용자 질의
            conversation_history: 멀티턴 대화 이력
            user_id: 사용자 ID (x-client-user 헤더에 사용)

        Returns:
            최종 NPSAgentState (dict)
        """
        if "TEST" in user_id:
            file_handler = logging.FileHandler("logs/cx_agent.log", encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        td_cache_query = getattr(settings, "TD_CACHE_QUERY", None)
        bu_cache_query = getattr(settings, "BU_CACHE_QUERY", None)

        if td_cache_query and messages[-1][1] == td_cache_query:
            self.logger.info(f"[insight/execute][{user_id}]TD 캐싱 사용")
            return self.latest_td_nps_cache
        elif bu_cache_query and messages[-1][1] == bu_cache_query:
            self.logger.info(f"[insight/execute][{user_id}]BU 캐싱 사용")
            return self.latest_bu_nps_cache
        
        if len(messages) > 1:
            init_state = {
                "conversation_history" : messages[:-1],
                "current_query" : messages[-1][1]
            }
        else:
            init_state = {
                "current_query" : messages[-1][1]
            }

        if user_id:
            init_state["user_id"] = user_id

        if user_id is None:
            user_id = ""

        langfuse = get_client()
        trace_id = Langfuse.create_trace_id(seed=user_id)
        with langfuse.start_as_current_span(
            name='TEST', trace_context={"trace_id":trace_id}
        ):
            hanlder = CallbackHandler()
            insight = await self.graph.ainvoke(init_state, config={"callbacks": [hanlder]})
            self.logger.info(f"Trace ID : {trace_id}")
        
        self.logger.info(insight['final_answer'])
        self.logger.info(f"Langfuse Handler : {hanlder.last_trace_id}")

        return insight['final_answer'].replace('~', '∼')

        


    # -----------------------------------------------------------------
    # 그래프 빌드
    # -----------------------------------------------------------------

    def _build_graph(self) -> StateGraph:
        """LangGraph StateGraph를 구성하고 컴파일한다.

        노드(intent_classifier, policy_guard, query_planner 등)를 등록하고,
        의도·정책 기반 조건부 엣지와 순차 엣지를 연결한다.

        Returns:
            컴파일된 StateGraph 인스턴스
        """
        graph = StateGraph(NPSAgentState)

        graph.add_node("intent_classifier", self._intent_classifier)
        graph.add_node("unsafe_responder", self._unsafe_responder)
        graph.add_node("policy_guard", self._policy_guard)
        graph.add_node("policy_violation_responder", self._policy_violation_responder)
        graph.add_node("query_planner", self._query_planner)
        graph.add_node("tool_executor", ToolNode(tools=self._nps_tools))
        graph.add_node("nps_analyst", self._nps_analyst)
        graph.add_node("manual_qa", self._manual_qa)
        graph.add_node("general_responder", self._general_responder)

        graph.set_entry_point("intent_classifier")

        graph.add_conditional_edges(
            "intent_classifier",
            self._route_by_intent,
            {
                "unsafe_responder": "unsafe_responder",
                "policy_guard": "policy_guard",
                "manual_qa": "manual_qa",
                "general_responder": "general_responder",
            },
        )

        graph.add_conditional_edges(
            "policy_guard",
            self._route_by_policy_guard,
            {
                "query_planner": "query_planner",
                "policy_violation_responder": "policy_violation_responder",
            },
        )

        graph.add_conditional_edges(
            "query_planner",
            self._route_query_planner,
            {
                "tool_executor": "tool_executor",
                "nps_analyst": "nps_analyst"
            },
        )
        graph.add_edge("tool_executor", "query_planner")
        graph.add_edge("nps_analyst", END)
        graph.add_edge("manual_qa", END)
        graph.add_edge("general_responder", END)
        graph.add_edge("unsafe_responder", END)
        graph.add_edge("policy_violation_responder", END)

        return graph.compile()

    # -----------------------------------------------------------------
    # 라우팅
    # -----------------------------------------------------------------

    @staticmethod
    def _route_by_intent(state: NPSAgentState) -> str:
        """의도 분류 결과에 따라 다음 노드를 결정한다.

        Returns:
            라우팅할 노드 이름 (unsafe_responder / policy_guard / manual_qa / general_responder)
        """
        intent = state.get("intent", "general_chat")
        routing_map = {
            "unsafe": "unsafe_responder",
            "nps_analysis": "policy_guard",
            "manual": "manual_qa",
            "general_chat": "general_responder",
        }
        return routing_map.get(intent, "general_responder")

    @staticmethod
    def _route_by_policy_guard(state: NPSAgentState) -> str:
        """정책 위반 여부에 따라 다음 노드를 결정한다.

        Returns:
            'policy_violation_responder' (위반 시) 또는 'query_planner' (통과 시)
        """
        if state.get("policy_violated", False):
            return "policy_violation_responder"
        return "query_planner"

    # -----------------------------------------------------------------
    # 노드 메서드
    # -----------------------------------------------------------------

    async def _intent_classifier(self, state: NPSAgentState) -> dict:
        """사용자 질의를 4가지 의도(unsafe/nps_analysis/manual/general_chat)로 분류한다."""
        context_text = self._build_context_from_history(state.get("conversation_history"))
        current_query = self._get_current_query(state)

        prompt = INTENT_CLASSIFIER_PROMPT.format(
            accumulated_context=context_text
        )

        classifier_llm = self.llm.with_structured_output(IntentClassification)
        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=current_query),
        ]

        try:
            result: IntentClassification = await self._call_llm(classifier_llm, messages, state, is_first=True)
        except Exception as e:
            self.logger.exception(f"intent_classifier error: {e}")
            raise

        self.logger.info(f"Intent classified: {result.intent} | reason: {result.reason}")

        return {
            "intent": result.intent,
            "intent_reason": result.reason,
            "original_query": current_query,
        }


    async def _query_planner(self, state: NPSAgentState) -> dict:
        """질의 분석 + Tool 기반 데이터 조회를 통합 수행한다.

        첫 진입 시 CX 도메인 맥락(계층, 개체명, 최신 데이터 날짜)을 포함한
        시스템 프롬프트와 함께 bind_tools로 LLM을 호출한다.
        재진입 시(tool 결과 수신 후) 추가 호출 여부를 판단한다.
        """
        current_query = self._get_current_query(state)
        existing_messages = state.get("messages", [])

        # 첫 진입: 시스템 프롬프트 + 사용자 질의로 LLM 호출
        if not existing_messages:
            user_id = state.get("user_id", "")
            cx_hierarchy_text = self._cx_hierarchy
            department_list_text = await self._load_department_list(user_id=user_id)
            latest_data_info = await self._load_latest_data_info(user_id=user_id)
            context_text = self._build_context_from_history(state.get("conversation_history"))

            prompt = QUERY_PLANNER_PROMPT.format(
                cx_hierarchy_text=cx_hierarchy_text,
                schema_overview=SCHEMA_OVERVIEW,
                department_list_text=department_list_text,
                latest_data_info=latest_data_info,
                accumulated_context=context_text,
                current_date=date.today().isoformat(),
            )

            llm_with_tools = self.llm.bind_tools(self._nps_tools, tool_choice="any")
            messages = [
                SystemMessage(content=prompt),
                HumanMessage(content=current_query),
            ]
            try:
                response = await self._call_llm(llm_with_tools, messages, state)
            except Exception as e:
                self.logger.exception(f"query_planner error: {e}")
                raise

            tool_calls = response.tool_calls if hasattr(response, "tool_calls") and response.tool_calls else []
            self.logger.info(f"query_planner: tool_calls={len(tool_calls)}")
            reasons = []
            for tc in tool_calls:
                args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
                name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
                if name == "report_skip_reason":
                    skip_reason = args.get("reason", "(없음)")
                    considered = args.get("considered_tools", [])
                    self.logger.info(
                        f"  → SKIP | reason={skip_reason} | considered={considered}"
                    )
                    reasons.append(f"[SKIP] {skip_reason} (검토: {', '.join(considered)})")
                    continue
                reason = args.get('check_reason', '(없음)')
                self.logger.info(
                    f"  → tool={name} | channel={args.get('channel')} | "
                    f"spectrum_cols={args.get('spectrum_columns')} | "
                    f"filters={args.get('filters')} | order_by={args.get('order_by')} | "
                    f"reason={reason}"
                )
                if reason and reason != "(없음)":
                    reasons.append(f"[{name}] {reason}")

            return {
                "messages": [
                    SystemMessage(content=prompt),
                    HumanMessage(content=current_query),
                    response,
                ],
                "tool_call_count": 1,
                "query_reasons": reasons,
            }

        # 재진입 (tool 결과 수신 후): 추가 호출 여부 판단
        # SystemMessage를 재진입 전용 프롬프트로 교체 — 첫 진입의 "툴 선택 로직" 대신
        # "결과 검토 → 충분하면 종료 / 부족하면 추가 호출" 판단에 집중하도록 유도
        reentry_messages = [SystemMessage(content=QUERY_PLANNER_REENTRY_PROMPT)] + existing_messages[1:]
        llm_with_tools = self.llm.bind_tools(self._nps_tools, tool_choice="any")
        try:
            response = await self._call_llm(llm_with_tools, reentry_messages, state)
        except Exception as e:
            self.logger.exception(f"query_planner re-entry error: {e}")
            raise

        tool_call_count = state.get("tool_call_count", 0) + 1
        reentry_tool_calls = response.tool_calls if hasattr(response, "tool_calls") and response.tool_calls else []
        self.logger.info(
            f"query_planner re-entry: tool_calls={len(reentry_tool_calls)}, count={tool_call_count}"
        )
        for tc in reentry_tool_calls:
            args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
            name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
            if name == "report_skip_reason":
                self.logger.info(
                    f"  → SKIP | reason={args.get('reason')} | considered={args.get('considered_tools', [])}"
                )
                continue
            self.logger.info(
                f"  → tool={name} | channel={args.get('channel')} | "
                f"spectrum_cols={args.get('spectrum_columns')} | "
                f"filters={args.get('filters')} | order_by={args.get('order_by')}"
            )

        # report_skip_reason만 호출된 경우 실질적 도구 호출 없음
        real_tool_calls = [tc for tc in reentry_tool_calls if (tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")) != "report_skip_reason"]
        has_more_tool_calls = bool(real_tool_calls)
        update: dict = {
            "messages": [response],
            "tool_call_count": tool_call_count,
        }

        if not has_more_tool_calls or tool_call_count >= 3:
            # 루프 종료: ToolMessage들에서 query_results 추출
            query_results = []
            tools_used = []
            queries_executed = []
            for msg in existing_messages:
                if isinstance(msg, ToolMessage):
                    try:
                        result = json.loads(msg.content)
                        query_results.append({
                            "purpose": result.get("view_id", ""),
                            "sql": result.get("sql", ""),
                            "data": result.get("data"),
                            "row_count": result.get("row_count", 0),
                            "error": result.get("error"),
                        })
                        tool_name = getattr(msg, "name", "") or ""
                        if tool_name:
                            tools_used.append(tool_name)
                        queries_executed.append(result.get("sql", ""))
                    except (json.JSONDecodeError, TypeError):
                        pass
            update["query_results"] = query_results
            update["tools_used"] = tools_used
            update["queries_executed"] = queries_executed

        return update

    @staticmethod
    def _route_query_planner(state: NPSAgentState) -> str:
        """query_planner의 마지막 메시지에 tool_calls가 있으면 tool_executor로, 없으면 nps_analyst로 라우팅."""
        messages = state.get("messages", [])
        if not messages:
            return "nps_analyst"

        last_message = messages[-1]
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            if state.get("tool_call_count", 0) >= 3:
                return "nps_analyst"
            return "tool_executor"
        return "nps_analyst"
    

    async def _nps_analyst(self, state: NPSAgentState) -> dict:
        """SQL 실행 결과를 바탕으로 NPS 분석 보고서를 생성한다."""
        context_text = self._build_context_from_history(state.get("conversation_history"))
        current_query = self._get_current_query(state)
        original_query = state.get("original_query", "")

        query_results = state.get("query_results", [])
        # 정책상 금지된 툴 호출 시 대응
        blocked_query = next(filter(lambda q: "UNSUPPORTED_DATA" in q['purpose'], query_results), None)
        if blocked_query:
            _, block_type = blocked_query['purpose'].split("-")
            return {
                "final_answer": settings.UNSUPPORTED_DATA_MSGS[block_type],
                "tools_used": state.get("tools_used", []),
                "queries_executed": state.get("queries_executed", []),
                "messages": [],
            }
        query_reasons = state.get("query_reasons", [])
        query_results_text = self._build_query_results_text(query_results, query_reasons)

        prompt = NPS_ANALYST_PROMPT.format(
            accumulated_context=context_text,
            query_results=query_results_text,
        )

        # 원래 질의와 정제된 질의가 다르면 둘 다 전달
        if original_query and original_query != current_query:
            human_content = (
                f"[원래 사용자 질의] {original_query}\n"
                f"[정제된 분석 질의] {current_query}"
            )
        else:
            human_content = current_query

        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=human_content),
        ]

        try:
            response: AIMessage = await self._call_llm(self.llm, messages, state)
        except Exception as e:
            self.logger.exception(f"nps_analyst error: {e}")
            raise

        final_content = extract_text_content(response.content)
        final_content = self.wrap_second_table_to_improvement(final_content)
        messages.append(response)

        return {
            "final_answer": final_content,
            "tools_used": state.get("tools_used", []),
            "queries_executed": state.get("queries_executed", []),
            "messages": messages,
        }

    async def _unsafe_responder(self, state: NPSAgentState) -> dict:
        """유해 요청에 대해 고정 거부 응답."""
        return {
            "final_answer": "비윤리적이거나, 개인정보 관련 내용이 포함되어 있어 답변을 드릴 수 없습니다.",
            "tools_used": [],
            "queries_executed": [],
        }

    async def _policy_guard(self, state: NPSAgentState) -> dict:
        """NPS 분석 요청에 대해 개별 직원/영업점/본부 조회 정책 위반 여부를 LLM으로 판단."""
        current_query = self._get_current_query(state)

        guard_llm = self.llm.with_structured_output(PolicyGuardResult)
        messages = [
            SystemMessage(content=POLICY_GUARD_PROMPT),
            HumanMessage(content=current_query),
        ]

        try:
            result: PolicyGuardResult = await self._call_llm(guard_llm, messages, state)
        except Exception as e:
            self.logger.exception(f"policy_guard error: {e}")
            raise

        self.logger.info(f"Policy guard: violated={result.is_violation} | reason: {result.reason}")

        return {
            "policy_violated": result.is_violation,
            "policy_reason": result.reason,
        }

    async def _policy_violation_responder(self, state: NPSAgentState) -> dict:
        """정책 위반 시 고정 거부 응답."""
        return {
            "final_answer": (
                "정책상 직원, 영업점, 지역본부 단위의 NPS 조회는 허용하지 않습니다.\n\n"
                "채널 전체 단위(예: '영업점 채널 NPS 현황')나 고객경험단계 단위(예: '영업점 채널 고객경험단계 별 NPS 현황')는 "
                "조회 가능합니다. 질문을 변경하여 다시 시도해주세요."
            ),
            "tools_used": [],
            "queries_executed": [],
        }

    async def _manual_qa(self, state: NPSAgentState) -> dict:
        """CXM 매뉴얼 문서를 기반으로 사용자 질문에 답변한다."""
        context_text = self._build_context_from_history(state.get("conversation_history"))
        current_query = self._get_current_query(state)

        manual_result = read_manual()
        manual_content = manual_result.get("content", "매뉴얼을 불러올 수 없습니다.")

        # CX 계층 정보도 추가.
        cx_hierarchy_text = self._cx_hierarchy
        manual_content += f"""<CX 고객경험관리 체계 주요 계층 정보> + {cx_hierarchy_text}"""

        prompt = MANUAL_QA_PROMPT.format(
            accumulated_context=context_text,
            manual_content=manual_content,
        )

        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=current_query),
        ]

        try:
            response: AIMessage = await self._call_llm(self.llm, messages, state)
        except Exception as e:
            self.logger.exception(f"manual_qa error: {e}")
            raise

        final_content = extract_text_content(response.content)
        messages.append(response)

        return {
            "final_answer": final_content,
            "tools_used": ["read_manual"],
            "queries_executed": [],
            "messages": messages,
        }

    async def _general_responder(self, state: NPSAgentState) -> dict:
        """NPS 분석 범위 밖의 일반 질문에 역할 범위를 안내한다."""
        current_query = self._get_current_query(state)

        messages = [
            SystemMessage(content=GENERAL_RESPONDER_PROMPT),
            HumanMessage(content=current_query),
        ]

        try:
            response: AIMessage = await self._call_llm(self.llm, messages, state)
        except Exception as e:
            self.logger.exception(f"general_responder error: {e}")
            raise

        final_content = extract_text_content(response.content)
        messages.append(response)

        return {
            "final_answer": final_content,
            "tools_used": [],
            "queries_executed": [],
            "messages": messages,
        }

    # -----------------------------------------------------------------
    # SQL 전처리 (별칭 치환 + 무효 컬럼 조건 제거)
    # -----------------------------------------------------------------

    _table_columns_cache: dict[str, set[str]] = {}

    async def _get_table_columns(self, table: str, user_id: str = "") -> set[str]:
        """INST1 스키마의 테이블 컬럼명 조회 (mcp_executor 사용, 캐싱)"""
        if table not in ReportGenerationAgent._table_columns_cache:
            query = (
                f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = 'INST1' AND TABLE_NAME = '{table}'"
            )
            try:
                result = await self.mcp_executor.execute_tool(
                    "mysql_query", {"query": query}, emp_no=user_id,
                )
                data = result if isinstance(result, list) else result.get("data", [])
                ReportGenerationAgent._table_columns_cache[table] = {
                    row.get("COLUMN_NAME", "") for row in data if row.get("COLUMN_NAME")
                }
            except Exception as e:
                self.logger.warning(f"Failed to get columns for {table}: {e}")
                return set()
        return ReportGenerationAgent._table_columns_cache[table]

    async def _strip_invalid_conditions(self, sql: str, user_id: str = "") -> str:
        """INST1 테이블에 없는 컬럼 조건을 WHERE/AND에서 제거"""
        stripped = sql.strip()
        if (stripped.upper().startswith("WITH")
                or sql.count("SELECT") > 1
                or re.search(r'\bOVER\s*\(', sql, re.IGNORECASE)):
            return sql

        m = re.search(r'FROM\s+INST1\.(\w+)', sql, re.IGNORECASE)
        if not m:
            return sql
        table = m.group(1)
        valid_cols = await self._get_table_columns(table, user_id)
        if not valid_cols:
            return sql

        where_match = re.search(r'\bWHERE\b(.+?)(?=\bLIMIT\b|\bORDER\b|\bGROUP\b|;|\Z)',
                                sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return sql

        where_body = where_match.group(1)
        before_where = sql[:where_match.start()]
        after_where = sql[where_match.end():]

        conditions: list[str] = []
        rest = where_body
        while rest.strip():
            # BETWEEN ... AND '...' 패턴
            m = re.match(r"\s*(\w+)\s+BETWEEN\s+'[^']*'\s+AND\s+'[^']*'(.*)", rest, re.DOTALL)
            if m:
                # rest에서 trailing group(2)를 제외한 부분이 BETWEEN 조건 전체
                cond_text = rest[:len(rest) - len(m.group(2))] if m.group(2) else rest
                conditions.append(cond_text.strip())
                rest = m.group(2)
                rest = re.sub(r'^\s*AND\b', '', rest, count=1)
                continue
            # 일반 조건: 컬럼 = '값' 또는 컬럼 >= '값' 등
            m = re.match(r"\s*(\w+)\s*(?:=|>=|<=|<>|!=|>|<|LIKE|IN)\s*(?:'[^']*'|\d+)(.*)$",
                         rest, re.DOTALL | re.IGNORECASE)
            if m:
                cond_text = rest[:len(rest) - len(m.group(2))] if m.group(2) else rest
                conditions.append(cond_text.strip())
                rest = m.group(2)
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
                logging.getLogger(__name__).debug(f"무효 컬럼 조건 제거: {cond}")
                continue
            valid_conditions.append(cond)

        if valid_conditions:
            new_where = " WHERE " + " AND ".join(valid_conditions) + " "
        else:
            new_where = " "

        return before_where + new_where + after_where

    _DEFAULT_LIMIT = 100

    @staticmethod
    def _ensure_limit(sql: str) -> str:
        """LIMIT 절이 없으면 기본 LIMIT을 추가한다."""
        stripped = sql.strip().rstrip(";")
        # CTE / UNION / 이미 LIMIT이 있으면 건너뜀
        if (stripped.upper().startswith("WITH")
                or re.search(r'\bUNION\b', stripped, re.IGNORECASE)
                or re.search(r'\bLIMIT\s+\d+', stripped, re.IGNORECASE)):
            return sql
        return stripped + f" LIMIT {ReportGenerationAgent._DEFAULT_LIMIT};"

    async def _rewrite_sql(self, sql: str, user_id: str = "") -> str:
        """잔여 별칭 치환(fallback) + 무효 컬럼 조건 제거 + LIMIT 보장."""
        result = sql
        for alias, code in sorted(TABLE_ALIAS_CODE_MAP.items(), key=lambda x: -len(x[0])):
            result = re.sub(rf'\b{re.escape(alias)}\b', code, result)
        result = await self._strip_invalid_conditions(result, user_id)
        result = self._ensure_limit(result)
        return result

    # -----------------------------------------------------------------
    # SQL 실행 (self.mcp_executor 사용)
    # -----------------------------------------------------------------

    async def _execute_sql_query(self, query: str, user_id: str = "") -> dict[str, Any]:
        """SQL을 전처리(별칭 치환 + 무효 컬럼 제거) 후 mcp_executor로 실행한다."""
        translated = translate_sql(query)
        self.logger.info(f"[execute_sql] Original: {query}")
        self.logger.info(f"[execute_sql] Translated: {translated}")

        # 전처리: 잔여 별칭 치환 + 무효 컬럼 조건 제거 (mcp_executor로 메타데이터 조회)
        preprocessed = await self._rewrite_sql(translated, user_id)
        self.logger.info(f"[execute_sql] Preprocessed: {preprocessed}")

        try:
            result = await self.mcp_executor.execute_tool("mysql_query", {"query": preprocessed}, emp_no=user_id)
            data = result if isinstance(result, list) else result.get("data", [])
            row_count = len(data) if data else 0
            self.logger.info(f"[execute_sql] Result: success=True, rows={row_count}")
            return {
                "success": True,
                "data": data,
                "row_count": row_count,
                "error": None,
            }
        except Exception as e:
            self.logger.exception(f"[execute_sql] Failed: {e}")
            return {"success": False, "data": None, "row_count": 0, "error": str(e)}

    # -----------------------------------------------------------------
    # 데이터 로더 (인스턴스 레벨 캐싱)
    # -----------------------------------------------------------------

    def _load_cx_hierarchy(self, survey_type: Literal["TD", "BU"] = "") -> str:
        """TSV 파일에서 CX 계층을 읽어 Markdown으로 변환."""
        hierarchy_path = Path(__file__).parent / "resources" / "cx_hierarchy.txt"
        try:
            cx_hierarchy_cache = convert_hierarchy_markdown(input_path=hierarchy_path, survey_type=survey_type)
            return cx_hierarchy_cache
        except Exception as e:
            self.logger.warning(f"Failed to load CX hierarchy from file: {e}")
            return "(CX 계층 파일 로드 실패)"

    async def _load_department_list(self, user_id: str = "") -> str:
        """DB에서 개선부서 목록을 콤마 구분 텍스트로 변환."""
        if self._department_list_cache is not None:
            return self._department_list_cache

        query = """
            SELECT DISTINCT 개선부서명 FROM INST1.TSCCVMGF4
            WHERE 배분여부=1 AND 개선부서명 IS NOT NULL
            ORDER BY 개선부서명
        """

        try:
            result = await self.mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
            data = result if isinstance(result, list) else result.get("data", [])
        except Exception as e:
            self.logger.warning(f"Failed to load department list from DB: {e}")
            self._department_list_cache = "(DB 조회 실패)"
            return self._department_list_cache

        names = [row.get("개선부서명", "") for row in data if row.get("개선부서명")]
        self._department_list_cache = ", ".join(names)
        self.logger.info(f"Department list loaded: {len(names)} departments")
        return self._department_list_cache

    async def _load_latest_data_info(self, user_id: str = "") -> str:
        """DB에서 TD/BU 최신 데이터 날짜 정보를 텍스트로 변환."""
        if self._latest_data_info_cache is not None:
            return self._latest_data_info_cache

        td_query = """
            SELECT DISTINCT 조사년도, 반기구분명
            FROM INST1.TSCCVMGF1
            ORDER BY 조사년도 DESC, 반기구분명 DESC
            LIMIT 1
        """

        bu_query = """
            SELECT 채널명, MAX(기준년월일) AS 최신일자
            FROM INST1.TSCCVMGF4
            GROUP BY 채널명 
            ORDER BY 채널명
        """

        try:
            td_result = await self.mcp_executor.execute_tool("mysql_query", {"query": td_query}, emp_no=user_id)
            td_data = td_result if isinstance(td_result, list) else td_result.get("data", [])

            try:
                bu_result = await self.mcp_executor.execute_tool("mysql_query", {"query": bu_query}, emp_no=user_id)
                bu_data = bu_result if isinstance(bu_result, list) else bu_result.get("data", [])
            except Exception:
                bu_data = []
        except Exception as e:
            self.logger.warning(f"Failed to load latest data info from DB: {e}")
            self._latest_data_info_cache = "(DB 조회 실패 - 최신 데이터 정보 불명)"
            return self._latest_data_info_cache

        lines: list[str] = []

        if td_data:
            row = td_data[0]
            lines.append(f"■ TD (Top-Down) 최신: {row.get('조사년도', '?')}년 {row.get('반기구분명', '?')}")
            lines.append("")

        if bu_data:
            lines.append("■ BU (Bottom-Up) 최신 데이터")
            for row in bu_data:
                ch = row.get("채널명", "?")
                dt = row.get("최신일자", "데이터 없음")
                lines.append(f"  - {ch}: 최신 데이터={dt}")
            lines.append("")

        if not lines:
            self._latest_data_info_cache = "(활성 조사 없음)"
        else:
            self._latest_data_info_cache = "\n".join(lines).strip()

        self.logger.info(f"Latest data info loaded: TD={len(td_data)}, BU={len(bu_data)} channels")
        return self._latest_data_info_cache

    # -----------------------------------------------------------------
    # 텍스트 빌더 유틸리티
    # -----------------------------------------------------------------

    @staticmethod
    def _build_context_from_history(
        conversation_history: list[tuple[str, str]] | None,
    ) -> str:
        """대화 이력에서 최근 10턴을 텍스트로 변환한다.

        에이전트 응답은 200자로 잘라서 포함한다.

        Returns:
            대화 맥락 텍스트. 이력이 없으면 '없음'.
        """
        if not conversation_history:
            return "없음"

        recent = conversation_history[-3:]
        lines = []
        for role, content in recent:
            if role == "user":
                lines.append(f"[사용자] {content}")
            else:
                truncated = content
                lines.append(f"[에이전트] {truncated}")
        return "\n".join(lines)

    @staticmethod
    def _get_current_query(state: dict) -> str:
        """state에서 현재 질의를 추출한다.

        current_query가 있으면 우선 사용하고, 없으면 대화 이력의 마지막 사용자 발화를 반환한다.
        """
        if state.get("current_query"):
            return state["current_query"]
        history = state.get("conversation_history", [])
        for role, content in reversed(history):
            if role == "user":
                return content
        return ""

    @staticmethod
    def _build_query_results_text(query_results: list[dict], query_reasons: list[str], max_rows: int = 50) -> str:
        """SQL 실행 결과 리스트를 nps_analyst 프롬프트에 삽입할 텍스트로 변환한다.

        Args:
            query_results: sql_executor에서 반환된 결과 딕셔너리 리스트
            max_rows: 결과당 표시할 최대 행 수 (기본 50)

        Returns:
            CSV 형식의 결과 텍스트. 결과가 없으면 안내 문구.
        """
        if not query_results:
            return "(조회된 데이터 없음)"

        parts = []
        for i, result in enumerate(query_results, 1):
            purpose = result.get("purpose", "")
            data = result.get("data", [])
            row_count = result.get("row_count", 0)
            error = result.get("error")

            header = f"--- 쿼리 {i}: {purpose} ---"

            if error:
                parts.append(f"{header}\n(오류: {error})")
                continue

            if not data:
                parts.append(f"{header}\n(결과 없음: 0건)")
                continue

            df = pd.DataFrame(data[:max_rows])
            numeric_cols = df.select_dtypes(include="number").columns
            df[numeric_cols] = df[numeric_cols].round(2)
            table = df.to_csv(index=False)

            truncation_note = ""
            if row_count > max_rows:
                truncation_note = f"\n(총 {row_count}건 중 상위 {max_rows}건만 표시)"

            parts.append(f"{header}\n{table}{truncation_note}")

        # 쿼리 조회 이유
        reason_parts = ["쿼리 조회 의도"]
        for qr in query_reasons:
            reason_parts.append(qr)
            
        return "\n\n".join(reason_parts) + "\n\n".join(parts)

    @staticmethod
    def wrap_second_table_to_improvement(text: str, toggle_name: str = "더보기") -> str:
        """
        '### 상세 분석' 섹션 내 마크다운 테이블이 2개 이상일 때,
        두 번째 테이블 시작점부터 '### 개선 방향' 섹션 직전까지를
        <details><summary>...</summary>...</details>로 감싸 반환합니다.
        조건을 만족하지 않으면 원문(text)을 그대로 반환합니다.

        Args:
            text: 전체 문서 문자열
            toggle_name: details summary에 들어갈 텍스트 (기본값: '더보기')

        Returns:
            str: 변환된 문서 문자열
        """
        # 섹션 헤더 찾기
        m_details = re.search(r'(?m)^###\s*상세\s*분석\s*$', text)
        m_improve = re.search(r'(?m)^###\s*개선\s*방향\s*$', text)
        if not m_details or not m_improve:
            return text  # 필수 섹션 없으면 그대로 반환

        details_start = m_details.end()  # '### 상세 분석' 줄 끝 이후부터
        improve_start = m_improve.start()  # '### 개선 방향' 줄 시작 위치

        section = text[details_start:improve_start]

        # 마크다운 테이블 탐지 (헤더줄 |..., 구분줄 |---|... 형태, 데이터줄 |... 최소 1줄)
        table_regex = re.compile(
            r'(?m)'                 # 멀티라인
            r'^\|.*\n'              # 헤더 행 (|로 시작)
            r'^\|\s*[-:]+(?:\s*\|\s*[-:]+)*\s*\|\s*$\n'  # 구분 행 (|---|---|...)
            r'(?:^\|.*\n)+'         # 데이터 행(최소 1줄 이상)
        )

        tables = list(table_regex.finditer(section))
        if len(tables) < 2:
            return text  # 테이블이 2개 미만이면 변경 없음

        # 두 번째 테이블 시작 지점(문서 전체 기준)
        second_table_rel_start = tables[1].start()
        second_table_abs_start = details_start + second_table_rel_start

        # 두 번째 테이블 시작부터 '### 개선 방향' 이전까지를 감쌀 내용
        left_output = text[second_table_abs_start:improve_start]

        details_block = f"<details>\n<summary>{toggle_name}</summary>\n{left_output}\n</details>"

        # 해당 범위를 details 블록으로 치환
        new_text = text[:second_table_abs_start] + details_block + text[improve_start:]
        return new_text
