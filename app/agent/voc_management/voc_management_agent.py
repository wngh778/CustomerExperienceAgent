import re
import ast
import pytz
import time
import random
import string
import pickle
import pandas as pd

from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Tuple, Dict, Optional, Set, Literal, Any
from typing_extensions import TypedDict

from langgraph.graph import StateGraph, START, END
from langchain.schema import HumanMessage, SystemMessage

from agent.agent_template import Agent
from core.util import (
    create_azurechatopenai,
    load_resource_file,
    pydantic_to_description_json
)
from core.config import settings
from core.logger import get_logger
from .model import (
    VOC, 
    Keywords, 
    Opinion, 
    VOCSuggestState, 
    AgentSuggestion, 
    OpinionwWithPlan, 
    OpinionwWithMaintained, 
    SuggestionoResponse, 
    FeedbackResponse
)
from .utils.load_files import *
from .utils.text_preprocessing import *

default_resource_path = "/".join(os.path.abspath(__file__).split("/")[:-1])

BU_CHAN = {"00": "해당무", "01": "브랜드", "02": "플랫폼", "03": "대면채널", "04": "고객센터", "05": "상품", "06": "KB 스타뱅킹", "07": "영업점", "08": "고객센터", "09": "상품", "99": "설계시 입력"}
class VocManagementAgent(Agent):
    """
    VOC(Voice of Customer) 관리 에이전트.

    이 에이전트는 Langfuse 기반 프롬프트 로딩, Azure OpenAI/ChatOpenAI LLM 초기화,
    키워드 추출, CX(고객 경험) 매칭, 관련성 검증, 제안 생성 파이프라인을 통해
    VOC 입력으로부터 제안 리포트를 생성합니다.

    주요 기능:
    - 리소스(프롬프트, 쿼리) 로딩 및 Langfuse 통합
    - 키워드 추출 및 CX 요소 콘텐츠 매칭
    - LLM 기반 관련성 재검증
    - 제안 리포트 생성 및 결과 반환
    """

    def __init__(self, prompt_path: str = default_resource_path + "/resources",
                 tool_description_path: str = default_resource_path + "/tool_description",
                max_voc_items: int = None) -> None:
        """
        에이전트 초기화.

        매개변수:
        - prompt_path: 프롬프트 파일들이 위치한 디렉터리 경로
        - tool_description_path: 도구 설명 파일들이 위치한 디렉터리 경로

        동작:
        - 기본 프롬프트/쿼리 저장용 딕셔너리 초기화
        - 상위 Agent 초기화 호출
        - MCP 실행기 및 Langfuse 핸들러 기본값 설정
        """
                
        self.prompts = {}
        self.queries = {}
        super().__init__(prompt_path, tool_description_path)
        self.mcp_executor = None
        self.logger = get_logger(__name__)
        self.define_graph()

        # 파라미터
        self.max_voc_items = max_voc_items if max_voc_items is not None else settings.MAX_VOC_ITEMS 

    def load_resources(self):
        """
        로컬 리소스 파일 로딩.

        동작:
        - prompt_path/prompt 디렉터리의 .txt 프롬프트 파일을 self.prompts에 로딩
        - prompt_path/query 디렉터리의 .sql 쿼리 파일을 self.queries에 로딩
        """
        for p_name in os.listdir(self.prompt_path + f"/prompt/"):
            if p_name.endswith(".txt"):
                self.prompts[p_name] = load_resource_file(self.prompt_path + f"/prompt/" + p_name)

        for q_name in os.listdir(self.prompt_path + "/query/"):
            if q_name.endswith(".sql"):
                self.queries[q_name] = load_resource_file(self.prompt_path + "/query/" + q_name)

    def load_langfuse_resources(self):
        return

    def define_graph(self):
        workflow = StateGraph(VOCSuggestState)
        workflow.add_node("keyword_extraction", self.keyword_extraction)
        workflow.add_node("load_similar_vocs", self.load_similar_vocs)
        workflow.add_node("filter_with_relevance", self.filter_with_relevance)
        workflow.add_node("gen_suggestion", self.gen_suggestion)

        workflow.add_edge(START, "keyword_extraction")
        workflow.add_edge("keyword_extraction", "load_similar_vocs")
        workflow.add_edge("load_similar_vocs", "filter_with_relevance")
        workflow.add_edge("filter_with_relevance", "gen_suggestion")
        workflow.add_edge("gen_suggestion", END)
        
        self.graph = workflow.compile()

    async def query_with_keyword(self, cxc, keywords):
        # 배분되었으며 검토의견이 작성된 VOC들만 추출
        # 그 중에서 keyword로 유사한 VOC를 찾고, 그 VOC들에 대한 개선의견 추출.
        
        search_query_template = self.queries['search_vocs_suggestions.sql']
        keywords_condition = " OR ".join([f"문항응답내용 LIKE '%{keyword}%'" for keyword in keywords])
        prev_year = datetime.now(pytz.timezone('Asia/Seoul')).year - 1
        query = search_query_template.format(
            cxc=cxc,
            keywords_condition=keywords_condition,
            prev_year = prev_year,
            limit_len=settings.MAX_VOC_SEARCH_ITEMS
        )
        similar_vocs = await self.mcp_executor.execute_tool("mysql_query", {"query": query})

        return similar_vocs
        

    def load_system_prompt(self, node_name):
        if node_name == "keyword_extract_prompt":
            return self.prompts['keywords_extract_no_rag.txt']
        elif node_name == "filter_with_relevance":
            return self.prompts['relevance_check.txt']
        elif node_name == "opinion_with_plan":
            return self.prompts['opinion_with_plan.txt']
        elif node_name == "opinion_with_all_maintained":
            return self.prompts['opinion_with_all_maintained.txt']
        elif node_name == "opinion_with_mixed_references":
            return self.prompts['opinion_with_mixed_references.txt']
        elif node_name == "opinion_without_reference":
            return self.prompts['opinion_without_reference.txt']
        else:
            raise ValueError(f"No Proper Prompt for node : {node_name}")

    async def keyword_extraction(self, state):
        system_prompt = self.load_system_prompt("keyword_extract_prompt")
        voc = state.target_voc.voc
        user_query = f"<voc> {voc} </voc>"

        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query),
        ]
        response = await self.llm.ainvoke(prompt)
        keywords = ast.literal_eval(re.sub(r".*(\[.*\]).*", "\\1", response.content))

        elapsed_time = time.time() - self.time
        self.time = time.time()

        return {
            "keywords" : keywords
        }

    async def load_similar_vocs(self, state):
        similar_vocs = await self.query_with_keyword(state.target_voc.cxc_code, state.keywords)

        opinions = []
        for i, record in enumerate(similar_vocs):
            opinions.append(
                Opinion(
                    index=i, 
                    voc=record['문항응답내용'], 
                    opinion_type=record['검토구분'],
                    opinion=record['과제검토의견내용'],
                    opinion_ts=record['검토년월일'],
                    proj_name=record['과제추진사업내용'],
                    proj_start=record['개선이행시작년월일'],
                    proj_end=record['개선이행종료년월일'],
                )
            )

        elapsed_time = time.time() - self.time
        self.time = time.time()

        return {
            "opinions" : opinions
        }

    async def filter_with_relevance(self, state):
        system_prompt = self.load_system_prompt("filter_with_relevance").format(
            max_count=self.max_voc_items
        )
        voc = state.target_voc.voc
        user_query = f"""<target_voc> {voc} </target_voc>\n\n"""

        for opinion in state.opinions:
            user_query += f"""
            <candidate_voc index={opinion.index}>
                <voc> {opinion.voc} </voc>
            </opinion>"""

        max_voc_items = min(self.max_voc_items, len(state.opinions))
        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query),
        ]
        class RelevantVOCs(BaseModel):
            indices: List[str] = Field(description="target_voc와 관련성 높은 candidate_voc의 index list", max_length=max_voc_items)

        response = await self.llm.ainvoke(prompt)
        relevant_voc_index = ast.literal_eval(re.sub(r".*(\[.*\]).*", "\\1", response.content))

        if len(relevant_voc_index) > max_voc_items:
            raise ValueError("Too many selected VOCs")

        elapsed_time = time.time() - self.time
        self.time = time.time()

        return {
            "relevant_voc_index" : set(relevant_voc_index)
        }

    def format_report_template(self, target_voc, relevant_opinions, suggestion):
        header = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "■ AI 검토 리포트\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        suggestion_section = (
            "1) 검토 의견 제안\n"
            f"- 검토구분 : {suggestion.opinion_type}\n"
            f"- 검토의견 : {suggestion.opinion}\n"
        )

        suggestion_section += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        prev_review_section = "2) 기존 검토 의견\n"
        len_project = len([x for x in relevant_opinions if x.opinion])

        # 에이전트가 제안하는 개선의견과 동일한 검토구분을 가진 검토 의견들만 제시.
        view_opinions = [x for x in relevant_opinions if x.opinion is not None and x.opinion != "" and x.opinion_type == suggestion.opinion_type]
        if len(view_opinions) == 0:
            prev_review_section += (
                "해당 VOC와 유사한 VOC들의 기존 검토 의견이 존재하지 않습니다.\n"
            )
        else:
            for opinion in view_opinions:
                review = opinion.opinion
                voc = opinion.voc.replace("\n","")
                opinion_ts = opinion.opinion_ts
                fmt_opinion_ts = ".".join([opinion_ts[:4], opinion_ts[4:6], opinion_ts[6:]])
                proj_name = opinion.proj_name if opinion.proj_name is not None else "-"

                if opinion.proj_start is not None and opinion.proj_end is not None:
                    proj_period = f"{opinion.proj_start} ~ {opinion.proj_end}"
                elif opinion.proj_start is not None and opinion.proj_end is None:
                    proj_period = f"{opinion.proj_start} ~ "
                elif opinion.proj_start is None and opinion.proj_end is not None:
                    proj_period = f" ~ {opinion.proj_end}"
                else:
                    proj_period = "-"

                if opinion.opinion_type == "개선예정":
                    prev_review_section += (
                        f"- 관련 VOC : {voc}\n"
                        f"- 검토의견 : {review}({fmt_opinion_ts})\n"
                        f"- 개선과제명 : {proj_name}\n"
                        f"- 과제기간 : {proj_period}\n\n\n"
                    )
                else:
                    prev_review_section += (
                        f"- 관련 VOC : {voc}\n"
                        f"- 검토의견 : {review}({fmt_opinion_ts})\n\n\n"
                    )

        
        report = header + suggestion_section + prev_review_section
        return report

    async def gen_suggestion(self, state):
        # 유저 쿼리 생성
        relevant_opinions = [opinion for opinion in state.opinions if str(opinion.index) in state.relevant_voc_index]
        opinion_types = [opinion.opinion_type for opinion in relevant_opinions]
        
        voc = state.target_voc.voc
        user_query = f"""<target_voc> {voc} </target_voc>"""
        for opinion in relevant_opinions:
            user_query += f"""
            <opinion index={opinion.index}>
                <previous_voc> {opinion.voc} </previous_voc>
                <opinion_type> {opinion.opinion_type} </opinion_type>
                <opinion_comment> {opinion.opinion} </opinion_comment>
            </opinion>
            """

        # 유사한 VOC가 있는 경우 그 VOC들에 대한 검토의견들을 참고하여 작성\
        # CASE1 : 검토의견 중 개선예정이 1개라도 포함 -> 개선예정 
        # CASE2 : 검토의견 모두가 현행유지 -> 현행유지
        # 유사한 VOC가 없으면 에이전트가 상황을 고려하여 생성
        # 개선 예정이 1개라도 있으면 개선예정으로 분류
        if relevant_opinions:
            if "개선예정" in opinion_types:
                kst = pytz.timezone("Asia/Seoul")
                today = datetime.now(kst).strftime("%Y%m%d")
                output_model = OpinionwWithPlan
                system_prompt = self.load_system_prompt("opinion_with_plan").format(today=today, output_format=pydantic_to_description_json(output_model))
            else:
                if all([tp == "현행유지" for tp in opinion_types]):
                    output_model = OpinionwWithMaintained
                    system_prompt = self.load_system_prompt("opinion_with_all_maintained").format(output_format=pydantic_to_description_json(output_model))
                else:
                    output_model = AgentSuggestion
                    system_prompt = self.load_system_prompt("opinion_with_mixed_references").format(output_format=pydantic_to_description_json(output_model))
        else:
            output_model = AgentSuggestion
            system_prompt = self.load_system_prompt("opinion_without_reference").format(output_format=pydantic_to_description_json(output_model))

        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query),
        ]

        response = await self.llm.ainvoke(prompt)
        json_data = ast.literal_eval(re.sub(r".*(\{.*\}).*", "\\1", response.content))
        suggestion = output_model(**json_data)
        report = self.format_report_template(
            target_voc=state.target_voc,
            relevant_opinions=relevant_opinions,
            suggestion=suggestion
        )

        elapsed_time = time.time() - self.time
        self.time = time.time()

        return {
            "suggestion" : suggestion,
            "report" : report
        }

    async def execute(self, request):
        """
        VOC 요청 처리 파이프라인 실행.

        매개변수:
        - request: VOC 처리 요청 딕셔너리. 예:
          {
            "voc": {
              "voc": "...",
              "qusnInvlTagtpUniqID": "...",
              "qusnid": "...",
              "qsitmid": "..."
            },
            "cxc": "...",
            "qusnid": "...", 
            "qsitmid": "..."
          }
        - langfuse_handler: Langfuse 핸들러(프롬프트 로딩/추적에 사용)

        동작:
        - Azure ChatOpenAI LLM 초기화 및 Langfuse 프롬프트 로딩
        - 키워드 추출 → CX 매칭 → 관련성 검증 → 제안 생성 순으로 파이프라인 실행
        - 결과에서 제안 리포트/종류/타임스탬프 및 각종 식별자(qusnid, qusnInvlTagtpUniqID, qsitmid) 정리

        반환값:
        - dict:
          {
            "suggestionReport": str 또는 None,
            "suggestionType": str 또는 None,
            "ts": str 또는 None,
            "qusnid": str 또는 None,
            "qusnInvlTagtpUniqID": str 또는 None,
            "qsitmid": str 또는 None
          }
        """
        try:    
            voc_dict = request.get("voc", {})
            cxc_code = request.get("sq", "")
            # cxc_code = request.get("cxc_code", "")
            cxc_name = request.get("cxc_name", "")

            voc = voc_dict.get("voc")
            qusnInvlTagtpUniqID = voc_dict.get("qusnInvlTagtpUniqId")
            qusnid = voc_dict.get("qusnId")
            qsitmid = voc_dict.get("qsitmId")

            # 고객경험요소명 추출
            # NPS 관리 시스템이 적용되기 전 임시로 사용
            init_state = VOCSuggestState(
                target_voc= VOC(
                    qusn_id=qusnid,
                    qusn_invl_tagtp_uniq_idd=qusnInvlTagtpUniqID,
                    qsitm_id=qsitmid,
                    voc=voc, 
                    cxc_code=cxc_code,
                    cxc_name=cxc_name,
                ),
                keywords=Keywords(items=[]),
                opinions=[],
            )

            self.time = time.time()
            result = await self.graph.ainvoke(init_state)
    
            kst = pytz.timezone('Asia/Seoul')
            ts = datetime.now(kst).strftime("%Y%m%d%H%M%S")
    
            # 테스트용일 때만 저장
            if "test_id" in request:
                test_id = request['test_id']
                test_path = request['test_path']
                with open(f"{test_path}/{ts}_{test_id}.pkl", "wb") as f:
                    pickle.dump(result, f)
    
            response = SuggestionoResponse(
                suggestionType=result['suggestion'].opinion_type,
                suggestionReport=result['report'],
                ts=ts,
                qusnId=qusnid,
                qusnInvlTagtpUniqId=qusnInvlTagtpUniqID,
                qsitmId=qsitmid
            )
    
            return response.model_dump()
        except Exception as e:
            self.logger.error(f"검토의견 생성 중 오류가 발생했습니다. {e}")

    async def execute_feedback(self, request):
        try:
            voc_dict = request.get("voc", {})
            voc_text = voc_dict.get("voc", "")
            chan = request.get("chan", "")
            cx = request.get("cx", "")
            cxc = request.get("sq", "")
            suggestion_type = voc_dict.get("suggestionType", "")
            suggestion_text = voc_dict.get("suggestionText", "")
            proj_name = voc_dict.get("ImpProjName", "")
            
            cx_cxc_name = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["extract_sq_name.sql"].format(chan=chan, cx=cx, cxc=cxc)})
            if not isinstance(cx_cxc_name, list) or len(cx_cxc_name) == 0:
                self.logger.error(f"없는 서비스 품질 요소 또는 고객경험요소입니다. 코드: {cxc}")
                raise Exception
            ch_name = BU_CHAN.get(chan)
            cx_name = cx_cxc_name[0]["고객경험단계구분명"]
            cxc_name = cx_cxc_name[0]["서비스품질요소명"]

            prompt_template = load_prompt("feedback_generate.txt")
            prompt = prompt_template.format(voc=voc_text.strip(), suggestion_text=suggestion_text.strip(), suggestion_type=suggestion_type, ch=ch_name, cx=cx_name, cxc=cxc_name, proj_name=proj_name)
            msg = SystemMessage(content=prompt)
            llm_feedback = await self.llm.ainvoke([msg])

            res = FeedbackResponse(
                ts=datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d %H:%M:%S"),
                qusnId=voc_dict.get("qusnId", ""),
                qusnInvlTagtpUniqId=voc_dict.get("qusnInvlTagtpUniqId", ""),
                feedbackContent=llm_feedback.content.strip()
            )
            return res.model_dump()
        except Exception as e:
            self.logger.error(f"피드백 작성 중 오류가 발생했습니다. {e}")
