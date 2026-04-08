import time
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from contextvars import ContextVar

from langchain_core.tools import StructuredTool
from langchain.prompts import ChatPromptTemplate
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from core.util import create_azurechatopenai, load_resource_file, add_random_char, format_date
from core.config import settings
from agent.agent_template import Agent
from .model import *
from .session import *
from .resources.discover_cx_elements import (
    get_channel_code,
    get_cx_stage_code,
    CXE_QUERY_TEMPLATE,
    VOC_QUERY_TEMPLATE,
    _split_into_batches,
    format_voc_batch,
    format_cxe_discover_messages,
    format_cxe_merge_messages,
    CXDiscoveryResult,
    CXMergeResult
)

from langchain_core.tools import StructuredTool


import pandas as pd
import re
import os

default_resource_path = "/".join(os.path.abspath(__file__).split("/")[:-1]) + "/resources"

TD_VOC_TABLE = "inst1.TSCCVMGD3"
BU_VOC_TABLE = "inst1.TSCCVMGF4"
SAMPLE_SIZE = 500

class SQLAgent(Agent):
    """SQL Agent 클래스"""
    _cur_user_id : ContextVar[str] = ContextVar("cur_user_id")

    def __init__(self, prompt_path:str=default_resource_path+"/prompt", tool_description_path:str=default_resource_path+"/tool_description"):
        self.llm_mini = create_azurechatopenai(model_name='gpt-5')
        super().__init__(prompt_path, tool_description_path)
        self.mcp_executor = None

    def load_resources(self):
        self.prompts = {f_name: load_resource_file(self.prompt_path + "/" + f_name) for f_name in os.listdir(self.prompt_path) if f_name != ".ipynb_checkpoints"}
        self.queries = {f_name: load_resource_file(default_resource_path + "/query/" + f_name) for f_name in os.listdir(default_resource_path+"/query") if f_name != ".ipynb_checkpoints"}
        with open(default_resource_path+"/recommend_question_list", "r") as f:
            self.recommend_question_list = f.readlines()

    def load_langfuse_resources(self):
        return
    
    def define_chains(self):
        """Agent를 생성합니다."""
        # 시스템 프롬프트 로드
        recommend_question_prompt = self.prompts["recommend_question_prompt"]
        
        recommend_prompt = ChatPromptTemplate.from_messages([
            ("system", recommend_question_prompt)
        ])

        # Agent 생성
        self.recommend_agent = recommend_prompt | self.llm_mini.with_structured_output(RecommendQuestion)

# ----------------------------------------------
# 아래만 사용
# ----------------------------------------------
    async def case_analysis(self, survey_id:str, user_id:str):
        """
        고객 CASE 조사 분석
        """

        token = self._cur_user_id.set(user_id)
        try:
            user_id = self._cur_user_id.get()
            questions_data = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["case_survey_template"].format(SURVEYID=survey_id)}, emp_no=user_id)
            survey_result = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["case_survey_statistic"].format(SURVEYID=survey_id)}, emp_no=user_id)
            survey_result = pd.DataFrame(survey_result)
            self.logger.info("데이터 추출 완료")

            count = 1
            question_analysis = []
            for question in questions_data:
                q_id = question["문항ID"]
                q_text = question["문항내용"]
                multi_choices = "복수응답" if question["복수선택여부"] == "1" else "단일응답"
                origin_num = re.findall(r'^[Q0-9\-]{,5}\.?', q_text.strip())
                if origin_num and origin_num[0] != "":
                    q_num = ""
                else:
                    q_num = f"{count}. "
                q_type = question["문항구분"]

                sub_df = survey_result[survey_result["문항ID"] == q_id]
                total_response_num = sum(sub_df["응답자수"]) if len(sub_df) != 0 else 0

                if q_type == "선택형":
                    response_list = question["문항선택항목내용리스트"].split("|")
                    response_id_list = question["문항선택항목ID리스트"].split("|")
                    response_details = []
                    for option_id, option_text in zip(response_id_list, response_list):
                        temp = sub_df[sub_df["문항선택항목"] == option_text.strip()]
                        if temp.empty:
                            response_details.append(f"- {option_text}: 0명 (0.0%)")
                        else:
                            response_details.append(f"- {option_text}: {temp['응답자수'].values[0]}명 ({temp['응답비중'].values[0]}%)")
                    question_analysis.append(f"#### {q_num}{q_text}<br>" + "&nbsp;" * (len(q_num)+1) + f"**{q_type}({multi_choices}) | 응답자수: {total_response_num}**\n" + '\n'.join(response_details))

                elif q_type == "점수형":
                    is_nps = question["NPS환산대상여부"]
                    score_list = sorted(list(sub_df['문항선택항목']), key=lambda x: int(x))
                    header = "| " + " | ".join([str(i) + "점" for i in score_list]) + " |"
                    separator = "| " + " | ".join(["---"] * len(score_list)) + " |"
                    value_list = []
                    for i in score_list:
                        check_df = sub_df[sub_df['문항선택항목'] == str(i)]
                        value_list.append(f"{check_df['응답자수'].values[0]}명 ({check_df['응답비중'].values[0]})")
                    values = "| " + " | ".join(value_list) + " |"
                    markdown_table = "\n".join([header, separator, values])
                    sub_df.loc[:, "문항선택항목"] = pd.to_numeric(sub_df["문항선택항목"], errors="coerce")
                    if int(is_nps):
                        rec_count = sub_df[sub_df["문항선택항목"].between(9, 10)]["응답자수"].sum()
                        not_rec_count = sub_df[sub_df["문항선택항목"].between(0, 6)]["응답자수"].sum()
                        avg_score = (rec_count - not_rec_count) / total_response_num * 100
                        score_text = f"NPS(추천비율 - 비추천비율): {avg_score:.1f}점"
                    else:
                        avg_score = (sub_df["응답자수"] * sub_df["문항선택항목"]).sum() / total_response_num
                        score_text = f"평균 점수(가중 평균): {avg_score:.1f}점"
                    question_analysis.append(f"#### {q_num}{q_text}<br>" + "&nbsp;" * (len(q_num)+1) + f"**{q_type} | 응답자수: {total_response_num}**\n- {score_text}\n" + markdown_table)
                elif q_type == "서술형":
                    if sub_df["서술형원문모음"].empty:
                        responses = []
                        examples = "서술형 응답 원문이 없습니다."
                    else:
                        # invalid voc 필터
                        vocs = sub_df["서술형원문모음"]
                        responses = [
                            voc.strip() for voc in vocs.values[0].split("|")
                            if len(voc.strip()) > 3
                            and not re.fullmatch(r'\s+', voc)
                            and not re.fullmatch(r'[ㄱ-ㅎㅏ-ㅣ]+', voc)
                            and not re.fullmatch(r'[^A-Za-z0-9가-힣]+', voc)
                        ]
                        examples = ', '.join(responses[:3])
                    headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
                    response_summary = await self.llm.ainvoke(f"다음 설문 문항 응답들을 2문장 정도로 요약해주세요. {responses}", extra_headers=headers)
                    question_analysis.append(f"#### {q_num}{q_text}<br>" + "&nbsp;" * (len(q_num)+1) + f"**{q_type} | 응답자수: {total_response_num}**\n- 응답 예시: {examples}" + f"\n- AI 요약: {response_summary.content}")
                else: # 평가형 문항
                    is_nps = question["NPS환산대상여부"]
                    score_list = sorted(list(sub_df['문항선택항목']), key=lambda x: int(x))
                    header = "| " + " | ".join([str(i) + "점" for i in score_list]) + " |"
                    separator = "| " + " | ".join(["---"] * len(score_list)) + " |"
                    value_list = []
                    for i in score_list:
                        check_df = sub_df[sub_df['문항선택항목'] == str(i)]
                        value_list.append(f"{check_df['응답자수'].values[0]}명 ({check_df['응답비중'].values[0]})")
                    values = "| " + " | ".join(value_list) + " |"
                    markdown_table = "\n".join([header, separator, values])
                    sub_df.loc[:, "문항선택항목"] = pd.to_numeric(sub_df["문항선택항목"], errors="coerce")
                    if int(is_nps):
                        rec_count = sub_df[sub_df["문항선택항목"].between(9, 10)]["응답자수"].sum()
                        not_rec_count = sub_df[sub_df["문항선택항목"].between(0, 6)]["응답자수"].sum()
                        avg_score = (rec_count - not_rec_count) / total_response_num * 100
                        score_text = f"NPS(추천비율 - 비추천비율): {avg_score:.1f}점"
                    else:
                        avg_score = (sub_df["응답자수"] * sub_df["문항선택항목"]).sum() / total_response_num
                        score_text = f"평균 점수(가중 평균): {avg_score:.1f}점"
                    question_analysis.append(f"#### {q_num}{q_text}<br>" + "&nbsp;" * (len(q_num)+1) + f"**{q_type} | 응답자수: {total_response_num}**\n- {score_text}\n" + markdown_table)
                count += 1

            # 최종 보고서 생성
            survey_info = await self.mcp_executor.execute_tool("mysql_query", {"query": f"select * from inst1.TSCCVMGF5 where 설문ID='{survey_id}'"}, emp_no=user_id)

            report = '\n\n'.join(question_analysis)
            report_title = f"{survey_info[0]['설문제목명']} 설문 결과 분석 (참여자 수: {survey_info[0]['설문응답건수']}명)\n\n"
            structured_part = '\n\n'.join([question_analysis[i] for i, data in enumerate(questions_data) if data["문항구분"] != "서술형"])
            headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
            st_report_summary = await self.llm.ainvoke(f"아래 설문 결과 3~4개의 bullet으로 요약해줘\n---\n{structured_part}", extra_headers=headers)
            non_structured_part = '\n\n'.join([question_analysis[i] for i, data in enumerate(questions_data) if data["문항구분"] == "서술형"])
            headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
            non_st_report_summary = await self.llm.ainvoke(f"아래 설문 결과 3~4개의 bullet으로 요약해줘\n---\n{non_structured_part}", extra_headers=headers)
            final_report = "### [조사 개요]\n"
            final_report += f"- 조사명 : {survey_info[0]['설문제목명']}\n"
            final_report += f"- 조사목적 : {survey_info[0]['설문목적상세내용']}\n"
            final_report += f"- 조사대상 : {survey_info[0]['설문조사대상명']}\n"
            final_report += f"- 조사기간 : {survey_info[0]['설문응답시작일시']} ~ {survey_info[0]['설문응답종료일시']}\n"
            final_report += f"- 조사방법 : {survey_info[0]['설문조사종류명']}\n"
            final_report += f"- 응답자수 : {int(survey_info[0]['설문응답건수']):,}\n---\n"
            final_report += "### [설문 결과 요약]\n"
            final_report += "#### 주요 내용 요약\n"
            final_report += f"{st_report_summary.content}\n"
            final_report += "#### VOC 요약\n"
            final_report += f"{non_st_report_summary.content}" + "\n---\n"
            final_report += "### [설문 결과 상세]\n"
            final_report += report
            return report_title, final_report
        except Exception as e:
            self.logger.error(f"[case_analysis] 에러 발생: {e}", stacklevel=3)
            raise e
        finally:
            self._cur_user_id.reset(token)

    async def voc_analysis(self, survey_type, user_id, channel_name="", keyword=""):
        """
        VOC 원문 분석 도구
        """

        token = self._cur_user_id.set(user_id)
        try:
            user_id = self._cur_user_id.get()
            # 키워드 분석
            if keyword != "":
                if survey_type == "Top-Down (TD)":
                    query = self.queries["voc_analysis_TD_keyword"].format(keyword=keyword)
                else:
                    query = self.queries["voc_analysis_BU_keyword"].format(keyword=keyword)
                keyword_voc = await self.mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
                if keyword_voc == []:
                    return f"\"{keyword}\" 키워드로 검색된 VOC가 없습니다."
                elif isinstance(keyword_voc, str):
                    self.logger.error("VOC를 추출하는 도중 에러가 발생했습니다: " + keyword_voc)
                    return "VOC를 추출하는 도중 에러가 발생했습니다."
                voc_df = pd.DataFrame(keyword_voc[:SAMPLE_SIZE])
                voc_data = voc_df.to_markdown()
                headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
                response = await self.llm.ainvoke(self.prompts["keyword_voc_summary_prompt"].format(survey_type=survey_type, keyword=keyword, voc_data=voc_data), extra_headers=headers)
                start_date = format_date(voc_df['기준년월일'].min())
                end_date = format_date(voc_df['기준년월일'].max())
                content = f"## {keyword} 키워드 포함 VOC 분석\n## ({start_date}~{end_date}, 총 {len(voc_df)}개)\n"
                content += response.content
                return content.replace("~", "～")

            def create_voc_per_cx(data):
                output = ""
                for item in data:
                    output += f"고객경험단계: {item['고객경험단계']}\n"
                    # 고객경험단계 별 voc 개수 200개 초과시 200개로 downsize
                    voc_list = item['원문'].split('|') if len(item['원문'].split('|')) > 200 else item['원문'].split('|')[:200]
                    output += f"VOC 리스트: {voc_list}\n"
                    output += f"\n---\n"
                return output
            # 채널 별 분석
            if survey_type == "Top-Down (TD)":
                query = f"select max(조사년도) as year from {TD_VOC_TABLE}"
                res = await self.mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
                current_year = res[0]["year"]
                query = f"select max(반기구분명) as half from {TD_VOC_TABLE} where 조사년도={current_year}"
                res = await self.mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
                current_half = res[0]["half"]
                last_half = "상반기" if current_half == "하반기" else "하반기"
                last_year = current_year
                # 전반기가 하반기이면 연도가 바뀜
                if last_half == "하반기":
                    last_year = str(int(current_year) - 1)
                current_voc = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["voc_analysis_TD_channel"].format(channel_name=channel_name, year=current_year, half=current_half, sample_size=SAMPLE_SIZE)}, emp_no=user_id)
                last_voc = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["voc_analysis_TD_channel"].format(channel_name=channel_name, year=last_year, half=last_half, sample_size=SAMPLE_SIZE)}, emp_no=user_id)

                period_text = "1년"
                content = f"## {channel_name} 채널 VOC 분석 리포트\n## ({current_year[2:4]}년 {current_half})\n"
            else:
                query = f"select max(기준년월일) as date from {BU_VOC_TABLE}"
                res = await self.mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
                today_date = str(res[0]["date"])
                current_week = (datetime.strptime(today_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
                last_week = (datetime.strptime(today_date, "%Y%m%d") - timedelta(days=14)).strftime("%Y%m%d")
                current_voc = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["voc_analysis_BU_channel"].format(channel_name=channel_name, start_date=current_week, end_date=today_date, sample_size=SAMPLE_SIZE)}, emp_no=user_id)
                last_voc = await self.mcp_executor.execute_tool("mysql_query", {"query": self.queries["voc_analysis_BU_channel"].format(channel_name=channel_name, start_date=last_week, end_date=current_week, sample_size=SAMPLE_SIZE)}, emp_no=user_id)

                period_text = "2주"
                content = f"## {channel_name} 채널 VOC 분석 리포트\n## ({format_date(last_week)}~{format_date(today_date)})\n"

            if current_voc == [] and last_voc == []:
                return f"최근 {period_text}간 {channel_name} 채널에 수집된 VOC가 없습니다."
            elif isinstance(current_voc, str):
                self.logger.error("VOC를 추출하는 도중 에러가 발생했습니다: " + current_voc)
                return "VOC를 추출하는 도중 에러가 발생했습니다."

            if survey_type == "Top-Down (TD)":
                total_voc = f"## {current_year} {current_half}의 VOC\n"
                total_voc += create_voc_per_cx(current_voc)
                total_voc += f"\n## {last_year} {last_half}의 VOC\n"
                total_voc += create_voc_per_cx(last_voc)
            else:
                total_voc = f"## {format_date(current_week)}~{format_date(today_date)}의 VOC\n"
                total_voc += create_voc_per_cx(current_voc)
                total_voc += f"\n## {format_date(last_week)}~{format_date(current_week)}의 VOC\n"
                total_voc += create_voc_per_cx(last_voc)

            headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
            response = await self.llm.ainvoke(self.prompts["channel_voc_summary_prompt"].format(survey_type=survey_type, channel_name=channel_name, total_voc=total_voc), extra_headers=headers)

            content += response.content
            return content.replace("~", "～")
        except Exception as e:
            self.logger.error(f"[voc_analysis] 에러 발생: {e}", stacklevel=2)
            raise e
        finally:
            self._cur_user_id.reset(token)
    
    async def generate_recommend_question(self, response:str, user_id:str):
        headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}
        self.llm_mini = create_azurechatopenai(model_name='gpt-5', headers=headers)
        self.define_chains()
        rec_q = await self.recommend_agent.ainvoke({"response": response, "recommend_question_list": self.recommend_question_list})
        return [rec_q.q1, rec_q.q2, rec_q.q3]
        
    async def execute(self, user_id:str, messages:list, today_date, langfuse_handler):
        return


    async def discover_cx_elements(self, user_id, channel, cx_stage, start_date, end_date):
        # 채널, 고객경험단계구분 조회
        init_start = time.time()
        channel_code = await get_channel_code(channel, self.mcp_executor)
        cx_stage_code = await get_cx_stage_code(channel_code, cx_stage, self.mcp_executor)
        self.logger.info(f"'{channel}' 채널의 '{cx_stage}' 고객경험단계에서 신규 고객경험요소 추출 시작 (기간 : {start_date} ~ {end_date})")


        # 고객경험요소명 추출
        get_existing_element_query = CXE_QUERY_TEMPLATE.format(
            channel_code=channel_code,
            cx_stage_code=cx_stage_code,
        )
        cxe = await self.mcp_executor.execute_tool("mysql_query", {"query": get_existing_element_query})
        sq_list = list(set([e['서비스품질요소'] for e in cxe]))
        sq = ",".join(sq_list)

        # 고객경험요소 발굴용 후보 VOC 조회
        get_candidate_vocs_query = VOC_QUERY_TEMPLATE.format(
            channel_code=channel_code,
            cx_stage_code=cx_stage_code,
            start_date=start_date,
            end_date=end_date,
        )

        LIMIT = 500
        offset = 0
        candidates_vocs = []
        while True:
            paginated_query = f"{get_candidate_vocs_query} LIMIT {LIMIT} OFFSET {offset}"
            page = await self.mcp_executor.execute_tool("mysql_query", {"query": paginated_query})
            candidates_vocs += page
            page_size = len(page)
            offset += page_size
            if page_size < LIMIT:
                break
        
        # No VoC
        if not candidates_vocs:
            self.logger.warning(f"BU '{channel}' 채널의 '{cx_stage}' 고객경험단계의 {start_date} ~ {end_date} 기간 동안 신규 고객경험요소 발굴용 VOC가 존재하지 않습니다")
            if settings.DISCOVER_CXE_MODE == "TEST":
                n_candidates_voc = 0
                n_batch = 0
                n_raw_cxe = 0
                n_final_cxe = 0
                n_relevant_voc = 0
                avg_batch_time = 0
                elapsed_time = 0
                return n_candidates_voc, n_batch, avg_batch_time, elapsed_time, n_raw_cxe, n_final_cxe, n_relevant_voc, []
            return []

        # 배치 분할
        id_batch, item_batch = _split_into_batches(candidates_vocs, settings.DISCOVER_CXE_BATCH_SIZE)
        all_batch_outputs = []

        self.logger.info(f"고객경험요소 발굴 데이터 크기 : {len(candidates_vocs)}개, 배치 갯수 : {len(item_batch)} (배치사이즈 : {settings.DISCOVER_CXE_BATCH_SIZE})")

        batch_times = []
        for batch_idx, (voc_ids, batch) in enumerate(zip(id_batch, item_batch)):
            bs = time.time()
            voc_batch_text = format_voc_batch(voc_ids, batch)
            messages = format_cxe_discover_messages(
                channel, cx_stage, # 채널과 고객경험단계 
                sq, cxe, # 고객경험단계별 기존 서비스품질요소, 고객경험요소
                voc_batch_text # VOC 목록 배치
            )

            lc_messages = [
                SystemMessage(content=messages[0]["content"]),
                HumanMessage(content=messages[1]["content"])
            ]

            headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}

            structured_llm = self.llm.with_structured_output(CXDiscoveryResult) 
            result = await structured_llm.ainvoke(lc_messages, extra_headers=headers)
            # result = CXDiscoveryResult(**json.loads(result.content))
            self.logger.info(f"배치번호 : {batch_idx}, 추출 고객경험요소 수: {len(result.discovered_elements)}")
            all_batch_outputs.append(result)
            batch_times.append(time.time() - bs)


        # LLM을 통해 발굴된 신규 고객경험요소들 중복이나 의미적으로 유사한 고객경험요소를 종합.
        discovered_cxe_list = sum([x.discovered_elements for x in all_batch_outputs], []) 
        filtered_result = await self.merge_discovered_cxe(user_id, channel, cx_stage, sq, cxe, discovered_cxe_list)
        # filtered_result = CXMergeResult(**json.loads(_filtered_result.content))

        self.logger.info(f"Raw 추출 고객경험요소 수: {len(discovered_cxe_list)}, 통합 후 고객경험요소 수 : {len(filtered_result.discovered_elements)}")

        # 병합된 신규 고객경험요소와 관련된 VOC 목록을 재정렬.
        output = []
        for obj in filtered_result.discovered_elements:
            relevant_cxe_list = obj.relevant_discovered_cxe_id_list
            relevant_voc_ids = sum([discovered_cxe_list[idx].relevant_voc_id_list for idx in relevant_cxe_list], [])
            for idx in relevant_voc_ids:
                output.append({
                    "채널" : channel,
                    "고객경험단계" : cx_stage,
                    "서비스품질요소" : obj.parent_factor_name if obj.parent_factor_name in sq_list else "", # 틀린 서비스품질요소를 반환한 경우 아예 매핑이 안되게
                    "고객경험요소" : obj.element_name,
                    "근거" : obj.reasoning,
                    "설문응답종료년월일" : candidates_vocs[idx]['설문응답종료년월일'],
                    "설문ID" : candidates_vocs[idx]['설문ID'],
                    "설문참여대상자고유ID" : candidates_vocs[idx]['설문참여대상자고유ID'],
                    "문항ID" : candidates_vocs[idx]['문항ID'],
                    "VOC원문" : candidates_vocs[idx]['VOC원문'],
                })
            
        # SV50의 정렬순서 때문에 중복이 생길 수 있음. 그 부분을 제거.
        output = list({frozenset(d.items()): d for d in output}.values())
        elapsed_time = time.time() - init_start
        self.logger.info(f"배치 평균 시간 : {sum(batch_times)/len(batch_times):.2f} sec (배치 사이즈 : {settings.DISCOVER_CXE_BATCH_SIZE})")
        self.logger.info(f"총 실행 시간 : {elapsed_time:.2f} sec (VOC 갯수 : {len(candidates_vocs)})")

        if settings.DISCOVER_CXE_MODE == "TEST":
            n_candidates_voc = len(candidates_vocs)
            n_batch = len(item_batch)
            n_raw_cxe = len(discovered_cxe_list)
            n_final_cxe = len(filtered_result.discovered_elements)
            n_relevant_voc = len(output)
            avg_batch_time = sum(batch_times)/len(batch_times)
            return n_candidates_voc, n_batch, avg_batch_time, elapsed_time, n_raw_cxe, n_final_cxe, n_relevant_voc, output

        return output


    async def merge_discovered_cxe(self, user_id, channel, cx_stage, sq, cxe, discovered_cxe_list):
        # 배치별로 실행된 결과에 중복이 많아 이를 합치기
        messages = format_cxe_merge_messages(
            channel, cx_stage, sq, # 서비스 품질요소
            discovered_cxe_list # 신규 고객경험단계
        )
        merge_messages = [
            SystemMessage(content=messages[0]["content"]),
            HumanMessage(content=messages[1]["content"])
        ]
        headers = {"kb-key": self.AZURE_OPENAI_API_KEY, "x-client-user": add_random_char(user_id)}

        structured_llm = self.llm.with_structured_output(CXMergeResult) 
        result = await structured_llm.ainvoke(merge_messages, extra_headers=headers)
        return result
