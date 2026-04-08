from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse, Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path

from core.logger import get_logger
from core.config import settings
from core.pii_masking import (
    check_pii, 
    mask_pii, 
    mask_md_bytes,
    mask_docx_bytes,
    mask_xlsx_bytes
)
from core.util import (
    convert_input_messages,
    load_resource_file,
    keep_latest_files,
    check_pii_in_chat_history,
    create_azurechatopenai,
    format_discover_cxe_message
)
from core.mcp_util import get_mcp_executor
from api.model import (
    FabrixRequest,
    Option,
    Selectitem,
    HITL,
    Description,
    Textbox,
    ResponseProtocol,
    FileInfo,
    Report,
    FieldType
)
from agent import get_agent

import pandas as pd
import holidays
import requests
import asyncio
import base64
import fcntl
import glob
import json
import pytz
import time
import os
import re

PII_MSG = "⚠️ 보안에 위배되는 개인정보 관련 내용이 포함되어 있어 답변을 드릴 수 없습니다. 보안 위배 내용이 아니라고 생각하시거나 문제가 지속되는 경우 관리자에게 문의해 주세요."
CONTENT_FILTER_MSG = "⚠️ Azure 콘텐츠 필터에 의해 답변이 차단되었습니다. 문제가 지속되는 경우 관리자에게 문의해 주세요."
EXCEED_USAGE_MSG = "🚧 현재 급격한 사용량 증가로 호출이 불가능합니다. 잠시 기다리신 후에 다시 요청해 주세요." 
EXCEED_MAX_TOKEN_MSG = "⛔ 요청하신 내용의 길이가 최대 허용량을 초과하였습니다. 새 채팅방을 개설하여 이용해 주세요."
BASE_ERROR_MSG = "🚨 에이전트 실행 중 오류가 발생했습니다. 문제가 지속되는 경우 관리자에게 문의해 주세요."
NON_ADMIN_USER_MSG = "해당 기능은 현재 준비 중입니다. 빠른 시일 내 제공될 예정이니 양해 부탁드립니다."
NON_VALID_USER_MSG = "🔒 운영 정책에 따라 해당 기능은 권한이 부여된 사용자에 한해 제공됩니다."

logger = get_logger("main")

ENV = os.getenv("ENV_PATH")
logger.info(f"ENV: {ENV}")

headers = {"kb-key": settings.AZURE_OPENAI_API_KEY}
mcp_executor = None
agent = None
sql_agent = None
report_agent = None
kst = pytz.timezone("Asia/Seoul")

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logger.info("Get mcp excutor...")
    global mcp_executor
    mcp_executor = await get_mcp_executor()
    logger.info("Get agents...")
    global agent, sql_agent, report_agent
    agent = get_agent("report", mcp_executor)
    sql_agent = get_agent("sql", mcp_executor)
    report_agent = get_agent("doc_report", mcp_executor)

    if ENV == "serving":
        logger.info("Discover CXE...")
        asyncio.create_task(discover_cxe())
        logger.info("Generate TD, BU cache...")
        asyncio.create_task(cache_messages())
        logger.info("Request report file...")
        asyncio.create_task(request_reports())
        scheduler = AsyncIOScheduler()
        scheduler.add_job(discover_cxe, CronTrigger(hour=7, minute=0, timezone="Asia/Seoul"))
        scheduler.add_job(cache_messages, CronTrigger(hour=13, minute=30, timezone="Asia/Seoul"))
        scheduler.add_job(request_reports, CronTrigger(hour=14, minute=30, timezone="Asia/Seoul"))
        scheduler.start()

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down CX Agent...")

# === 전역 예외 핸들러 === 
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

async def file_send(data):
    data = ResponseProtocol(event="CHUNK", content=data)
    yield f"{json.dumps(data.model_dump())}\n\n"
    
async def send(data, chat=False):
    if chat:
        chat_text = data.get("content", "")
        event = data.get("event", "CHUNK")
        recommend_queries = data.get("recommend_queries")
        for i in range(0, len(chat_text), 2):
            data = ResponseProtocol(event=event, content=chat_text[i:i+2])
            yield f"data: {json.dumps(data.model_dump())}\n\n"
        if recommend_queries:
            rq_output = {"event": event, "recommend_queries": recommend_queries}
            yield f"data: {json.dumps(rq_output)}\n\n"
    else:
        if isinstance(data, dict):
            yield f"data: {json.dumps(data)}\n\n"
        # elif isinstance(data, Report):
        #     json_data = data.model_dump()
        #     content = json_data.pop("content", "")
        #     json_data.pop("type")
        #     json_data["content"] = "123123"
        #     report_mode = ResponseProtocol(event="CHUNK", content=f"report: {json.dumps(json_data)}")
        #     yield f"data: {json.dumps(report_mode.model_dump())}\n\n"
        #     for c in range(len(content), 10):
        #         data = ResponseProtocol(event="CHUNK", content=c)
        #         yield f"data: {json.dumps(data.model_dump())}\n\n"
        elif isinstance(data, list):
            for i in data:
                data = ResponseProtocol(event="CHUNK", content=i)
                yield f"data: {json.dumps(data.model_dump())}\n\n"
        else:
            data = ResponseProtocol(event="CHUNK", content=data)
            yield f"data: {json.dumps(data.model_dump())}\n\n"
        
async def SQL_executor(
    mcp_executor,
    SQL: str,
    db_name: str
) -> pd.DataFrame:
    """
    MCP executor로 SQL 실행 -> DataFrame 반환

    Args:
        mcp_executor  : MCPToolExecutor
        SQL     (str) : 실행할 SQL
        db_name (str) : SQL을 실행할 대상 DataBase 식별자

    Returns:
        pd.DataFrame : SQL 실행 결과
    """
    res = await mcp_executor.execute_tool(db_name, {'query': SQL})
    extracted = [row for row in res]
    df = pd.DataFrame(extracted)
    return df

async def make_td_nps_report_date_params(mcp_executor) -> dict:
    """
    Top-down 보고서에 필요한 날짜를 계산하여 반환
    테이블에 적재된 데이터 기준 가장 최신 조사년도와 반기구분 값 활용

    Args:
        mcp_executor: MCPToolExecutor

    Returns:
        dict:
            yyyy        (str) : 최근 TD 조사년도
            yyyyhf      (str) : 최근 TD 조사반기
            yyyy_b1hf   (str) : 전반기 TD 조사년도
            yyyyhf_b1hf (str) : 전반기 TD 조사반기
    """

    SQL = """
    SELECT 조사년도, 반기구분명
    FROM inst1.TSCCVMGC1
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2 DESC
    LIMIT 1
    """
    last_date   = await SQL_executor(mcp_executor, SQL, 'mysql_query')
    yyyy        = str(last_date['조사년도'].values[0])
    yyyyhf      = last_date['반기구분명'].values[0]
    yyyy_b1hf   = str(int(yyyy)-1) if yyyyhf == '상반기' else yyyy
    yyyyhf_b1hf = '하반기' if yyyyhf == '상반기' else '상반기'

    date_params = {
        'yyyy'       : yyyy,
        'yyyyhf'     : yyyyhf,
        'yyyy_b1hf'  : yyyy_b1hf,
        'yyyyhf_b1hf': yyyyhf_b1hf,
    }

    return date_params

# === 메인 api 엔드포인트 ===
@app.post("/run_agent")
async def run_agent(request: FabrixRequest):
    try:
        request = json.loads(request.input_value)
        logger.info("요청 도착!")
        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        only_file_output = False # 파일 출력할때를 위해 필요 (없으면 finally 에러남)

        # 파일 정보 요청 처리
        if request.get("file_info"):
            file_info_request = await get_file_info(request.get("file_info"))
            if file_info_request:
                file_info_json = [file.model_dump_json() for file in file_info_request]
                file_info_content = json.dumps(file_info_json, indent=4)
                only_file_output = True
                return StreamingResponse(file_send(file_info_content), media_type="text/event-stream")

        # 배치 에이전트로부터 보고서 당겨오기
        if request.get("request_reports"):
            await request_reports()
            logger.info("모든 보고서 수신이 완료되었습니다.")
            return

        filtered_body = request.get("filtered_body", {})
        stream = filtered_body.get("stream", False)
        messages = convert_input_messages(filtered_body.get("messages", []))
        if messages == []:
            logger.error(f"messages가 없습니다. {request}")
            return
        user = filtered_body.get("user", {})
        user_id = user.get("x-client-user", settings.MCP_USER_ID)
        logger.info(f"사번: {user_id}")
        
        # 커스텀 AOAI 객체 선언
        llm = create_azurechatopenai(user_id=user_id)

        # MCP user_id 할당
        contextvar_token = mcp_executor._cur_user_id.set(user_id)

        # PII 필터
        if check_pii_in_chat_history(filtered_body.get("messages", [])):
            logger.info(f"[run_agent] 개인정보 차단: {request}")
            return StreamingResponse(send({"event": "CHUNK", "content": PII_MSG}), media_type="text/event-stream")

        agent.llm = llm
        agent._build_graph()

        # 권한 확인
        valid_user = user_id in settings.VALID_USER_IDS
        admin_user = user_id in settings.ADMIN_USER_IDS
        # 보고서 작성 도구
        check_report = await handle_report_creation(messages)
        if check_report is not None:
            if admin_user or valid_user:
                logger.info("보고서 작성 도구!")
                return StreamingResponse(send(check_report), media_type="text/event-stream")
            else:
                # 권한 없을 시 메시지 발송
                return StreamingResponse(send({"event": "CHUNK", "content": NON_VALID_USER_MSG}), media_type="text/event-stream")

         # 신규 고객경험요소 제안 도구
        discover_cxe = await handle_discover_cxe(messages, user_id)
        if discover_cxe is not None:
            if admin_user or valid_user:
                logger.info("신규 고객경험요소 제안 도구!")
                data = ResponseProtocol(event="CHUNK", content=discover_cxe)
                return StreamingResponse(send(data.model_dump()), media_type="text/event-stream")
            else:
                # 권한 없을 시 메시지 발송
                return StreamingResponse(send({"event": "CHUNK", "content": NON_VALID_USER_MSG}), media_type="text/event-stream")
        # 고객경험 CASE 조사 도구
        case_investigation = await handle_case_investigation(messages, user_id)
        if case_investigation is not None:
            logger.info("고객경험 CASE 조사!")
            data = ResponseProtocol(event="CHUNK", content=case_investigation)
            return StreamingResponse(send(data.model_dump()), media_type="text/event-stream")
        # VOC 원문 분석 도구
        voc_analysis = await handle_voc_analysis(messages, user_id)
        if voc_analysis is not None:
            logger.info("VOC 원문 분석 도구!")
            return StreamingResponse(send(voc_analysis), media_type="text/event-stream")
       
        result = await agent.execute(user_id, messages)
        data = {"event": "CHUNK", "content": result}
        if isinstance(result, str):
            data["recommend_queries"] = await sql_agent.generate_recommend_question(result, user_id)
        return StreamingResponse(send(data, chat=True), media_type="text/event-stream")

    except Exception as e:
        logger.error(f"[run_agent] 기타 오류 발생, {e}")
        if "rate_limit" in str(e):
            data = {"event": "CHUNK", "content": EXCEED_USAGE_MSG}
        elif "length exceeded" in str(e):
            data = {"event": "CHUNK", "content": EXCEED_MAX_TOKEN_MSG}
        elif "content filter" in str(e):
            data = {"event": "CHUNK", "content": CONTENT_FILTER_MSG}
        else:
            data = {"event": "CHUNK", "content": BASE_ERROR_MSG}
        return StreamingResponse(send(data), media_type="text/event-stream")
    finally:
        if not only_file_output:
            mcp_executor._cur_user_id.reset(contextvar_token)

#################################################
# ============= 배치 스케줄링 함수 ==============
#################################################

# 주요 질문 캐싱
async def cache_messages():
    lock_file_path = f"./cache_messages.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        holiday = holidays.KR(years=today_date.year)

        if today_date.weekday() < 5 and today_date not in holiday:
            date_params = await make_td_nps_report_date_params(mcp_executor)
            year = date_params["yyyy"]
            half = date_params["yyyyhf"]
            td_query = f"{year}년도 {half} TD NPS 타행포함 결과 정리해줘. 은행NPS는 가장 상위 레벨이므로 분리해서 분석해주고, 그 아래 다른 채널별 NPS를 보여줘. 표에는 KB의 NPS, KB의 추천비율, KB의 비추천비율, 직상위 기관의 NPS, 1위 기관의 NPS를 보여줘."
            bu_query = "최근 BU NPS 모든 채널 결과 정리해줘."
            td_result, bu_result = await asyncio.gather(agent.execute(settings.MCP_USER_ID, [("user", td_query)]), agent.execute(settings.MCP_USER_ID, [("user", bu_query)]))
            agent.latest_td_nps_cache = td_result
            agent.latest_bu_nps_cache = bu_result
            with open(settings.INSIGHT_CACHE_OUTPUT_PATH + "output.json", "w") as f:
                json.dump({"td_result": td_result, "bu_result": bu_result}, f, indent=4, ensure_ascii=False)
    except IOError:
        time.sleep(100)
        print('파일 불러오기!')
        with open(settings.INSIGHT_CACHE_OUTPUT_PATH + "output.json", "r") as f:
            cache_json = json.load(f)
        agent.latest_td_nps_cache = cache_json.get("td_result", "")
        agent.latest_bu_nps_cache = cache_json.get("bu_result", "")
        return
    # except Exception as e:
    #     logger.error(f"주요 질문 캐싱 중 에러 발생: {e}")
    finally:
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()

# 신규 고객경험요소 발굴 (격주)
async def discover_cxe():
    lock_file_path = f"./discover_cxe.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        holiday = holidays.KR(years=today_date.year)

        if today_date.weekday() < 5 and today_date not in holiday:
            # 날짜 및 데이터 확인
            has_batch_res = False
            now = datetime.now(kst)
            today_str = now.strftime("%Y%m%d")
            batch_record_path = f"{settings.DISCOVER_CXE_BATCH_DIR}/batch_record.json"
            if os.path.exists(batch_record_path):
                with open(batch_record_path, "r") as f:
                    batch_record = json.load(f)
                latest = batch_record['latest_run_datetime']
                latest_date = kst.localize(datetime.strptime(latest, "%Y%m%d"))
                if now - latest_date < timedelta(days=settings.DISCOVER_CXE_BATCH_CYCLE):
                    logger.info(f"최근({latest})의 신규 고객경험요소 추출 데이터 존재")
                    has_batch_res = True
                else:
                    logger.info(f"최근({latest})의 신규 고객경험요소 추출 데이터 업데이트 예정 (주기 : {settings.DISCOVER_CXE_BATCH_CYCLE}일)")
            else:
                logger.info(f"현재 날짜 : ({today_str}) 고객경험요소 추출 결과가 없습니다.")

            # 설문 체계 추출
            if not has_batch_res:
                logger.info(f"신규 고객경험요소 추출 시작")
                survey_structure_query = """SELECT
                    N1.인스턴스내용 AS 채널
                    , N2.인스턴스내용 AS 고객경험단계
                    FROM (
                        SELECT DISTINCT 설문조사대상구분, 고객경험단계구분
                        FROM INST1.TSCCVCI18
                        WHERE 설문조사방식구분='02' AND 설문조사종류구분='03'
                    ) A
                    LEFT JOIN INST1.TSCCVCI04 N1 ON N1.그룹회사코드="KB0" AND A.설문조사대상구분=N1.인스턴스코드 AND N1.인스턴스식별자='142447000'
                    LEFT JOIN INST1.TSCCVCI04 N2 ON N2.그룹회사코드="KB0" AND A.고객경험단계구분=N2.인스턴스코드 AND N2.인스턴스식별자='142594000'; """
                survey_structure = await mcp_executor.execute_tool("mysql_query", {"query": survey_structure_query})

                # 고객경험단계별 고객경험요소 추출 후 데이터 쌓기
                paths = []
                batch_path = settings.DISCOVER_CXE_BATCH_DIR
                for item in survey_structure:
                    chan = item['채널']
                    cx_stage = item['고객경험단계']
                    start_date = settings.DISCOVER_CXE_START_DATE
                    end_date = today_str
                    _cx_stage = cx_stage.replace("/"," ")
                    filepath = f"{settings.DISCOVER_CXE_BATCH_DIR}/{chan}_{_cx_stage}.csv"
                    paths.append(filepath)
                    discovered_cxe = await sql_agent.discover_cx_elements(settings.MCP_USER_ID, chan, cx_stage, start_date, end_date)

                    if discovered_cxe:
                        df = pd.DataFrame(discovered_cxe)
                        os.makedirs(batch_path, exist_ok=True)
                        df.to_csv(filepath, index=False, encoding='utf-8-sig')

                    # RateLimit 방지용
                    await asyncio.sleep(5)

                if any(os.path.exists(path) for path in paths):
                    with open(batch_record_path, "w") as f:
                        json.dump({"latest_run_datetime" : today_str}, f)

                logger.info(f"신규 고객경험요소 추출 종료")

    except Exception as e:
        logger.error(f"신규 고객경험요소 발굴 중 에러 발생: {e}")
    finally:
        if "lock_file" in locals() and lock_file:
            keep_latest_files(settings.DISCOVER_CXE_BATCH_DIR, keep=4, is_dir=True)
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()

# 정기 보고서 파일 요청
async def request_reports():
    lock_file_path = f"./request_reports.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        holiday = holidays.KR(years=today_date.year)
        if today_date.weekday() < 5 and today_date not in holiday:
            for report_type in ["td_nps_report", "bu_nps_weekly_report", "bu_nps_monthly_report", "region_group_nps_biweekly_report"]:
                headers = {
                    "Content-Type": "application/json",
                    "x-openapi-token": settings.BATCH_AGENT_OPENAPI_TOKEN,
                    "x-generative-ai-client": settings.BATCH_AGENT_GENERATIVE_CLIENT,
                }
                payload = {
                    "isStream": False,
                    "agentId": settings.BATCH_AGENT_ID,
                    "contents": [json.dumps({"requestType": "report", "reportType": report_type})]
                }
                res = requests.post(settings.BATCH_AGENT_URL, headers=headers, json=payload, verify=False)
                files_dict = json.loads(res.json()["content"])
                old_files = glob.glob(settings.REPORT_OUTPUT_PATH + f"*{report_type}*")
                cur_files = []
                for f_name, cont in files_dict.items():
                    file_bytes = base64.b64decode(cont.encode("utf-8"))
                    with open(f_name, "wb") as f:
                        f.write(file_bytes)
                    cur_files.append(f_name)
                for old_f_name in old_files:
                    if old_f_name not in cur_files:
                        os.remove(old_f_name)
    except Exception as e:
        logger.error(f"정기 보고서 파일 요청 중 에러 발생: {e}")
    finally:
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()
#################################################
# ================ 업무 도구 ====================
#################################################

async def get_hitl_instance(data: dict):
    """
    HITL 인스턴스를 생성합니다.
    Args:
    - data (dict): HITL 데이터
    Returns:
    - HITL: HITL 인스턴스
    """
    hitl_instance = None
    if data["type"] == "select_item":
        options = [
            Option(
                label=option["label"], description=Description(**option["description"])
            )
            for option in data["options"]
        ]
        hitl_instance = Selectitem(
            options=options, **{k: v for k, v in data.items() if k != "options"}
        )
    elif data["type"] == "textbox":
        hitl_instance = Textbox(**{k: v for k, v in data.items()})

    return HITL(content=hitl_instance)

async def handle_report_creation(messages):
    """보고서 작성 요청 처리"""
    if messages[-1] == ("user", "보고서 작성"):
        item = {
            "type": "select_item",
            "label": "작성할 보고서 유형을 선택해 주세요.",
            "description": "작성할 보고서 유형을 선택해 주세요.",
            "sensitive": False,
            "options": [
                {
                    "label": "Top-down NPS 보고서",
                    "description": {
                        "type": "markdown",
                        "content": "반기별 진행되는 Top-down 설문조사 분석 보고서",
                    },
                },
                {
                    "label": "Bottom-up NPS 주간 보고서",
                    "description": {
                        "type": "markdown",
                        "content": "가장 최신일자까지 진행된 Bottom-up 설문조사 분석 보고서",
                    },
                },
                {
                    "label": "Bottom-up NPS 월간 보고서",
                    "description": {
                        "type": "markdown",
                        "content": "가장 최신일자까지 진행된 Bottom-up 설문조사 분석 보고서",
                    },
                },
                {
                    "label": "영업추진그룹별 NPS 보고서",
                    "description": {
                        "type": "markdown",
                        "content": "영업추진그룹별 대면채널 NPS 격주 보고서",
                    },
                }
            ],
        }
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-3:] == [
        ('user', '보고서 작성'),
        ('assistant', '작성할 보고서 유형을 선택해 주세요.'),
        ('user', '영업추진그룹별 NPS 보고서')
    ]:
        item = {
            "type": "select_item", 
            "label": "대상 지역을 선택해 주세요.",
            "sensitive": False,
            "options": []
        }
        for ch in ["강남", "강북", "수도권", "영남", "충청호남"]:
            item["options"].append(
                {
                    "label": f"{ch}",
                    "description": {
                        "type": "markdown",
                        "content": f"{ch}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-3:] == [
        ('user', '보고서 작성'),
        ('assistant', '작성할 보고서 유형을 선택해 주세요.'),
        ('user', 'Top-down NPS 보고서')
    ]:
        item = {
            "type": "select_item", 
            "label": "대상 채널을 선택해 주세요.",
            "sensitive": False,
            "options": []
        }
        for ch in ["은행", "브랜드", "플랫폼", "대면채널", "고객센터", "상품", "전체"]:
            item["options"].append(
                {
                    "label": f"{ch}",
                    "description": {
                        "type": "markdown",
                        "content": f"{ch}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-5:-1] == [
        ('user', '보고서 작성'),
        ('assistant', '작성할 보고서 유형을 선택해 주세요.'),
        ('user', '영업추진그룹별 NPS 보고서'),
        ('assistant', '대상 지역을 선택해 주세요.')
    ]:
        group_name_list = ["강남", "강북", "수도권", "영남", "충청호남"]
        group_name = messages[-1][1]
        if group_name not in group_name_list:
            return f"**'{group_name}'은 없는 지역입니다. 주어진 지역 목록에서 선택해 주세요.**"
        files = glob.glob(settings.REPORT_OUTPUT_PATH + f"region_group_nps_biweekly_report*{group_name}*.xlsx")
        if len(files) != 1:
            return "**보고서 파일이 없습니다. 관리자에게 문의해 주세요**"

        report_file_name = files[0].split("/")[-1]
        fff = FileInfo(data='dummy', file_name=report_file_name)
        file_info_json = [fff.model_dump_json()]
        return f"file: {json.dumps(file_info_json, indent=4)}"

    report_types = {"Bottom-up NPS 주간 보고서": "bu_nps_weekly_report", "Bottom-up NPS 월간 보고서": "bu_nps_monthly_report"}
    if messages[-3:-1] == [
        ('user', '보고서 작성'),
        ('assistant', '작성할 보고서 유형을 선택해 주세요.'),
    ] and report_types.get(messages[-1][1]):
        files = glob.glob(settings.REPORT_OUTPUT_PATH + f"{report_types.get(messages[-1][1])}_*.xlsx")
        if len(files) != 1:
            return "**보고서 파일이 없습니다. 관리자에게 문의해 주세요**"

        report_file_name = files[0].split("/")[-1]
        fff = FileInfo(data='dummy', file_name=report_file_name)
        file_info_json = [fff.model_dump_json()]
        return f"file: {json.dumps(file_info_json, indent=4)}"
    
    if messages[-5:-1] == [
        ('user', '보고서 작성'),
        ('assistant', '작성할 보고서 유형을 선택해 주세요.'),
        ('user', 'Top-down NPS 보고서'),
        ('assistant', '대상 채널을 선택해 주세요.'),
    ]:
        channel_name_dict = {"전체": "_all", "은행": "_bank", "브랜드": "_brand", "플랫폼": "_platform", "대면채널": "_face", "고객센터": "_center", "상품": "_product"}
        channel_name = channel_name_dict.get(messages[-1][1])
        if not channel_name:
            return "**올바른 채널이 아닙니다. 주어진 채널 목록에서 선택해 주세요.**"
        files = glob.glob(settings.REPORT_OUTPUT_PATH + f"td_nps_report_*{channel_name}.*")
        if len(files) != 2:
            return "**보고서 파일이 없습니다. 관리자에게 문의해 주세요.**"
        docx_file_name = ""
        for f in files: 
            if f[-3:] == ".md":
                md_file_name = f.split("/")[-1]
                _md_file = load_resource_file(f, "rb")
                md_file, check_pii = mask_md_bytes(_md_file)
                if check_pii:
                    logger.info(f"[run_agent] 개인정보 검출: {md_file_name}")
            elif f[-5:] == ".docx":
                docx_file_name = f.split("/")[-1]
                _docx_file = load_resource_file(f, "rb")
                docx_file, check_pii = mask_docx_bytes(_docx_file)
                if check_pii:
                    logger.info(f"[run_agent] 개인정보 검출: {docx_file_name}")
                docx_file = load_resource_file(f, "rb")
        if channel_name == "_all":
            encoded_docx = base64.b64encode(docx_file).decode("utf-8")
            file_instance = FileInfo(data=encoded_docx, file_name=docx_file_name)
            file_info_json = [file_instance.model_dump_json()]
            return [f"file: {json.dumps(file_info_json, indent=4)}"]
        else:
            report_instance = Report(type=FieldType("report"), title=f"Top-down NPS 보고서 ({messages[-1][1]})", content=md_file)
            return [f"report: {report_instance.model_dump_json()}"]

async def handle_case_investigation(messages:list, user_id:str):
    """고객경험 CASE 조사 처리"""
    if messages[-1] == ("user", "고객경험 CASE 조사"):
        textbox = {
            "type": "textbox",
            "label": "조사명 관련 키워드를 입력해 주세요.",
            "description": "조사명 관련 키워드를 입력해 주세요.",
            "sensitive": False,
            "placeholder": "입력란",
            "pattern": "",
        }
        data = await get_hitl_instance(textbox)
        return f"hitl: {data.model_dump_json()}"

    if messages[-3:-1] == [
        ('user', '고객경험 CASE 조사'),
        ('assistant', '조사명 관련 키워드를 입력해 주세요.')
    ]:
        survey_title = messages[-1][1]
        query = f"select 설문ID, 설문제목명, 설문목적상세내용, 설문응답시작일시, 설문응답종료일시 from inst1.TSCCVMGF5 where LOWER(REPLACE(설문제목명, ' ', '')) LIKE CONCAT('%', LOWER(REPLACE('{survey_title}', ' ', '')), '%') order by 설문응답종료일시 desc"

        survey_title_list = await mcp_executor.execute_tool("mysql_query", {"query": query}, emp_no=user_id)
        count = len(survey_title_list)
        if count > 5:
            error_message = "**일치하는 설문이 5개를 초과합니다. '고객경험 CASE 조사' 버튼을 다시 누르고, 키워드를 조금 더 구체적으로 입력해 주세요.**"
            return error_message            
        elif 0 < count <= 5:
            item = {
                "type": "select_item", 
                "label": "분석할 조사명을 선택해 주세요.",
                "sensitive": False,
                "options": []
            }
            for i in range(count):
                item["options"].append(
                    {
                        "label": f"{datetime.fromisoformat(survey_title_list[i]['설문응답시작일시']).strftime('%y/%m/%d')}~{datetime.fromisoformat(survey_title_list[i]['설문응답종료일시']).strftime('%y/%m/%d')} {survey_title_list[i]['설문제목명']} ({survey_title_list[i]['설문ID']})",
                        "description": {
                            "type": "markdown",
                            "content": f"{survey_title_list[i]['설문목적상세내용']} (설문기간: {datetime.fromisoformat(survey_title_list[i]['설문응답시작일시']).strftime('%y/%m/%d')}~{datetime.fromisoformat(survey_title_list[i]['설문응답종료일시']).strftime('%y/%m/%d')})",
                        },
                    }
                )
            data = await get_hitl_instance(item)
            return f"hitl: {data.model_dump_json()}"
        else:
            error_message = "**입력된 값과 매핑된 영업그룹이 없습니다. 다시 보고서 작성 버튼을 눌러 진행해 주세요.**"
            return error_message
    
    if messages[-5:-3] == [
        ('user', '고객경험 CASE 조사'),
        ('assistant', '조사명 관련 키워드를 입력해 주세요.')
    ] and messages[-2] == ('assistant', '분석할 조사명을 선택해 주세요.'):
        survey_id = messages[-1][1].split("(")[-1][:-1]
        report_title, report_content = await sql_agent.case_analysis(survey_id, user_id)
        report_instance = Report(
                type=FieldType("report"), title=report_title, content=report_content
            )
        return f"report: {report_instance.model_dump_json()}"

async def get_file_info(file_list: List) -> Optional[List[FileInfo]]:
    """
    파일 정보를 가져옵니다.

    Args:
    - file_list (List): 파일 이름 또는 FileInfo 객체의 리스트

    Returns:
    - List[FileInfo]: 파일 정보 또는 None 의 리스트
    """
    file_info_list = []
    check_pii = False

    for file in file_list:
        if isinstance(file, dict):
            # 파일 정보가 dictionary로 주어진 경우
            file_id = file.get("id", "")
            file_name = file.get("file_name", "")
            file_data = file.get("data", "")
            logger.info(f"파이프라인에서 딕셔너리로 파일 요청 : {file_name}")

            if file_name.endswith(".md"):
                masked_file_data, check_pii = mask_md_bytes(file_data)
            elif file_name.endswith(".docx"):
                masked_file_data, check_pii = mask_docx_bytes(file_data)
            elif file_name.endswith(".xlsx"):
                masked_file_data, check_pii = mask_xlsx_bytes(file_data)
            else:
                raise ValueError(f"개인정보 검사를 위해서는 .md, .docx, .xlsx 파일이어야 합니다. 파일이름 : {file_name}")
            
            if check_pii:
                logger.info(f"[run_agent] 개인정보 검출: {file_name}")

            file_info_list.append(
                FileInfo(id=file_id, data=masked_file_data, file_name=file_name)
            )
        elif isinstance(file, str):
            # 파일 정보가 FileInfo 객체로 주어진 경우
            file = json.loads(file)
            file_id = file.get("id", "")
            file_name = file.get("file_name", "")
            logger.info(f"파이프라인에서 JSON으로 파일 요청 : {file_name}")
            ext = os.path.splitext(file_name)[1]
            file_path = os.path.join(settings.REPORT_OUTPUT_PATH, file_name)
            if not os.path.exists(file_path):
                logger.error(f"파일이 존재하지 않습니다: {file_path}")
                return None
            try:
                with open(file_path, "rb") as f:
                    file_content = f.read()

                if file_name.endswith(".md"):
                    masked_file_content, check_pii = mask_md_bytes(file_content)
                elif file_name.endswith(".docx"):
                    masked_file_content, check_pii = mask_docx_bytes(file_content)
                elif file_name.endswith(".xlsx"):
                    masked_file_content, check_pii = mask_xlsx_bytes(file_content)
                else:
                    raise ValueError(f"개인정보 검사를 위해서는 .md, .docx, .xlsx 파일이어야 합니다. 파일이름 : {file_name}")
                
                if check_pii:
                    logger.info(f"[run_agent] 개인정보 검출: {file_name}")

                file_data = base64.b64encode(masked_file_content).decode("utf-8")
                file_info_list.append(
                    FileInfo(id=file_id, data=file_data, file_name=file_name)
                )
            except Exception as e:
                logger.error(f"파일 처리 중 오류 발생: {e}")
                return None
        else:
            logger.error(f"지원하지 않는 파일 형식: {type(file)}")
            return None

    return file_info_list

async def handle_voc_analysis(messages, user_id):
    """VOC 원문 분석 처리"""
    if messages[-1] == ("user", "최근 VOC 데이터 분석해줘."):
        item = {
            "type": "select_item", 
            "label": "분석하고 싶은 설문방식을 선택하세요.",
            "sensitive": False,
            "options": []
        }
        for typ in ["Top-Down (TD)", "Bottom-Up (BU)"]:
            item["options"].append(
                {
                    "label": f"{typ}",
                    "description": {
                        "type": "markdown",
                        "content": f"{typ}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-3:-1] == [
        ('user', '최근 VOC 데이터 분석해줘.'),
        ('assistant', '분석하고 싶은 설문방식을 선택하세요.'),
    ]:
        item = {
            "type": "select_item", 
            "label": "VOC를 분석할 채널을 선택해 주세요.",
            "sensitive": False,
            "options": []
        }
        if messages[-1][1] == "Top-Down (TD)":
            ch_list = ["브랜드", "플랫폼", "대면채널", "고객센터", "상품", "키워드 검색"]
        elif messages[-1][1] == "Bottom-Up (BU)":
            ch_list = ["KB 스타뱅킹", "영업점", "고객센터", "상품", "키워드 검색"]
        else:
            return "유효하지 않은 값입니다. (TD, BU 중 선택)"
        for ch in ch_list:
            item["options"].append(
                {
                    "label": f"{ch}",
                    "description": {
                        "type": "markdown",
                        "content": f"{ch}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"
    
    if messages[-5:-3] == [
        ('user', '최근 VOC 데이터 분석해줘.'),
        ('assistant', '분석하고 싶은 설문방식을 선택하세요.'),
    ] and messages[-2:] == [
        ('assistant', 'VOC를 분석할 채널을 선택해 주세요.'),
        ('user', '키워드 검색'),
    ]:
        item = {
            "type": "textbox",
            "label": "VOC를 검색할 키워드를 입력해 주세요.",
            "description": "VOC를 검색할 키워드를 입력해 주세요.",
            "sensitive": False,
            "placeholder": "입력란",
            "pattern": "[ㄱ-ㅎ가-힣a-zA-Z0-9\s]+",
        }
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-5:-3] == [
        ('user', '최근 VOC 데이터 분석해줘.'),
        ('assistant', '분석하고 싶은 설문방식을 선택하세요.'),
    ] and messages[-2] == ('assistant', 'VOC를 분석할 채널을 선택해 주세요.'):
        survey_type = messages[-3][1]
        channel_name = messages[-1][1]
        if channel_name not in ["브랜드", "플랫폼", "대면채널", "고객센터", "상품", "KB 스타뱅킹", "영업점"]:
            return "유효하지 않은 채널입니다. 다시 입력해 주세요. (TD-[브랜드, 플랫폼, 대면채널, 고객센터, 상품] BU-[KB 스타뱅킹, 영업점, 대면채널, 상품])"
        voc_analysis = await sql_agent.voc_analysis(survey_type, user_id=user_id, channel_name=channel_name)
        report_instance = Report(
                type=FieldType("report"), title=f"{survey_type} {channel_name} 채널 VOC 분석", content=voc_analysis
            )
        return f"report: {report_instance.model_dump_json()}"

    if messages[-7:-5] == [
        ('user', '최근 VOC 데이터 분석해줘.'),
        ('assistant', '분석하고 싶은 설문방식을 선택하세요.'),
    ] and messages[-4:-1] == [
        ('assistant', 'VOC를 분석할 채널을 선택해 주세요.'),
        ('user', '키워드 검색'),
        ('assistant', 'VOC를 검색할 키워드를 입력해 주세요.'),
    ]:
        survey_type = messages[-5][1]
        keyword = messages[-1][1]
        voc_analysis = await sql_agent.voc_analysis(survey_type, user_id=user_id, keyword=keyword)
        report_instance = Report(
                type=FieldType("report"), title=f"{survey_type} {keyword} 키워드 VOC 분석", content=voc_analysis
            )
        return f"report: {report_instance.model_dump_json()}"




async def handle_discover_cxe(messages, user_id):
    """신규 고객경험요소 제안 도구"""
    if messages[-1] == ("user", "신규 고객경험요소 제안"):
        item = {
            "type": "select_item", 
            "label": "신규 고객경험요소 후보를 조회할 채널을 선택하세요(BU)",
            "sensitive": False,
            "options": []
        }
        for typ in ["KB 스타뱅킹", "영업점", "고객센터", "상품"]:
            item["options"].append(
                {
                    "label": f"{typ}",
                    "description": {
                        "type": "markdown",
                        "content": f"{typ}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-3:-1] == [
        ('user', '신규 고객경험요소 제안'),
        ('assistant', '신규 고객경험요소 후보를 조회할 채널을 선택하세요(BU)'),
    ]:
        channel_name = messages[-1][1]
        cxs_query = f"""SELECT DISTINCT N2.인스턴스내용 AS 고객경험단계
            FROM (
                SELECT DISTINCT 설문조사대상구분, 고객경험단계구분
                FROM INST1.TSCCVCI18
                WHERE 설문조사방식구분='02' AND 설문조사종류구분='03'
            ) A
            LEFT JOIN INST1.TSCCVCI04 N1 ON N1.그룹회사코드="KB0" AND A.설문조사대상구분=N1.인스턴스코드 AND N1.인스턴스식별자='142447000'
            LEFT JOIN INST1.TSCCVCI04 N2 ON N2.그룹회사코드="KB0" AND A.고객경험단계구분=N2.인스턴스코드 AND N2.인스턴스식별자='142594000'
            WHERE N1.인스턴스내용 = '{channel_name}'; """
        _cx_stage = await mcp_executor.execute_tool("mysql_query", {"query": cxs_query})
        cx_stage = [x["고객경험단계"] for x in _cx_stage]

        item = {
            "type": "select_item", 
            "label": "신규 고객경험요소 후보를 조회할 고객경험단계를 선택하세요(BU)",
            "sensitive": False,
            "options": []
        }
        for typ in cx_stage:
            item["options"].append(
                {
                    "label": f"{typ}",
                    "description": {
                        "type": "markdown",
                        "content": f"{typ}",
                    },
                }
            )
        data = await get_hitl_instance(item)
        return f"hitl: {data.model_dump_json()}"

    if messages[-5:-3] == [ 
        ('user', '신규 고객경험요소 제안'),
        ('assistant', '신규 고객경험요소 후보를 조회할 채널을 선택하세요(BU)'),
    ] and messages[-2] == ('assistant', '신규 고객경험요소 후보를 조회할 고객경험단계를 선택하세요(BU)'):
        channel_name = messages[-3][1]
        cx_stage_name = messages[-1][1]
        batch_path = settings.DISCOVER_CXE_BATCH_DIR

        if batch_path:
            # 가장 최근 날짜 데이터가 있는 경우만 바로 추출해서 파일로 사용
            files = []
            for filepath in os.listdir(batch_path):
                if filepath.endswith(".csv"):
                    df = pd.read_csv(os.path.join(batch_path, filepath))
                    files.append(df)
            total_df = pd.concat(files)
            start_date = settings.DISCOVER_CXE_START_DATE
            batch_record_path = f"{settings.DISCOVER_CXE_BATCH_DIR}/batch_record.json"
            if os.path.exists(batch_record_path):
                with open(batch_record_path, "r") as f:
                    batch_record = json.load(f)
                    end_date = batch_record['latest_run_datetime']
                    now = datetime.now(kst)
                    latest_date = kst.localize(datetime.strptime(end_date, "%Y%m%d"))
                    if now - latest_date >= timedelta(days=settings.DISCOVER_CXE_BATCH_CYCLE):
                        logger.warning(f"업데이트 주기 {settings.DISCOVER_CXE_BATCH_CYCLE}알에 따라 업데이트 되지 않았습니다. 최종 업데이트 날짜 : {latest_date}")
                    else:
                        logger.info(f"업데이트 주기 {settings.DISCOVER_CXE_BATCH_CYCLE}에 맞는 최신 데이터입니다. 최종 업데이트 날짜 : {latest_date}")
            else:
                raise ValueError("추출된 고객경험요소 파일이 없습니다")
            report_content = format_discover_cxe_message(channel_name, cx_stage_name, start_date, end_date, total_df)
            title = f"'{channel_name}' 채널 '{cx_stage_name}' 단계 신규 고객경험요소 제안"
            report_instance = Report(
                    type=FieldType("report"), title=title, content=report_content
                )
            return f"report: {report_instance.model_dump_json()}"

        return "신규 고객경험요소 제안 정보가 없습니다."
    