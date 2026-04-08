from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse, Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path

from core.logger import get_logger
from core.config import settings
from core.util import (
    convert_input_messages,
    load_resource_file,
    keep_latest_files,
    create_azurechatopenai
)
from core.mcp_util import get_mcp_executor
from core.pii_masking import mask_pii
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
from agent.report_generation.reports.report_types.utils import make_td_nps_report_date_params
from agent import get_agent

import asyncio
import pandas as pd
import holidays
import asyncio
import sqlite3
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
REQUEST_ERROR_MSG = "⚠️ 정상 request가 아닙니다. request를 확인하세요."

logger = get_logger("main")

ENV = os.getenv("ENV_PATH")
logger.info(f"ENV: {ENV}")

headers = {"kb-key": settings.AZURE_OPENAI_API_KEY}
mcp_executor = None
governance_agent = None
report_agent = None
data_analysis_agent = None
voc_management_agent = None
kst = pytz.timezone("Asia/Seoul")

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logger.info("Get mcp excutor...")
    global mcp_executor
    mcp_executor = await get_mcp_executor()
    logger.info("Get agents...")
    global governance_agent, report_agent, data_analysis_agent, voc_management_agent
    governance_agent = get_agent("governance", mcp_executor)
    report_agent = get_agent("doc_report", mcp_executor)
    data_analysis_agent = get_agent("analysis", mcp_executor)
    voc_management_agent = get_agent("voc", mcp_executor)

    if ENV == "serving":
        logger.info("Generate governance message...")
        asyncio.create_task(generate_governance_message())
        logger.info("Generate report...")
        asyncio.create_task(generate_report())
        # logger.info("Analyze voc...")
        # await analyze_voc_data()
        scheduler = AsyncIOScheduler()
        scheduler.add_job(analyze_voc_data, CronTrigger(hour=3, minute=0, timezone="Asia/Seoul"))
        scheduler.add_job(generate_report, CronTrigger(hour=13, minute=30, timezone="Asia/Seoul"))
        scheduler.start()

# === 전역 예외 핸들러 === 
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

# === streaming response generator ===
async def event_generator(stream_result):
    async for event in stream_result:
        if event["event"] == "on_chain_stream" and event["name"] == "agent":
            chunk = event["data"].get("chunk")
            data = {"event": "CHUNK", "content": chunk["messages"][-1].content}
            yield f"data: {json.dumps(data)}\n\n"

async def file_send(data):
    data = ResponseProtocol(event="CHUNK", content=data)
    yield f"{json.dumps(data.model_dump())}\n\n"
    
async def send(data):
    if isinstance(data, dict):
        yield f"data: {json.dumps(data)}\n\n"
    elif isinstance(data, list):
        for i in data:
            data = ResponseProtocol(event="CHUNK", content=i)
            yield f"data: {json.dumps(data.model_dump())}\n\n"
    else:
        data = ResponseProtocol(event="CHUNK", content=data)
        yield f"data: {json.dumps(data.model_dump())}\n\n"

# === 메인 api 엔드포인트 ===
@app.post("/run_agent")
async def run_agent(request: FabrixRequest):
    try:
        request = json.loads(request.input_value)
        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        user_id = request.get("user_id", settings.MCP_USER_ID)
        logger.info(f"요청 도착! {user_id}")

        # 커스텀 AOAI 객체 선언
        llm = create_azurechatopenai(user_id=user_id)

        # MCP user_id 할당
        contextvar_token = mcp_executor._cur_user_id.set(user_id)

        # KBDS API 요청 처리
        if request.get("requestType"):
            request_type = request.get("requestType")
            logger.info(f"{request_type} 요청")
            date = request.get("date", today_date.strftime("%Y%m%d"))
            # 데이터 분석 에이전트
            if request_type == "voc":
                page_num = request.get("page", 0)
                file_name = settings.DATA_ANALYSIS_OUTPUT_PATH + f"output_{date}_{page_num}.json"
                if os.path.exists(file_name):
                    res = load_resource_file(file_name)
                    return {"event": "CHUNK", "content": json.dumps(res)}
                else:
                    await analyze_voc_data(request)
                    res = load_resource_file(file_name)
                    return {"event": "CHUNK", "content": json.dumps(res)}
            # 거버넌스 메시지
            elif request_type == "governance_message":
                page_num = request.get("page", 0)
                try:
                    res = load_resource_file(settings.GOVERNANCE_OUTPUT_PATH + f"output_{date}_{page_num}.json")
                except Exception as e:
                    logger.info(f"{date} 일자 데이터가 없어 해당 일자 데이터를 생성합니다. {e}")
                    await generate_governance_message(date=date)
                    res = load_resource_file(settings.GOVERNANCE_OUTPUT_PATH + f"output_{date}_{page_num}.json")
                return {"event": "CHUNK", "content": json.dumps(res)}
            # 개선의견
            elif request_type == "voc_suggestion":
                logger.info("voc_suggestion"+ str(request))
                voc_management_agent.llm = llm
                # PII 마스킹
                voc_content = request.get('voc', {}).get('voc', '')
                masked_voc = mask_pii(voc_content)
                if voc_content != masked_voc:
                    logger.info(f"voc에서 pii 발견됨.\n원문:{voc_content}\n마스킹:{masked_voc}")
                    request['voc']['voc'] = masked_voc
                res = await voc_management_agent.execute(request)
                return {"event": "CHUNK", "content": json.dumps(res)}
            # 피드백
            elif request_type == "voc_feedback":
                logger.info("voc_feedback"+ str(request))
                voc_management_agent.llm = llm
                # PII 마스킹
                voc_content = request.get('voc', {}).get('voc', '')
                suggestion_content = request.get('voc', {}).get('suggestionText', '')
                masked_voc = mask_pii(voc_content)
                masked_suggestion = mask_pii(suggestion_content)
                if voc_content != masked_voc or suggestion_content != masked_suggestion:
                    logger.info(f"voc에서 pii 발견됨.\n원문:{voc_content}\n마스킹:{masked_voc}")
                    logger.info(f"개선의견에서 pii 발견됨.\n원문:{suggestion_content}\n마스킹:{masked_suggestion}")
                    request['voc']['voc'] = masked_voc
                    request['voc']['suggestionText'] = masked_suggestion
                res = await voc_management_agent.execute_feedback(request)
                return {"event": "CHUNK", "content": json.dumps(res)}
            # 보고서
            elif request_type == "report":
                logger.info("report"+ str(request))
                report_type = request.get("reportType", "")
                if report_type:
                    res = {}
                    files = glob.glob(settings.REPORT_OUTPUT_PATH + report_type + "_*")
                    for f_name in files:
                        with open(f_name, "rb") as f:
                            content = f.read()
                        encoded_content = base64.b64encode(content).decode("utf-8")
                        res[f_name] = encoded_content
                    return {"event": "CHUNK", "content": json.dumps(res)}
                else:
                    logger.error("report_type이 없습니다.")
            # 보고서 생성용
            elif request_type == "make_report":
                logger.info("make_report"+ str(request))
                report_type = request.get("reportType", "")
                await generate_report(report_type=report_type)
                logger.info(f"{report_type} 보고서 생성이 완료되었습니다.")
                return
            else:
                return HTTPException(status_code=500, detail=REQUEST_ERROR_MSG)

    except Exception as e:
        logger.error(f"[run_agent] 기타 오류 발생, {e}")
        if "Rate limit" in str(e):
            data = {"event": "CHUNK", "content": EXCEED_USAGE_MSG}
        elif "length exceeded" in str(e):
            data = {"event": "CHUNK", "content": EXCEED_MAX_TOKEN_MSG}
        elif "content filter" in str(e):
            data = {"event": "CHUNK", "content": CONTENT_FILTER_MSG}
        else:
            data = {"event": "CHUNK", "content": BASE_ERROR_MSG}
        return StreamingResponse(send(data), media_type="text/event-stream")

    finally:
        mcp_executor._cur_user_id.reset(contextvar_token)
#################################################
# =============== 배치 스케줄링 =================
#################################################

# 거버넌스 메세지 스케줄링 (일별)
async def generate_governance_message(request_type="governance", date=datetime.now(pytz.timezone("Asia/Seoul")).date().strftime("%Y%m%d")):
    lock_file_path = "./governance_batch.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        page_num = 0
        file_name = f"output_{date}_{page_num}.json"
        request = {
            "contents": [
                {
                    "requestType": request_type, # top_down, bottom_up
                    "page": page_num,
                    "date": date,
                }
            ],
        }
        res = await governance_agent.generate_governance(request)
        with open(settings.GOVERNANCE_OUTPUT_PATH + file_name, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=4, ensure_ascii=False)
        # 모든 page 다 저장
        while res.get("response", {}).get("page", {}).get("lastYn") == False:
            page_num += 1
            request["contents"][0]["page"] = page_num
            res = await governance_agent.generate_governance(request)
            file_name = f"output_{date}_{page_num}.json"
            with open(settings.GOVERNANCE_OUTPUT_PATH + file_name, "w") as f:
                json.dump(res, f, indent=4, ensure_ascii=False)
    except IOError:
        return
    finally:
        # 90일치 파일만 저장
        keep_latest_files(settings.GOVERNANCE_OUTPUT_PATH, keep=90)
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()

# 데이터분석 스케줄링 (일별)
async def analyze_voc_data(request=None):
    lock_file_path = "./voc_batch.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        if request is not None:
            base_date = request.get("date", today_date.strftime("%Y%m%d"))
            page_num = request.get("page", 0)
        else:
            base_date = today_date - timedelta(days=1)
            base_date = base_date.strftime("%Y%m%d")
            page_num = 0

        finish = False
        max_iter = 500
        while finish == False and max_iter >= page_num:
            result = {}
            request = {
                "requestType": "voc",
                "page": page_num,
                "date": base_date,
            }
            page_res = await data_analysis_agent.execute_data_anlysis(request)
            json_data = page_res.model_dump(by_alias=True)
            # KBDS와 규격 합의 (content안에 content)
            result["content"] = json_data
            if (result['content']['page']['totalPages'] == page_num + 1) or (result['content']['page']['lastYn'] == True):
                if result['content']['page']['lastYn'] == False:
                    logger.info(f"{base_date} 데이터 분석 중 오류 발생 {result}")
                else:
                    logger.info(f"{base_date} 데이터분석 끝")
                finish = True
            with open(settings.DATA_ANALYSIS_OUTPUT_PATH + f"output_{base_date}_{page_num}.json", "w", encoding="utf-8") as f:
                json.dump(result, f)
            page_num += 1
    except IOError:
        return
    finally:
        # 200개 파일만 저장 (TD가 아주 많을수도 있기 때문)
        keep_latest_files(settings.DATA_ANALYSIS_OUTPUT_PATH, keep=200)
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()
    

# 레포트 생성 스케줄링 (월간, 주간, 반기)
async def generate_report(report_type:str=None):
    lock_file_path = "./report_batch.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        files = []
        # 매주 월요일 DB 체크후 새로운 TD 조사 결과 쌓인 경우 TD 보고서 생성
        files = glob.glob(settings.REPORT_OUTPUT_PATH + "td_nps_report*.*")
        if len(files) == 0 or "td_nps_report" == report_type or today_date.weekday() == 0:
            if files:
                yyyyhf = re.sub(r'.*([0-9]{4,4})_([상하]반기).*', '\\1\\2', files[0])
                date_params = await make_td_nps_report_date_params(mcp_executor)
                # 가장 최근 데이터가 이전과 동일할때는 생성 X
                if yyyyhf == date_params["yyyy"] + date_params["yyyyhf"]:
                    return
            res = await report_agent.execute("settings.MCP_USER_ID", "td_nps_report", today_date)
            for f_name in files:
                os.remove(f_name)
            logger.info(f"TD 보고서 생성 완료: {res}")

        # 매주 1일 주간 보고서 생성
        files = glob.glob(settings.REPORT_OUTPUT_PATH + "bu_nps_weekly_report*.*")
        if len(files) == 0 or report_type == "bu_nps_weekly_report" or today_date.weekday() == 0:
            res = await report_agent.execute("settings.MCP_USER_ID", "bu_nps_weekly_report", today_date)
            for past_file in files:
                os.remove(past_file)
            logger.info(f"BU 주간 보고서 생성 완료: {res}")
        
        # 매월 1일 월간 보고서 생성
        files = glob.glob(settings.REPORT_OUTPUT_PATH + "bu_nps_monthly_report*.*")
        if len(files) == 0 or today_date.day == 1 or report_type == "bu_nps_monthly_report":
            res = await report_agent.execute("settings.MCP_USER_ID", "bu_nps_monthly_report", today_date)
            for f_name in files:
                os.remove(f_name)
            logger.info(f"BU 월간 보고서 생성 완료: {res}")

        # 매주 월요일날 지역영업그룹 보고서 생성
        files = glob.glob(settings.REPORT_OUTPUT_PATH + "region_group_nps_biweekly_report*.*")
        if len(files) == 0 or report_type == "region_group_nps_biweekly_report" or (report_type is None and today_date.weekday() == 0):
            res = await report_agent.execute("settings.MCP_USER_ID", "region_group_nps_biweekly_report", today_date)
            for f_name in files:
                os.remove(f_name)
            logger.info(f"지역영업그룹 보고서 생성 완료: {res}")
    except IOError:
        return
    finally:
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()

@app.get("/health")
async def health_check():
    """
    헬스 체크를 수행합니다.
    Returns:
    - JSONResponse: 상태 코드와 메시지
    """
    logger.info("Health check request received")
    return JSONResponse(content={"status": "ok"})
