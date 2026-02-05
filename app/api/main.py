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
    check_pii_in_chat_history,
    create_azurechatopenai
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
from agent.report_generation.reports.report_types.utils import make_td_nps_report_date_params
from agent import get_agent

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
    logger.info("Generate governance message...")
    await generate_governance_message()
    logger.info("Generate report...")
    await generate_report(report_type="bu_nps_monthly_report") # region_group_nps_biweekly_report
    logger.info("Analyze voc...")
    await analyze_voc_data()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(generate_governance_message, CronTrigger(hour=0, minute=5, timezone="Asia/Seoul"))
    scheduler.add_job(generate_report, CronTrigger(hour=0, minute=30, timezone="Asia/Seoul"))
    scheduler.add_job(analyze_voc_data, CronTrigger(hour=0, minute=50, timezone="Asia/Seoul"))
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
        user_id = request.get("user_id", "cxagent")
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
                    await analyze_voc_data(date)
                    res = load_resource_file(file_name)
                    return {"event": "CHUNK", "content": json.dumps(res)}
            # 거버넌스 메시지
            elif request_type == "governance_message":
                page_num = request.get("page", 0)
                res = load_resource_file(settings.GOVERNANCE_OUTPUT_PATH + f"output_{date}_{page_num}.json")
                return {"event": "CHUNK", "content": json.dumps(res)}
            # 개선의견
            elif request_type == "voc_suggestion":
                logger.info("voc_suggestion"+ str(request))
                voc_management_agent.llm = llm
                res = await voc_management_agent.execute(request)
                return {"event": "CHUNK", "content": json.dumps(res)}
            # 피드백
            elif request_type == "voc_feedback":
                logger.info("voc_feedback"+ str(request))
                voc_management_agent.llm = llm
                res = await voc_management_agent.execute_feedback(request)
                return {"event": "CHUNK", "content": json.dumps(res)}

        # 파일 정보 요청 처리
        if request.get("file_info"):
            file_info_request = await get_file_info(request.get("file_info"))
            if file_info_request:
                file_info_json = [file.model_dump_json() for file in file_info_request]
                file_info_content = json.dumps(file_info_json, indent=4)
                return StreamingResponse(file_send(file_info_content), media_type="text/event-stream")

        filtered_body = request.get("filtered_body", {})
        stream = filtered_body.get("stream", False)
        if check_pii_in_chat_history(filtered_body.get("messages", [])):
            logger.info(f"[run_agent] 개인정보 차단: {request}")
            return StreamingResponse(send({"event": "CHUNK", "content": PII_MSG}), media_type="text/event-stream")
        messages = convert_input_messages(filtered_body.get("messages", []))
        if messages == []:
            logger.error(f"messages가 없습니다. {request}")
            return

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
async def generate_governance_message(request_type="governance"):
    lock_file_path = "./governance_batch.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date().strftime("%Y%m%d")
        page_num = 0
        file_name = f"output_{today_date}_{page_num}.json"
        request = {
            "contents": [
                {
                    "requestType": request_type, # top_down, bottom_up
                    "page": page_num,
                    "date": today_date,
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
            file_name = f"output_{today_date}_{page_num}.json"
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
async def analyze_voc_data(date_str=""):
    lock_file_path = "./voc_batch.lock"
    try:
        lock_file = open(lock_file_path, "w")
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        base_date = today_date - timedelta(days=1)
        base_date = date_str if date_str != "" else base_date.strftime("%Y%m%d")
        page_num = 0
        finish = False
        max_iter = 500
        while finish == False and max_iter >= page_num:
            result = {}
            request = {
                "requestType": "voc",
                "page": 0,
                "date": base_date,
            }
            page_res = await data_analysis_agent.execute_data_anlysis(request)
            json_data = page_res.model_dump(by_alias=True)
            # KBDS와 규격 합의 (content안에 content)
            result["content"] = json_data
            if (result['content']['page']['totalPages'] == page_num + 1) or (result['content']['page']['lastYn'] == True):
                if result['content']['page']['lastYn'] == False:
                    logger.info(f"{today_date} 데이터 분석 중 오류 발생 {result}")
                else:
                    logger.info(f"{today_date} 데이터분석 끝")
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
        # DB 체크후 새로운 TD 조사 결과 쌓인 경우 TD 보고서 생성
        if "td_nps_report" == report_type:
            files = glob.glob(settings.REPORT_OUTPUT_PATH + "td_nps_report*.*")
            if files:
                yyyyhf = re.sub(r'.*([0-9]{4,4})_([상하]반기).*', '\\1\\2', files[0])
                date_params = await make_td_nps_report_date_params(mcp_executor)
                # 가장 최근 데이터가 이전과 동일할때는 생성 X
                if yyyyhf == date_params["yyyy"] + date_params["yyyyhf"]:
                    return
            # 과거 레포트 파일 삭제
            for past_file in files:
                os.remove(past_file)
            res = await report_agent.execute("3902163", "td_nps_report", today_date)
            logger.info(f"TD 보고서 생성 완료: {res}")
        # 매주 1일 주간 보고서 생성
        if today_date.weekday() == 0 or report_type == "bu_nps_weekly_report":
            files = glob.glob(settings.REPORT_OUTPUT_PATH + "bu_nps_weekly*.*")
            # 과거 레포트 파일 삭제
            for past_file in files:
                os.remove(past_file)
            res = await report_agent.execute("3902163", "bu_nps_weekly_report", today_date)
            logger.info(f"BU 주간 보고서 생성 완료: {res}")
        # 매월 1일 월간 보고서 생성
        if today_date.day == 1 or report_type == "bu_nps_monthly_report":
            files = glob.glob(settings.REPORT_OUTPUT_PATH + "bu_nps_monthly*.*")
            # 과거 레포트 파일 삭제
            for past_file in files:
                os.remove(past_file)
            res = await report_agent.execute("3902163", "bu_nps_monthly_report", today_date)
            logger.info(f"BU 월간 보고서 생성 완료: {res}")
        # 짝수주차 월요일날 지역영업그룹 보고서 생성
        if (today_date.isocalendar().week % 2 == 0 and today_date.weekday() == 0) or report_type == "region_group_nps_biweekly_report":
            files = glob.glob(settings.REPORT_OUTPUT_PATH + "region_group_nps_biweekly*.*")
            # 과거 레포트 파일 삭제
            for past_file in files:
                os.remove(past_file)
            res = await report_agent.execute("3902163", "region_group_nps_biweekly_report", today_date)
            logger.info(f"지역영업그룹 보고서 생성 결과: {res}")
    except IOError:
        return
    finally:
        if "lock_file" in locals() and lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()
