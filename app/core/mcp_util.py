from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_core.tools import BaseTool
from typing import Dict, List, Optional
from contextvars import ContextVar

from .config import settings
from .logger import get_logger
from .pii_masking import mask_pii

import requests
import asyncio
import hashlib
import base64
import mcp_sdk
import json
import uuid
import hmac
import ast
import os
import logging
from pathlib import Path

logger = get_logger(__name__)

# 세션별로 유지되는 user_id 컨텍스트 변수
user_id_var = ContextVar("user_id", default=settings.MCP_USER_ID)

def set_user_id(user_id: str) -> None:
    """현재 컨텍스트(세션)에 user_id 설정"""
    user_id_var.set(user_id)

def get_user_id() -> str | None:
    """현재 컨텍스트(세션)의 user_id 조회. 없으면 None"""
    return user_id_var.get()

def reset_user_id() -> None:
    """현재 컨텍스트(세션)의 user_id 제거(초기화)"""
    user_id_var.set(None)

class MCPToolExecutor:
    def __init__(self):
        self.tools = []
        self.server = settings.MCP_URL
        self.user_id = settings.MCP_USER_ID
        self.secret_key = settings.MCP_SECRET_KEY
        self.mysql_conn_id = settings.MCP_MYSQL_CONN_ID
        
        self.auth_token_file_path = "/".join(os.path.abspath(__file__).split("/")[:-1]) + "/mcp_auth_token.json"
        self.token = self.load_auth_token()
        self.client = None

    def load_auth_token(self):
        with open(self.auth_token_file_path, "r") as f:
            auth_token = json.load(f)
        return auth_token.get("auth_token", "")

    def save_auth_token(self):
        auth_token = {"auth_token": self.token}
        with open(self.auth_token_file_path, "w", encoding="utf-8") as f:
            json.dump(auth_token, f, indent=4, ensure_ascii=False)
        logger.info("Save mcp auth token!")

    def fetch_jwt_token(self):
        """토큰 발급"""
        try:
            response = requests.post(
                f"https://{self.server}/token",
                json={"user_id": self.user_id, "secret_key": self.secret_key},
                verify=False
            )
            response.raise_for_status()
            if response.status_code == 200:
                data = response.json()
                self.token = data["jwt_token"]
                self.save_auth_token()
                logger.info(f"MCP JWT 토큰 발급 완료! token: {self.token}")

        except Exception as e:
            raise Exception(f"MCP JWT 토큰 요청 중 오류 발생!!! {str(e)}")

    def _get_servers(self, headers: dict) -> dict:
        """servers 딕셔너리 생성 (헤더는 기본만)"""
        servers = {}
        
        servers["mysql-mcp-server"] = {
            "url": f"https://{self.server}/mysql/{self.user_id}:{self.mysql_conn_id}", 
            "transport": "sse", 
            "headers": headers
        }
        servers["workb-mcp-server"] = {
            "url": f"https://{self.server}/workb/{self.user_id}", 
            "transport": "sse", 
            "headers": headers
        }
        return servers

    async def initialize_tools(self):
        """MCP 클라이언트와 도구 초기화"""
        should_reinitialize = (
            self.client is None or
            self._token_maybe_expired
        )
        if not should_reinitialize:
            logger.info(f"[{self.initialize_tools.__name__}] 이미 유효한 client 존재 -> 스킵")
            return

        logger.info(f"[{self.initialize_tools.__name__}] 재초기화 시작 (client 없음 or 토큰 만료 의심)")
        # 플래그 초기화 (재시도 시작 시점에)
        self._token_maybe_expired = False

        self.fetch_jwt_token()

        basic_headers = {"Authorization": f"Bearer {self.token}"}
        self._servers = self._get_servers(basic_headers)

        # JWT 토큰 재발급 받고 set 필요
        mcp_sdk.set_request_context(
            emp_no=get_user_id(),
            auth_token=self.token,
            agent="고객경험관리 에이전트"
        )
        try:
            self.client = MultiServerMCPClient(self._servers)
            self.tools = await self.client.get_tools()
        except Exception as eg:
            logger.info(f"[{self.initialize_tools.__name__}] 토큰 재발급")
            self.fetch_jwt_token()
            basic_headers = {"Authorization": f"Bearer {self.token}"}
            self._servers = self._get_servers(basic_headers)
            self.client = MultiServerMCPClient(self._servers)
            self.tools = await self.client.get_tools()

        logger.info(f"Tool list: {[tool.name for tool in self.tools]}")

    async def _invoke_and_parse(self, tool: BaseTool, payload: dict) -> Optional[dict]:
        """Tool을 한 번 호출하고 결과를 파싱해서 반환. 실패 시 None"""
        try:
            result_str = await tool.ainvoke(payload)
            parsed = json.loads(result_str)
            if parsed.get("success") == True:
                if parsed["data"] != []:
                    masked_data = mask_pii(str(parsed["data"]))
                    if masked_data != str(parsed["data"]):
                        logger.warning(f"마스킹 대상 정보 발견: {payload}")
                    return ast.literal_eval(masked_data)
                else:
                    return parsed["data"]
            else:
                logger.error(f"[{self._invoke_and_parse.__name__}] Tool {tool.name} returned success=False: {parsed}")
                return None
        except Exception as e:
            logger.warning(f"[{self._invoke_and_parse.__name__}] Tool {tool.name} invocation failed -> {e}")
            self._token_maybe_expired = True
            return None

    async def execute_tool(self, tool_name: str, input_data: dict, emp_no: Optional[str]="") -> Optional[dict]:
        """MCP Tool 실행 (최대 2회 시도: 최초 + 재초기화 후 1회 재시도)"""
        if not isinstance(input_data, dict):
            raise TypeError("Type of input data must be dictionary")

        payload = input_data.copy()
        # 최대 2회 시도 (attempt 0: 최초, attempt 1: 재시도)
        for attempt in range(2):
            # 재시도 단계에서는 초기화 수행
            if attempt > 0:
                logger.warning(f"Retrying tool '{tool_name}' after reinitialization (attempt {attempt + 1})")
                await self.initialize_tools()

            # tools가 없으면 초기화(최초 시도 시 한번 검증하고 로직 수행)
            if not self.tools:
                await self.initialize_tools()

            # tool 찾기
            target_tool = next(
                (t for t in self.tools if t.name == tool_name),
                None
            )

            if not target_tool:
                if attempt == 0:
                    logger.warning(f"Tool '{tool_name}' not found in initial attempt")
                    continue
                else:
                    logger.warning(f"Tool '{tool_name}' still not found after retry")
                    return None

            # 툴 호출 직전에 세팅
            mcp_sdk.set_request_context(
                emp_no=get_user_id(),
                auth_token=self.token,
                agent="고객경험관리 에이전트"
            )
            # 툴 호출 & 결과 파싱
            result = await self._invoke_and_parse(target_tool, payload)
            if result is not None:
                return result

            # 실패 케이스의 경우 재시도
            if attempt == 0:
                logger.warning(f"Tool '{tool_name}' failed on first attempt -> will retry after reinitialization")
                continue

        # 모든 시도 실패
        logger.error(f"Tool '{tool_name}' failed after all attempts")
        return None

async def get_mcp_executor():
    executor = MCPToolExecutor()
    await executor.initialize_tools()
    return executor
