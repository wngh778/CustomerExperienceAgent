from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_core.tools import BaseTool
from httpx import HTTPStatusError
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Any
from pydantic import Field
from contextvars import ContextVar

from .config import settings
from .logger import get_logger
from .pii_masking import mask_pii

import requests
import asyncio
import hashlib
import base64
import json
import uuid
import hmac
import ast
import os

logger = get_logger(__name__)

class DynamicInvokeTool(BaseTool):
    """ainvoke를 래핑한 커스텀 Tool: 매 호출 시 동적 헤더 적용"""
    
    # BaseTool 기본 + 커스텀 필드
    original_tool: BaseTool = Field(..., description="Wrapped original tool")
    client_servers: Dict[str, Dict] = Field(..., description="Server configurations")
    token: str = Field(..., description="Auth token for headers")
    server_name: str = Field(..., description='Belongs to what server')

    def __init__(self, original_tool: BaseTool, client_servers: Dict[str, Dict], token: str, server_name: str):
        super().__init__(
            name=original_tool.name,
            description=original_tool.description,
            original_tool=original_tool,
            client_servers=client_servers,
            token=token,
            server_name=server_name
        )

    def _generate_mcp_user_key(self, client_id: str, client_secret: str, strf_timestamp: str, request_id: str, emp_no: str) -> str:
        # 3. HMAC-SHA256
        message = strf_timestamp + request_id + emp_no
        signature = base64.b64encode(
            hmac.new(
                client_secret.encode(),
                message.encode(),
                hashlib.sha256
            ).digest()
        ).decode()

        # 4. payload 구성
        payload = {
            "client_id": client_id,
            "emp_no": emp_no,
            "timestamp": strf_timestamp,
            "request_id": request_id,
            "signature": signature
        }

        # Base64
        return base64.b64encode(json.dumps(payload).encode()).decode()

    def _create_dynamic_headers(self, emp_no: str, call_site: str) -> Dict[str, str]:
        """동적으로 헤더 생성"""
        # 1. YYYYMMDDHHmmss, 14자리
        strf_timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y%m%d%H%M%S')
        # 2. ID
        request_id = str(uuid.uuid4())
        # 3. HMAC-SHA256
        mcp_user_key = self._generate_mcp_user_key(
            client_id=settings.MCP_USER_ID,
            client_secret=settings.MCP_SECRET_KEY,
            strf_timestamp=strf_timestamp,
            request_id=request_id,
            emp_no=emp_no
        )
        logger.info(f"[{self._create_dynamic_headers.__name__} in **{self.name} Tool**] MCP UserKey logged | emp_no={emp_no} | call_site={call_site} | timestamp={strf_timestamp} | request-id={request_id}")

        headers = {
            "Authorization": f"Bearer {self.token}",
            "MCP-User-Key": mcp_user_key
        }

        return headers

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        """동기 실행: _arun을 asyncio로 래핑해 호출 (동적 헤더 적용)"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._arun(*args, **kwargs))
            loop.close()
            return result
        except Exception as e:
            raise RuntimeError(f"동기 실행 중 오류: {e}")

    async def _arun(self, *args: Any, **kwargs: Any) -> Any:
        """비동기 실행: 원본 ainvoke 호출 (동적 헤더 적용 및 세션 최적화)"""
        payload = args[0] if args else kwargs
        emp_no, call_site = payload.pop("emp_no"), payload.pop("call_site")
        config = kwargs.get('config', None)
        mcp_user_key = None

        # 1. 동적으로 헤더 생성
        dynamic_headers = self._create_dynamic_headers(emp_no=emp_no, call_site=call_site)
        mcp_user_key = dynamic_headers.get("MCP-User-Key", "unknown")

        # 2. 임시 서버 설정
        temp_servers = {name: {**cfg, "headers": dynamic_headers} 
                        for name, cfg in self.client_servers.items()}
        temp_client = MultiServerMCPClient(temp_servers)

        try:
            # 3. Tool 소속 서버(self.server_name)의 세션만 열기
            async with temp_client.session(self.server_name) as session:
                result = await session.call_tool(self.name, args[0] if args else kwargs)
                if hasattr(result, 'content'):
                    return result.content[0].text if result.content else "{}"
                return json.dumps(result)
        except Exception as e:
            logger.error("*** MCP Tool Exception 발생 ***")
            logger.error(f"예외 메시지: {str(e)}")

            if hasattr(e, "__context__") and e.__context__:
                logger.error("→ __context__: " + type(e.__context__).__name__ + " | " + str(e.__context__.response.status_code) + " | " + str(e.__context__))
                if "401" in str(e) or isinstance(e.__context__, HTTPStatusError) and e.__context__.response.status_code == 401:
                    raise Exception("401 Unauthorized(Invalid or Expired MCP Auth Token Detected)")
            return json.dumps({"success": False, "error": str(e)})

class DynamicHeaderMCPClient(MultiServerMCPClient):
    """MultiServerMCPClient 상속: get_tools()에서 ainvoke 래퍼 적용"""
    def __init__(self, servers: dict, token: str):
        super().__init__(servers)
        self.token = token
        self._servers = servers  # 캐싱용

    async def get_tools(self) -> List[BaseTool]:
        wrapped_tools = []
        
        # self._servers는 { "server-name": { "url": "...", ... } } 구조
        for server_name in self._servers.keys():
            try:
                # 특정 서버의 세션만 열어서 해당 서버의 도구 목록 가져오기
                async with self.session(server_name) as session:
                    # mcp-sdk의 load_mcp_tools 또는 내부 로직으로 도구 로드
                    from langchain_mcp_adapters.tools import load_mcp_tools
                    server_tools = await load_mcp_tools(session)
                    
                    for tool in server_tools:
                        # 래핑할 때 서버 이름을 전달
                        wrapped = DynamicInvokeTool(
                            original_tool=tool,
                            client_servers=self._servers,
                            token=self.token,
                            server_name=server_name
                        )
                        wrapped_tools.append(wrapped)
            except Exception as e:
                logger.error(f"CRITICAL: {tool.name} wrapping 실패 -> {type(e).__name__}: {str(e)}")
                raise
                
        return wrapped_tools

class MCPToolExecutor:
    _cur_user_id : ContextVar[str] = ContextVar("cur_user_id", default=settings.MCP_USER_ID)

    def __init__(self):
        self.tools = []
        self.server = settings.MCP_HOSTNAME
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

        try:
            # DynamicHeaderMCPClient로 초기화 시도 (래퍼 적용)
            self.client = DynamicHeaderMCPClient(self._servers, self.token)
            self.tools = await self.client.get_tools()
        except Exception as eg:
            logger.info(f"[{self.initialize_tools.__name__}] 토큰 재발급")
            self.fetch_jwt_token()
            basic_headers = {"Authorization": f"Bearer {self.token}"}
            self._servers = self._get_servers(basic_headers)
            self.client = DynamicHeaderMCPClient(self._servers, self.token)
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
                        logger.info(f"마스킹 대상 정보 발견: {payload}")
                    return ast.literal_eval(masked_data)
                else:
                    return parsed["data"]
            else:
                logger.error(f"[{self._invoke_and_parse.__name__}] Tool {tool.name} returned success=False: {parsed}")
                return None
        except Exception as e:
            logger.info(f"[{self._invoke_and_parse.__name__}] Tool {tool.name} invocation failed -> {e}")
            if "401" in str(e).lower() or "unauthorized" in str(e).lower():
                logger.warning(f"[{self._invoke_and_parse.__name__}] 401 Unauthorized 감지 -> 토큰 만료 플래그 ON")
                self._token_maybe_expired = True
            return None

    async def execute_tool(self, tool_name: str, input_data: dict, emp_no: Optional[str] = "", call_site: Optional[str] = "") -> Optional[dict]:
        """MCP Tool 실행 (최대 2회 시도: 최초 + 재초기화 후 1회 재시도)"""
        if not isinstance(input_data, dict):
            raise TypeError("Type of input data must be dictionary")

        payload = input_data.copy()
        payload["emp_no"] = self._cur_user_id.get() or settings.MCP_USER_ID # 시스템 배치의 경우, MCP 계정 ID로 사용
        payload["call_site"] = call_site or self.execute_tool.__name__

        # 최대 2회 시도 (attempt 0: 최초, attempt 1: 재시도)
        for attempt in range(2):
            # 재시도 단계에서는 초기화 수행
            if attempt > 0:
                logger.info(f"Retrying tool '{tool_name}' after reinitialization (attempt {attempt + 1})")
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
                    logger.info(f"Tool '{tool_name}' not found in initial attempt")
                    continue
                else:
                    logger.info(f"Tool '{tool_name}' still not found after retry")
                    return None

            # 툴 호출 & 결과 파싱
            result = await self._invoke_and_parse(target_tool, payload)
            if result is not None:
                return result

            # 실패 케이스의 경우 재시도
            if attempt == 0:
                logger.info(f"Tool '{tool_name}' failed on first attempt -> will retry after reinitialization")
                continue

        # 모든 시도 실패
        logger.error(f"Tool '{tool_name}' failed after all attempts")
        return None

async def get_mcp_executor():
    executor = MCPToolExecutor()
    await executor.initialize_tools()
    return executor
