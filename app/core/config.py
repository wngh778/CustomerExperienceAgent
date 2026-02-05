from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal, List
from pathlib import Path
from dotenv import load_dotenv

import os

load_dotenv()

class Settings(BaseSettings):
    # === 환경 ===
    ENV: Literal["stg", "prd"] = os.getenv("ENV_TYPE", "stg")
    DEBUG: bool = True
    
    # === LLM 설정 ===
    # serv/trnn 환경에 따라 key, endpoint가 다름
    if os.getenv("ENV_PATH") != "serving":
        if ENV == "stg":
            AZURE_OPENAI_API_KEY: str = "e54ff071a5d2404eb509d5eb489ae3c4"
            AZURE_OPENAI_ENDPOINT: str = "https://cm-hea-genai-stg-apim.azure-api.net/trnn/"
        else:
            AZURE_OPENAI_API_KEY: str = "2f6ca5a2df0f487d87cd050f1199715c"
            AZURE_OPENAI_ENDPOINT: str = "https://cm-hea-genai-apim.azure-api.net/trnn/"
    else:
        if ENV == "stg":
            AZURE_OPENAI_API_KEY: str = "2c41078e46da43ffa4ac9821184f11b8"
            AZURE_OPENAI_ENDPOINT: str = "https://cm-hea-genai-stg-apim.azure-api.net/serv/"
        else:
            AZURE_OPENAI_API_KEY: str = ""
            AZURE_OPENAI_ENDPOINT: str = "https://cm-hea-genai-apim.azure-api.net/serv/"

    TEMPERATURE: float = 0.2
    MAX_TOKENS: int = 16384
    MODEL_NAME: str = "gpt-5"
    FALLBACK_MODEL_NAME: str = "gpt-5"
    OPENAI_API_VERSION: str = "2025-08-07"
    API_VERSION: str = "2025-08-07"
    MAX_RETRIES: int = 4
    TIMEOUT: int = 30

    # === MCP 설정 ===
    MCP_USER_ID: str = "tea000f"
    if ENV == "stg":
        MCP_SECRET_KEY: str = "HdIuu0ITti3SKavBDvb2DQeBPM0eAVv9AWU0NJX7yos"
        MCP_HOSTNAME: str = "stg-mcp.kbstar.com"
        MCP_MYSQL_CONN_ID: str = "uUfHOs2lMdu4KTpGTevY"
        MCP_IMPALA_CONN_ID: str = "iulitMYoyMyPIFKZfb-W"
    else:
        MCP_SECRET_KEY: str = "Hjpwk3ek7mQ1s6AMm8ZQkWUBjWUSUbNI5c9d1egPpP0"
        MCP_HOSTNAME: str = "mcp.kbstar.com"
        MCP_MYSQL_CONN_ID: str = "Xg1YnJFxbMIEQM4UE1kr"
        MCP_IMPALA_CONN_ID: str = "l_Ot8KPsy5RKfqX9pXt4"

    # === 로깅 ===
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # === 배치 아웃풋 경로 ===
    REPORT_OUTPUT_PATH: str = "./output/report/"
    GOVERNANCE_OUTPUT_PATH: str = "./output/governance/"
    DATA_ANALYSIS_OUTPUT_PATH: str = "./output/data_analysis/"
    INSIGHT_CACHE_OUTPUT_PATH: str = "./output/insight/"
    
    # === 데이터분석 병렬 강도 ===
    SEMAPHORE: int = 40

    # === VOC 관리 에이전트 파라미터 ===
    MAX_VOC_SEARCH_ITEMS: int = 20
    MAX_VOC_ITEMS: int = 3
    
    # === 인사이트 캐시 질문 ===
    TD_CACHE_QUERY: str = "가장 최근 TD NPS 결과 타행과 비교하여 분석해줘."
    BU_CACHE_QUERY: str = "가장 최근 BU NPS 결과 분석해줘."

# 싱글톤 인스턴스
settings = Settings()