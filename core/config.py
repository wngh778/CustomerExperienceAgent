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
            AZURE_OPENAI_API_KEY: str = os.getenv("STG_TRNN_AZURE_OPENAI_API_KEY")
            AZURE_OPENAI_ENDPOINT: str = os.getenv("STG_TRNN_AZURE_OPENAI_ENDPOINT")
        else:
            AZURE_OPENAI_API_KEY: str = os.getenv("PRD_TRNN_AZURE_OPENAI_API_KEY")
            AZURE_OPENAI_ENDPOINT: str = os.getenv("PRD_TRNN_AZURE_OPENAI_ENDPOINT")
    else:
        if ENV == "stg":
            AZURE_OPENAI_API_KEY: str = os.getenv("STG_SERV_AZURE_OPENAI_API_KEY")
            AZURE_OPENAI_ENDPOINT: str = os.getenv("STG_SERV_AZURE_OPENAI_ENDPOINT")
        else:
            AZURE_OPENAI_API_KEY: str = os.getenv("PRD_SERV_AZURE_OPENAI_API_KEY")
            AZURE_OPENAI_ENDPOINT: str = os.getenv("PRD_SERV_AZURE_OPENAI_ENDPOINT")

    TEMPERATURE: float = 0.2
    MAX_TOKENS: int = 16384
    MODEL_NAME: str = "gpt-5"
    SQL_AGENT_MODEL_NAME: str = "gpt-5"
    OPENAI_API_VERSION: str = "2025-08-07"
    API_VERSION: str = "2025-08-07"
    MAX_RETRIES: int = 4
    TIMEOUT: int = 30

    # === MCP 설정 ===
    if ENV == "stg":
        MCP_USER_ID: str = os.getenv("STG_MCP_USER_ID")
        MCP_SECRET_KEY: str = os.getenv("STG_MCP_SECRET_KEY")
        MCP_URL: str =os.getenv("STG_MCP_HOST_NAME")
        MCP_MYSQL_CONN_ID: str = os.getenv("STG_MYSQL_CONN_ID")
        MCP_IMPALA_CONN_ID: str = os.getenv("STG_IMPALA_CONN_ID")
    else:
        MCP_USER_ID: str = os.getenv("PRD_MCP_USER_ID")
        MCP_SECRET_KEY: str = os.getenv("PRD_MCP_SECRET_KEY")
        MCP_URL: str = os.getenv("PRD_MCP_HOST_NAME")
        MCP_MYSQL_CONN_ID: str = os.getenv("PRD_MYSQL_CONN_ID")
        MCP_IMPALA_CONN_ID: str = os.getenv("PRD_IMPALA_CONN_ID")

    # === 로깅 ===
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    INSIGHT_CACHE_OUTPUT_PATH: str = os.getenv("INSIGHT_CACHE_OUTPUT_PATH")
    
    # === 인사이트 캐시 질문 ===
    TD_CACHE_QUERY: str = "가장 최근 TD NPS 결과 타행과 비교하여 분석해줘."
    BU_CACHE_QUERY: str = "가장 최근 BU NPS 결과 분석해줘."

    # === 채팅 에이전트 파라미터 ===
    INSIGHT_AGENT_MODE: str = "SERV" # 인사이트 에이전트 실행 시 모드

    # === 보고서 전달용 ===
    REPORT_OUTPUT_PATH: str = "./output/report/"

    # === 신규 고객경험요소 발굴 배치 ===
    DISCOVER_CXE_BATCH_SIZE: int = 64
    DISCOVER_CXE_BATCH_CYCLE: int = 14
    DISCOVER_CXE_VOC_PRINT_LIMIT: int = 5
    DISCOVER_CXE_BATCH_DIR: str = "./output/discover_cxe"
    DISCOVER_CXE_START_DATE: str = "20250101"
    DISCOVER_CXE_MODE: str = "SERV" # SERV | TEST

    # === 배치 에이전트 커넥터 정보 ===
    BATCH_AGENT_URL: str = "https://fabrix-catalog-apim-serv-genaihub.kbonecloud.com/generative-ai-connector/kb0/eewf6dtxgwfzwsdcudwiu2nu6kpj/1" + "/openapi/agent-chat/v1/agent-messages"
    BATCH_AGENT_OPENAPI_TOKEN: str = "Bearer eyJ4NXQiOiJObVJoT0RRNE5XSTJNMlE1TURnMk16TXhOVEUzWVdGa016ZGhZall6TW1OaU5ETTNOVFl5TWpBMVpqY3dZbUUzTW1KbE1tTTNaREk0WldSbFpHTTNOZyIsImtpZCI6Ik5tUmhPRFE0TldJMk0yUTVNRGcyTXpNeE5URTNZV0ZrTXpkaFlqWXpNbU5pTkRNM05UWXlNakExWmpjd1ltRTNNbUpsTW1NM1pESTRaV1JsWkdNM05nX1JTMjU2IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI1ZGE3N2FlYS1mYWY0LTQ4MDItOTVlMC04ODY0NzI5ZDAwMDYiLCJhdXQiOiJBUFBMSUNBVElPTiIsImF1ZCI6IlY4VWp6bGlHaF9YTEFaQk5QWTFHODBFY2ZwRWEiLCJuYmYiOjE3NzQ4NTUwMjgsImF6cCI6IlY4VWp6bGlHaF9YTEFaQk5QWTFHODBFY2ZwRWEiLCJzY29wZSI6ImRlZmF1bHQiLCJpc3MiOiJodHRwczpcL1wvbWdtdC1zaGFyZWQtYXBpbXByZC1vcGR2LWlucy5zZHNkZXYuY28ua3I6NDQzXC9vYXV0aDJcL3Rva2VuIiwiZXhwIjo0OTMwNjE1MDI4LCJpYXQiOjE3NzQ4NTUwMjgsImp0aSI6IjNlOTMwMDhiLWE3YjQtNGY2Ni04MDJiLTYxNTRlNmQ0NWVmOSIsImNsaWVudF9pZCI6IlY4VWp6bGlHaF9YTEFaQk5QWTFHODBFY2ZwRWEifQ.aee_Q_u_47fUz3o8m6MRwod2ZuqIyNB7-BuMbQeALh4WgQaPnu1Lu88SX9FUWiJCfphMOP-sy-SPXvSLUWc1Ez7b-JWOTZiVODb23A27CUTDHU2ieyj8N-bofPjcR1wPJgqbzPsEHqRQ3qmU-o9I8GvCs8KKVlwVQB5ajvVrU14tfE5Yso-SSRaKRRxe3PYGje9iVepa18GmIDrklcBUOUSNcYWuu7QumxaPuok4lLbFa5FHfZEl0cP_0XlDXStPrzF1vIWNBut7YVJvIKjfuSQoBtsG42i57XzFrIrOcNUxwNHrcucbgixgl56JBXMwM5vg3_ruO7PLe4UnRMyQgA"
    BATCH_AGENT_GENERATIVE_CLIENT: str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjbGllbnRJZCI6IjIxNTI2IiwiY2xpZW50U2VjcmV0IjoiZDI0ODlhY2VlMGMwNDU0YWJlODJiYjczOWMyOGRmY2QzZmYyZjI0ZDllODhkOGExIiwiZXhwIjoxODA2Njc3OTk5fQ.I1a_7aJ9ZtTtJJFcfSGz5Jsq7wpQGydMkNqMMd8jKOk"
    BATCH_AGENT_ID: int = 1411

    # === Langfuse ===
    LANGFUSE_ENABLED: bool = False
    if ENV == "stg":
        LANGFUSE_PUBLIC_KEY: str = "pk-lf-26b1209c-2596-4033-b186-57f2fa553127"
        LANGFUSE_SECRET_KEY: str = "sk-lf-f99e2e53-03ae-44a7-964f-21a1ebc9be63"
        LANGFUSE_URL: str = "http://stg-langfuse-genaihub.kbonecloud.com"
    else:
        LANGFUSE_PUBLIC_KEY: str = "pk-lf-1d468ca3-1287-422e-8257-aafcd327c39b"
        LANGFUSE_SECRET_KEY: str = "sk-lf-6603b4c2-7b94-4b4f-9118-fa32efb79c75"
        LANGFUSE_URL: str = "https://langfuse-genaihub.kbonecloud.com"


    # === 권한 제어 ===
    ADMIN_USER_IDS: tuple[str, ...] = (
        '2349813', # 박세운
        '3902163', # 서승원
        '2856837', # 염주호
        '5927807', # 이찬우
        '5916902', # 한지윤
    )
    VALID_USER_IDS: tuple[str, ...] = (
        'Q000563', # 김지용
        'Q000601', # 김지혜
        '2349813', # 박세운
        '3902163', # 서승원
        'Q000408', # 신동주
        '2856837', # 염주호
        '5927807', # 이찬우
        '5916902', # 한지윤
        "0009267", # 박선현
        "2507734", # 설광호
        "3158852", # 조인대
        "3826441", # 홍석환
        "1907704", # 남지숙
        "3828901", # 오효진
        "2851830", # 오승현
        "3826884", # 하정민
        "1651243", # 김동탁
        "1627676", # 김동현
        "2833184", # 유희정
        "3835871", # 허정우
        "2538725", # 서아름
        "3171250", # 장명호
        "2353991", # 박민지
        "3171661", # 전찬미
        "2855971", # 유수빈
        "2208877", # 문현성
    )

# VOC 조회 데이터 정책상 금지
    UNSUPPORTED_DATA_MSGS: dict[str,str] = {
        "VOC_TYPE_SENTIMENT" : "VOC 유형/감정 분석에 대한 데이터는 현재 운영정책상 조회 불가능합니다. 연관 질문에 있는 다른 분석을 사용해주세요."
    }
    
# 싱글톤 인스턴스
settings = Settings()
