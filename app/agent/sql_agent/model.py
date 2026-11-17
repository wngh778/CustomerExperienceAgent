from typing import List, Dict, Any, Optional, Callable
from pydantic import BaseModel, Field

class QueryInput(BaseModel):
    """쿼리 입력 모델"""
    query: str = Field(description="실행할 SQL 쿼리")

class SampleDataInput(BaseModel):
    """샘플 데이터 입력 모델"""
    table_name: str = Field(description="샘플 데이터를 조회할 테이블 이름")
    limit: int = Field(default=5, description="조회할 샘플 데이터 개수 (기본값: 5)")

class SchemaInput(BaseModel):
    """테이블 스키마 조회 입력 모델"""
    table_name: str = Field(description="스키마를 조회할 테이블 이름")

class ListTablesInput(BaseModel):
    """인자가 없는 툴을 위한 빈 입력 모델"""
    pass

class IntentModel(BaseModel):
    intent: List[str] = Field(description="분류된 질의 유형 리스트.")
    
class RewrittenQuery(BaseModel):
    query: str = Field(description="오타가 수정되고 문맥을 반영하여 재작성된 쿼리")

class RecommendQuestion(BaseModel):
    q1: str = Field(description="추천질문1")
    q2: str = Field(description="추천질문2")
    q3: str = Field(description="추천질문3")