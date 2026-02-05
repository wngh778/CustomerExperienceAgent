from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Literal, Union
from enum import Enum

class BaseRequest(BaseModel):
    stream: Optional[bool] = False
    user: Dict[str, Any]
    messages: List[dict]

class FabrixRequest(BaseModel):
    input_value: str

    
# File Upload

class FileInfo(BaseModel):
    data: str
    file_name: str

# Human In the Loop

class FieldType(str, Enum):
    select_item = "select_item"
    textbox = "textbox"
    report = "report"

class Description(BaseModel):
    type: str
    content: str

class Option(BaseModel):
    label: str
    description: Description

class DefaultField(BaseModel):
    type: FieldType

class InfoField(DefaultField):
    label: str
    sensitive: bool

class Selectitem(InfoField):
    options: List[Option]

class Textbox(InfoField):
    placeholder: str
    pattern: str

class Report(DefaultField):
    title: str
    content: str

class HITL(BaseModel):
    content: Union[Selectitem, Textbox] | None

class ResponseProtocol(BaseModel):
    """기본 응답 프로토콜"""
    content: str = ""
    event: str = "CHUNK"

class RAGActionResponse(BaseModel):
    augmented_query: str
    hyde_query: str
    results: List

class RAGAction(BaseModel):
    type: Literal["RAG"] = "RAG"
    plugin: Literal["RAG"] = "RAG"
    api: Literal["RAG"] = "RAG"
    response: RAGActionResponse

class ActionsResponseProtocol(ResponseProtocol):
    """연관 질문 및 출처를 위한 모델"""
    actions: Optional[List[RAGAction]] = None
    code: Optional[str] = None
    status: Optional[str] = None
    references: Optional[List] = None
    recommend_queries: Optional[list] = None
