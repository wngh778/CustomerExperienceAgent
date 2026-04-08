"""InsightAgent Structured Output 모델.

LangGraph 각 노드에서 LLM의 ``with_structured_output()``에 전달되는
Pydantic 모델을 정의한다. 각 모델은 LLM 응답의 JSON 스키마 역할을 한다.
"""

from typing import Literal
from pydantic import BaseModel, Field


class IntentClassification(BaseModel):
    """intent_classifier 노드의 의도 분류 결과.

    Attributes:
        intent: 분류된 의도 (unsafe / nps_analysis / manual / general_chat)
        reason: 분류 근거 (1문장)
    """
    intent: Literal["unsafe", "nps_analysis", "manual", "general_chat"] = Field(
        description="분류된 의도"
    )
    reason: str = Field(description="분류 근거 (1문장)")


class PolicyGuardResult(BaseModel):
    """policy_guard 노드의 정책 위반 판단 결과.

    개별 직원/영업점/영업본부 단위 NPS 조회 요청을 차단한다.

    Attributes:
        is_violation: True이면 정책 위반
        reason: 판단 근거 (1문장)
    """
    is_violation: bool = Field(
        description="True이면 정책 위반 (개별 직원/영업점/영업본부 단위 NPS 조회)"
    )
    reason: str = Field(description="판단 근거 (1문장)")


class FilterCondition(BaseModel):
    """SQL WHERE 조건 하나를 표현하는 필터.

    LLM tool-calling에서 사용되므로 JSON Schema로 직렬화 가능해야 한다.
    values는 항상 list[str]로 통일:
      = / != / > / >= / < / <= / LIKE  →  ["단일값"]
      IN                               →  ["값1", "값2", ...]
      BETWEEN                          →  ["시작값", "끝값"]
    """
    column: str = Field(description="필터할 컬럼명 (한국어, DB 스키마와 일치)")
    op: Literal["=", "!=", ">", ">=", "<", "<=", "IN", "BETWEEN", "LIKE"] = Field(
        default="=", description="비교 연산자"
    )
    values: list[str] = Field(
        description="비교 값 리스트. = 등 단일 연산자는 원소 1개, IN은 여러 개, BETWEEN은 정확히 2개"
    )