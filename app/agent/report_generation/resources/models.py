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

