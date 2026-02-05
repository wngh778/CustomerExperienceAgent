from pydantic import BaseModel, Field
from typing import List, Tuple, Dict, Optional, Set, Literal, Any
from typing_extensions import TypedDict

class VOC(BaseModel):
    """특정 key를 가진 VOC의 정보를 가지고 있는 객체"""
    voc: str = Field(description="VOC 원문")
    cxc_code: str = Field(description="해당 VOC가 분류된 고객경험요소코드")
    cxc_name: str = Field(description="해당 VOC가 분류된 고객경험요소명")
    qusn_id: str = Field(description="해당 발생한 설문ID")
    qusn_invl_tagtp_uniq_idd: str = Field(description="해당 발생한 설문조사자고유 ID")
    qsitm_id: str = Field(description="해당 발생한 문항 ID")

class Keywords(BaseModel):
    """VOC에서 추출된 키워드들. DB에서 유사한 VOC들을 검색하기 위해 주요 어근과 함께 다양한 어미를 붙여서 포괄적 검색이 가능하도록 뽑힌 키워드들."""
    items: List[str] = Field(description="VOC에서 추출된 키워드들.")

class Opinion(BaseModel):
    """특정 VOC에 대해 담당자가 작성했던 검토의견을 담고 있는 객체"""
    index: int = Field(description="인덱스")
    voc: str = Field(description="검토 대상 VOC 원문")
    opinion_type: Literal["현행유지", "개선불가", "개선예정"] = Field(default=None, description="개선 담당자가 VOC를 검토한 의견의 구분")
    opinion: str | None = Field(default=None, description="개선 담당자가 VOC를 검토한 의견 상세내용")
    opinion_ts: str | None = Field(default=None, description="개선 담당자가 VOC를 검토한 시점(YYYYmmDD)")
    proj_name: str | None = Field(default=None, description="개선과제명")
    proj_start: str | None = Field(default=None, description="개선과제 시작일")
    proj_end: str | None = Field(default=None, description="개선과제 종료일")

class AgentSuggestion(BaseModel):
    """에이전트가 생성한 검토의견"""
    opinion_type: Literal["현행유지", "개선불가", "개선예정"] = Field(default=None, description="에이전트가 VOC를 검토한 의견의 구분")
    opinion: str | None = Field(default=None, description="에이전트가 <target_voc>에 대해 작성한 의견")
    opinion_ts: str | None = Field(default=None, description="VOC를 검토한 시점(YYYYmmDD). 오늘 날짜")
    proj_name: str | None = Field(default=None, description="에이전트가 제안한 개선과제명")
    proj_start: str | None = Field(default=None, description="에이전트가 제안한 개선과제 시작일")
    proj_end: str | None = Field(default=None, description="에이전트가 제안한 개선과제 종료일")

class OpinionwWithPlan(AgentSuggestion):
    """개선예정 전용 개선 의견 생성 모델"""
    opinion_type: Literal["개선예정"] = Field(description="개선예정")

class OpinionwWithMaintained(AgentSuggestion):
    """현행유지 전용 개선 의견 생성 모델"""
    opinion_type: Literal["현행유지"] = Field(description="현행유지")

class VOCSuggestState(BaseModel):
    target_voc: VOC = Field(description="검토 대상 VOC 원문과 VOC에 대한 key를 가지고 있는 개체")
    keywords: Keywords = Field(description="해당 VOC의 키워드")
    opinions: List[Opinion] = Field(description="해당 VOC와 연관어를 이용하여 찾은 모든 개선 의견들")
    relevant_voc_index: Set[int] = Field(default=None, description="개선의견들 중 실제로 VOC와 관련이 있는 개선 의견들의 ID 리스트")
    suggestion: Opinion = Field(default=None, description="에이전트가 작성한 검토의견")
    report: str = Field(default=None, description="검토 의견을 포함한 최종의견")

class SuggestionoResponse(BaseModel):
    suggestionType: str = Field(description="검토구분(현행유지, 개선예정, 개선불가)")
    suggestionReport: str = Field(description="'AI검토리포트' 칸에 들어갈 내용")
    ts: str = Field(description="처리일시")
    qusnId: str = Field(description="설문ID (VOC에 대한 KEY 역할)")
    qusnInvlTagtpUniqId: str = Field(description="설문참여대상자고유ID (VOC에 대한 KEY 역할)")
    qsitmId: str = Field(description="해당 발생한 문항 ID (VOC에 대한 KEY 역할)")

class FeedbackResponse(BaseModel):
    ts: str = Field(description="처리일시")
    qusnId: str = Field(description="설문ID (VOC에 대한 KEY 역할)")
    qusnInvlTagtpUniqId: str = Field(description="설문참여대상자고유ID (VOC에 대한 KEY 역할)")
    feedbackContent: str = Field(description="문항ID (VOC에 대한 KEY 역할)")
