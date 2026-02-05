from typing import Dict
from agent.voc_management.utils.load_files import load_template

def build_feedback_message(data: Dict[str, str], llm_feedback: str) -> str:
    """
    주어진 데이터와 LLM 피드백을 기반으로 템플릿을 채워 고객 피드백 메시지를 생성합니다.
    - data: 고객 관련 정보가 담긴 딕셔너리 (예: qusnInvlTagtpUniqID, cx, cxc)
    - llm_feedback: LLM이 생성한 피드백 문자열
    반환값: 템플릿을 포맷팅한 최종 피드백 메시지 문자열
    """
    feedback_template = load_template("feedback_template.txt")
    customer_id = data.get("qusnInvlTagtpUniqID", "").strip()
    cx = data.get("cx", "").strip()
    cxc = data.get("cxc", "").strip()
    return feedback_template.format(
        customer_id=customer_id or "고객",
        cx=cx or "",
        cxc=cxc or "",
        llm_feedback=llm_feedback.strip()
    )