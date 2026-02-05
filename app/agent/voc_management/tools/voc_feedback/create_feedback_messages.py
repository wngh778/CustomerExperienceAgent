from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from .generate_llm_feedback import *
from .build_feedback_message import *

async def create_feedback_messages(data: dict, llm) -> dict:
    """
    주어진 배치 데이터에서 제안 문구를 추출해 LLM 피드백을 생성하고,
    피드백 메시지를 구성하여 결과 리스트로 반환합니다.
    각 결과에는 생성 시간(ts), 질문 ID(qusnid), 태그 유형 고유 ID(qusnInvlTagtpUniqID),
    그리고 피드백 내용(feedbankContent)이 포함됩니다.
    """
    voc_dict = data.get("voc", {})
    voc_text = voc_dict.get("voc", "")
    suggestion_text = voc_dict.get("suggestionText", "")
    llm_feedback = await generate_llm_feedback(voc_text, suggestion_text, llm)
    feedbank_content = build_feedback_message(
        {
            "qusnInvlTagtpUniqID": voc_dict.get("qusnInvlTagtpUniqID", ""),
            "cx": data.get("cx", ""),
            "cxc": data.get("sq", "")
        },
        llm_feedback
    )
    result = {
        "ts": datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d %H:%M:%S"),
        "qusnid": voc_dict.get("qusnid", ""),
        "qusnInvlTagtpUniqID": voc_dict.get("qusnInvlTagtpUniqID", ""),
        "feedbankContent": feedbank_content
    }
    return result