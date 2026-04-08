from typing import Any
from langchain_core.messages import SystemMessage
from agent.voc_management.utils.load_files import load_prompt

async def generate_llm_feedback(voc_text, suggestion_text: str, llm) -> str:
    """
    주어진 제안 텍스트를 기반으로 LLM에 피드백 생성을 요청하고,
    결과 콘텐츠를 문자열로 반환합니다.

    - suggestion_text: 피드백 생성을 위한 입력 제안 텍스트
    - 반환값: LLM이 생성한 피드백 문자열
    """
    prompt_template = load_prompt("feedback_generate.txt")
    prompt = prompt_template.format(voc=voc_text, suggestionText=suggestion_text.strip())
    msg = SystemMessage(content=prompt)
    result = await llm.ainvoke([msg])
    try:
        return result.content.strip()
    except AttributeError:
        if isinstance(result, dict) and "content" in result:
            return str(result["content"]).strip()
        return str(result).strip()