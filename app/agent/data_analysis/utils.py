import re
import xml.etree.ElementTree as ET
from typing import Dict, Optional, List, Any, Union
from asyncio import Semaphore

import random

from langchain_openai import AzureChatOpenAI 
from langchain.schema import BaseMessage, HumanMessage, SystemMessage

from core.config import settings

from core.util import create_azurechatopenai
from agent.data_analysis.model.consts import CommonCode

sem = Semaphore(settings.SEMAPHORE)

class DataAnalysisUtils:
    """
    `데이터 분석 에이전트` Utils Static 클래스
    """

    @staticmethod
    def get_final_cxe_dict(stge_cxe_dict: Dict[str, Dict[str, Dict[str, str]]], cx_stge_dstcd: str) -> Dict[str, Dict[str, str]]:
        """
        VOC의 `고객경험단계구분`의 유무(무:'00', 유: 그외)를 기준으로
        채널 > 고객경험단계 별 고객경험요소 사전({'cxe_cd': {cxe_meta}})을 반환한다.
        =============================================================
        1) 고객경험단계구분 == '00',
            -> 채널의 모든 고객경험단계에 해당하는 고객경험요소들을 사전을 반환
        2) 고객경험단계구분 != '00',
            -> 채널의 해당 고객경험단계에 해당하는 고객경험요소들만 사전으로 반환
        """

        if cx_stge_dstcd == CommonCode.NO_CX_STAGE_CD.value:
            # 고객경험단계가 '00' 해당무 일때, 채널의 모든 고객경험요소 첨부
            final_cxe_dict: Dict[str, Dict[str, Dict[str, str]]] = {}
            for cx_stge, cxe_dict in stge_cxe_dict.items():
                final_cxe_dict.update(cxe_dict)
                
            return final_cxe_dict
        
        else:
            return stge_cxe_dict[cx_stge_dstcd]

    @staticmethod
    def get_prev_word_set_key(chnl_cd: str, cxe_cd: Optional[str]) -> str:
        """
        개체어 추출 시, 이전 추출 결과 SET을 가져오기 위한
        Key값 세팅 util 메서드
        """

        final_cxe_cd = cxe_cd
        if not final_cxe_cd:
            # 미분류 VOC일 경우, 미분류 ID로 세팅
            final_cxe_cd = CommonCode.NO_CXE_ID.value
        
        prev_word_set_key = CommonCode.PREV_WORD_SET_DICT_KEY.value.format(
            chnl_cd=chnl_cd,
            cxe_cd=final_cxe_cd
        )
        return prev_word_set_key


    @staticmethod
    async def ainvoke_llm(prompt: List[BaseMessage], llm, failback_llm):

        final_llm = llm
        final_failback_llm = failback_llm

        async with sem:
            try:
                return await final_llm.ainvoke(prompt)
            except:
                print("Swiching to fallback model")
                return await final_failback_llm.ainvoke(prompt)


    @staticmethod
    def clean_code_fences(s: str) -> str:
        """
        LLM 응답에 포함될 수 있는 코드펜스(``` 또는 ```xml 등)를 제거합니다.
        여러 블록이 있더라도 우선 전체에서 펜스를 제거합니다.
        """
        # ```xml, ```json, ``` 등 시작 펜스 제거
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s.strip())
        # 끝 펜스 제거
        s = re.sub(r"\s*```$", "", s)
        return s.strip()

    @staticmethod
    def extract_xml(s: str) -> Optional[str]:
        """
        문자열에서 XML 본문을 추출합니다.
        - 여러 잡다한 텍스트가 섞여 있어도 첫 번째 루트 태그를 찾아서
          해당 루트 태그의 시작부터 끝까지를 추출하려고 시도합니다.
        - 실패 시 None 반환.
        """
        s = DataAnalysisUtils.clean_code_fences(s)
    
        # 만약 전체가 XML이라면 바로 반환 시도
        try:
            ET.fromstring(s)
            return s
        except ET.ParseError:
            pass
    
        # 첫 번째 루트 태그 시작 패턴을 찾음: <tag ...>
        # 태그 이름 캡처
        start_tag_match = re.search(r"<\s*([a-zA-Z_][\w\-.:]*)\b[^>]*>", s)
        if not start_tag_match:
            return None
    
        tag_name = start_tag_match.group(1)
    
        # 해당 태그의 종료 태그를 찾기 위해, 문자열 내의 태그 균형을 계산
        # 단순 스택 대신 태그 카운팅 방식(같은 이름의 시작/종료 태그 수)을 사용
        # 주의: self-closing <tag/> 고려
        # 이 방법은 완벽하진 않지만 현실적인 LLM 출력에 대해 꽤 견고함.
        start_idx = start_tag_match.start()
        substring = s[start_idx:]
    
        # 정규식으로 동일 태그의 시작/self-close/종료를 순차적으로 탐색
        tag_open = re.compile(rf"<\s*{re.escape(tag_name)}\b[^>/]*>")
        tag_self_close = re.compile(rf"<\s*{re.escape(tag_name)}\b[^>]*?/>")
        tag_close = re.compile(rf"</\s*{re.escape(tag_name)}\s*>")
    
        pos = 0
        count = 0
        while True:
            # 다음 이벤트(열림, self-close, 닫힘) 중 가장 가까운 것 찾기
            m_open = tag_open.search(substring, pos)
            m_self = tag_self_close.search(substring, pos)
            m_close = tag_close.search(substring, pos)
    
            candidates = [(m_open, "open"), (m_self, "self"), (m_close, "close")]
            candidates = [(m, t) for m, t in candidates if m]
            if not candidates:
                break
    
            # 가장 앞선 매치를 고름
            m, t = min(candidates, key=lambda x: x[0].start())
    
            if t == "open":
                count += 1
            elif t == "self":
                # self-closing은 열림/닫힘을 동시에 소비
                # open을 1로 보고 바로 1 감소로 처리할 수도 있으나, 스택에 영향 없음
                # 여기서는 count 변화 없이 진행
                pass
            elif t == "close":
                count -= 1
                if count == 0:
                    end_idx_in_sub = m.end()
                    xml_str = substring[:end_idx_in_sub]
                    # 최종 파싱 검증
                    try:
                        ET.fromstring(xml_str)
                        return xml_str
                    except ET.ParseError:
                        # 태그 균형은 맞지만 내부가 깨져 있을 수 있음
                        # 그래도 반환해 볼 수 있으나, 여기서는 None
                        return None
            pos = m.end()
    
        return None

    @staticmethod
    def text_or_children_to_value(el: ET.Element) -> Union[str, Dict[str, Any]]:
        """
        요소가 텍스트만 가지면 문자열을 반환,
        자식을 가지면 dict로 변환해서 반환.
        모든 값은 문자열로 처리.
        """
        children = list(el)
        # normalize text
        text = (el.text or "").strip()
    
        if not children:
            # 텍스트만 있는 경우
            return text
    
        # 자식이 있는 경우: 자식들을 dict로 병합
        result: Dict[str, Any] = {}
        for child in children:
            key = child.tag
            val = DataAnalysisUtils.text_or_children_to_value(child)
    
            if key in result:
                # 동일 키가 이미 존재하면 리스트로 변환/추가
                if isinstance(result[key], list):
                    result[key].append(val)
                else:
                    result[key] = [result[key], val]
            else:
                result[key] = val
    
        # 혼합 콘텐츠(텍스트 + 자식)가 있는 경우, 텍스트를 별도 키로 저장할지 결정
        # 요구사항에는 텍스트만 있을 때만 '태그': 값 형태라고 했으므로,
        # 자식이 있으면 텍스트는 무시하거나 필요 시 옵션으로 보관할 수 있음.
        # 여기서는 무시.
        return result

    @staticmethod
    def xml_to_dict(root: ET.Element) -> Dict[str, Any]:
        """
        루트 Element를 {'루트태그': 값} 형태의 dict로 변환.
        """
        return {root.tag: DataAnalysisUtils.text_or_children_to_value(root)}

    @staticmethod
    def parse_llm_xml_to_dict(s: str) -> Dict[str, Any]:
        """
        LLM 응답 문자열을 받아 XML을 추출하고 dict로 변환.
        실패 시 빈 dict 반환.
        """
        xml_str = DataAnalysisUtils.extract_xml(s)
        if not xml_str:
            return {}
    
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError:
            return {}
    
        return DataAnalysisUtils.xml_to_dict(root)

    @staticmethod
    async def mcp_pagination(mcp_executor, tool_name:str, query:str) -> List[Dict[str, Any]]:
        """
        쿼리 조회 결과가 500개가 넘는 경우 여러번 반복으로 데이터 추출
        """
        page = 0
        len_result = -1
        origin_query = query.replace(";", "").strip()
        final_result = []
        while not (len_result == 0 or 0 < len_result < 500):
            offset_query = f"\nLIMIT 500 OFFSET {500 * page}"
            query = origin_query + offset_query
            res = await mcp_executor.execute_tool(tool_name, {"query": query})
            if not isinstance(res, list):
                return res
            len_result = len(res)
            final_result.extend(res)
            page += 1
        return final_result