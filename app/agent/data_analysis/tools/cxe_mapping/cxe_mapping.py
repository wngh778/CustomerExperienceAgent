import os
import asyncio
from pathlib import Path
import time

from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI
from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser

from core.config import settings
from core.util import load_resource_file, create_azurechatopenai
from core.logger import get_logger

from agent.agent_template import Agent
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.dto import CxeMapperRequestInfo
from agent.data_analysis.model.vo import VocInfo, CxeInfo
from agent.data_analysis.model.consts import CommonCode, CxeFailedMessage

# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 상수 - 프롬프트
SYSTEM_PROMPT_FILE_NAME = f"cxe_mapping_system.txt"
HUMAN_PROMPT_FILE_NAME = f"cxe_mapping_human.txt"


logger = get_logger("cxe_mapper")

class CxeMapper(Agent):
    """
    Cxe Mapper (고객경험요소 매퍼):
    채널 / 고객경험단계 별 고객경험요소를 Context로 VOC 원문에 연관된 고객경험요소를 매핑한다.
    ================================================================
    기능:
    1. Cxe Context 세팅 (고객경험단계 유/무) - '해당무'일 경우, 채널의 모든 고객경험요소를 Context로 활용
    2. LLM 실행 (결과는 복수, 가장 연관성이 높은 고객경험요소 순서로)
    3. 매핑 결과를 실제 고객경험요소와 비교하며, 최종 결과 세팅
    """

    def __init__(self, llm, req_info: CxeMapperRequestInfo,
                 ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                 prompt_path: Optional[str]=default_resource_path + '/prompt') -> None:

        self.req_info: CxeMapperRequestInfo = req_info
        
        # 1. llm Chat 모델 인스턴스 생성
        self.llm = llm
        self.failback_llm = llm        
        
        self.json_parser = JsonOutputParser()

        # 2. 시스템 프롬프트 세팅
        self.load_resources(prompt_path)

        # 3. 채널 > 고객경험단계 > 고객경험요소 Dict
        self.ch_stge_cxe_dict = ch_stge_cxe_dict


    def load_resources(self, prompt_path: str) -> None:
        """
        system prompt loading
        """

        # 고객경험요소 매핑 프롬프트
        system_prompt_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_FILE_NAME)
        self.system_prompt_str: str = system_prompt_str.format(
            RESPONSE_KEY=CommonCode.RESPONSE_KEY.value,
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            cxe_dictionary="{cxe_dictionary}" # 실행단계에서 넣을거임
        )
        human_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_FILE_NAME)
        
        self.human_prompt_str = human_prompt_str
        return

    
    def load_langfuse_resources(self) -> None:
        return


    async def execute(self, voc_infos: List[VocInfo]) -> List[VocInfo]:
        """
        [1] 최 상위 호출 메서드
        ===========================================
        1. NLP - CXE 매핑 생성 (비동기)
        2. 생성된 결과 Validation and Result Setting
        3. Return
        """

        # 1. 고객경험매핑 생성
        # batch 사이즈만큼 코루틴으로 비동기 실행
        batch_size = self.req_info.batch_size
        for i in range(0, len(voc_infos), batch_size):
            voc_chunk = voc_infos[i:i + batch_size]
            # gather() 에는 * 연산자를 통해 인자 풀기
            await asyncio.gather(*[self.generate_cx_mapping_wrap(voc_info) for voc_info in voc_chunk])
            await asyncio.sleep(self.req_info.batch_sleep)

        return voc_infos



    async def generate_cx_mapping_wrap(self, voc_info: VocInfo) -> None:
        """
        [2] 각 VOC 별 고객경험요소 매핑 wrapping
        ==================================
        실제 실행 부를 감싸고 있는 wrapping 메서드
        - logging
        - try-catch
        """

        # 1. VOC 요약 생성 시작 시간
        generate_start_time = time.time()

        try:
            # 2-1. 실제 고객경험요소 매핑 실행 부분
            await self.generate_cxe_mapping(voc_info)
                        
        except Exception as e:
            logger.exception(f"에러 발생 -> SKIP VOC: [{voc_info.get_pk_dict()}]")
            
        finally:
            # 3. VOC 요약 종료 시간
            generate_end_time = time.time()
                
            elapsed_time = generate_start_time - generate_end_time


    async def generate_cxe_mapping(self, voc_info: VocInfo) -> None:
        """
        [3] 각 VOC 별 고객경험요소 매핑 실제 실행 부
        ==================================
        1. 프롬프트 세팅 & LLM 실행
        2. 결과 valid and setting
        """

        # 1. generate cxe 매핑 result
        generated_response = await DataAnalysisUtils.ainvoke_llm(self.get_prompt(voc_info), self.llm, self.failback_llm)

        # 2. result setting
        self.valid_and_set_result(voc_info, generated_response.content)
        return

    
    def valid_and_set_result(self, voc_info: VocInfo, content: str) -> None:
        """
        [3-2] 매핑 결과 valid and setting
        ==================================
        1. 결과 valid (빈 문자열, 빈 값)
        2. 생성된 응답 XML -> Dict 로 파싱
        3. 생성된 응답을 실제 고객경험요소 배열과 비교하여 환각을 체크하고(임계치 3글자), 성공 / 실패 결과를 세팅
        4. 만약 성공이 하나라도 있다면, 성공결과를 포함시킨다.
                성공이 하나라도 없다면, 실패 이유를 세팅하고 실패 배열중 대표 결과를 세팅한다.
        5. 응답 반환
        """
        
        # 1. 빈 문자열일 경우, 별도의 처리 없음
        if not content or not content.strip():
            self.set_cxe_not_classified(voc_info)
            return

        # 2. 생성 응답 Parsing
        parsed_response: Dict[str, List[str]] = DataAnalysisUtils.parse_llm_xml_to_dict(content)
        gen_cxe_nm_list = parsed_response.get(CommonCode.RESPONSE_KEY.value, {}).get(CommonCode.RESULT_KEY.value)
        if not gen_cxe_nm_list:
            # 추출 실패
            self.set_cxe_not_classified(voc_info)
            return
        elif isinstance(gen_cxe_nm_list, str):
            gen_cxe_nm_list = [gen_cxe_nm_list]

        
        # 3. 성공 / 실패 리스트 세팅
        current_seq: int = 1
        success_cxe_list: List[CxeInfo] = []
        failed_cxe_list: List[CxeInfo] = []

        stge_cxe_dict = self.ch_stge_cxe_dict[voc_info.final_qsitm_pol_taget_dstcd]
        cxe_dict = DataAnalysisUtils.get_final_cxe_dict(stge_cxe_dict, voc_info.cx_stge_dstcd)
        cxe_nm_list = [cxe_meta[CommonCode.CXE_NAME.value] for cxe_meta in cxe_dict.values()]
        for gen_cxe_nm in list(dict.fromkeys(gen_cxe_nm_list)): # 순서 유지하면서 중복 제거
            
            # 4. 환각이 아닌, 실제 고객경험요소인지 확인
            # 임계치 기준 몇 자 이내로 같게끔 보정
            final_cxe_nm = CxeMapper.is_within_threshold(gen_cxe_nm, cxe_nm_list, self.req_info.cxe_diff_threshold)
            if not final_cxe_nm:
                # 4-1. 실패 고객경험요소 저장
                failed_cxe_list.append(
                    CxeInfo(cxe_nm=gen_cxe_nm)
                )
            else:
                # 4-2. 성공시 고객경험요소명으로 해당하는 고객경험요소 메타정보 매칭
                final_cxe_cd = None
                final_cxe_meta = None
                for cxe_cd, cxe_meta in cxe_dict.items():
                    if final_cxe_nm == cxe_meta[CommonCode.CXE_NAME.value]:
                        final_cxe_cd = cxe_cd
                        final_cxe_meta = cxe_meta
                        break
                
                # 4-3. 성공 CxeInfo 세팅
                success_cxe_list.append(
                    CxeInfo(
                        seq=current_seq,
                        sq_cd=final_cxe_meta[CommonCode.SQ_CD.value],
                        sq_nm=final_cxe_meta[CommonCode.SQ_NAME.value],
                        cxe_cd=final_cxe_cd,
                        cxe_nm=final_cxe_nm
                    )
                )
                current_seq = current_seq + 1
                
        # 5. 결과 세팅
        if success_cxe_list:
            # 5-1. 성공한 고객경험요소가 하나라도 있으면 반환
            voc_info.cxe_success_yn = True
            voc_info.cxe_result = success_cxe_list
            
        elif failed_cxe_list:
            # 5-2. 성공한 고객경험요소 없이, 실패 건만 있다면 -> 존재하지 않는 고객경험요소
            # seq 세팅 후 실패 건 반환
            for cxe_info in failed_cxe_list:
                cxe_info.seq = current_seq
                current_seq = current_seq + 1
            voc_info.cxe_result = failed_cxe_list
            voc_info.cxe_failed_reason = CxeFailedMessage.UNKNOWN_CXE.value
        
        else:
            # 5-3. 성공과 실패 모두 없다면 -> 분류 실패
            self.set_cxe_not_classified(voc_info)
        return


    def set_cxe_not_classified(self, voc_info: VocInfo) -> None:
        """
        [~3-2-1] 실패 결과 세팅 공통 메서드
        ==================================
        매핑 결과가 validation에서 실패했을 경우, 공통적으로 실패 결과를 세팅하는 메서드
        """
        
        # 1. 기본 CxeInfo 세팅
        cxe_info = CxeInfo()

        # 2. 기본 CxeInfo 및 실패 원인(분류 실패) 세팅
        voc_info.cxe_result = [cxe_info]
        voc_info.cxe_failed_reason = CxeFailedMessage.NOT_CLASSIFIED.value
        return

        
    def get_prompt(self, voc_info: VocInfo) -> List[BaseMessage]:
        """
        [~3-1] 프롬프트 세팅
        =======================================================
        System Prompt와 Voc별 필요한 정보를 함쳐서 prompt 세팅
        이때, 고객경험단계 유무에 따라 해당무일 경우는 해당 채널의 모든 고객경험요소를 Context로 첨부한다.
        """

        # 1. 시스템 프롬프트 세팅
        system_prompt_formatted = self.system_prompt_str.format(
            cxe_dictionary=self.get_cxe_dictionary(
                self.ch_stge_cxe_dict[voc_info.final_qsitm_pol_taget_dstcd],
                voc_info.cx_stge_dstcd
            )
        )
        sytem_prompt = SystemMessage(content=system_prompt_formatted)

        # 2. 휴면 프롬프트 세팅
        voc_context = f"""
        <voc>{voc_info.orin_voc_content}</voc>
        """.strip()
        
        human_prompt = HumanMessage(content=self.human_prompt_str.format(
            voc_context=voc_context,
        ))
        
        return [sytem_prompt, human_prompt]


    @staticmethod
    def get_cxe_dictionary(stge_cxe_dict: Dict[str, Dict[str, Dict[str, str]]], cx_stge_dstcd: str) -> str:
        """
        [~3-1-1] 고객경험요소 Context 세팅 부
        =====================================================
        1. DataAnalysisUtils.get_final_cxe_dict()를 이용해서,
           고객경험단계 유무에 따른 고객경험요소 사전을 반환 받는다.
        2. 반환된 고객경험요소를 LLM이 이해할 수 있도록 Context String(XML 형식)으로 세팅해서 반환한다.
        """

        # 해당 고객경험단계구분에 해당하는 고객경험요소 Dict get
        final_cxe_dict: Dict[str, Dict[str, str]] = DataAnalysisUtils.get_final_cxe_dict(stge_cxe_dict, cx_stge_dstcd)
        
        cxe_rows = []
        for cxe_cd, cxe_meta_dict in final_cxe_dict.items():
            row = f"""
            <item>
                <name>{cxe_meta_dict[CommonCode.CXE_NAME.value]}</name>
                <desc>{cxe_meta_dict[CommonCode.CXE_DESC.value]}</desc>
            </item>
            """.strip()
            cxe_rows.append(row)

        return "\n".join(cxe_rows)


    @staticmethod
    def is_within_threshold(str1: str, str2_list: List[str], threshold: int) -> Optional[str]:
        """
        [~3-2-2] 계산한 두 글자 간의 차이와 임계치를 비교해서
        LLM이 생성한 고객경험요소와 실제 고객경험요소가 동일한 것인지를 판별하는 메서드
        ==================================================================================
        str1, str2 사이의 차이(편집 거리)가 threshold 이하이면 실제 고객경험요소명,
        초과하면 None을 반환한다.
        """

        for str2 in str2_list:
            diff = CxeMapper.edit_distance(str1, str2)
            if diff <= threshold:
                return str2
        
        return None
    
    @staticmethod
    def edit_distance(s1: str, s2: str) -> int:
        """
        [~3-2-2-1] 두 글자 간의 차이를 반환하는 Utils 메서드
        ==================================================================================
        두 문자열 s1, s2 사이의 Levenshtein distance(삽입·삭제·교체 1회당 비용 1)를 반환.
        시간 복잡도 O(len(s1) * len(s2)), 메모리 O(min(len(s1), len(s2))).
        """
        # 공백 제거
        s1 = s1.replace(" ", "")
        s2 = s2.replace(" ", "")
    
        # 짧은 문자열을 기준으로 메모리를 절약
        if len(s1) > len(s2):
            s1, s2 = s2, s1   # 이제 len(s1) <= len(s2)
    
        previous = list(range(len(s1) + 1))   # dp[i-1][*]   (i = 0)
        for j, ch2 in enumerate(s2, start=1):
            current = [j]                     # dp[*][j-1] + 1 (삽입)
            for i, ch1 in enumerate(s1, start=1):
                cost = 0 if ch1 == ch2 else 1
                # 최소값: 삭제, 삽입, 교체
                current.append(min(
                    previous[i] + 1,      # 삭제
                    current[i-1] + 1,     # 삽입
                    previous[i-1] + cost  # 교체(또는 일치)
                ))
            previous = current
        return previous[-1]