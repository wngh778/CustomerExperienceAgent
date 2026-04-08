from __future__ import annotations
import os
import asyncio
from pathlib import Path
import time

from typing import Any, Dict, List, Optional

from langchain.schema import BaseMessage, HumanMessage, SystemMessage

from core.config import settings
from core.util import load_resource_file, create_azurechatopenai
from core.logger import get_logger

from agent.agent_template import Agent
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.dto import VocProblemReasonDetectRequestInfo
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.consts import CommonCode

# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 상수 - 프롬프트
SYSTEM_PROMPT_CXE_FILE_NAME = f"voc_problem_reason_cxe_system.txt"
SYSTEM_PROMPT_NO_CXE_FILE_NAME = f"voc_problem_reason_no_cxe_system.txt"

HUMAN_PROMPT_FILE_NAME = f"voc_problem_reason_human.txt"


logger = get_logger("voc_prblm_resn")

class VocProblemReasonDetector(Agent):
    """
    VocProblemReasonDetector (문제원인식별):
    감정대분류가 '부정'인 VOC를 고객경험요소 추출 유무에 따라 Context를 달리하여 문제원인을 식별한다.
    ================================================================
    기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 문제원인을 식별  
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 문제원인을 식별
    2. LLM 실행
    3. 최종 결과 세팅
    """

    def __init__(self, llm, req_info: VocProblemReasonDetectRequestInfo,
                 ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                 ch_stge_dict: Dict[str, List[str]],
                 prompt_path: Optional[str] = default_resource_path + '/prompt') -> None:

        self.req_info: VocProblemReasonDetectRequestInfo = req_info
        
        # 1. llm Chat 모델 인스턴스 생성
        self.llm = llm
        self.failback_llm = llm
        
        # 2. 시스템 프롬프트 세팅
        self.load_resources(prompt_path)
        

        # 3. 조사채널, 고객경험단계 별 고객경험요소 Dict
        self.ch_stge_cxe_dict = ch_stge_cxe_dict
        
        # 4. 고객경험조사채널 별, 고객경험단계 명 리스트 Dict
        self.ch_stge_dict = ch_stge_dict


    def load_resources(self, prompt_path: str) -> None:
        """
        system prompt loading
        """

        # 고객경험요소 매칭 VOC
        system_prompt_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_CXE_FILE_NAME)
        self.system_prompt_cxe_str: str = system_prompt_cxe_str.format(
            RESPONSE_KEY=CommonCode.RESPONSE_KEY.value,
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            cxe_context="{cxe_context}" # 이 값은 실행 시점에 넣을거임
        )
        
        # 고객경험요소 미분류 VOC
        system_prompt_no_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_NO_CXE_FILE_NAME)
        self.system_prompt_no_cxe_str: SystemMessage = system_prompt_no_cxe_str.format(
            RESPONSE_KEY=CommonCode.RESPONSE_KEY.value,
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            qsitm_pol_taget_nm="{qsitm_pol_taget_nm}", # 이 값은 실행 시점에 넣을거임
            cx_stage_list="{cx_stage_list}" # 이 값은 실행 시점에 넣을거임
        )

        # 공통 휴먼 프롬프트
        self.human_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_FILE_NAME)
        return

    
    def load_langfuse_resources(self) -> None:
        return


    async def execute(self, voc_infos: List[VocInfo]) -> List[VocInfo]:
        """
        [1] 최 상위 호출 메서드
        ===========================================
        1. NLP - VOC 문제원인 생성 (비동기)
        2. 생성된 결과 Validation and Result Setting
        3. Return
        """

        # 1. VOC 문제원인 생성
        # batch 사이즈만큼 코루틴으로 비동기 실행
        batch_size = self.req_info.batch_size
        for i in range(0, len(voc_infos), batch_size):
            voc_chunk = voc_infos[i:i + batch_size]
            # gather() 에는 * 연산자를 통해 인자 풀기
            await asyncio.gather(*[self.generate_problem_reason_wrap(voc_info) for voc_info in voc_chunk])
            await asyncio.sleep(self.req_info.batch_sleep)
        
        return voc_infos



    async def generate_problem_reason_wrap(self, voc_info: VocInfo) -> None:
        """
        [2] 각 VOC 별 문제원인 식별 wrapping
        ==================================
        실제 실행 부를 감싸고 있는 wrapping 메서드
        - logging
        - try-catch
        """

        # 1. VOC 문제원인 식별 시작 시간
        generate_start_time = time.time()

        try:
            # 2-1. 실제 VOC 문제원인 식별 실행 부분
            await self.generate_problem_reason(voc_info)
                        
        except Exception as e:
            logger.exception(f"에러 발생 -> SKIP VOC: [{voc_info.get_pk_dict()}]")
            
        finally:
            # 3. VOC 문제원인 식별 종료 시간
            generate_end_time = time.time()
                
            elapsed_time = generate_start_time - generate_end_time


    
    async def generate_problem_reason(self, voc_info: VocInfo) -> None:
        """
        [3] 각 VOC 별 문제원인 식별 실제 실행부
        ==============================================
        1. 고객경험요소 추출 여부에 따라 프롬프트 세팅 & LLM에 감정 분석 요청
        2. 결과 valid and setting
        3. 세팅된 결과 적재
        4. 반환
        """

        # 1. generate problem reason result
        generated_response = await DataAnalysisUtils.ainvoke_llm(self.get_prompt(voc_info), self.llm, self.failback_llm)

        # 2. result validation and setting
        if not generated_response.content:
            generated_reason = ""
        else:
            parsed_response: Dict[str, List[str]] = DataAnalysisUtils.parse_llm_xml_to_dict(generated_response.content.strip())
            generated_reason = parsed_response.get(CommonCode.RESPONSE_KEY.value, {}).get(CommonCode.RESULT_KEY.value)

        # 3. result setting
        voc_info.prblm_reason_success_yn = True
        voc_info.prblm_reason_result = generated_reason
        
        return



    def get_prompt(self, voc_info: VocInfo) -> List[BaseMessage]:
        """
        [4] 고객경험요소 추출 여부에 따라 프롬프트 라우팅
        =================================================
        고객경험요소 추출 성공 -> 고객경험요소 Context 첨부
                     추출 실패 -> 채널, 고객경험단계 Context 첨부
        """

        if voc_info.cxe_success_yn:
            # CXE 유
            return self.get_prompt_cxe_voc(voc_info)
        else:
            # CXE 무
            return self.get_prompt_no_cxe_voc(voc_info)

            

    def get_prompt_cxe_voc(self, voc_info: VocInfo) -> List[BaseMessage]:
        """
        [4-1] 고객경험요소 추출 성공 프롬프트 세팅
        =================================================
        1. 추출된 해당 고객경험요소의 명과 설명을 Context로 세팅
        2. VOC 원문, 추천이유, 추출된 개체어를 Input prompt로 세팅
        3. 반환
        """

        
        # 1. VOC의 고객경험단계 유무를 기준으로, 채널별 고객경험요소 사전 세팅
        final_cxe_dict = DataAnalysisUtils.get_final_cxe_dict(
            self.ch_stge_cxe_dict[voc_info.final_qsitm_pol_taget_dstcd],
            voc_info.cx_stge_dstcd
        )

        # 2. 매칭된 고객경험요소의 명과 설명 Context 세팅
        rep_cxe_info = voc_info.cxe_result[0]
        final_cxe_meta = final_cxe_dict.get(rep_cxe_info.cxe_cd, None)

        cxe_context = ""
        if final_cxe_meta:
            cxe_context = f"""
            | 고객경험요소 | 설명 |
            |--------------|------|
            | {final_cxe_meta.get(CommonCode.CXE_NAME.value)} | {final_cxe_meta.get(CommonCode.CXE_DESC.value)} |
            """

        # System 프롬프트 치환
        system_prompt_formatted_str = self.system_prompt_cxe_str.format(
            cxe_context=cxe_context
        )
        
        
        # Human 프롬프트 치환
        human_prompt_formatted_str = self.human_prompt_str.format(
            rcmdn_resn_qsitm_name=voc_info.rcmdn_resn_qsitm_name,
            rcmdn_resn_qsitm_desc=voc_info.rcmdn_resn_qsitm_desc,
            rcmdn_resn_qsitm_content=voc_info.rcmdn_resn_qsitm_content,
            voc_content=voc_info.orin_voc_content,
            prdct_svc_word=rep_cxe_info.prdct_svc_word,
            pfrm_qalty_word=rep_cxe_info.pfrm_qalty_word,
        )
        
        return [SystemMessage(content=system_prompt_formatted_str), HumanMessage(content=human_prompt_formatted_str)]


        
    def get_prompt_no_cxe_voc(self, voc_info: VocInfo) -> List[BaseMessage]:
        """
        [4-2] 고객경험요소 추출 실패 프롬프트 세팅
        =================================================
        1. 고객경험단계구분의 유무('00' - 해당무)에 따라 고객경험단계 Context 세팅
            - 유: 특정 고객경험단계만 Context로 첨부
            - 무: 해당 채널의 모든 고객경험단계를 Context로 첨부
        2. VOC 원문, 추천이유, 추출된 개체어를 Input prompt로 세팅
        3. 반환
        """

        cx_stge_context = []

        # 1. 고객경험단계 유무에 따라 Context 세팅
        # 1-1. 고객경험단계가 '00' - 해당무일 경우
        if voc_info.cx_stge_dstcd == CommonCode.NO_CX_STAGE_CD.value:    
            cx_stge_context = self.ch_stge_dict[voc_info.final_qsitm_pol_taget_dstcd] # 채널의 해당하는 모든 고객경험단계를 Context로 세팅
        
        # 1-2.특정 고객경험단계가 존재하는 VOC일 경우
        else:
            cx_stge_context = [voc_info.cx_stge_dstic_nm] # 해당 고객경험단계명만 Context로 세팅
            
        rep_cxe_info = voc_info.cxe_result[0]

        # 2. System 프롬프트 포맷팅
        system_prompt_formatted_str = self.system_prompt_no_cxe_str.format(
            qsitm_pol_taget_nm=voc_info.final_qsitm_pol_taget_nm,
            cx_stage_list=cx_stge_context,
        )
        
        # 3. Human 프롬프트 포맷팅
        human_prompt_formatted_str = self.human_prompt_str.format(
            rcmdn_resn_qsitm_name=voc_info.rcmdn_resn_qsitm_name,
            rcmdn_resn_qsitm_desc=voc_info.rcmdn_resn_qsitm_desc,
            rcmdn_resn_qsitm_content=voc_info.rcmdn_resn_qsitm_content,
            voc_content=voc_info.orin_voc_content,
            prdct_svc_word=rep_cxe_info.prdct_svc_word,
            pfrm_qalty_word=rep_cxe_info.pfrm_qalty_word,
        )

        # 4. 반환
        return [SystemMessage(content=system_prompt_formatted_str), HumanMessage(content=human_prompt_formatted_str)]