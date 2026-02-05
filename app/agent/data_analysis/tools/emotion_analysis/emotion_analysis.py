import os
import asyncio
from pathlib import Path
import time

from collections import defaultdict
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI
from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser

from core.config import settings
from core.util import load_resource_file, create_azurechatopenai
from core.logger import get_logger

from agent.agent_template import Agent
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.dto import EmotionAnlaysisRequestInfo
from agent.data_analysis.model.vo import VocInfo, EmotionInfo
from agent.data_analysis.model.consts import CommonCode, EmotionAnalysisMessage



# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 상수 - 프롬프트
VERSION = "v0"

SYSTEM_PROMPT_CXE_FILE_NAME = f"emotion_analysis_cxe_system.txt"
HUMAN_PROMPT_CXE_FILE_NAME = f"emotion_analysis_cxe_human.txt"
SYSTEM_PROMPT_NO_CXE_FILE_NAME = f"emotion_analysis_no_cxe_system.txt"
HUMAN_PROMPT_NO_CXE_FILE_NAME = f"emotion_analysis_no_cxe_human.txt"


logger = get_logger("emotion_analysis")

class EmotionAnalyzer(Agent):
    """
    EmotionAnalyzer (감정 분석):
    고객경험요소 추출 유무에 따라 Context를 달리하며 VOC의 감정을 분석한다.
    ================================================================
    기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 감정을 분석  
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 감정을 분석
    2. LLM 실행
    3. 분석 결과를 실제 감정과 비교하며, 최종 결과 세팅
    """
    
    def __init__(self, llm, req_info: EmotionAnlaysisRequestInfo,
                 emotion_dict: Dict[str, EmotionInfo],
                 ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                 prompt_path: Optional[str] = default_resource_path + '/prompt') -> None:

        self.req_info: EmotionAnlaysisRequestInfo = req_info
        
        # 1. llm Chat 모델 인스턴스 생성
        self.llm = llm
        self.failback_llm = llm
        
        self.json_parser = JsonOutputParser()

        # 2. system prompt 세팅
        self.load_resources(prompt_path)

        # 3. 감정 매핑을 위한 정보 세팅
        self.emotion_dict = emotion_dict

        # 4. 고객경험요소 매핑을 위한 정보 세팅
        self.ch_stge_cxe_dict = ch_stge_cxe_dict


    def load_resources(self, prompt_path: str):
        """
        system prompt loading
        """
        # 프롬프트 세팅 (고객경험요소 추출)
        system_prompt_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_CXE_FILE_NAME)
        system_prompt_cxe_formatted_str = system_prompt_cxe_str.format(
            RESULT_KEY=CommonCode.RESULT_KEY.value,
        )
        self.system_cxe_prompt: SystemMessage = SystemMessage(content=system_prompt_cxe_formatted_str)
        self.user_cxe_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_CXE_FILE_NAME)

        # 프롬프트 세팅 (고객경험요소 미추출)
        system_prompt_no_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_NO_CXE_FILE_NAME)
        system_prompt_no_cxe_formatted_str = system_prompt_no_cxe_str.format(
            RESULT_KEY=CommonCode.RESULT_KEY.value,
        )
        self.system_no_cxe_prompt: SystemMessage = SystemMessage(content=system_prompt_no_cxe_formatted_str)
        self.user_no_cxe_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_NO_CXE_FILE_NAME)


    def load_langfuse_resources(self) -> None:
        return


    async def execute(self, voc_infos: List[VocInfo]) -> List[VocInfo]:
        """
        [1] 최 상위 호출 메서드
        ===========================================
        1. NLP - VOC의 감정 분석 (비동기)
        2. 생성된 결과 Validation and Result Setting
        3. return
        """

        # 1. VOC 감정 분석 생성 및 결과 세팅
        # batch 사이즈만큼 코루틴으로 비동기 실행
        batch_size = self.req_info.batch_size
        for i in range(0, len(voc_infos), batch_size):
            voc_chunk = voc_infos[i:i + batch_size]
            # gather() 에는 * 연산자를 통해 인자 풀기
            await asyncio.gather(*[self.generate_voc_emotion_wrap(voc_info) for voc_info in voc_chunk])
            await asyncio.sleep(self.req_info.batch_sleep)

        return voc_infos


    
    async def generate_voc_emotion_wrap(self, voc_info:VocInfo) -> None:
        """
        [2] 각 VOC 별 감정 분석 wrapping
        ============================
        실제 실행 부를 감싸고 있는 wrapping 메서드
        - loggging
        - try-catch
        """

        # 1. 감정 분석 시작 시간
        generate_start_time = time.time()

        try:
            # 2-1. 실제 감정 분석 실행 부분
            await self.generate_voc_emotion(voc_info)
                        
        except Exception as e:
            logger.exception(f"에러 발생 SKIP... VOC: [{voc_info.get_pk_dict()}]")
            
        finally:
            # 3. 감정 분석 종료 시간
            generate_end_time = time.time()
                
            elapsed_time = generate_start_time - generate_end_time
    


    
    async def generate_voc_emotion(self, voc_info: VocInfo) -> None:
        """
        [3] 각 VOC 별 감정 분석 & 결과 매핑 실제 실행 부
        ================================
        1. 고객경험요소 추출 여부에 따라 프롬프트 세팅 & LLM에 감정 분석 요청
        2. 결과 valid and setting
        """
        
        # 1. generate emotion result
        gen_response = await DataAnalysisUtils.ainvoke_llm(self.get_prompt(voc_info), self.llm, self.failback_llm)
        
        # 2. map and setting result
        self.map_emotion_result(voc_info, gen_response.content)
        return

    
    def map_emotion_result(self, voc_info: VocInfo, gen_content: str) -> None:
        """
        [5] 감정 분석 결과 valid and setting
        ==================================
        1. 생성 결과 valid & parsing
        2. 감정을 실제 감정중분류명과 비교해서, 환각 검증 및 결과 세팅
        3. 응답 반환
        """

        # 1. 생성 결과 검증
        if not gen_content:
            voc_info.emtn_failed_reason = EmotionAnalysisMessage.LLM_EMPTY_RESPONSE.value
            return
        
        # 2. JSON 결과값 Parsing
        parsed_content: Dict = self.json_parser.parse(gen_content)

        # 3. 감정분석 결과 검증
        gen_emtn_mid_nm = parsed_content.get(CommonCode.RESULT_KEY.value, None)
        if not gen_emtn_mid_nm:
            voc_info.emtn_failed_reason = EmotionAnalysisMessage.LLM_EMPTY_RESPONSE.value
            return

        # 4. 감정분석 결과 환각 검증
        final_emtn_info = None
        for cd, emtn in self.emotion_dict.items():
            if gen_emtn_mid_nm.replace(" ", "") == emtn.emtn_mid_nm.replace(" ", ""):
                final_emtn_info = emtn

        
        if final_emtn_info:
            # 4-1. 감정분석 성공
            voc_info.emtn_success_yn = True
            voc_info.emtn_result = final_emtn_info

        else:
            # 4-2. 감정분석 실패 처리 (환각)
            failed_emtn_info = EmotionInfo(emtn_mid_nm=gen_emtn_mid_nm)
            voc_info.emtn_result = failed_emtn_info
            voc_info.emtn_failed_reason = EmotionAnalysisMessage.UNKNOWN_EMOTION.value

        return



    def get_prompt(self, voc_info: VocInfo) -> List[BaseMessage]:
        """
        [4] 프롬프트 세팅
        ==================================
        System Prompt와 Voc별 필요한 정보를 함쳐서 prompt 세팅
        고객경험요소 분류 성공 여부에 따라서,
        성공 -> 고객경험요소 Context 첨부
        실패 -> 채널, 고객경험단계 Context 첨부
        """
        system_prompt: Optional[SystemMessage] = None
        human_prompt_str: Optinoal[str] = None
        cxe_and_desc_context: Optional[str] = None

        if voc_info.cxe_success_yn:
            # 1. 고객경험요소-설명 Context 세팅
            cxe_and_desc_context = self.get_cxe_desc(voc_info)
            # 2-1. 고객경험요소 분류된 VOC 프롬프트 세팅
            system_prompt = self.system_cxe_prompt
            human_prompt_str = self.user_cxe_prompt_str
        else:
            # 2-2. 고객경험요소 미분류 VOC 프롬프트 세팅
            system_prompt = self.system_no_cxe_prompt
            human_prompt_str = self.user_no_cxe_prompt_str

        # 3. Human 프롬프트 변수 치환
        human_prompt = HumanMessage(content=human_prompt_str.format(
            rcmdn_resn_qsitm_name=voc_info.rcmdn_resn_qsitm_name,
            rcmdn_resn_qsitm_desc=voc_info.rcmdn_resn_qsitm_desc,
            rcmdn_resn_qsitm_content=voc_info.rcmdn_resn_qsitm_content,
            voc_content=voc_info.orin_voc_content,
            cxe_and_desc_context=cxe_and_desc_context,
        ))

        # 4. 반환
        return [ system_prompt, human_prompt ]
        
        
    def get_cxe_desc(self, voc_info: VocInfo) -> str:
        """
        [4-1] 고객경험요소 Context 세팅 부
        ==================================
        고객경험요소 사전에서 voc의 고객경험요소에 해당하는 정보를 Context로 세팅한다.
        """
        
        # 1. 대표 고객경험요소 (index 0)
        rep_cxe_cd = voc_info.cxe_result[0].cxe_cd
        
        # 2. VOC의 고객경험단계 유무를 기준으로, 채널별 고객경험요소 사전 세팅
        final_cxe_dict = DataAnalysisUtils.get_final_cxe_dict(
            self.ch_stge_cxe_dict[voc_info.final_qsitm_pol_taget_dstcd],
            voc_info.cx_stge_dstcd
        )
        final_cxe_meta = final_cxe_dict.get(rep_cxe_cd, None)
        
        context = ""
        if final_cxe_meta:
            context: str = f"""
            | 고객경험요소 | 설명 |
            |--------------|------|
            | {final_cxe_meta.get(CommonCode.CXE_NAME.value)} | {final_cxe_meta.get(CommonCode.CXE_DESC.value)} |
            """
        
        return context
    
        